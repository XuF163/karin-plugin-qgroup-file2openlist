import fs from 'node:fs'
import { Readable } from 'node:stream'
import { setTimeout as sleep } from 'node:timers/promises'
import { createThrottleTransform } from '@/model/shared/rateLimit'
import { encodePathForUrl, normalizePosixPath } from '@/model/shared/path'
import { fetchTextSafely, formatErrorMessage, isAbortError } from '@/model/shared/errors'
import { withGlobalTransferLimit } from '@/model/shared/transferLimiter'
import { createTransferTempFilePath, parseContentLengthHeader, safeUnlink, shouldSpoolToDisk, streamToFile } from '@/model/shared/transferSpool'
import { logger } from 'node-karin'

export const isRetryableWebDavError = (error: unknown) => {
  const msg = formatErrorMessage(error)
  return /ECONNRESET|ETIMEDOUT|EAI_AGAIN|ENOTFOUND|ECONNREFUSED|UND_ERR|socket hang up/i.test(msg)
}

const webdavMkcolOk = (status: number) => status === 201 || status === 405

export const webdavPropfindListNames = async (params: {
  davBaseUrl: string
  auth: string
  dirPath: string
  timeoutMs: number
}) => {
  const { davBaseUrl, auth, dirPath, timeoutMs } = params
  const controller = new AbortController()
  const timer = setTimeout(() => controller.abort(), timeoutMs)

  try {
    const url = `${davBaseUrl}${encodePathForUrl(dirPath)}`
    const body = `<?xml version="1.0" encoding="utf-8" ?>
<d:propfind xmlns:d="DAV:">
  <d:prop>
    <d:displayname />
  </d:prop>
</d:propfind>`

    const res = await fetch(url, {
      method: 'PROPFIND',
      headers: {
        Authorization: auth,
        Depth: '1',
        'Content-Type': 'application/xml; charset=utf-8',
      },
      body,
      redirect: 'follow',
      signal: controller.signal,
    })

    if (!res.ok) return new Set<string>()
    // PROPFIND 响应可能很长（目录文件较多时），这里不能截断，否则会导致“已存在文件”判断失效
    const text = await res.text()
    if (!text) return new Set<string>()

    const names = new Set<string>()
    const hrefRegex = /<d:href>([^<]+)<\/d:href>/gi
    let match: RegExpExecArray | null
    while ((match = hrefRegex.exec(text))) {
      const href = match[1] ?? ''
      const decoded = decodeURIComponent(href)
      const cleaned = decoded.replace(/\/+$/, '')
      const base = cleaned.split('/').filter(Boolean).pop()
      if (base) names.add(base)
    }
    return names
  } catch {
    return new Set<string>()
  } finally {
    clearTimeout(timer)
  }
}

export type WebDavEntry = { name: string, isDir: boolean }

export const webdavPropfindListEntries = async (params: {
  davBaseUrl: string
  auth?: string
  dirPath: string
  timeoutMs: number
}) => {
  const { davBaseUrl, auth, dirPath, timeoutMs } = params
  const controller = new AbortController()
  const timer = setTimeout(() => controller.abort(), timeoutMs)

  try {
    const url = `${davBaseUrl}${encodePathForUrl(dirPath)}`
    const requestPath = encodePathForUrl(dirPath).replace(/\/+$/, '')
    const body = `<?xml version="1.0" encoding="utf-8" ?>
<d:propfind xmlns:d="DAV:">
  <d:prop>
    <d:displayname />
    <d:resourcetype />
  </d:prop>
</d:propfind>`
    const headers: Record<string, string> = {
      Depth: '1',
      'Content-Type': 'application/xml; charset=utf-8',
    }
    const authHeader = String(auth ?? '').trim()
    if (authHeader) headers.Authorization = authHeader

    const res = await fetch(url, {
      method: 'PROPFIND',
      headers,
      body,
      redirect: 'follow',
      signal: controller.signal,
    })

    if (!res.ok) return [] as WebDavEntry[]
    const text = await res.text()
    if (!text) return [] as WebDavEntry[]

    const out: WebDavEntry[] = []
    const responseRegex = /<d:response\b[\s\S]*?<\/d:response>/gi
    let match: RegExpExecArray | null
    while ((match = responseRegex.exec(text))) {
      const block = match[0] ?? ''
      const hrefMatch = block.match(/<d:href>([^<]+)<\/d:href>/i)
      if (!hrefMatch) continue
      const href = hrefMatch[1] ?? ''
      const decoded = decodeURIComponent(href)
      const cleaned = decoded.replace(/\/+$/, '')

      // 注意：dirPath='/' 时 requestPath 可能为空字符串，endsWith('') 永远为 true，会导致列表被清空
      if (requestPath && cleaned.endsWith(requestPath)) continue

      const name = cleaned.split('/').filter(Boolean).pop()
      if (!name) continue

      const isDir = /<d:collection\b/i.test(block) || /\/$/.test(decoded)
      out.push({ name, isDir })
    }

    const selfBase = normalizePosixPath(dirPath).split('/').filter(Boolean).pop()
    const unique = new Map<string, WebDavEntry>()
    for (const it of out) {
      if (selfBase && it.name === selfBase) continue
      unique.set(`${it.name}::${it.isDir ? 'd' : 'f'}`, it)
    }
    return [...unique.values()]
  } catch {
    return [] as WebDavEntry[]
  } finally {
    clearTimeout(timer)
  }
}

export const webdavHeadExists = async (params: {
  davBaseUrl: string
  auth: string
  filePath: string
  timeoutMs: number
}) => {
  const { davBaseUrl, auth, filePath, timeoutMs } = params
  const controller = new AbortController()
  const timer = setTimeout(() => controller.abort(), timeoutMs)
  try {
    const url = `${davBaseUrl}${encodePathForUrl(filePath)}`
    const res = await fetch(url, {
      method: 'HEAD',
      headers: { Authorization: auth },
      redirect: 'follow',
      signal: controller.signal,
    })
    return res.ok
  } catch {
    return false
  } finally {
    clearTimeout(timer)
  }
}

export const copyWebDavToWebDav = async (params: {
  sourceDavBaseUrl: string
  sourceAuth?: string
  sourcePath: string
  targetDavBaseUrl: string
  targetAuth: string
  targetPath: string
  timeoutMs: number
}) => {
  const { sourceDavBaseUrl, sourceAuth, sourcePath, targetDavBaseUrl, targetAuth, targetPath, timeoutMs } = params

  await withGlobalTransferLimit(`copyWebDavToWebDav:${sourcePath}=>${targetPath}`, async () => {
    const controller = new AbortController()
    const timer = setTimeout(() => controller.abort(), timeoutMs)
    let tmpFilePath: string | undefined

    try {
      const sourceUrl = `${sourceDavBaseUrl}${encodePathForUrl(sourcePath)}`
      const targetUrl = `${targetDavBaseUrl}${encodePathForUrl(targetPath)}`

      let downloadRes: Response
      try {
        const downloadHeaders: Record<string, string> = {}
        const authHeader = String(sourceAuth ?? '').trim()
        if (authHeader) downloadHeaders.Authorization = authHeader
        downloadRes = await fetch(sourceUrl, {
          method: 'GET',
          headers: downloadHeaders,
          redirect: 'follow',
          signal: controller.signal,
        })
      } catch (error) {
        if (isAbortError(error)) throw new Error('源端读取超时')
        throw new Error(`源端读取失败: ${formatErrorMessage(error)}`)
      }

      if (!downloadRes.ok) {
        const body = await fetchTextSafely(downloadRes)
        throw new Error(`源端读取失败: ${downloadRes.status} ${downloadRes.statusText}${body ? ` - ${body}` : ''}`)
      }
      if (!downloadRes.body) throw new Error('源端读取失败: 响应体为空')

      const headers: Record<string, string> = { Authorization: targetAuth }
      const contentType = downloadRes.headers.get('content-type')
      const contentLength = parseContentLengthHeader(downloadRes.headers.get('content-length'))
      if (contentType) headers['Content-Type'] = contentType
      if (contentLength) headers['Content-Length'] = String(contentLength)

      const spoolToDisk = shouldSpoolToDisk(contentLength)

      let putRes: Response
      try {
        let body: any
        if (spoolToDisk) {
          tmpFilePath = createTransferTempFilePath('webdav-copy')
          logger.info(`[传输][落盘] WebDAV复制检测到大文件，将先下载落盘再上传: ${sourcePath} -> ${targetPath}`)
          await streamToFile(Readable.fromWeb(downloadRes.body as any), tmpFilePath, { signal: controller.signal })
          const stat = await fs.promises.stat(tmpFilePath)
          headers['Content-Length'] = String(stat.size)
          body = fs.createReadStream(tmpFilePath) as any
        } else {
          body = Readable.fromWeb(downloadRes.body as any)
        }

        putRes = await fetch(targetUrl, {
          method: 'PUT',
          headers,
          body,
          // @ts-expect-error Node fetch streaming body requires duplex
          duplex: 'half',
          redirect: 'follow',
          signal: controller.signal,
        })
      } catch (error) {
        if (isAbortError(error)) throw new Error('目标端写入超时（请检查对端 OpenList 连接/权限）')
        throw new Error(`目标端写入失败: ${formatErrorMessage(error)}`)
      }

      if (!putRes.ok) {
        const body = await fetchTextSafely(putRes)
        throw new Error(`目标端写入失败: ${putRes.status} ${putRes.statusText}${body ? ` - ${body}` : ''}`)
      }
    } catch (error) {
      try {
        controller.abort()
      } catch {}
      throw error
    } finally {
      clearTimeout(timer)
      if (tmpFilePath) await safeUnlink(tmpFilePath)
    }
  })
}

export const createWebDavDirEnsurer = (davBaseUrl: string, auth: string, timeoutMs: number) => {
  const ensured = new Map<string, Promise<void>>()
  const requestTimeoutMs = Math.max(1_000, Math.floor(timeoutMs) || 0)

  const ensureDir = async (dirPath: string) => {
    const normalized = normalizePosixPath(dirPath)
    if (normalized === '/') return

    const segments = normalized.split('/').filter(Boolean)
    let current = ''

    for (const segment of segments) {
      current += `/${segment}`

      const existing = ensured.get(current)
      if (existing) {
        await existing
        continue
      }

      const promise = (async () => {
        const controller = new AbortController()
        const timer = setTimeout(() => controller.abort(), requestTimeoutMs)

        try {
          const url = `${davBaseUrl}${encodePathForUrl(current)}`
          let res: Response
          try {
            res = await fetch(url, {
              method: 'MKCOL',
              headers: { Authorization: auth },
              redirect: 'follow',
              signal: controller.signal,
            })
          } catch (error) {
            if (isAbortError(error)) throw new Error(`MKCOL 超时: ${current}`)
            throw new Error(`MKCOL 请求失败: ${current} - ${formatErrorMessage(error)}`)
          }

          if (!webdavMkcolOk(res.status)) {
            const body = await fetchTextSafely(res)
            throw new Error(`MKCOL 失败: ${current} -> ${res.status} ${res.statusText}${body ? ` - ${body}` : ''}`)
          }
        } finally {
          clearTimeout(timer)
        }
      })()

      ensured.set(current, promise)
      try {
        await promise
      } catch (error) {
        ensured.delete(current)
        throw error
      }
    }
  }

  return { ensureDir }
}

export const downloadAndUploadByWebDav = async (params: {
  sourceUrl: string
  sourceHeaders?: Record<string, string>
  targetUrl: string
  auth: string
  timeoutMs: number
  rateLimitBytesPerSec?: number
  /** 已知文件大小（可选；用于提前判断是否落盘） */
  expectedSize?: number
}) => {
  const { sourceUrl, sourceHeaders, targetUrl, auth, timeoutMs, rateLimitBytesPerSec, expectedSize } = params
  await withGlobalTransferLimit(`downloadAndUploadByWebDav:${targetUrl}`, async () => {
    const controller = new AbortController()
    const timer = setTimeout(() => controller.abort(), timeoutMs)
    let tmpFilePath: string | undefined

    try {
      let downloadRes: Response
      try {
        downloadRes = await fetch(sourceUrl, {
          headers: sourceHeaders,
          redirect: 'follow',
          signal: controller.signal,
        })
      } catch (error) {
        if (isAbortError(error)) throw new Error('下载超时')
        throw new Error(`下载请求失败: ${formatErrorMessage(error)}`)
      }

      if (!downloadRes.ok) {
        const body = await fetchTextSafely(downloadRes)
        throw new Error(`下载失败: ${downloadRes.status} ${downloadRes.statusText}${body ? ` - ${body}` : ''}`)
      }
      if (!downloadRes.body) throw new Error('下载失败: 响应体为空')

      const headers: Record<string, string> = {
        Authorization: auth,
      }
      const contentType = downloadRes.headers.get('content-type')
      const contentLength = parseContentLengthHeader(downloadRes.headers.get('content-length'))
      if (contentType) headers['Content-Type'] = contentType
      if (contentLength) headers['Content-Length'] = String(contentLength)

      const sizeForDecision = (typeof expectedSize === 'number' && Number.isFinite(expectedSize) && expectedSize > 0)
        ? Math.floor(expectedSize)
        : contentLength
      const spoolToDisk = shouldSpoolToDisk(sizeForDecision)

      let putRes: Response
      try {
        let bodyStream: any
        if (spoolToDisk) {
          tmpFilePath = createTransferTempFilePath('webdav-download-upload')
          logger.info(`[传输][落盘] 检测到大文件，将先下载落盘再上传: ${targetUrl}`)
          await streamToFile(Readable.fromWeb(downloadRes.body as any), tmpFilePath, { signal: controller.signal })
          const stat = await fs.promises.stat(tmpFilePath)
          headers['Content-Length'] = String(stat.size)

          bodyStream = fs.createReadStream(tmpFilePath) as any
        } else {
          bodyStream = Readable.fromWeb(downloadRes.body as any)
        }

        const throttle = createThrottleTransform(rateLimitBytesPerSec || 0)
        if (throttle) bodyStream = bodyStream.pipe(throttle)

        putRes = await fetch(targetUrl, {
          method: 'PUT',
          headers,
          body: bodyStream as any,
          // @ts-expect-error Node fetch streaming body requires duplex
          duplex: 'half',
          redirect: 'follow',
          signal: controller.signal,
        })
      } catch (error) {
        if (isAbortError(error)) throw new Error('上传超时（请检查 OpenList 连接/权限）')
        throw new Error(`上传请求失败: ${formatErrorMessage(error)}`)
      }

      if (!putRes.ok) {
        const body = await fetchTextSafely(putRes)
        const hint = putRes.status === 401
          ? '（账号/密码错误，或未开启 WebDAV）'
          : putRes.status === 403
            ? '（没有 WebDAV 管理/写入权限，或目标目录不可写/不在用户可访问范围）'
          : ''
        throw new Error(`上传失败: ${putRes.status} ${putRes.statusText}${hint}${body ? ` - ${body}` : ''}`)
      }
    } catch (error) {
      try {
        controller.abort()
      } catch {}
      throw error
    } finally {
      clearTimeout(timer)
      if (tmpFilePath) await safeUnlink(tmpFilePath)
    }
  })
}

/**
 * 在某些错误场景中，WebDAV 需要短暂等待再重试（网络抖动/连接复用）。
 */
export const sleepMs = async (ms: number) => {
  const v = Math.max(0, Math.floor(ms) || 0)
  if (v <= 0) return
  await sleep(v)
}
