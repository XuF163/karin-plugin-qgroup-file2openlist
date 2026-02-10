import fs from 'node:fs'
import { Readable } from 'node:stream'
import { setTimeout as sleep } from 'node:timers/promises'
import { buildOpenListApiBaseUrl } from './url'
import { normalizePosixPath } from '@/model/shared/path'
import { fetchTextSafely, formatErrorMessage, isAbortError } from '@/model/shared/errors'
import { withGlobalTransferLimit } from '@/model/shared/transferLimiter'
import { createTransferTempFilePath, parseContentLengthHeader, safeUnlink, shouldSpoolToDisk, streamToFile } from '@/model/shared/transferSpool'
import { logger } from 'node-karin'

export type OpenListApiResponse<T = unknown> = {
  code?: number
  message?: string
  data?: T
}

export type WebDavEntry = { name: string, isDir: boolean }

export const openlistApiReadJson = async <T>(res: Response): Promise<OpenListApiResponse<T>> => {
  try {
    return await res.json() as any
  } catch {
    return {}
  }
}

export const openlistApiLogin = async (params: {
  baseUrl: string
  username: string
  password: string
  timeoutMs: number
}) => {
  const { baseUrl, username, password, timeoutMs } = params
  const apiBaseUrl = buildOpenListApiBaseUrl(baseUrl)
  if (!apiBaseUrl) throw new Error(`OpenList API 地址不正确: ${baseUrl}`)

  const controller = new AbortController()
  const timer = setTimeout(() => controller.abort(), Math.max(1_000, Math.floor(timeoutMs) || 0))

  try {
    const url = `${apiBaseUrl}/auth/login`
    let res: Response
    try {
      res = await fetch(url, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          username: String(username ?? ''),
          password: String(password ?? ''),
        }),
        redirect: 'follow',
        signal: controller.signal,
      })
    } catch (error) {
      if (isAbortError(error)) throw new Error(`OpenList 登录超时: ${baseUrl}`)
      throw new Error(`OpenList 登录请求失败: ${baseUrl} - ${formatErrorMessage(error)}`)
    }

    if (!res.ok) {
      const body = await fetchTextSafely(res)
      throw new Error(`OpenList 登录失败: ${baseUrl} -> ${res.status} ${res.statusText}${body ? ` - ${body}` : ''}`)
    }

    const json = await openlistApiReadJson<{ token?: string }>(res)
    if (typeof (json as any)?.code === 'number' && (json as any).code !== 200) {
      const msg = json?.message ? ` - ${json.message}` : ''
      throw new Error(`OpenList 登录失败: ${baseUrl} -> code=${(json as any).code}${msg}`)
    }
    if (typeof json?.data?.token === 'string' && json.data.token) return json.data.token
    const hint = json?.message ? ` - ${json.message}` : ''
    throw new Error(`OpenList 登录失败: ${baseUrl} 未获取到 token${hint}`)
  } finally {
    clearTimeout(timer)
  }
}

export const openlistApiPost = async <T>(params: {
  baseUrl: string
  token?: string
  apiPath: string
  body: any
  timeoutMs: number
}) => {
  const { baseUrl, token, apiPath, body, timeoutMs } = params
  const apiBaseUrl = buildOpenListApiBaseUrl(baseUrl)
  if (!apiBaseUrl) throw new Error('OpenList API 地址不正确，请检查 baseUrl')

  const controller = new AbortController()
  const timer = setTimeout(() => controller.abort(), Math.max(1_000, Math.floor(timeoutMs) || 0))

  try {
    const url = `${apiBaseUrl}${apiPath.startsWith('/') ? apiPath : `/${apiPath}`}`
    let res: Response
    try {
      const headers: Record<string, string> = {
        'Content-Type': 'application/json',
      }
      const authToken = String(token ?? '').trim()
      if (authToken) headers.Authorization = authToken

      res = await fetch(url, {
        method: 'POST',
        headers,
        body: JSON.stringify(body ?? {}),
        redirect: 'follow',
        signal: controller.signal,
      })
    } catch (error) {
      if (isAbortError(error)) throw new Error(`OpenList API 请求超时: ${apiPath}`)
      throw new Error(`OpenList API 请求失败: ${apiPath} - ${formatErrorMessage(error)}`)
    }

    if (!res.ok) {
      const text = await fetchTextSafely(res)
      throw new Error(`OpenList API 失败: ${apiPath} -> ${res.status} ${res.statusText}${text ? ` - ${text}` : ''}`)
    }

    const json = await openlistApiReadJson<T>(res)
    if (typeof json?.code === 'number' && json.code !== 200) {
      const msg = json?.message ? ` - ${json.message}` : ''
      throw new Error(`OpenList API 失败: ${apiPath} -> code=${json.code}${msg}`)
    }
    return json
  } finally {
    clearTimeout(timer)
  }
}

export const openlistApiListEntries = async (params: {
  baseUrl: string
  token?: string
  dirPath: string
  timeoutMs: number
  perPage?: number
}) => {
  const { baseUrl, token, dirPath, timeoutMs } = params
  type FsListItem = { name?: string, is_dir?: boolean }
  type FsListData = { content?: FsListItem[], total?: number }

  const normalized = normalizePosixPath(dirPath)
  const requestedPerPage = typeof params.perPage === 'number' ? params.perPage : 1000
  const perPage = Math.max(1, Math.min(5000, Math.floor(requestedPerPage) || 1000))
  const out: WebDavEntry[] = []

  const maxPages = 20_000
  for (let page = 1; page <= maxPages; page++) {
    const json = await openlistApiPost<FsListData>({
      baseUrl,
      token,
      apiPath: '/fs/list',
      timeoutMs,
      body: {
        path: normalized,
        password: '',
        page,
        per_page: perPage,
        refresh: false,
      },
    })

    const items = Array.isArray(json?.data?.content) ? json.data.content : []
    for (const it of items) {
      const name = String(it?.name ?? '').trim()
      if (!name) continue
      out.push({ name, isDir: Boolean(it?.is_dir) })
    }

    const total = typeof json?.data?.total === 'number' ? json.data.total : undefined
    if (!items.length) break
    if (typeof total === 'number' && Number.isFinite(total) && out.length >= total) break
  }

  return out
}

export const openlistApiGetRawUrl = async (params: {
  baseUrl: string
  token?: string
  filePath: string
  timeoutMs: number
}) => {
  const { baseUrl, token, filePath, timeoutMs } = params
  type FsGetData = { raw_url?: string }
  const normalized = normalizePosixPath(filePath)
  const json = await openlistApiPost<FsGetData>({
    baseUrl,
    token,
    apiPath: '/fs/get',
    timeoutMs,
    body: { path: normalized, password: '', refresh: false },
  })
  const rawUrl = typeof json?.data?.raw_url === 'string' ? json.data.raw_url : ''
  if (!rawUrl) throw new Error(`OpenList 获取 raw_url 失败: ${normalized}`)
  return rawUrl
}

export const openlistApiPathExists = async (params: {
  baseUrl: string
  token?: string
  path: string
  timeoutMs: number
}) => {
  const { baseUrl, token, path: filePath, timeoutMs } = params
  try {
    await openlistApiPost({
      baseUrl,
      token,
      apiPath: '/fs/get',
      timeoutMs,
      body: { path: normalizePosixPath(filePath), password: '', refresh: false },
    })
    return true
  } catch (error) {
    const msg = formatErrorMessage(error)
    if (/code=404\b/i.test(msg) || /not found/i.test(msg) || /object not found/i.test(msg)) return false
    return false
  }
}

export const createOpenListApiDirEnsurer = (baseUrl: string, token: string | undefined, timeoutMs: number) => {
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
        try {
          await openlistApiPost({
            baseUrl,
            token,
            apiPath: '/fs/mkdir',
            timeoutMs: requestTimeoutMs,
            body: { path: current },
          })
        } catch (error) {
          const msg = formatErrorMessage(error)
          if (/exist/i.test(msg) || /already/i.test(msg) || /重复/i.test(msg)) return
          throw error
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

export const downloadAndUploadByOpenListApiPut = async (params: {
  sourceUrl: string
  sourceHeaders?: Record<string, string>
  targetBaseUrl: string
  targetToken?: string
  targetPath: string
  timeoutMs: number
  /** 已知文件大小（可选；用于提前判断是否落盘） */
  expectedSize?: number
}) => {
  const { sourceUrl, sourceHeaders, targetBaseUrl, targetToken, targetPath, timeoutMs, expectedSize } = params

  const apiBaseUrl = buildOpenListApiBaseUrl(targetBaseUrl)
  if (!apiBaseUrl) throw new Error('目标 OpenList API 地址不正确，请检查目标 OpenList 地址')

  await withGlobalTransferLimit(`downloadAndUploadByOpenListApiPut:${targetPath}`, async () => {
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
        if (isAbortError(error)) throw new Error('源端读取超时')
        throw new Error(`源端读取失败: ${formatErrorMessage(error)}`)
      }

      if (!downloadRes.ok) {
        const body = await fetchTextSafely(downloadRes)
        throw new Error(`源端读取失败: ${downloadRes.status} ${downloadRes.statusText}${body ? ` - ${body}` : ''}`)
      }
      if (!downloadRes.body) throw new Error('源端读取失败: 响应体为空')

      const headers: Record<string, string> = {
        'File-Path': encodeURIComponent(normalizePosixPath(targetPath)),
      }
      const authToken = String(targetToken ?? '').trim()
      if (authToken) headers.Authorization = authToken
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
        let body: any
        if (spoolToDisk) {
          tmpFilePath = createTransferTempFilePath('openlist-api-put')
          logger.info(`[传输][落盘] 检测到大文件，将先下载落盘再上传(API): ${targetPath}`)
          await streamToFile(Readable.fromWeb(downloadRes.body as any), tmpFilePath, { signal: controller.signal })
          const stat = await fs.promises.stat(tmpFilePath)
          headers['Content-Length'] = String(stat.size)
          body = fs.createReadStream(tmpFilePath) as any
        } else {
          body = Readable.fromWeb(downloadRes.body as any)
        }

        putRes = await fetch(`${apiBaseUrl}/fs/put`, {
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

      const json = await openlistApiReadJson(putRes)
      if (typeof json?.code === 'number' && json.code !== 200) {
        const msg = json?.message ? ` - ${json.message}` : ''
        throw new Error(`目标端写入失败: code=${json.code}${msg}`)
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
 * 从 URL 下载并 PUT 到 OpenList WebDAV。
 * - 这里是“同端 API -> 目标端 API put”的情况，不需要额外限速。
 * - 需要限速/同端 WebDAV 上传走 webdav.ts 的实现。
 */
export const downloadToBuffer = async (url: string, timeoutMs: number) => {
  const controller = new AbortController()
  const timer = setTimeout(() => controller.abort(), timeoutMs)
  try {
    const res = await fetch(url, { redirect: 'follow', signal: controller.signal })
    if (!res.ok) throw new Error(`${res.status} ${res.statusText}`)
    return Buffer.from(await res.arrayBuffer())
  } finally {
    clearTimeout(timer)
  }
}

/**
 * 有些场景需要“兜底等待一下”（例如协议端文件入库延迟）。
 * 该函数不保证精确等待，仅用于降低重复代码。
 */
export const sleepMs = async (ms: number) => {
  const v = Math.max(0, Math.floor(ms) || 0)
  if (v <= 0) return
  await sleep(v)
}
