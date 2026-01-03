import fs from 'node:fs'
import path from 'node:path'
import { Readable, Transform } from 'node:stream'
import { setTimeout as sleep } from 'node:timers/promises'
import { pathToFileURL } from 'node:url'
import { dir } from '@/dir'
import { config, time } from '@/utils'
import { karin, logger } from 'node-karin'

type SyncMode = 'full' | 'incremental'

const MAX_FILE_TIMEOUT_SEC = 3000
const MIN_FILE_TIMEOUT_SEC = 10
const DEFAULT_PROGRESS_REPORT_EVERY = 10
const MAX_TRANSFER_CONCURRENCY = 5

interface ExportedGroupFile {
  path: string
  fileId: string
  name: string
  size?: number
  uploadTime?: number
  uploaderId?: string
  uploaderName?: string
  md5?: string
  sha1?: string
  sha3?: string
  url?: string
  busid?: number
}

interface ExportError {
  fileId?: string
  path?: string
  message: string
}

interface GroupFileSyncStateV1 {
  version: 1
  groupId: string
  updatedAt: number
  lastSyncAt?: number
  files: Record<string, {
    fileId?: string
    size?: number
    uploadTime?: number
    md5?: string
    sha1?: string
    syncedAt: number
  }>
}

const pickFirstString = (...values: unknown[]) => {
  for (const value of values) {
    if (typeof value === 'string' && value) return value
  }
}

const pickFirstNumber = (...values: unknown[]) => {
  for (const value of values) {
    if (typeof value === 'number' && Number.isFinite(value)) return value
    if (typeof value === 'string' && value && Number.isFinite(Number(value))) return Number(value)
  }
}

const csvEscape = (value: unknown) => {
  const str = String(value ?? '')
  if (/[",\n]/.test(str)) return `"${str.replaceAll('"', '""')}"`
  return str
}

const ensureDir = (dirPath: string) => fs.mkdirSync(dirPath, { recursive: true })

const readJsonSafe = (filePath: string): any => {
  try {
    if (!fs.existsSync(filePath)) return {}
    const raw = fs.readFileSync(filePath, 'utf8')
    return raw ? JSON.parse(raw) : {}
  } catch {
    return {}
  }
}

const writeJsonSafe = (filePath: string, data: unknown) => {
  ensureDir(path.dirname(filePath))
  fs.writeFileSync(filePath, JSON.stringify(data, null, 2), 'utf8')
}

const normalizePosixPath = (inputPath: string, { ensureLeadingSlash = true, stripTrailingSlash = true } = {}) => {
  let value = String(inputPath ?? '').trim().replaceAll('\\', '/')
  value = value.replace(/\/+/g, '/')
  if (!value) value = '/'
  if (ensureLeadingSlash && !value.startsWith('/')) value = `/${value}`
  if (stripTrailingSlash && value.length > 1) value = value.replace(/\/+$/, '')
  return value
}

const safePathSegment = (input: string) => {
  const value = String(input ?? '')
    .replaceAll('\0', '')
    .replaceAll('\\', '_')
    .replaceAll('/', '_')
    .trim()
  return value || 'unnamed'
}

const encodePathForUrl = (posixPath: string) => {
  const normalized = normalizePosixPath(posixPath)
  const segments = normalized.split('/').filter(Boolean).map(encodeURIComponent)
  return `/${segments.join('/')}`
}

const createThrottleTransform = (bytesPerSec: number) => {
  const limit = Math.floor(bytesPerSec || 0)
  if (!Number.isFinite(limit) || limit <= 0) return null

  let nextTime = Date.now()
  const msPerByte = 1000 / limit

  return new Transform({
    transform(chunk, _enc, cb) {
      void (async () => {
        const size = Buffer.isBuffer(chunk) ? chunk.length : Buffer.byteLength(chunk)
        const now = Date.now()
        const start = Math.max(now, nextTime)
        nextTime = start + size * msPerByte
        const waitMs = start - now
        if (waitMs > 0) await sleep(waitMs)
        cb(null, chunk)
      })().catch((err) => cb(err as any))
    },
  })
}

const webdavPropfindListNames = async (params: {
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

const buildUploadFileCandidates = (filePath: string) => {
  const normalized = filePath.replaceAll('\\', '/')
  const candidates = [
    filePath,
    normalized,
  ]

  try {
    candidates.push(pathToFileURL(filePath).href)
  } catch {}

  if (/^[a-zA-Z]:\//.test(normalized)) {
    candidates.push(`file:///${normalized}`)
  }

  return [...new Set(candidates.filter(Boolean))]
}

const getGroupSyncStatePath = (groupId: string) => path.join(dir.DataDir, 'group-file-sync-state', `${String(groupId)}.json`)

const readGroupSyncState = (groupId: string): GroupFileSyncStateV1 => {
  const raw = readJsonSafe(getGroupSyncStatePath(groupId))
  if (raw && typeof raw === 'object' && raw.version === 1 && raw.files && typeof raw.files === 'object') {
    return {
      version: 1,
      groupId: String(raw.groupId ?? groupId),
      updatedAt: typeof raw.updatedAt === 'number' ? raw.updatedAt : Date.now(),
      lastSyncAt: typeof raw.lastSyncAt === 'number' ? raw.lastSyncAt : undefined,
      files: raw.files as GroupFileSyncStateV1['files'],
    }
  }

  return {
    version: 1,
    groupId: String(groupId),
    updatedAt: Date.now(),
    files: {},
  }
}

const writeGroupSyncState = (groupId: string, state: GroupFileSyncStateV1) => {
  const next: GroupFileSyncStateV1 = {
    version: 1,
    groupId: String(groupId),
    updatedAt: Date.now(),
    lastSyncAt: typeof state.lastSyncAt === 'number' ? state.lastSyncAt : undefined,
    files: state.files ?? {},
  }
  writeJsonSafe(getGroupSyncStatePath(groupId), next)
}

const normalizeSyncMode = (value: unknown, fallback: SyncMode): SyncMode => {
  const v = String(value ?? '').trim().toLowerCase()
  if (v === 'full' || v === '全量') return 'full'
  if (v === 'incremental' || v === '增量' || v === 'inc') return 'incremental'
  return fallback
}

const getGroupSyncTarget = (cfg: any, groupId: string) => {
  const list = cfg?.groupSyncTargets
  if (!Array.isArray(list)) return undefined
  return list.find((it: any) => String(it?.groupId) === String(groupId))
}

const getGroupFileListCompat = async (bot: any, groupId: string, folderId?: string) => {
  const groupNum = Number(groupId)
  if (Number.isFinite(groupNum)) {
    const onebot = bot?._onebot
    if (!folderId && typeof onebot?.getGroupRootFiles === 'function') {
      const res = await onebot.getGroupRootFiles(groupNum)
      return {
        files: Array.isArray(res?.files) ? res.files : [],
        folders: Array.isArray(res?.folders) ? res.folders : [],
      }
    }

    if (folderId && typeof onebot?.getGroupFilesByFolder === 'function') {
      const res = await onebot.getGroupFilesByFolder(groupNum, folderId)
      return {
        files: Array.isArray(res?.files) ? res.files : [],
        folders: Array.isArray(res?.folders) ? res.folders : [],
      }
    }
  }

  if (typeof bot?.getGroupFileList === 'function') {
    try {
      const res = await bot.getGroupFileList(groupId, folderId)
      return {
        files: Array.isArray(res?.files) ? res.files : [],
        folders: Array.isArray(res?.folders) ? res.folders : [],
      }
    } catch (error) {
      logger.debug(`[群文件导出] getGroupFileList 调用失败，将尝试 OneBot 扩展: ${String(error)}`)
    }
  }

  if (!Number.isFinite(groupNum)) {
    throw new Error('群号无法转换为 number，且当前适配器不支持 getGroupFileList')
  }

  if (!folderId && typeof bot?.getGroupRootFiles === 'function') {
    const res = await bot.getGroupRootFiles(groupNum)
    return {
      files: Array.isArray(res?.files) ? res.files : [],
      folders: Array.isArray(res?.folders) ? res.folders : [],
    }
  }

  if (folderId && typeof bot?.getGroupFilesByFolder === 'function') {
    const res = await bot.getGroupFilesByFolder(groupNum, folderId)
    return {
      files: Array.isArray(res?.files) ? res.files : [],
      folders: Array.isArray(res?.folders) ? res.folders : [],
    }
  }

  throw new Error('当前适配器不支持获取群文件列表（getGroupFileList / getGroupRootFiles / getGroupFilesByFolder 均不可用）')
}

const resolveGroupFileUrl = async (bot: any, contact: any, groupId: string, file: ExportedGroupFile) => {
  if (!file.fileId) throw new Error('缺少 fileId')

  const reasons: string[] = []

  if (typeof bot?.getFileUrl === 'function') {
    try {
      const url = await bot.getFileUrl(contact, file.fileId)
      if (typeof url === 'string' && url) return url
      reasons.push('getFileUrl 返回空值')
    } catch (error: any) {
      reasons.push(`getFileUrl: ${error?.message ?? String(error)}`)
    }
  }

  const groupNum = Number(groupId)
  if (!Number.isFinite(groupNum)) {
    throw new Error(reasons[0] ?? '群号无法转换为 number')
  }

  const onebot = bot?._onebot
  if (typeof onebot?.nc_getFile === 'function') {
    try {
      const res = await onebot.nc_getFile(file.fileId)
      if (typeof res?.url === 'string' && res.url) return res.url
      reasons.push('nc_getFile 返回空值')
    } catch (error: any) {
      reasons.push(`nc_getFile: ${error?.message ?? String(error)}`)
    }
  }

  if (typeof onebot?.getGroupFileUrl === 'function') {
    try {
      const res = await onebot.getGroupFileUrl(groupNum, file.fileId, file.busid)
      if (typeof res?.url === 'string' && res.url) return res.url
      reasons.push('onebot.getGroupFileUrl 返回空值')
    } catch (error: any) {
      reasons.push(`onebot.getGroupFileUrl: ${error?.message ?? String(error)}`)
    }

    try {
      const res = await onebot.getGroupFileUrl(groupNum, file.fileId)
      if (typeof res?.url === 'string' && res.url) return res.url
    } catch {}
  }

  if (typeof bot?.getGroupFileUrl === 'function') {
    try {
      const res = await bot.getGroupFileUrl(groupNum, file.fileId, file.busid)
      if (typeof res?.url === 'string' && res.url) return res.url
      reasons.push('getGroupFileUrl 返回空值')
    } catch (error: any) {
      reasons.push(`getGroupFileUrl: ${error?.message ?? String(error)}`)
    }

    try {
      const res = await bot.getGroupFileUrl(groupNum, file.fileId)
      if (typeof res?.url === 'string' && res.url) return res.url
    } catch (error: any) {
      reasons.push(`getGroupFileUrl(no busid): ${error?.message ?? String(error)}`)
    }
  }

  throw new Error(reasons[0] ?? '无法获取下载URL（未找到可用接口）')
}

const buildOpenListDavBaseUrl = (baseUrl: string) => {
  const normalized = String(baseUrl ?? '').trim().replace(/\/+$/, '')
  if (!normalized) return ''
  return `${normalized}/dav`
}

const buildOpenListAuthHeader = (username: string, password: string) => {
  const user = String(username ?? '')
  const pass = String(password ?? '')
  const token = Buffer.from(`${user}:${pass}`).toString('base64')
  return `Basic ${token}`
}

const formatErrorMessage = (error: unknown) => {
  if (error instanceof Error) {
    const base = error.message || String(error)
    const cause: any = (error as any).cause
    if (cause) {
      const causeMsg = cause instanceof Error ? cause.message : String(cause)
      const causeCode = typeof cause === 'object' && cause && 'code' in cause ? String((cause as any).code) : ''
      const extra = [causeCode, causeMsg].filter(Boolean).join(' ')
      if (extra && extra !== base) return `${base} (${extra})`
    }
    return base
  }
  return String(error)
}

const isAbortError = (error: unknown) => {
  return Boolean(error && typeof error === 'object' && 'name' in (error as any) && (error as any).name === 'AbortError')
}

const fetchTextSafely = async (res: Response) => {
  try {
    const text = await res.text()
    return text.slice(0, 500)
  } catch {
    return ''
  }
}

const webdavMkcolOk = (status: number) => status === 201 || status === 405

const createWebDavDirEnsurer = (davBaseUrl: string, auth: string, timeoutMs: number) => {
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
        const url = `${davBaseUrl}${encodePathForUrl(current)}`
        const controller = new AbortController()
        const timer = setTimeout(() => controller.abort(), requestTimeoutMs)
        try {
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

          if (webdavMkcolOk(res.status) || res.ok) return

          const body = await fetchTextSafely(res)
          const hint = res.status === 401
            ? '（账号/密码错误，或未开启 WebDAV）'
            : res.status === 403
              ? '（没有 WebDAV 管理/写入权限，或目标目录不可写/不在用户可访问范围）'
              : ''
          throw new Error(`MKCOL 失败: ${current} -> ${res.status} ${res.statusText}${hint}${body ? ` - ${body}` : ''}`)
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

const downloadAndUploadByWebDav = async (params: {
  sourceUrl: string
  targetUrl: string
  auth: string
  timeoutMs: number
  rateLimitBytesPerSec?: number
}) => {
  const { sourceUrl, targetUrl, auth, timeoutMs, rateLimitBytesPerSec } = params

  const controller = new AbortController()
  const timer = setTimeout(() => controller.abort(), timeoutMs)

  try {
    let downloadRes: Response
    try {
      downloadRes = await fetch(sourceUrl, { redirect: 'follow', signal: controller.signal })
    } catch (error) {
      if (isAbortError(error)) throw new Error('下载超时（URL可能已失效）')
      throw new Error(`下载请求失败: ${formatErrorMessage(error)}`)
    }

    if (!downloadRes.ok) {
      const body = await fetchTextSafely(downloadRes)
      const hint = downloadRes.status === 403 ? '（可能URL已过期，需要重新获取）' : ''
      throw new Error(`下载失败: ${downloadRes.status} ${downloadRes.statusText}${hint}${body ? ` - ${body}` : ''}`)
    }
    if (!downloadRes.body) throw new Error('下载失败: 响应体为空')

    const headers: Record<string, string> = { Authorization: auth }
    const contentType = downloadRes.headers.get('content-type')
    const contentLength = downloadRes.headers.get('content-length')
    if (contentType) headers['Content-Type'] = contentType
    if (contentLength) headers['Content-Length'] = contentLength

    const sourceStream = Readable.fromWeb(downloadRes.body as any)
    const throttle = createThrottleTransform(Math.floor(rateLimitBytesPerSec || 0))
    const bodyStream = throttle ? sourceStream.pipe(throttle) : sourceStream

    let putRes: Response
    try {
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
  } finally {
    clearTimeout(timer)
  }
}

const activeGroupSync = new Set<string>()

const withGroupSyncLock = async <T>(groupId: string, fn: () => Promise<T>) => {
  const key = String(groupId)
  if (activeGroupSync.has(key)) throw new Error('该群同步任务正在进行中，请稍后再试')
  activeGroupSync.add(key)
  try {
    return await fn()
  } finally {
    activeGroupSync.delete(key)
  }
}

const buildRemotePathForItem = (item: ExportedGroupFile, targetDir: string, flat: boolean) => {
  const relativeParts = (flat ? [item.name] : item.path.split('/')).filter(Boolean).map(safePathSegment)
  return normalizePosixPath(path.posix.join(targetDir, ...relativeParts))
}

const isSameSyncedFile = (
  prev: GroupFileSyncStateV1['files'][string] | undefined,
  item: ExportedGroupFile,
) => {
  if (!prev) return false
  if (prev.fileId && item.fileId && prev.fileId !== item.fileId) return false

  const md5Ok = prev.md5 && item.md5 && prev.md5 === item.md5
  const sha1Ok = prev.sha1 && item.sha1 && prev.sha1 === item.sha1
  if (md5Ok || sha1Ok) return true

  const sizeOk = typeof prev.size === 'number' && typeof item.size === 'number' && prev.size === item.size
  const timeOk = typeof prev.uploadTime === 'number' && typeof item.uploadTime === 'number' && prev.uploadTime === item.uploadTime

  if (sizeOk && timeOk) return true
  if (timeOk && prev.fileId && item.fileId && prev.fileId === item.fileId) return true
  if (sizeOk && prev.fileId && item.fileId && prev.fileId === item.fileId) return true

  return false
}

const runWithConcurrency = async <T>(items: T[], concurrency: number, fn: (item: T, index: number) => Promise<void>) => {
  const limit = Math.max(1, Math.floor(concurrency) || 1)
  const executing = new Set<Promise<void>>()

  for (let index = 0; index < items.length; index++) {
    const item = items[index]
    const task = (async () => fn(item, index))()
    executing.add(task)
    task.finally(() => executing.delete(task))

    if (executing.size >= limit) {
      await Promise.race(executing)
    }
  }

  await Promise.all(executing)
}

const runWithAdaptiveConcurrency = async <T>(
  items: T[],
  options: {
    initial: number
    max: number
    fn: (item: T, index: number) => Promise<void>
    onAdjust?: (current: number, reason: string) => void
  },
) => {
  const max = Math.max(1, Math.floor(options.max) || 1)
  let current = Math.min(max, Math.max(1, Math.floor(options.initial) || 1))
  const onAdjust = options.onAdjust

  const results: Array<{ ok: boolean, ms: number, reason?: string }> = []
  const pushResult = (ok: boolean, ms: number, reason?: string) => {
    results.push({ ok, ms, reason })
    if (results.length > 20) results.shift()

    if (results.length < 10) return
    if (results.length % 5 !== 0) return

    const failCount = results.filter(r => !r.ok).length
    const failRate = failCount / results.length
    const avgMs = results.reduce((acc, r) => acc + r.ms, 0) / results.length

    const hasTimeout = results.some(r => (r.reason || '').includes('超时'))
    if (hasTimeout || failRate >= 0.2) {
      if (current > 1) {
        current -= 1
        onAdjust?.(current, hasTimeout ? 'timeout' : `failRate=${failRate.toFixed(2)}`)
      }
      return
    }

    if (failCount === 0 && current < max) {
      if (avgMs < 60_000 || results.length === 20) {
        current += 1
        onAdjust?.(current, 'stable')
      }
    }
  }

  let nextIndex = 0
  const executing = new Set<Promise<void>>()

  const launch = (index: number) => {
    const item = items[index]
    const start = Date.now()
    const task = (async () => {
      try {
        await options.fn(item, index)
        pushResult(true, Date.now() - start)
      } catch (error: any) {
        const msg = formatErrorMessage(error)
        pushResult(false, Date.now() - start, msg)
        throw error
      }
    })()

    executing.add(task)
    task.finally(() => executing.delete(task))
    return task
  }

  while (nextIndex < items.length || executing.size) {
    while (nextIndex < items.length && executing.size < current) {
      launch(nextIndex)
      nextIndex++
    }
    if (executing.size) await Promise.race(executing)
  }
}

const collectAllGroupFiles = async (bot: any, groupId: string, startFolderId?: string) => {
  const files: ExportedGroupFile[] = []
  const visitedFolders = new Set<string>()

  const walk = async (folderId: string | undefined, prefix: string) => {
    if (folderId) {
      if (visitedFolders.has(folderId)) return
      visitedFolders.add(folderId)
    }

    const { files: rawFiles, folders: rawFolders } = await getGroupFileListCompat(bot, groupId, folderId)

    for (const raw of rawFiles) {
      const fileId = pickFirstString(raw?.fid, raw?.file_id, raw?.fileId, raw?.id)
      const name = pickFirstString(raw?.name, raw?.file_name, raw?.fileName) ?? (fileId ? `file-${fileId}` : 'unknown-file')
      const filePath = prefix ? `${prefix}/${name}` : name

      files.push({
        path: filePath,
        fileId: fileId ?? '',
        name,
        size: pickFirstNumber(raw?.size, raw?.file_size, raw?.fileSize),
        uploadTime: pickFirstNumber(raw?.uploadTime, raw?.upload_time),
        uploaderId: pickFirstString(raw?.uploadId, raw?.uploader, raw?.uploader_id),
        uploaderName: pickFirstString(raw?.uploadName, raw?.uploader_name),
        md5: pickFirstString(raw?.md5),
        sha1: pickFirstString(raw?.sha1),
        sha3: pickFirstString(raw?.sha3),
        busid: pickFirstNumber(raw?.busid, raw?.busId),
      })
    }

    for (const raw of rawFolders) {
      const folderId = pickFirstString(raw?.id, raw?.folder_id, raw?.folderId)
      if (!folderId) continue
      const folderName = pickFirstString(raw?.name, raw?.folder_name, raw?.folderName) ?? folderId
      const nextPrefix = prefix ? `${prefix}/${folderName}` : folderName
      await walk(folderId, nextPrefix)
    }
  }

  await walk(startFolderId, '')
  return files
}

const writeExportFile = (format: 'json' | 'csv', outPath: string, payload: any, list: ExportedGroupFile[]) => {
  ensureDir(path.dirname(outPath))

  if (format === 'json') {
    fs.writeFileSync(outPath, JSON.stringify(payload, null, 2), 'utf8')
    return
  }

  const header = ['path', 'name', 'fileId', 'size', 'uploadTime', 'uploaderId', 'uploaderName', 'md5', 'sha1', 'sha3', 'url', 'busid']
  const rows = [header.join(',')]
  for (const item of list) {
    rows.push([
      csvEscape(item.path),
      csvEscape(item.name),
      csvEscape(item.fileId),
      csvEscape(item.size ?? ''),
      csvEscape(item.uploadTime ?? ''),
      csvEscape(item.uploaderId ?? ''),
      csvEscape(item.uploaderName ?? ''),
      csvEscape(item.md5 ?? ''),
      csvEscape(item.sha1 ?? ''),
      csvEscape(item.sha3 ?? ''),
      csvEscape(item.url ?? ''),
      csvEscape(item.busid ?? ''),
    ].join(','))
  }
  fs.writeFileSync(outPath, rows.join('\n'), 'utf8')
}

const parseArgs = (text: string) => {
  const raw = text.trim()
  const tokens = raw ? raw.split(/\s+/).filter(Boolean) : []
  const format: 'json' | 'csv' = /(^|\s)(--csv|csv)(\s|$)/i.test(raw) ? 'csv' : 'json'
  const withUrl = !/(^|\s)(--no-url|--nourl|no-url|nourl)(\s|$)/i.test(raw)
  const urlOnly = /(^|\s)(--url-only|--urlonly|url-only|urlonly)(\s|$)/i.test(raw)
  const sendFile = /(^|\s)(--send-file|--sendfile|send-file|sendfile)(\s|$)/i.test(raw)

  let groupId: string | undefined
  for (let i = 0; i < tokens.length; i++) {
    const token = tokens[i]
    const nextToken = tokens[i + 1]

    if (/^--(group|gid|groupid)$/i.test(token) && nextToken && /^\d+$/.test(nextToken)) {
      groupId = nextToken
      break
    }

    const assignMatch = token.match(/^(group|gid|groupid)=(\d+)$/i)
    if (assignMatch) {
      groupId = assignMatch[2]
      break
    }
  }

  if (!groupId) {
    for (let i = 0; i < tokens.length; i++) {
      const token = tokens[i]
      const prevToken = tokens[i - 1]
      if (!/^\d+$/.test(token)) continue
      if (prevToken && /^--(folder|max|concurrency|group|gid|groupid)$/i.test(prevToken)) continue
      groupId = token
      break
    }
  }

  const folderMatch = raw.match(/--folder\s+(\S+)/i) ?? raw.match(/(^|\s)folder=(\S+)/i)
  const folderId = folderMatch ? folderMatch[folderMatch.length - 1] : undefined

  const maxMatch = raw.match(/--max\s+(\d+)/i) ?? raw.match(/(^|\s)max=(\d+)/i)
  const maxFiles = maxMatch ? Number(maxMatch[maxMatch.length - 1]) : undefined

  const concurrencyMatch = raw.match(/--concurrency\s+(\d+)/i) ?? raw.match(/(^|\s)concurrency=(\d+)/i)
  const concurrency = concurrencyMatch ? Number(concurrencyMatch[concurrencyMatch.length - 1]) : undefined
  const concurrencySpecified = Boolean(concurrencyMatch)

  const help = /(^|\s)(--help|-h|help|\?)(\s|$)/i.test(raw)

  return { groupId, format, withUrl, urlOnly, sendFile, folderId, maxFiles, concurrency, concurrencySpecified, help }
}

const parseSyncArgs = (text: string) => {
  const raw = text.trim()
  const base = parseArgs(text)

  const toMatch = raw.match(/--to\s+(\S+)/i) ?? raw.match(/(^|\s)to=(\S+)/i)
  const to = toMatch ? toMatch[toMatch.length - 1] : undefined
  const toSpecified = Boolean(toMatch)

  const flatFlag = /(^|\s)(--flat|flat)(\s|$)/i.test(raw)
  const keepFlag = /(^|\s)(--keep|--no-flat|keep|no-flat)(\s|$)/i.test(raw)
  const flatSpecified = flatFlag || keepFlag
  const flat = flatFlag ? true : keepFlag ? false : undefined

  const help = /(^|\s)(--help|-h|help|\?)(\s|$)/i.test(raw)
  const concurrency = base.concurrency
  const concurrencySpecified = base.concurrencySpecified
  const timeoutMatch = raw.match(/--timeout\s+(\d+)/i) ?? raw.match(/(^|\s)timeout=(\d+)/i)
  const timeoutSec = timeoutMatch ? Number(timeoutMatch[timeoutMatch.length - 1]) : undefined
  const timeoutSpecified = Boolean(timeoutMatch)

  const modeFull = /(^|\s)(--full|full)(\s|$)/i.test(raw)
  const modeInc = /(^|\s)(--inc|--incremental|inc|incremental)(\s|$)/i.test(raw)
  const mode: SyncMode | undefined = modeFull ? 'full' : modeInc ? 'incremental' : undefined

  return {
    groupId: base.groupId,
    folderId: base.folderId,
    maxFiles: base.maxFiles,
    concurrency,
    concurrencySpecified,
    flat,
    flatSpecified,
    to,
    toSpecified,
    timeoutSec,
    timeoutSpecified,
    mode,
    help,
  }
}

const helpText = [
  '群文件导出用法：',
  '- 请私聊发送：#导出群文件 <群号> [参数]',
  '- 示例：#导出群文件 123456',
  '- #导出群文件 123456 --no-url：只导出列表，不解析URL',
  '- #导出群文件 123456 --url-only：仅输出URL（更方便复制）',
  '- #导出群文件 123456 --csv：导出为CSV（默认JSON）',
  '- #导出群文件 123456 --folder <id>：从指定文件夹开始导出',
  '- #导出群文件 123456 --max <n>：最多导出n条文件记录',
  '- #导出群文件 123456 --concurrency <n>：解析URL并发数（默认3）',
  '- #导出群文件 123456 --send-file：尝试发送导出文件（依赖协议端支持）',
  '提示：下载URL通常有时效，过期后需重新导出。',
].join('\n')

export const exportGroupFiles = karin.command(/^#?(导出群文件|群文件导出)(.*)$/i, async (e) => {
  if (!e.isPrivate) {
    await e.reply('请私聊使用该命令，并在参数中指定群号\n例如：#导出群文件 123456')
    return true
  }

  const argsText = e.msg.replace(/^#?(导出群文件|群文件导出)/i, '')
  const { groupId, format, withUrl, urlOnly, sendFile, folderId, maxFiles, concurrency, help } = parseArgs(argsText)
  if (help) {
    await e.reply(helpText)
    return true
  }

  if (!groupId) {
    await e.reply(`缺少群号参数\n\n${helpText}`)
    return true
  }

  const groupContact = karin.contactGroup(groupId)

  await e.reply(`开始导出群文件列表，请稍候...\n- 群号：${groupId}\n- 格式：${format}\n- 包含URL：${withUrl ? '是' : '否'}`)

  const errors: ExportError[] = []
  let list: ExportedGroupFile[] = []
  try {
    list = await collectAllGroupFiles(e.bot, groupId, folderId)
  } catch (error: any) {
    logger.error(error)
    await e.reply(`导出失败：${formatErrorMessage(error)}`)
    return true
  }

  const limitedList = typeof maxFiles === 'number' && Number.isFinite(maxFiles) && maxFiles > 0
    ? list.slice(0, Math.floor(maxFiles))
    : list

  if (withUrl) {
    const urlConcurrency = typeof concurrency === 'number' && Number.isFinite(concurrency) && concurrency > 0 ? concurrency : 3
    await runWithConcurrency(limitedList, urlConcurrency, async (item) => {
      try {
        item.url = await resolveGroupFileUrl(e.bot, groupContact, groupId, item)
      } catch (error: any) {
        errors.push({
          fileId: item.fileId,
          path: item.path,
          message: formatErrorMessage(error),
        })
      }
    })
  }

  const exportDir = path.join(dir.karinPath, 'data', 'group-files-export')
  const exportName = `group-files-${groupId}-${time('YYYYMMDD-HHmmss')}.${format}`
  const exportPath = path.join(exportDir, exportName)

  const payload = {
    type: 'group-files-export',
    plugin: { name: dir.name, version: dir.version },
    groupId,
    folderId: folderId ?? null,
    exportedAt: time(),
    withUrl,
    fileCount: limitedList.length,
    errors,
    files: limitedList,
  }

  try {
    writeExportFile(format, exportPath, payload, limitedList)
  } catch (error: any) {
    logger.error(error)
    await e.reply(`写入导出文件失败：${formatErrorMessage(error)}`)
    return true
  }

  const urlOk = withUrl ? limitedList.filter(v => typeof v.url === 'string' && v.url).length : 0
  const urlFail = withUrl ? (limitedList.length - urlOk) : 0
  const summary = [
    `导出完成：${limitedList.length} 个文件`,
    withUrl ? `URL：成功 ${urlOk} / 失败 ${urlFail}` : null,
    `导出文件：${exportName}`,
    `导出文件已保存至：${exportPath}`,
  ].filter(Boolean).join('\n')
  await e.reply(summary)

  const textMax = 200
  const preview = limitedList.slice(0, textMax)
  const errorByFileId = new Map<string, string>()
  for (const err of errors) {
    if (err.fileId && err.message && !errorByFileId.has(err.fileId)) errorByFileId.set(err.fileId, err.message)
  }
  const compactError = (message: string) => message.replace(/\s+/g, ' ').slice(0, 120)

  const lines = preview.map((item, index) => {
    if (!withUrl) return `${index + 1}. ${item.path}\t${item.fileId}`

    if (urlOnly) return item.url ? item.url : `(获取URL失败) ${item.path} (${item.fileId})`

    if (item.url) return `${index + 1}. ${item.path}\n${item.url}`
    const errMsg = errorByFileId.get(item.fileId)
    return `${index + 1}. ${item.path}\n(获取URL失败) fileId=${item.fileId}${errMsg ? `\n原因：${compactError(errMsg)}` : ''}`
  })

  const chunks: string[] = []
  const maxChunkLen = 1500
  let buf = ''
  for (const line of lines) {
    const next = buf ? `${buf}\n\n${line}` : line
    if (next.length > maxChunkLen) {
      if (buf) chunks.push(buf)
      buf = line
    } else {
      buf = next
    }
  }
  if (buf) chunks.push(buf)

  const maxMessages = 10
  for (const chunk of chunks.slice(0, maxMessages)) {
    await e.reply(chunk)
  }
  if (limitedList.length > preview.length) {
    await e.reply(`（已省略 ${limitedList.length - preview.length} 条，可使用 --max 调整）`)
  } else if (chunks.length > maxMessages) {
    await e.reply(`（消息过长，已省略后续内容；可使用 --max 减少条数）`)
  }

  if (sendFile && typeof e.bot?.uploadFile === 'function') {
    const candidates = buildUploadFileCandidates(exportPath)
    for (const fileParam of candidates) {
      try {
        await e.bot.uploadFile(e.contact, fileParam, exportName)
        break
      } catch {}
    }
  }

  return true
}, {
  priority: 9999,
  log: true,
  name: '导出群文件',
  permission: 'all',
})

const syncHelpText = [
  '群文件同步到 OpenList 用法：',
  '- 私聊：#同步群文件 <群号> [参数]',
  '- 群聊：#同步群文件（默认同步本群；建议先在 WebUI 配置同步对象群）',
  '- 示例：#同步群文件 123456',
  '- #同步群文件 123456 --to /目标目录：上传到指定目录（默认使用配置 openlistTargetDir）',
  '- #同步群文件 123456 --flat：不保留群文件夹结构，全部平铺到目标目录',
  '- #同步群文件 123456 --keep：强制保留目录结构（覆盖群配置 flat）',
  '- #同步群文件 123456 --folder <id>：从指定文件夹开始',
  '- #同步群文件 123456 --max <n>：最多处理 n 个文件',
  '- #同步群文件 123456 --concurrency <n>：并发数（会覆盖群配置的并发）',
  '- #同步群文件 123456 --timeout <sec>：单文件超时秒数（仅影响单个文件）',
  '- #同步群文件 123456 --full/--inc：覆盖群配置的同步模式（全量/增量）',
  '前置：请先在配置文件填写 openlistBaseUrl/openlistUsername/openlistPassword。',
].join('\n')

export const syncGroupFilesToOpenListCore = async (params: {
  bot: any
  groupId: string
  folderId?: string
  maxFiles?: number
  flat: boolean
  targetDir: string
  mode: SyncMode
  urlConcurrency: number
  transferConcurrency: number
  fileTimeoutSec: number
  retryTimes: number
  retryDelayMs: number
  progressReportEvery?: number
  downloadLimitKbps?: number
  uploadLimitKbps?: number
  report?: (message: string) => Promise<void> | void
}) => {
  const {
    bot,
    groupId,
    folderId,
    maxFiles,
    flat,
    targetDir,
    mode,
    urlConcurrency,
    transferConcurrency,
    fileTimeoutSec,
    retryTimes,
    retryDelayMs,
    progressReportEvery,
    downloadLimitKbps,
    uploadLimitKbps,
    report,
  } = params

  const cfg = config()
  const baseUrl = String(cfg.openlistBaseUrl ?? '').trim()
  const username = String(cfg.openlistUsername ?? '').trim()
  const password = String(cfg.openlistPassword ?? '').trim()
  const defaultTargetDir = String(cfg.openlistTargetDir ?? '/').trim()

  if (!baseUrl || !username || !password) {
    throw new Error([
      '请先配置 OpenList 信息（openlistBaseUrl/openlistUsername/openlistPassword）',
      `配置文件位置：${dir.ConfigDir}/config.json`,
    ].join('\n'))
  }

  const davBaseUrl = buildOpenListDavBaseUrl(baseUrl)
  if (!davBaseUrl) throw new Error('OpenList 地址不正确，请检查 openlistBaseUrl')

  const finalTargetDir = normalizePosixPath(targetDir || defaultTargetDir)
  const auth = buildOpenListAuthHeader(username, password)

  const safeFileTimeoutSec = Math.min(
    MAX_FILE_TIMEOUT_SEC,
    Math.max(MIN_FILE_TIMEOUT_SEC, Math.floor(fileTimeoutSec || 0)),
  )
  const safeProgressReportEvery = Math.max(
    0,
    Math.floor(typeof progressReportEvery === 'number' ? progressReportEvery : DEFAULT_PROGRESS_REPORT_EVERY),
  )
  const rateDown = Math.max(0, Math.floor(typeof downloadLimitKbps === 'number' ? downloadLimitKbps : 0))
  const rateUp = Math.max(0, Math.floor(typeof uploadLimitKbps === 'number' ? uploadLimitKbps : 0))
  const effectiveRateLimitBytesPerSec = (() => {
    const a = rateDown > 0 ? rateDown * 1024 : 0
    const b = rateUp > 0 ? rateUp * 1024 : 0
    if (a > 0 && b > 0) return Math.min(a, b)
    return a > 0 ? a : b > 0 ? b : 0
  })()
  const transferTimeoutMs = safeFileTimeoutSec * 1000
  const webdavTimeoutMs = 15_000
  const dirEnsurer = createWebDavDirEnsurer(davBaseUrl, auth, webdavTimeoutMs)

  const groupContact = karin.contactGroup(groupId)

  const state = readGroupSyncState(groupId)

  return await withGroupSyncLock(groupId, async () => {
    report && await report([
      '开始同步群文件到 OpenList，请稍候...',
      `- 群号：${groupId}`,
      `- 目标目录：${finalTargetDir}`,
      `- 模式：${mode === 'incremental' ? '增量' : '全量'}`,
      `- 保留目录结构：${flat ? '否' : '是'}`,
      `- 并发：URL ${urlConcurrency} / 传输 ${transferConcurrency}`,
    ].join('\n'))

    let list: ExportedGroupFile[] = []
    list = await collectAllGroupFiles(bot, groupId, folderId)

    const limitedList = typeof maxFiles === 'number' && Number.isFinite(maxFiles) && maxFiles > 0
      ? list.slice(0, Math.floor(maxFiles))
      : list

    const candidates = limitedList.map((item) => {
      const remotePath = buildRemotePathForItem(item, finalTargetDir, flat)
      return { item, remotePath }
    })

    let skipped = 0
    let needSync = mode === 'incremental'
      ? candidates.filter(({ item, remotePath }) => {
          const prev = state.files[remotePath]
          const ok = isSameSyncedFile(prev, item)
          if (ok) skipped++
          return !ok
        })
      : candidates

    // 增量同步：额外检查 OpenList 目标目录是否已存在同名文件，存在则跳过
    if (mode === 'incremental' && needSync.length) {
      const dirs = Array.from(new Set(
        needSync.map(({ remotePath }) => normalizePosixPath(path.posix.dirname(remotePath))),
      ))

      const namesByDir = new Map<string, Set<string>>()
      await runWithConcurrency(dirs, 3, async (dirPath) => {
        const names = await webdavPropfindListNames({
          davBaseUrl,
          auth,
          dirPath,
          timeoutMs: webdavTimeoutMs,
        })
        namesByDir.set(dirPath, names)
      })

      needSync = needSync.filter(({ remotePath }) => {
        const dirPath = normalizePosixPath(path.posix.dirname(remotePath))
        const base = path.posix.basename(remotePath)
        const names = namesByDir.get(dirPath)
        if (names && names.has(base)) {
          skipped++
          return false
        }
        return true
      })
    }

    if (!needSync.length) {
      report && await report(`没有需要同步的文件（增量模式已跳过 ${skipped} 条）。`)
      return { total: limitedList.length, skipped, urlOk: 0, ok: 0, fail: 0 }
    }

    const urlErrors: ExportError[] = []
    await runWithConcurrency(needSync, Math.max(1, Math.floor(urlConcurrency) || 1), async ({ item }) => {
      try {
        item.url = await resolveGroupFileUrl(bot, groupContact, groupId, item)
      } catch (error: any) {
        urlErrors.push({ fileId: item.fileId, path: item.path, message: formatErrorMessage(error) })
      }
    })

    const withUrl = needSync.filter(({ item }) => typeof item.url === 'string' && item.url).map(v => v)
    report && await report(`URL获取完成：成功 ${withUrl.length} / 失败 ${needSync.length - withUrl.length}（增量跳过 ${skipped}）`)

    if (!withUrl.length) {
      report && await report('没有可用的下载URL，无法同步到 OpenList')
      return { total: limitedList.length, skipped, urlOk: 0, ok: 0, fail: 0 }
    }

    const syncErrors: Array<{ path: string, fileId: string, message: string }> = []
    let okCount = 0

    const shouldRefreshUrl = (message: string) => {
      return /403|URL已过期|url已过期|url可能已失效|需要重新获取|下载超时/.test(message)
    }

    const transferOne = async (sourceUrl: string, targetUrl: string) => {
      await downloadAndUploadByWebDav({
        sourceUrl,
        targetUrl,
        auth,
        timeoutMs: transferTimeoutMs,
        rateLimitBytesPerSec: effectiveRateLimitBytesPerSec || undefined,
      })
    }

    report && await report('开始下载并上传到 OpenList，请稍候...')

    const transferInitial = Math.min(MAX_TRANSFER_CONCURRENCY, Math.max(1, Math.floor(transferConcurrency) || 1))
    const adaptiveTransfer = effectiveRateLimitBytesPerSec <= 0

    const transferFn = async ({ item, remotePath }: typeof withUrl[number], index: number) => {
      logger.info(`[群文件同步][${groupId}] 同步中 (${index + 1}/${withUrl.length}): ${item.path}`)

      const remoteDir = normalizePosixPath(path.posix.dirname(remotePath))
      const targetUrl = `${davBaseUrl}${encodePathForUrl(remotePath)}`

      let lastError: unknown
      let succeeded = false
      const attempts = Math.max(0, Math.floor(retryTimes) || 0) + 1

      for (let attempt = 1; attempt <= attempts; attempt++) {
        try {
          await dirEnsurer.ensureDir(remoteDir)

          const currentUrl = item.url as string
          if (!currentUrl) throw new Error('缺少下载URL')

          await transferOne(currentUrl, targetUrl)

          okCount++
          succeeded = true
          lastError = undefined
          state.files[remotePath] = {
            fileId: item.fileId,
            size: item.size,
            uploadTime: item.uploadTime,
            md5: item.md5,
            sha1: item.sha1,
            syncedAt: Date.now(),
          }
          break
        } catch (error: any) {
          lastError = error
          const msg = formatErrorMessage(error)

          if (attempt < attempts) {
            if (shouldRefreshUrl(msg)) {
              try {
                item.url = await resolveGroupFileUrl(bot, groupContact, groupId, item)
              } catch {}
            }

            const delay = Math.max(0, Math.floor(retryDelayMs) || 0) * Math.pow(2, attempt - 1)
            if (delay > 0) await new Promise(resolve => setTimeout(resolve, delay))
            continue
          }
        }
      }

      if (!succeeded && lastError) {
        syncErrors.push({
          path: item.path,
          fileId: item.fileId,
          message: formatErrorMessage(lastError),
        })
      }

      if (safeProgressReportEvery > 0 && (index + 1) % safeProgressReportEvery === 0) {
        report && await report(`同步进度：${index + 1}/${withUrl.length}（成功 ${okCount}）`)
      }
    }

    if (adaptiveTransfer) {
      logger.info(`[群文件同步][${groupId}] 未配置限速，将自适应调整传输并发（最大 ${MAX_TRANSFER_CONCURRENCY}）`)
      await runWithAdaptiveConcurrency(withUrl, {
        initial: transferInitial,
        max: MAX_TRANSFER_CONCURRENCY,
        fn: transferFn,
        onAdjust: (current, reason) => {
          logger.info(`[群文件同步][${groupId}] 自适应调整传输并发=${current} (${reason})`)
        },
      })
    } else {
      await runWithConcurrency(withUrl, transferInitial, transferFn)
    }

    state.lastSyncAt = Date.now()
    writeGroupSyncState(groupId, state)

    const failCount = withUrl.length - okCount
    report && await report(`同步完成：成功 ${okCount} / 失败 ${failCount}（增量跳过 ${skipped}）`)

    if (failCount) {
      const preview = syncErrors.slice(0, 5).map((it) => `${it.path} (${it.fileId})\n${it.message}`).join('\n\n')
      report && await report(`失败示例（前5条）：\n${preview}`)
    }

    if (urlErrors.length) {
      const preview = urlErrors.slice(0, 5).map((it) => `${it.path ?? ''} (${it.fileId ?? ''})\n${it.message}`).join('\n\n')
      report && await report(`URL获取失败示例（前5条）：\n${preview}`)
    }

    return { total: limitedList.length, skipped, urlOk: withUrl.length, ok: okCount, fail: failCount }
  })
}

export const syncGroupFilesToOpenList = karin.command(/^#?(同步群文件|群文件同步)(.*)$/i, async (e) => {
  const argsText = e.msg.replace(/^#?(同步群文件|群文件同步)/i, '')
  const {
    groupId: parsedGroupId,
    folderId: parsedFolderId,
    maxFiles: parsedMaxFiles,
    concurrency,
    concurrencySpecified,
    flat,
    flatSpecified,
    to,
    toSpecified,
    timeoutSec,
    timeoutSpecified,
    mode: forcedMode,
    help,
  } = parseSyncArgs(argsText)
  if (help) {
    await e.reply(syncHelpText)
    return true
  }

  const cfg = config()
  const groupId = parsedGroupId ?? (e.isGroup ? e.groupId : undefined)
  if (!groupId) {
    await e.reply(`缺少群号参数\n\n${syncHelpText}`)
    return true
  }

  if (e.isGroup) {
    const role = (e.sender as any)?.role
    if (role !== 'owner' && role !== 'admin') {
      await e.reply('请群管理员使用该命令（或在私聊中操作）。')
      return true
    }
  }

  const defaults = cfg.groupSyncDefaults ?? {}
  const targetCfg = getGroupSyncTarget(cfg, groupId)

  const mode = forcedMode ?? (
    targetCfg
      ? normalizeSyncMode(targetCfg?.mode, normalizeSyncMode(defaults?.mode, 'incremental'))
      : 'full'
  )

  const urlC = concurrencySpecified
    ? (typeof concurrency === 'number' ? concurrency : 3)
    : (typeof targetCfg?.urlConcurrency === 'number' ? targetCfg.urlConcurrency : (typeof defaults?.urlConcurrency === 'number' ? defaults.urlConcurrency : 3))

  const transferC = concurrencySpecified
    ? (typeof concurrency === 'number' ? concurrency : 3)
    : (typeof targetCfg?.transferConcurrency === 'number' ? targetCfg.transferConcurrency : (typeof defaults?.transferConcurrency === 'number' ? defaults.transferConcurrency : 3))

  const fileTimeout = timeoutSpecified
    ? (typeof timeoutSec === 'number' ? timeoutSec : 600)
    : (typeof targetCfg?.fileTimeoutSec === 'number' ? targetCfg.fileTimeoutSec : (typeof defaults?.fileTimeoutSec === 'number' ? defaults.fileTimeoutSec : 600))

  const retryTimes = typeof targetCfg?.retryTimes === 'number'
    ? targetCfg.retryTimes
    : (typeof defaults?.retryTimes === 'number' ? defaults.retryTimes : 2)

  const retryDelayMs = typeof targetCfg?.retryDelayMs === 'number'
    ? targetCfg.retryDelayMs
    : (typeof defaults?.retryDelayMs === 'number' ? defaults.retryDelayMs : 1500)

  const progressEvery = typeof targetCfg?.progressReportEvery === 'number'
    ? targetCfg.progressReportEvery
    : (typeof defaults?.progressReportEvery === 'number' ? defaults.progressReportEvery : DEFAULT_PROGRESS_REPORT_EVERY)

  const downloadLimitKbps = typeof targetCfg?.downloadLimitKbps === 'number'
    ? targetCfg.downloadLimitKbps
    : (typeof defaults?.downloadLimitKbps === 'number' ? defaults.downloadLimitKbps : 0)

  const uploadLimitKbps = typeof targetCfg?.uploadLimitKbps === 'number'
    ? targetCfg.uploadLimitKbps
    : (typeof defaults?.uploadLimitKbps === 'number' ? defaults.uploadLimitKbps : 0)

  const targetDir = normalizePosixPath(
    toSpecified
      ? (to ?? '')
      : (String(targetCfg?.targetDir ?? '').trim() || path.posix.join(String(cfg.openlistTargetDir ?? '/'), String(groupId)))
  )

  const finalFlat = flatSpecified
    ? Boolean(flat)
    : (typeof targetCfg?.flat === 'boolean' ? targetCfg.flat : Boolean(defaults?.flat ?? false))

  const folderId = parsedFolderId ?? targetCfg?.sourceFolderId
  const maxFiles = typeof parsedMaxFiles === 'number' ? parsedMaxFiles : targetCfg?.maxFiles

  try {
    await syncGroupFilesToOpenListCore({
      bot: e.bot,
      groupId,
      folderId,
      maxFiles,
      flat: Boolean(finalFlat),
      targetDir,
      mode,
      urlConcurrency: Math.max(1, Math.floor(urlC) || 1),
      transferConcurrency: Math.max(1, Math.floor(transferC) || 1),
      fileTimeoutSec: Math.min(MAX_FILE_TIMEOUT_SEC, Math.max(MIN_FILE_TIMEOUT_SEC, Math.floor(fileTimeout) || MIN_FILE_TIMEOUT_SEC)),
      retryTimes: Math.max(0, Math.floor(retryTimes) || 0),
      retryDelayMs: Math.max(0, Math.floor(retryDelayMs) || 0),
      progressReportEvery: Math.max(0, Math.floor(progressEvery) || 0),
      downloadLimitKbps: Math.max(0, Math.floor(downloadLimitKbps) || 0),
      uploadLimitKbps: Math.max(0, Math.floor(uploadLimitKbps) || 0),
      report: (msg) => e.reply(msg),
    })
  } catch (error: any) {
    logger.error(error)
    await e.reply(formatErrorMessage(error))
    return true
  }

  return true
}, {
  priority: 9999,
  log: true,
  name: '同步群文件到OpenList',
  permission: 'all',
})
