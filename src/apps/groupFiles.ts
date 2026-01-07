import fs from 'node:fs'
import path from 'node:path'
import { Readable, Transform } from 'node:stream'
import { setTimeout as sleep } from 'node:timers/promises'
import { pathToFileURL } from 'node:url'
import { dir } from '@/dir'
import { config, time } from '@/utils'
import { karin, logger, hooks } from 'node-karin'

type SyncMode = 'full' | 'incremental'
type OpenListBackupTransport = 'auto' | 'webdav' | 'api'

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
  if (!value || value === '.' || value === '..') return 'unnamed'
  return value
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

const buildOpenListApiBaseUrl = (baseUrl: string) => {
  const normalized = String(baseUrl ?? '').trim().replace(/\/+$/, '')
  if (!normalized) return ''
  return `${normalized}/api`
}

const isSameOriginUrl = (a: string, b: string) => {
  try {
    return new URL(a).origin === new URL(b).origin
  } catch {
    return false
  }
}

const buildOpenListRawUrlAuthHeaders = (params: { rawUrl: string, baseUrl: string, token: string }) => {
  const { rawUrl, baseUrl, token } = params
  if (!token) return undefined
  if (!isSameOriginUrl(rawUrl, baseUrl)) return undefined
  return { Authorization: token }
}

const buildOpenListAuthHeader = (username: string, password: string) => {
  const user = String(username ?? '')
  const pass = String(password ?? '')
  const token = Buffer.from(`${user}:${pass}`).toString('base64')
  return `Basic ${token}`
}

const normalizeOpenListBackupTransport = (value: unknown, fallback: OpenListBackupTransport): OpenListBackupTransport => {
  const v = String(value ?? '').trim().toLowerCase()
  if (v === 'api') return 'api'
  if (v === 'webdav' || v === 'dav') return 'webdav'
  if (v === 'auto') return 'auto'
  return fallback
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

const isRetryableWebDavError = (error: unknown) => {
  const msg = formatErrorMessage(error)
  return /ECONNRESET|ETIMEDOUT|EAI_AGAIN|ENOTFOUND|ECONNREFUSED|UND_ERR|socket hang up/i.test(msg)
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

type OpenListApiResponse<T = unknown> = {
  code?: number
  message?: string
  data?: T
}

const openlistApiReadJson = async <T>(res: Response): Promise<OpenListApiResponse<T>> => {
  try {
    return await res.json() as any
  } catch {
    return {}
  }
}

const openlistApiLogin = async (params: {
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

const openlistApiPost = async <T>(params: {
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

const openlistApiListEntries = async (params: {
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

const openlistApiGetRawUrl = async (params: {
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

const openlistApiPathExists = async (params: {
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

const createOpenListApiDirEnsurer = (baseUrl: string, token: string | undefined, timeoutMs: number) => {
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

const downloadAndUploadByOpenListApiPut = async (params: {
  sourceUrl: string
  sourceHeaders?: Record<string, string>
  targetBaseUrl: string
  targetToken?: string
  targetPath: string
  timeoutMs: number
}) => {
  const { sourceUrl, sourceHeaders, targetBaseUrl, targetToken, targetPath, timeoutMs } = params

  const apiBaseUrl = buildOpenListApiBaseUrl(targetBaseUrl)
  if (!apiBaseUrl) throw new Error('目标 OpenList API 地址不正确，请检查目标 OpenList 地址')

  const controller = new AbortController()
  const timer = setTimeout(() => controller.abort(), timeoutMs)

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
    const contentLength = downloadRes.headers.get('content-length')
    if (contentType) headers['Content-Type'] = contentType
    if (contentLength) headers['Content-Length'] = contentLength

    const sourceStream = Readable.fromWeb(downloadRes.body as any)

    let putRes: Response
    try {
      putRes = await fetch(`${apiBaseUrl}/fs/put`, {
        method: 'PUT',
        headers,
        body: sourceStream as any,
        // @ts-expect-error Node fetch streaming body requires duplex
        duplex: 'half',
        redirect: 'follow',
        signal: controller.signal,
      })
    } catch (error) {
      if (isAbortError(error)) throw new Error('目标端写入超时（请检查对端OpenList连接/权限）')
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
  } finally {
    clearTimeout(timer)
  }
}

const retryAsync = async <T>(
  fn: () => Promise<T>,
  options: {
    retries: number
    delaysMs?: number[]
    isRetryable?: (error: unknown) => boolean
  },
) => {
  const retries = Math.max(0, Math.floor(options.retries) || 0)
  const delaysMs = options.delaysMs?.length ? options.delaysMs : [300, 900, 2_000]
  const isRetryable = options.isRetryable ?? (() => true)

  let lastError: unknown
  for (let attempt = 0; attempt <= retries; attempt++) {
    try {
      return await fn()
    } catch (error) {
      lastError = error
      if (attempt >= retries || !isRetryable(error)) throw error
      const delay = delaysMs[Math.min(attempt, delaysMs.length - 1)]
      await sleep(delay)
    }
  }

  throw lastError
}

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
  sourceHeaders?: Record<string, string>
  targetUrl: string
  auth: string
  timeoutMs: number
  rateLimitBytesPerSec?: number
}) => {
  const { sourceUrl, sourceHeaders, targetUrl, auth, timeoutMs, rateLimitBytesPerSec } = params

  const controller = new AbortController()
  const timer = setTimeout(() => controller.abort(), timeoutMs)

  try {
    let downloadRes: Response
    try {
      downloadRes = await fetch(sourceUrl, { headers: sourceHeaders, redirect: 'follow', signal: controller.signal })
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
  if (!e.isPrivate) return false

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
  if (!e.isPrivate) return false

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

type WebDavEntry = { name: string, isDir: boolean }

const webdavPropfindListEntries = async (params: {
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
    const body = `<?xml version="1.0" encoding="utf-8" ?>\n<d:propfind xmlns:d="DAV:">\n  <d:prop>\n    <d:displayname />\n    <d:resourcetype />\n  </d:prop>\n</d:propfind>`
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

      // 注意：dirPath='/' 时 requestPath 会变成空字符串，endsWith('') 永远为 true，会导致列表被清空
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

const safeHostDirName = (baseUrl: string) => {
  try {
    const u = new URL(String(baseUrl))
    const host = u.hostname || String(baseUrl)
    const port = u.port ? `_${u.port}` : ''
    const raw = `${host}${port}`.replaceAll('.', '_').replaceAll(':', '_')
    return safePathSegment(raw)
  } catch {
    const raw = String(baseUrl ?? '').replace(/^https?:\/\//i, '').replaceAll('.', '_').replaceAll(':', '_')
    return safePathSegment(raw)
  }
}

const webdavHeadExists = async (params: {
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

const copyWebDavToWebDav = async (params: {
  sourceDavBaseUrl: string
  sourceAuth?: string
  sourcePath: string
  targetDavBaseUrl: string
  targetAuth: string
  targetPath: string
  timeoutMs: number
}) => {
  const { sourceDavBaseUrl, sourceAuth, sourcePath, targetDavBaseUrl, targetAuth, targetPath, timeoutMs } = params
  const controller = new AbortController()
  const timer = setTimeout(() => controller.abort(), timeoutMs)

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
    const contentLength = downloadRes.headers.get('content-length')
    if (contentType) headers['Content-Type'] = contentType
    if (contentLength) headers['Content-Length'] = contentLength

    const sourceStream = Readable.fromWeb(downloadRes.body as any)

    let putRes: Response
    try {
      putRes = await fetch(targetUrl, {
        method: 'PUT',
        headers,
        body: sourceStream as any,
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
  } finally {
    clearTimeout(timer)
  }
}

const parseBackupOpenListArgs = (text: string) => {
  const raw = text.trim()
  const tokens = raw ? raw.split(/\s+/).filter(Boolean) : []
  const help = /(^|\s)(--help|-h|help|\?)(\s|$)/i.test(raw)

  const first = tokens[0]
  const sourceBaseUrl = first && /^https?:\/\//i.test(first) ? first : undefined
  const restRaw = sourceBaseUrl ? raw.slice(first.length).trim() : raw

  const srcMatch = restRaw.match(/--src\s+(\S+)/i) ?? restRaw.match(/(^|\s)src=(\S+)/i)
  const srcDir = srcMatch ? srcMatch[srcMatch.length - 1] : undefined
  const srcSpecified = Boolean(srcMatch)

  const toMatch = restRaw.match(/--to\s+(\S+)/i) ?? restRaw.match(/(^|\s)to=(\S+)/i)
  const toDir = toMatch ? toMatch[toMatch.length - 1] : undefined
  const toSpecified = Boolean(toMatch)

  const maxMatch = restRaw.match(/--max\s+(\d+)/i) ?? restRaw.match(/(^|\s)max=(\d+)/i)
  const maxFiles = maxMatch ? Number(maxMatch[maxMatch.length - 1]) : undefined

  const concurrencyMatch = restRaw.match(/--concurrency\s+(\d+)/i) ?? restRaw.match(/(^|\s)concurrency=(\d+)/i)
  const concurrency = concurrencyMatch ? Number(concurrencyMatch[concurrencyMatch.length - 1]) : undefined
  const concurrencySpecified = Boolean(concurrencyMatch)

  const timeoutMatch = restRaw.match(/--timeout\s+(\d+)/i) ?? restRaw.match(/(^|\s)timeout=(\d+)/i)
  const timeoutSec = timeoutMatch ? Number(timeoutMatch[timeoutMatch.length - 1]) : undefined
  const timeoutSpecified = Boolean(timeoutMatch)

  const scanMatch = restRaw.match(/--scan(?:-concurrency)?\s+(\d+)/i)
    ?? restRaw.match(/(^|\s)scan=(\d+)/i)
    ?? restRaw.match(/(^|\s)scanConcurrency=(\d+)/i)
    ?? restRaw.match(/(^|\s)scan_concurrency=(\d+)/i)
  const scanConcurrency = scanMatch ? Number(scanMatch[scanMatch.length - 1]) : undefined
  const scanConcurrencySpecified = Boolean(scanMatch)

  const perPageMatch = restRaw.match(/--per-page\s+(\d+)/i)
    ?? restRaw.match(/--perpage\s+(\d+)/i)
    ?? restRaw.match(/--page-size\s+(\d+)/i)
    ?? restRaw.match(/(^|\s)per[_-]?page=(\d+)/i)
    ?? restRaw.match(/(^|\s)pageSize=(\d+)/i)
    ?? restRaw.match(/(^|\s)per_page=(\d+)/i)
  const perPage = perPageMatch ? Number(perPageMatch[perPageMatch.length - 1]) : undefined
  const perPageSpecified = Boolean(perPageMatch)

  const modeFull = /(^|\s)(--full|full)(\s|$)/i.test(restRaw)
  const modeInc = /(^|\s)(--inc|--incremental|inc|incremental)(\s|$)/i.test(restRaw)
  const mode: SyncMode | undefined = modeFull ? 'full' : modeInc ? 'incremental' : undefined

  const transportApi = /(^|\s)(--api)(\s|$)/i.test(restRaw)
  const transportWebDav = /(^|\s)(--webdav|--dav)(\s|$)/i.test(restRaw)
  const transportAuto = /(^|\s)(--auto)(\s|$)/i.test(restRaw)
  const transportSpecified = transportApi || transportWebDav || transportAuto
  const transport: OpenListBackupTransport | undefined = transportApi ? 'api' : transportWebDav ? 'webdav' : transportAuto ? 'auto' : undefined

  return {
    sourceBaseUrl,
    srcDir,
    srcSpecified,
    toDir,
    toSpecified,
    maxFiles,
    concurrency,
    concurrencySpecified,
    timeoutSec,
    timeoutSpecified,
    scanConcurrency,
    scanConcurrencySpecified,
    perPage,
    perPageSpecified,
    mode,
    transport,
    transportSpecified,
    help,
  }
}

const openListToOpenListHelpText = [
  'OpenList -> OpenList 备份用法：',
  '- 私聊：#备份oplist [源OpenList地址] [参数]',
  '- 示例：#备份oplist https://pan.example.com',
  '- #备份oplist https://pan.example.com --src / --to /backup --inc',
  '- #备份oplist https://pan.example.com --api',
  '- #备份oplist https://pan.example.com --webdav',
  '- #备份oplist https://pan.example.com --full --concurrency 3 --timeout 600',
  '- #备份oplist https://pan.example.com --scan 30 --per-page 2000',
  '提示：备份目的端使用 openlistBaseUrl/openlistUsername/openlistPassword（与群文件备份/同步共用）。',
  '提示：传输默认 auto（下载走 API，上传走 WebDAV；失败自动回退到可用方式）。',
  '提示：--scan 只影响“获取文件列表”的并发；--per-page 只影响源端列表走 API 时的每页数量（值越大越快，但可能更吃内存/更容易被限流）。',
  '说明：会在目的端目标目录下创建子目录（源 OpenList 域名，"." 替换为 "_"），并同步其下所有文件。',
].join('\n')

const activeOpenListBackup = new Set<string>()

export const backupOpenListToOpenList = karin.command(/^#?备份oplist(.*)$/i, async (e) => {
  if (!e.isPrivate) return false

  const argsText = e.msg.replace(/^#?备份oplist/i, '')
  const {
    sourceBaseUrl: sourceBaseUrlArg,
    srcDir,
    srcSpecified,
    toDir,
    toSpecified,
    maxFiles,
    concurrency,
    concurrencySpecified,
    timeoutSec,
    timeoutSpecified,
    scanConcurrency: scanConcurrencyArg,
    scanConcurrencySpecified,
    perPage: perPageArg,
    perPageSpecified,
    mode: forcedMode,
    transport: forcedTransport,
    transportSpecified,
    help,
  } = parseBackupOpenListArgs(argsText)
  if (help) {
    await e.reply(openListToOpenListHelpText)
    return true
  }

  const cfg = config()

  const sourceBaseUrl = String(sourceBaseUrlArg ?? '').trim()

  const targetBaseUrl = String(cfg.openlistBaseUrl ?? '').trim()
  const targetUsername = String(cfg.openlistUsername ?? '').trim()
  const targetPassword = String(cfg.openlistPassword ?? '').trim()

  if (!sourceBaseUrl) {
    await e.reply(openListToOpenListHelpText)
    return true
  }

  if (!targetBaseUrl || !targetUsername || !targetPassword) {
    await e.reply('请先配置目的端 OpenList 信息（openlistBaseUrl/openlistUsername/openlistPassword）')
    return true
  }

  const srcDavBaseUrl = buildOpenListDavBaseUrl(sourceBaseUrl)
  const targetDavBaseUrl = buildOpenListDavBaseUrl(targetBaseUrl)
  if (!srcDavBaseUrl) {
    await e.reply('源 OpenList 地址不正确，请检查命令参数')
    return true
  }
  if (!targetDavBaseUrl) {
    await e.reply('目的端 OpenList 地址不正确，请检查 openlistBaseUrl')
    return true
  }

  const srcAuth = ''
  const targetAuth = buildOpenListAuthHeader(targetUsername, targetPassword)

  const transport = forcedTransport ?? 'auto'

  const mode = forcedMode ?? 'incremental'
  const copyConcurrency = concurrencySpecified
    ? (typeof concurrency === 'number' ? concurrency : 3)
    : 3

  const fileTimeout = timeoutSpecified
    ? (typeof timeoutSec === 'number' ? timeoutSec : 600)
    : 600

  const normalizedSrcDir = normalizePosixPath(srcSpecified ? (srcDir ?? '') : '/')
  const normalizedTargetBaseDir = normalizePosixPath(
    toSpecified
      ? (toDir ?? '')
      : (String(cfg.openlistTargetDir ?? '/').trim() || '/')
  )
  const targetRoot = normalizePosixPath(path.posix.join(normalizedTargetBaseDir, safeHostDirName(sourceBaseUrl)))

  const lockKey = `${sourceBaseUrl} -> ${targetBaseUrl}`
  if (activeOpenListBackup.has(lockKey)) {
    await e.reply('备份任务正在进行中，请稍后再试。')
    return true
  }

  let ticker: ReturnType<typeof setInterval> | undefined
  let tickChain: Promise<unknown> = Promise.resolve()

  activeOpenListBackup.add(lockKey)
  try {
    await e.reply([
      '开始备份 OpenList...',
      `源：${sourceBaseUrl}`,
      `源目录：${normalizedSrcDir}`,
      `目的端：${targetBaseUrl}`,
      `目的端目录：${targetRoot}`,
      `模式：${mode}`,
      `传输：${transport}`,
    ].join('\n'))

    const timeoutMs = Math.min(MAX_FILE_TIMEOUT_SEC, Math.max(MIN_FILE_TIMEOUT_SEC, Math.floor(fileTimeout) || MIN_FILE_TIMEOUT_SEC)) * 1000
    const listTimeoutMs = Math.min(15_000, timeoutMs)
    const listPerPage = perPageSpecified
      ? Math.max(1, Math.min(5000, Math.floor(perPageArg || 0) || 1000))
      : 1000

    const allowAutoFallback = transport === 'auto'

    // 默认策略：源端下载走 API，目标端上传走 WebDAV；允许通过 --api/--webdav 强制覆盖
    let sourceTransport: Exclude<OpenListBackupTransport, 'auto'> = transport === 'webdav' ? 'webdav' : 'api'
    let targetTransport: Exclude<OpenListBackupTransport, 'auto'> = transport === 'api' ? 'api' : 'webdav'

    let sourceToken: string | undefined
    let targetToken: string | undefined
    let targetTokenPromise: Promise<string> | undefined
    const getSourceToken = async () => {
      if (typeof sourceToken === 'string') return sourceToken
      // 源站点允许公开访问时，不需要登录；token 为空即 guest
      sourceToken = ''
      return sourceToken
    }
    const getTargetToken = async () => {
      if (typeof targetToken === 'string') return targetToken
      if (targetTokenPromise) return await targetTokenPromise
      targetTokenPromise = openlistApiLogin({
        baseUrl: targetBaseUrl,
        username: targetUsername,
        password: targetPassword,
        timeoutMs: listTimeoutMs,
      }).then((token) => {
        targetToken = token
        return token
      }).catch((error) => {
        targetTokenPromise = undefined
        throw error
      })
      return await targetTokenPromise
    }

    const targetDirEnsurerWebDav = createWebDavDirEnsurer(targetDavBaseUrl, targetAuth, timeoutMs)
    let targetDirEnsurerApi: ReturnType<typeof createOpenListApiDirEnsurer> | undefined
    const getTargetDirEnsurerApi = async () => {
      if (targetDirEnsurerApi) return targetDirEnsurerApi
      const token = await getTargetToken()
      targetDirEnsurerApi = createOpenListApiDirEnsurer(targetBaseUrl, token, timeoutMs)
      return targetDirEnsurerApi
    }

    const ensureTargetDir = async (dirPath: string) => {
      let webdavError: unknown
      if (targetTransport === 'webdav') {
        try {
          await retryAsync(() => targetDirEnsurerWebDav.ensureDir(dirPath), { retries: 2, isRetryable: isRetryableWebDavError })
          return
        } catch (error) {
          webdavError = error
          if (!allowAutoFallback) throw error
          targetTransport = 'api'
        }
      }

      const ensurer = await getTargetDirEnsurerApi()
      try {
        await retryAsync(() => ensurer.ensureDir(dirPath), { retries: 2, isRetryable: isRetryableWebDavError })
      } catch (error) {
        if (webdavError) {
          throw new Error([
            `目标端 WebDAV 不可用，已回退到 OpenList API，但仍失败。`,
            `目标：${targetBaseUrl}`,
            `目录：${dirPath}`,
            `WebDAV: ${formatErrorMessage(webdavError)}`,
            `API: ${formatErrorMessage(error)}`,
            '请检查 openlistUsername/openlistPassword 是否正确，或在目的端启用 WebDAV 管理/放行 MKCOL/PUT。',
          ].join('\n'))
        }
        throw error
      }
    }

    const files: string[] = []
    const visited = new Set<string>()
    const stack: string[] = [normalizedSrcDir]
    let scannedDirs = 0

    const max = typeof maxFiles === 'number' && Number.isFinite(maxFiles) && maxFiles > 0 ? Math.floor(maxFiles) : 0

    let ok = 0
    let skipped = 0
    let fail = 0

    let phase: 'scan' | 'copy' = 'scan'
    const startedAt = Date.now()
    ticker = setInterval(() => {
      const elapsed = Math.max(0, Math.floor((Date.now() - startedAt) / 1000))
      const msg = phase === 'scan'
        ? `备份进行中（${elapsed}s）\n阶段：扫描\n已扫描目录：${scannedDirs}\n已发现文件：${files.length}`
        : `备份进行中（${elapsed}s）\n阶段：复制\n进度：${ok + skipped + fail}/${files.length}\n成功 ${ok} 跳过 ${skipped} 失败 ${fail}`
      tickChain = tickChain.then(() => e.reply(msg)).catch(() => undefined)
    }, 60_000)

    const maxScanConcurrency = scanConcurrencySpecified
      ? Math.max(1, Math.min(200, Math.floor(scanConcurrencyArg || 0) || 1))
      : 50
    let scanConcurrency = scanConcurrencySpecified
      ? maxScanConcurrency
      : Math.max(1, Math.min(maxScanConcurrency, Math.max(8, (Math.floor(copyConcurrency) || 1) * 3)))
    const scanResults: Array<{ ok: boolean, ms: number, reason?: string }> = []
    const pushScanResult = (ok: boolean, ms: number, reason?: string) => {
      scanResults.push({ ok, ms, reason })
      if (scanResults.length > 20) scanResults.shift()

      if (scanResults.length < 10) return
      if (scanResults.length % 5 !== 0) return

      const failCount = scanResults.filter(r => !r.ok).length
      const failRate = failCount / scanResults.length
      const avgMs = scanResults.reduce((acc, r) => acc + r.ms, 0) / scanResults.length

      const hasTimeout = scanResults.some(r => (r.reason || '').includes('超时'))
      if (hasTimeout || failRate >= 0.2) {
        if (scanConcurrency > 1) scanConcurrency -= 1
        return
      }

      if (failCount === 0 && scanConcurrency < maxScanConcurrency) {
        if (avgMs < 60_000 || scanResults.length === 20) scanConcurrency += 1
      }
    }
    let stopScan = false
    let scanError: unknown

    const isRetryableListError = (error: unknown) => {
      const msg = formatErrorMessage(error)
      return /超时|timeout|ECONNRESET|ETIMEDOUT|EAI_AGAIN|ENOTFOUND|ECONNREFUSED|UND_ERR|socket hang up|\b429\b|\b5\d\d\b/i.test(msg)
    }

    const listDirEntries = async (dirPath: string) => {
      return await retryAsync(async () => {
        return sourceTransport === 'webdav'
          ? await webdavPropfindListEntries({
            davBaseUrl: srcDavBaseUrl,
            auth: srcAuth,
            dirPath,
            timeoutMs: listTimeoutMs,
          })
          : await openlistApiListEntries({
            baseUrl: sourceBaseUrl,
            token: await getSourceToken(),
            dirPath,
            timeoutMs: listTimeoutMs,
            perPage: listPerPage,
          })
      }, { retries: 2, isRetryable: isRetryableListError })
    }

    let scanning = 0
    let resolveScanDone: (() => void) | undefined
    const scanDone = new Promise<void>((resolve) => { resolveScanDone = resolve })

    const scheduleScan = () => {
      while (!stopScan && scanning < scanConcurrency) {
        const rawPath = stack.pop()
        if (!rawPath) break

        const current = normalizePosixPath(rawPath)
        if (visited.has(current)) continue
        visited.add(current)
        scannedDirs++
        scanning++

        ;(async () => {
          const start = Date.now()
          try {
            const entries = await listDirEntries(current)
            pushScanResult(true, Date.now() - start)

            for (const entry of entries) {
              if (stopScan) break
              const childPath = normalizePosixPath(path.posix.join(current, entry.name))
              if (entry.isDir) {
                if (!visited.has(childPath)) stack.push(childPath)
                continue
              }
              files.push(childPath)
              if (max > 0 && files.length >= max) {
                stopScan = true
                break
              }
            }
          } catch (error) {
            pushScanResult(false, Date.now() - start, formatErrorMessage(error))
            throw error
          }

          // 如果动态并发上调，尽快填满队列
          scheduleScan()
        })().catch((error) => {
          scanError = error
          stopScan = true
        }).finally(() => {
          scanning--
          scheduleScan()
          if ((stopScan || stack.length === 0) && scanning === 0) resolveScanDone?.()
        })
      }

      if ((stopScan || stack.length === 0) && scanning === 0) resolveScanDone?.()
    }

    scheduleScan()
    await scanDone
    if (scanError) throw scanError

    if (!files.length) {
      await e.reply('未发现可备份的文件（源目录为空或无法访问）。')
      return true
    }

    phase = 'copy'
    await ensureTargetDir(targetRoot)
    await e.reply(`已发现 ${files.length} 个文件，开始复制...`)

    const limit = Math.max(1, Math.min(MAX_TRANSFER_CONCURRENCY, Math.floor(copyConcurrency) || 1))

    await runWithConcurrency(files, limit, async (sourcePath, index) => {
      const rel = path.posix.relative(normalizedSrcDir, sourcePath)
      const relParts = rel.split('/').filter(Boolean).map(safePathSegment)
      const targetPath = normalizePosixPath(path.posix.join(targetRoot, ...relParts))
      const targetDirPath = normalizePosixPath(path.posix.dirname(targetPath))

      if (mode === 'incremental') {
        const exists = targetTransport === 'webdav'
          ? await webdavHeadExists({
            davBaseUrl: targetDavBaseUrl,
            auth: targetAuth,
            filePath: targetPath,
            timeoutMs: Math.max(5_000, Math.floor(timeoutMs / 5)),
          })
          : await openlistApiPathExists({
            baseUrl: targetBaseUrl,
            token: await getTargetToken(),
            path: targetPath,
            timeoutMs: Math.max(5_000, Math.floor(timeoutMs / 5)),
          })
        if (exists) {
          skipped++
          return
        }
      }

      try {
        await ensureTargetDir(targetDirPath)

        if (sourceTransport === 'webdav' && targetTransport === 'webdav') {
          await copyWebDavToWebDav({
            sourceDavBaseUrl: srcDavBaseUrl,
            sourceAuth: srcAuth,
            sourcePath,
            targetDavBaseUrl,
            targetAuth,
            targetPath,
            timeoutMs,
          })
        } else if (sourceTransport === 'webdav' && targetTransport === 'api') {
          await downloadAndUploadByOpenListApiPut({
            sourceUrl: `${srcDavBaseUrl}${encodePathForUrl(sourcePath)}`,
            sourceHeaders: srcAuth ? { Authorization: srcAuth } : undefined,
            targetBaseUrl: targetBaseUrl,
            targetToken: await getTargetToken(),
            targetPath,
            timeoutMs,
          })
        } else if (sourceTransport === 'api' && targetTransport === 'webdav') {
          const token = await getSourceToken()
          const rawUrl = await openlistApiGetRawUrl({
            baseUrl: sourceBaseUrl,
            token,
            filePath: sourcePath,
            timeoutMs: Math.max(5_000, Math.floor(timeoutMs / 5)),
          })
          const sourceHeaders = buildOpenListRawUrlAuthHeaders({ rawUrl, baseUrl: sourceBaseUrl, token })
          await downloadAndUploadByWebDav({
            sourceUrl: rawUrl,
            sourceHeaders,
            targetUrl: `${targetDavBaseUrl}${encodePathForUrl(targetPath)}`,
            auth: targetAuth,
            timeoutMs,
          })
        } else {
          const token = await getSourceToken()
          const rawUrl = await openlistApiGetRawUrl({
            baseUrl: sourceBaseUrl,
            token,
            filePath: sourcePath,
            timeoutMs: Math.max(5_000, Math.floor(timeoutMs / 5)),
          })
          const sourceHeaders = buildOpenListRawUrlAuthHeaders({ rawUrl, baseUrl: sourceBaseUrl, token })
          await downloadAndUploadByOpenListApiPut({
            sourceUrl: rawUrl,
            sourceHeaders,
            targetBaseUrl: targetBaseUrl,
            targetToken: await getTargetToken(),
            targetPath,
            timeoutMs,
          })
        }
        ok++
      } catch (error) {
        fail++
        logger.error(error)
      }
    })

    await e.reply(`备份完成：成功 ${ok}，跳过 ${skipped}，失败 ${fail}`)
    return true
  } catch (error: any) {
    logger.error(error)
    await e.reply(formatErrorMessage(error))
    return true
  } finally {
    if (ticker) clearInterval(ticker)
    await tickChain.catch(() => undefined)
    activeOpenListBackup.delete(lockKey)
  }
}, {
  priority: 9999,
  log: true,
  name: 'OpenList备份到对端OpenList',
  permission: 'all',
})

const activeGroupFileUploadBackups = new Map<string, Promise<void>>()

const enqueueGroupFileUploadBackup = (groupId: string, task: () => Promise<void>) => {
  const key = String(groupId)
  const previous = activeGroupFileUploadBackups.get(key) ?? Promise.resolve()
  const nextTask = previous.catch(() => undefined).then(task)
  activeGroupFileUploadBackups.set(key, nextTask)
  nextTask.finally(() => {
    if (activeGroupFileUploadBackups.get(key) === nextTask) activeGroupFileUploadBackups.delete(key)
  })
}

hooks.eventCall.notice((event: any, _plugin: any, next) => {
  try {
    if (!event || event.subEvent !== 'groupFileUploaded') return

    const groupId = String(event.groupId ?? '')
    if (!groupId) return

    const cfg = config()
    const targetCfg = getGroupSyncTarget(cfg, groupId)
    if (!targetCfg || targetCfg.enabled === false || targetCfg.uploadBackup !== true) return

    const file = event.content as any
    const fid = String(file?.fid ?? '').trim()
    const name = String(file?.name ?? '').trim()
    const size = typeof file?.size === 'number' && Number.isFinite(file.size) ? Math.max(0, Math.floor(file.size)) : undefined
    const getUrl = typeof file?.url === 'function' ? (file.url as () => Promise<string>) : null
    if (!fid || !name || !getUrl) return

    enqueueGroupFileUploadBackup(groupId, async () => {
      const baseUrl = String(cfg.openlistBaseUrl ?? '').trim()
      const username = String(cfg.openlistUsername ?? '').trim()
      const password = String(cfg.openlistPassword ?? '').trim()
      const defaultTargetDir = String(cfg.openlistTargetDir ?? '/').trim()

      if (!baseUrl || !username || !password) {
        logger.error(`[群上传备份][${groupId}] 缺少 OpenList 配置（openlistBaseUrl/openlistUsername/openlistPassword）`)
        return
      }

      const davBaseUrl = buildOpenListDavBaseUrl(baseUrl)
      if (!davBaseUrl) {
        logger.error(`[群上传备份][${groupId}] OpenList 地址不正确，请检查 openlistBaseUrl`)
        return
      }

      const defaults = cfg.groupSyncDefaults ?? {}

      const targetDir = normalizePosixPath(
        String(targetCfg?.targetDir ?? '').trim() || path.posix.join(String(defaultTargetDir || '/'), String(groupId)),
      )

      const item: ExportedGroupFile = {
        path: name,
        fileId: fid,
        name,
        size,
      }

      const remotePath = buildRemotePathForItem(item, targetDir, true)
      const remoteDir = normalizePosixPath(path.posix.dirname(remotePath))

      const state = readGroupSyncState(groupId)
      if (state.files?.[remotePath]?.fileId && String(state.files[remotePath].fileId) === fid) {
        logger.info(`[群上传备份][${groupId}] 已备份，跳过: ${name} (${fid})`)
        return
      }

      const auth = buildOpenListAuthHeader(username, password)

      const fileTimeoutSec = typeof targetCfg?.fileTimeoutSec === 'number'
        ? targetCfg.fileTimeoutSec
        : (typeof defaults?.fileTimeoutSec === 'number' ? defaults.fileTimeoutSec : 600)

      const safeFileTimeoutSec = Math.min(
        MAX_FILE_TIMEOUT_SEC,
        Math.max(MIN_FILE_TIMEOUT_SEC, Math.floor(fileTimeoutSec || 0)),
      )

      const rateDown = Math.max(0, Math.floor(typeof targetCfg?.downloadLimitKbps === 'number' ? targetCfg.downloadLimitKbps : (typeof defaults?.downloadLimitKbps === 'number' ? defaults.downloadLimitKbps : 0)))
      const rateUp = Math.max(0, Math.floor(typeof targetCfg?.uploadLimitKbps === 'number' ? targetCfg.uploadLimitKbps : (typeof defaults?.uploadLimitKbps === 'number' ? defaults.uploadLimitKbps : 0)))
      const effectiveRateLimitBytesPerSec = (() => {
        const a = rateDown > 0 ? rateDown * 1024 : 0
        const b = rateUp > 0 ? rateUp * 1024 : 0
        if (a > 0 && b > 0) return Math.min(a, b)
        return a > 0 ? a : b > 0 ? b : 0
      })()

      const retryTimes = typeof targetCfg?.retryTimes === 'number'
        ? targetCfg.retryTimes
        : (typeof defaults?.retryTimes === 'number' ? defaults.retryTimes : 2)

      const retryDelayMs = typeof targetCfg?.retryDelayMs === 'number'
        ? targetCfg.retryDelayMs
        : (typeof defaults?.retryDelayMs === 'number' ? defaults.retryDelayMs : 1500)

      const transferTimeoutMs = safeFileTimeoutSec * 1000
      const webdavTimeoutMs = 15_000
      const dirEnsurer = createWebDavDirEnsurer(davBaseUrl, auth, webdavTimeoutMs)
      const targetUrl = `${davBaseUrl}${encodePathForUrl(remotePath)}`

      logger.info(`[群上传备份][${groupId}] 开始: ${name} (${fid}) -> ${remotePath}`)

      let lastError: unknown
      const attempts = Math.max(0, Math.floor(retryTimes) || 0) + 1
      for (let attempt = 1; attempt <= attempts; attempt++) {
        try {
          await dirEnsurer.ensureDir(remoteDir)

          const url = await getUrl()
          if (!url) throw new Error('获取文件URL失败')

          await downloadAndUploadByWebDav({
            sourceUrl: url,
            targetUrl,
            auth,
            timeoutMs: transferTimeoutMs,
            rateLimitBytesPerSec: effectiveRateLimitBytesPerSec || undefined,
          })

          state.files[remotePath] = {
            fileId: fid,
            size,
            syncedAt: Date.now(),
          }
          state.lastSyncAt = Date.now()
          writeGroupSyncState(groupId, state)

          logger.info(`[群上传备份][${groupId}] 完成: ${name} -> ${remotePath}`)
          lastError = undefined
          break
        } catch (error) {
          lastError = error
          logger.error(error)

          if (attempt < attempts) {
            const delayMs = Math.max(0, Math.floor(retryDelayMs) || 0) * Math.pow(2, attempt - 1)
            if (delayMs > 0) await sleep(delayMs)
          }
        }
      }

      if (lastError) {
        logger.error(`[群上传备份][${groupId}] 失败: ${name} (${fid}) - ${formatErrorMessage(lastError)}`)
      }
    })
  } catch (error) {
    logger.error(error)
  } finally {
    next()
  }
})
