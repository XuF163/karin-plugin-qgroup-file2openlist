import path from 'node:path'
import { config } from '@/utils'
import { logger } from 'node-karin'
import { runWithConcurrency } from '@/model/shared/concurrency'
import { retryAsync } from '@/model/shared/retry'
import { formatErrorMessage } from '@/model/shared/errors'
import { encodePathForUrl, normalizePosixPath, safePathSegment } from '@/model/shared/path'
import type { OpenListBackupTransport, SyncMode } from '@/model/groupFiles/types'
import { buildOpenListAuthHeader, buildOpenListDavBaseUrl, buildOpenListRawUrlAuthHeaders } from './url'
import {
  createOpenListApiDirEnsurer,
  downloadAndUploadByOpenListApiPut,
  openlistApiGetRawUrl,
  openlistApiListEntries,
  openlistApiLogin,
  openlistApiPathExists,
} from './api'
import {
  copyWebDavToWebDav,
  createWebDavDirEnsurer,
  downloadAndUploadByWebDav,
  isRetryableWebDavError,
  webdavHeadExists,
  webdavPropfindListEntries,
} from './webdav'

const MAX_FILE_TIMEOUT_SEC = 3000
const MIN_FILE_TIMEOUT_SEC = 10

export type OpenListBackupConcreteTransport = Exclude<OpenListBackupTransport, 'auto'>
export type OpenListBackupFile = { sourcePath: string, targetPath: string }

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

const normalizeOpenListBackupTransport = (value: unknown, fallback: OpenListBackupTransport): OpenListBackupTransport => {
  const v = String(value ?? '').trim().toLowerCase()
  if (v === 'api') return 'api'
  if (v === 'webdav' || v === 'dav') return 'webdav'
  if (v === 'auto') return 'auto'
  return fallback
}

const buildTargetPath = (targetRoot: string, sourcePath: string) => {
  const rel = String(sourcePath ?? '').replace(/^\/+/, '')
  return normalizePosixPath(path.posix.join(targetRoot, rel))
}

export const scanOpenListFiles = async (params: {
  sourceTransport: OpenListBackupConcreteTransport
  sourceBaseUrl: string
  sourceToken: string
  sourceAuth?: string
  sourceDavBaseUrl?: string
  srcDir: string
  targetRoot: string
  timeoutMs: number
  perPage: number
  scanConcurrency: number
  onProgress?: (progress: { scannedDirs: number, files: number }) => void
}) => {
  const {
    sourceTransport,
    sourceBaseUrl,
    sourceToken,
    sourceAuth,
    sourceDavBaseUrl,
    srcDir,
    targetRoot,
    timeoutMs,
    perPage,
    scanConcurrency,
    onProgress,
  } = params

  const normalizedSrcDir = normalizePosixPath(String(srcDir ?? '/'))
  const normalizedTargetRoot = normalizePosixPath(String(targetRoot ?? '/'))
  const scanConcurrencyMax = Math.max(1, Math.min(200, Math.floor(scanConcurrency) || 20))

  const files: OpenListBackupFile[] = []
  let scannedDirs = 0

  const listDir = async (dirPath: string) => {
    if (sourceTransport === 'webdav') {
      if (!sourceDavBaseUrl) throw new Error('源端 WebDAV 地址不正确')
      return await webdavPropfindListEntries({
        davBaseUrl: sourceDavBaseUrl,
        auth: sourceAuth,
        dirPath,
        timeoutMs,
      })
    }

    return await openlistApiListEntries({
      baseUrl: sourceBaseUrl,
      token: sourceToken,
      dirPath,
      timeoutMs,
      perPage,
    })
  }

  const reportProgress = () => {
    if (!onProgress) return
    onProgress({ scannedDirs, files: files.length })
  }

  // 批量扫描目录（简单实现：每轮拿一批目录并发扫描）
  let pendingDirs: string[] = [normalizedSrcDir]
  while (pendingDirs.length) {
    const batch = pendingDirs.splice(0, scanConcurrencyMax)
    await runWithConcurrency(batch, scanConcurrencyMax, async (dirPath) => {
      scannedDirs++
      const entries = await listDir(dirPath)
      for (const it of entries) {
        const sourcePath = normalizePosixPath(path.posix.join(dirPath, it.name))
        const targetPath = buildTargetPath(normalizedTargetRoot, sourcePath)
        if (it.isDir) pendingDirs.push(sourcePath)
        else files.push({ sourcePath, targetPath })
      }
      reportProgress()
    })
  }

  reportProgress()
  return { files, scannedDirs }
}

export const existsOnTarget = async (params: {
  targetTransport: OpenListBackupConcreteTransport
  targetDavBaseUrl: string
  targetAuth: string
  targetBaseUrl: string
  getTargetToken: () => Promise<string>
  targetPath: string
  timeoutMs: number
}) => {
  const { targetTransport, targetDavBaseUrl, targetAuth, targetBaseUrl, getTargetToken, targetPath, timeoutMs } = params

  if (targetTransport === 'webdav') {
    return await webdavHeadExists({
      davBaseUrl: targetDavBaseUrl,
      auth: targetAuth,
      filePath: targetPath,
      timeoutMs,
    })
  }

  return await openlistApiPathExists({
    baseUrl: targetBaseUrl,
    token: await getTargetToken(),
    path: targetPath,
    timeoutMs,
  })
}

export const ensureTargetDir = async (params: {
  dirPath: string
  targetTransport: OpenListBackupConcreteTransport
  allowAutoFallback: boolean
  targetBaseUrl: string
  webdavEnsurer: { ensureDir: (dirPath: string) => Promise<void> }
  getApiEnsurer: () => Promise<{ ensureDir: (dirPath: string) => Promise<void> }>
}) => {
  const { dirPath, allowAutoFallback, targetBaseUrl, webdavEnsurer, getApiEnsurer } = params
  let { targetTransport } = params

  let webdavError: unknown
  if (targetTransport === 'webdav') {
    try {
      await retryAsync(() => webdavEnsurer.ensureDir(dirPath), { retries: 2, isRetryable: isRetryableWebDavError })
      return targetTransport
    } catch (error) {
      webdavError = error
      if (!allowAutoFallback) throw error
      targetTransport = 'api'
    }
  }

  const ensurer = await getApiEnsurer()
  try {
    await retryAsync(() => ensurer.ensureDir(dirPath), { retries: 2, isRetryable: isRetryableWebDavError })
    return targetTransport
  } catch (error) {
    if (webdavError) {
      throw new Error([
        '目标端 WebDAV 不可用，已回退到 OpenList API，但仍失败。',
        `目标：${targetBaseUrl}`,
        `目录：${dirPath}`,
        `WebDAV: ${formatErrorMessage(webdavError)}`,
        `API: ${formatErrorMessage(error)}`,
        '请检查 openlistUsername/openlistPassword 或目标端 WebDAV 权限。',
      ].join('\n'))
    }
    throw error
  }
}

export const copyOpenListFile = async (params: {
  sourceTransport: OpenListBackupConcreteTransport
  targetTransport: OpenListBackupConcreteTransport
  sourceBaseUrl: string
  sourceDavBaseUrl?: string
  sourceAuth?: string
  targetBaseUrl: string
  targetDavBaseUrl: string
  targetAuth: string
  sourcePath: string
  targetPath: string
  getSourceToken: () => Promise<string>
  getTargetToken: () => Promise<string>
  timeoutMs: number
}) => {
  const {
    sourceTransport,
    targetTransport,
    sourceBaseUrl,
    sourceDavBaseUrl,
    sourceAuth,
    targetBaseUrl,
    targetDavBaseUrl,
    targetAuth,
    sourcePath,
    targetPath,
    getSourceToken,
    getTargetToken,
    timeoutMs,
  } = params

  if (sourceTransport === 'webdav' && targetTransport === 'webdav') {
    if (!sourceDavBaseUrl) throw new Error('源端 WebDAV 地址不正确')
    await copyWebDavToWebDav({
      sourceDavBaseUrl,
      sourceAuth,
      sourcePath,
      targetDavBaseUrl,
      targetAuth,
      targetPath,
      timeoutMs,
    })
    return
  }

  if (sourceTransport === 'webdav' && targetTransport === 'api') {
    if (!sourceDavBaseUrl) throw new Error('源端 WebDAV 地址不正确')
    const sourceUrl = `${sourceDavBaseUrl}${encodePathForUrl(sourcePath)}`
    await downloadAndUploadByOpenListApiPut({
      sourceUrl,
      sourceHeaders: sourceAuth ? { Authorization: sourceAuth } : undefined,
      targetBaseUrl,
      targetToken: await getTargetToken(),
      targetPath,
      timeoutMs,
    })
    return
  }

  const token = await getSourceToken()
  const rawUrl = await openlistApiGetRawUrl({
    baseUrl: sourceBaseUrl,
    token,
    filePath: sourcePath,
    timeoutMs: Math.max(5_000, Math.floor(timeoutMs / 5)),
  })
  const sourceHeaders = buildOpenListRawUrlAuthHeaders({ rawUrl, baseUrl: sourceBaseUrl, token })

  if (targetTransport === 'webdav') {
    await downloadAndUploadByWebDav({
      sourceUrl: rawUrl,
      sourceHeaders,
      targetUrl: `${targetDavBaseUrl}${encodePathForUrl(targetPath)}`,
      auth: targetAuth,
      timeoutMs,
    })
    return
  }

  await downloadAndUploadByOpenListApiPut({
    sourceUrl: rawUrl,
    sourceHeaders,
    targetBaseUrl,
    targetToken: await getTargetToken(),
    targetPath,
    timeoutMs,
  })
}

const activeOpenListBackup = new Set<string>()

/**
 * OpenList -> OpenList 备份核心逻辑（不直接依赖 karin event 对象）。
 * - 负责扫描源端目录树、在目标端创建目录、按模式（增量/全量）复制文件。
 * - apps 层只需要解析参数并把 `report` 绑定到 `e.reply`。
 */
export const backupOpenListToOpenListCore = async (params: {
  /** 源 OpenList 基础地址，例如 https://pan.example.com */
  sourceBaseUrl: string
  /** 源端账号（可选，未提供则按 guest 访问） */
  sourceUsername?: string
  /** 源端密码（可选，未提供则按 guest 访问） */
  sourcePassword?: string
  /** 源目录（posix path），默认 '/' */
  srcDir?: string
  /** 目标根目录（posix path），默认使用配置 openlistTargetDir */
  toDir?: string
  /** Whether to append `/<sourceHost>` under toDir (default true). */
  appendHostDir?: boolean
  /** 最大文件数（仅限制扫描后列表长度） */
  maxFiles?: number
  /** 复制并发（默认 3） */
  concurrency?: number
  /** 扫描并发（默认 20） */
  scanConcurrency?: number
  /** API list per_page（默认 1000） */
  perPage?: number
  /** 单文件超时秒数（默认 600） */
  timeoutSec?: number
  /** 模式：增量/全量（默认增量） */
  mode?: SyncMode
  /** 传输：auto/api/webdav（默认 auto，可由配置 openListBackupTransport 提供） */
  transport?: OpenListBackupTransport
  /** 进度回调（例如绑定 e.reply） */
  report?: (message: string) => Promise<void> | void
}) => {
  const cfg = config()

  const sourceBaseUrl = String(params.sourceBaseUrl ?? '').trim()
  if (!sourceBaseUrl) throw new Error('缺少源 OpenList 地址')

  const sourceUsername = String(params.sourceUsername ?? '').trim()
  const sourcePassword = String(params.sourcePassword ?? '').trim()
  const hasSourceAuth = Boolean(sourceUsername && sourcePassword)

  const targetBaseUrl = String(cfg.openlistBaseUrl ?? '').trim()
  const targetUsername = String(cfg.openlistUsername ?? '').trim()
  const targetPassword = String(cfg.openlistPassword ?? '').trim()
  if (!targetBaseUrl || !targetUsername || !targetPassword) {
    throw new Error('请先配置目标端 OpenList 信息（openlistBaseUrl/openlistUsername/openlistPassword）')
  }

  const sourceDavBaseUrl = buildOpenListDavBaseUrl(sourceBaseUrl)
  const targetDavBaseUrl = buildOpenListDavBaseUrl(targetBaseUrl)
  if (!targetDavBaseUrl) throw new Error('目标端 OpenList 地址不正确，请检查 openlistBaseUrl')

  const normalizedSrcDir = normalizePosixPath(String(params.srcDir ?? '/'))
  const normalizedTargetBaseDir = normalizePosixPath(String(params.toDir ?? cfg.openlistTargetDir ?? '/'))
  const targetRoot = params.appendHostDir === false
    ? normalizedTargetBaseDir
    : normalizePosixPath(path.posix.join(normalizedTargetBaseDir, safeHostDirName(sourceBaseUrl)))

  const mode: SyncMode = params.mode ?? 'incremental'
  const transport: OpenListBackupTransport = params.transport ?? normalizeOpenListBackupTransport((cfg as any)?.openListBackupTransport, 'auto')

  const lockKey = `${sourceBaseUrl} -> ${targetBaseUrl}`
  if (activeOpenListBackup.has(lockKey)) throw new Error('备份任务正在进行中，请稍后再试。')
  activeOpenListBackup.add(lockKey)

  const enqueueReport = (() => {
    const report = params.report
    if (!report) return (_msg: string) => {}
    let chain: Promise<unknown> = Promise.resolve()
    return (msg: string) => {
      chain = chain.then(() => report(msg)).catch(() => undefined)
    }
  })()

  let ticker: ReturnType<typeof setInterval> | undefined

  try {
    const timeoutMs = Math.min(
      MAX_FILE_TIMEOUT_SEC,
      Math.max(MIN_FILE_TIMEOUT_SEC, Math.floor(params.timeoutSec || 600) || MIN_FILE_TIMEOUT_SEC),
    ) * 1000
    const listTimeoutMs = Math.min(15_000, timeoutMs)
    const listPerPage = Math.max(1, Math.min(5000, Math.floor(params.perPage || 0) || 1000))

    const allowAutoFallback = transport === 'auto'

    // 默认策略：源端下载偏向 API，目标端上传偏向 WebDAV；可通过 transport 覆盖
    const sourceTransport: OpenListBackupConcreteTransport = transport === 'webdav' ? 'webdav' : 'api'
    let targetTransport: OpenListBackupConcreteTransport = transport === 'api' ? 'api' : 'webdav'

    let sourceToken: string | undefined
    let sourceTokenPromise: Promise<string> | undefined
    let targetToken: string | undefined
    let targetTokenPromise: Promise<string> | undefined

    const getSourceToken = async () => {
      if (typeof sourceToken === 'string') return sourceToken
      if (!hasSourceAuth) {
        // 源站点允许公开访问时，不需要登录；token 为空 => guest
        sourceToken = ''
        return sourceToken
      }

      if (sourceTokenPromise) return await sourceTokenPromise
      sourceTokenPromise = openlistApiLogin({
        baseUrl: sourceBaseUrl,
        username: sourceUsername,
        password: sourcePassword,
        timeoutMs: listTimeoutMs,
      }).then((token) => {
        sourceToken = token
        return token
      }).catch((error) => {
        sourceTokenPromise = undefined
        throw error
      })
      return await sourceTokenPromise
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

    const targetAuth = buildOpenListAuthHeader(targetUsername, targetPassword)
    const targetDirEnsurerWebDav = createWebDavDirEnsurer(targetDavBaseUrl, targetAuth, timeoutMs)
    let targetDirEnsurerApi: ReturnType<typeof createOpenListApiDirEnsurer> | undefined
    const getTargetDirEnsurerApi = async () => {
      if (targetDirEnsurerApi) return targetDirEnsurerApi
      const token = await getTargetToken()
      targetDirEnsurerApi = createOpenListApiDirEnsurer(targetBaseUrl, token, timeoutMs)
      return targetDirEnsurerApi
    }

    const scanConcurrencyMax = Math.max(1, Math.min(200, Math.floor(params.scanConcurrency || 0) || 20))
    const maxConcurrency = Math.max(1, Math.min(50, Math.floor(params.concurrency || 0) || 3))

    let scannedDirs = 0
    let scannedFiles = 0
    const startAt = Date.now()

    ticker = setInterval(() => {
      const elapsed = Math.floor((Date.now() - startAt) / 1000)
      enqueueReport(`备份进行中（${elapsed}s）\n阶段：扫描\n已扫描目录：${scannedDirs}\n已发现文件：${scannedFiles}`)
    }, 10_000)

    const sourceAuth = hasSourceAuth ? buildOpenListAuthHeader(sourceUsername, sourcePassword) : ''

    const scanResult = await scanOpenListFiles({
      sourceTransport,
      sourceBaseUrl,
      sourceToken: await getSourceToken(),
      sourceAuth: sourceAuth || undefined,
      sourceDavBaseUrl,
      srcDir: normalizedSrcDir,
      targetRoot,
      timeoutMs: listTimeoutMs,
      perPage: listPerPage,
      scanConcurrency: scanConcurrencyMax,
      onProgress: (p) => {
        scannedDirs = p.scannedDirs
        scannedFiles = p.files
      },
    })

    const files = scanResult.files

    if (typeof params.maxFiles === 'number' && Number.isFinite(params.maxFiles) && params.maxFiles > 0) {
      files.splice(Math.floor(params.maxFiles))
    }

    if (!files.length) {
      enqueueReport('未发现需要备份的文件。')
      return { ok: 0, skipped: 0, fail: 0 }
    }

    if (ticker) clearInterval(ticker)
    ticker = setInterval(() => {
      const elapsed = Math.floor((Date.now() - startAt) / 1000)
      enqueueReport(`备份进行中（${elapsed}s）\n阶段：复制\n待处理：${files.length}`)
    }, 10_000)

    enqueueReport([
      '开始备份 OpenList...',
      `源：${sourceBaseUrl}`,
      `源目录：${normalizedSrcDir}`,
      `目标：${targetBaseUrl}`,
      `目标目录：${targetRoot}`,
      `模式：${mode}`,
      `传输：${transport}`,
      `文件数：${files.length}`,
    ].join('\n'))

    let skipped = 0
    let ok = 0
    let fail = 0

    const targetDavBase = targetDavBaseUrl

    const ensureDirForPath = async (filePath: string) => {
      const dirPath = normalizePosixPath(path.posix.dirname(filePath))
      targetTransport = await ensureTargetDir({
        dirPath,
        targetTransport,
        allowAutoFallback,
        targetBaseUrl,
        webdavEnsurer: targetDirEnsurerWebDav,
        getApiEnsurer: getTargetDirEnsurerApi,
      })
    }

    await runWithConcurrency(files, maxConcurrency, async ({ sourcePath, targetPath }) => {
      try {
        if (mode === 'incremental') {
          const exists = await existsOnTarget({
            targetTransport,
            targetDavBaseUrl: targetDavBase,
            targetAuth,
            targetBaseUrl,
            getTargetToken,
            targetPath,
            timeoutMs: listTimeoutMs,
          })
          if (exists) {
            skipped++
            return
          }
        }

        await ensureDirForPath(targetPath)

        await copyOpenListFile({
          sourceTransport,
          targetTransport,
          sourceBaseUrl,
          sourceDavBaseUrl,
          sourceAuth,
          targetBaseUrl,
          targetDavBaseUrl: targetDavBase,
          targetAuth,
          sourcePath,
          targetPath,
          getSourceToken,
          getTargetToken,
          timeoutMs,
        })

        ok++
      } catch (error) {
        fail++
        logger.error(error)
        if (allowAutoFallback) {
          // 简单回退：若 WebDAV 失败则切 API；若 API 失败则切 WebDAV（下一轮生效）
          const msg = formatErrorMessage(error)
          if (targetTransport === 'webdav' && /401|403|MKCOL|PUT/i.test(msg)) targetTransport = 'api'
          else if (targetTransport === 'api' && /fs\/put|fs\/mkdir|code=/i.test(msg)) targetTransport = 'webdav'
        }
      }
    })

    enqueueReport(`备份完成：成功 ${ok}，跳过 ${skipped}，失败 ${fail}`)
    return { ok, skipped, fail }
  } finally {
    if (ticker) clearInterval(ticker)
    activeOpenListBackup.delete(lockKey)
  }
}

