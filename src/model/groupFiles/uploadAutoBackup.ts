import path from 'node:path'
import { setTimeout as sleep } from 'node:timers/promises'
import { config } from '@/utils'
import { logger } from 'node-karin'
import { formatErrorMessage } from '@/model/shared/errors'
import { encodePathForUrl, normalizePosixPath, safePathSegment } from '@/model/shared/path'
import { buildOpenListAuthHeader, buildOpenListDavBaseUrl, createWebDavDirEnsurer, downloadAndUploadByWebDav } from '@/model/openlist'
import { locateGroupFileByIdWithRetry, normalizeGroupFileRelativePath, resolveGroupFileUrl } from './qgroup'
import { readGroupSyncState, writeGroupSyncState } from './state'
import type { ExportedGroupFile } from './types'

const MAX_FILE_TIMEOUT_SEC = 3000
const MIN_FILE_TIMEOUT_SEC = 10

type GroupSyncTarget = {
  groupId: string
  enabled?: boolean
  targetDir?: string
  flat?: boolean
  uploadBackup?: boolean
  fileTimeoutSec?: number
  retryTimes?: number
  retryDelayMs?: number
  downloadLimitKbps?: number
  uploadLimitKbps?: number
}

const getGroupSyncTarget = (cfg: any, groupId: string): GroupSyncTarget | undefined => {
  const list = cfg?.groupSyncTargets
  if (!Array.isArray(list)) return undefined
  return list.find((it: any) => String(it?.groupId) === String(groupId))
}

const activeGroupFileUploadBackups = new Map<string, Promise<void>>()
const uploadBackupSkipLoggedGroups = new Set<string>()
const uploadBackupUrlFallbackLoggedGroups = new Set<string>()

const enqueueGroupFileUploadBackup = (groupId: string, task: () => Promise<void>) => {
  const key = String(groupId)
  const previous = activeGroupFileUploadBackups.get(key) ?? Promise.resolve()
  const nextTask = previous.catch(() => undefined).then(task)
  activeGroupFileUploadBackups.set(key, nextTask)
  nextTask.finally(() => {
    if (activeGroupFileUploadBackups.get(key) === nextTask) activeGroupFileUploadBackups.delete(key)
  })
}

const buildRemotePathForItem = (item: ExportedGroupFile, targetDir: string, flat: boolean) => {
  const relativeParts = (flat ? [item.name] : item.path.split('/')).filter(Boolean).map(safePathSegment)
  return normalizePosixPath(path.posix.join(targetDir, ...relativeParts))
}

/**
 * 处理 notice.groupFileUploaded 事件：按群配置 uploadBackup 自动备份到 OpenList。
 * - 该函数内部会对同一群的上传备份串行排队，避免并发过高。
 */
export const handleGroupFileUploadedAutoBackup = (e: any) => {
  try {
    const groupId = String((e as any)?.groupId ?? '').trim()
    if (!groupId) return

    const cfg = config()
    const targetCfg = getGroupSyncTarget(cfg, groupId)
    const uploadBackupEnabled = targetCfg?.uploadBackup === true
      || ['true', '1', 'on'].includes(String((targetCfg as any)?.uploadBackup ?? '').trim().toLowerCase())
    if (!targetCfg || targetCfg.enabled === false || !uploadBackupEnabled) {
      if (!uploadBackupSkipLoggedGroups.has(groupId)) {
        uploadBackupSkipLoggedGroups.add(groupId)
        logger.info(`[群上传备份][${groupId}] uploadBackup 未启用或该群未配置，已跳过（可在 WebUI 开启 uploadBackup）`)
      }
      return
    }

    const file = (e as any).content as any
    const fid = String(file?.fid ?? '').trim()
    const name = String(file?.name ?? '').trim()
    const size = typeof file?.size === 'number' && Number.isFinite(file.size) ? Math.max(0, Math.floor(file.size)) : undefined
    const getUrl = typeof file?.url === 'function' ? (file.url as () => Promise<string>) : null
    if (!fid || !name || !getUrl) {
      logger.warn(`[群上传备份][${groupId}] 事件缺少必要字段（fid/name/url），已跳过`)
      return
    }

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

      const flat = typeof targetCfg?.flat === 'boolean'
        ? targetCfg.flat
        : Boolean(defaults?.flat ?? false)

      const item: ExportedGroupFile = {
        path: name,
        fileId: fid,
        name,
        size,
        busid: typeof file?.subId === 'number' && Number.isFinite(file.subId) ? Math.floor(file.subId) : undefined,
      }

      if (!flat) {
        const direct = String(file?.path ?? file?.filePath ?? file?.file_path ?? file?.fullPath ?? file?.full_path ?? '').trim()
        const normalized = direct ? normalizeGroupFileRelativePath(direct) : ''
        if (normalized) item.path = normalized

        const bot = (e as any)?.bot
        if (bot && !item.path.includes('/')) {
          try {
            const found = await locateGroupFileByIdWithRetry(bot, groupId, fid, {
              retries: 3,
              delayMs: 1200,
              timeoutMs: 15_000,
              maxFolders: 4000,
              expectedName: name,
              expectedSize: size,
            })
            if (found?.path) item.path = found.path
            if (typeof found?.busid === 'number' && Number.isFinite(found.busid)) item.busid = Math.floor(found.busid)
            if (!found?.path) {
              logger.warn(`[群上传备份][${groupId}] 未能解析群文件夹路径，将备份到群根目录：${name}`)
            }
          } catch (error) {
            logger.debug(`[群上传备份][${groupId}] 获取群内文件路径失败，将退化为根目录: ${formatErrorMessage(error)}`)
          }
        }
      }

      const resolveUrl = async () => {
        try {
          const url = await getUrl()
          if (typeof url === 'string' && url.trim()) return url.trim()
        } catch {}

        const bot = (e as any)?.bot
        const contact = (e as any)?.contact
        if (!bot) throw new Error('事件对象缺少 bot，无法通过接口获取下载 URL')

        const url = await resolveGroupFileUrl(bot, contact, groupId, item)
        if (typeof url === 'string' && url.trim()) {
          if (!uploadBackupUrlFallbackLoggedGroups.has(groupId)) {
            uploadBackupUrlFallbackLoggedGroups.add(groupId)
            logger.debug(`[群上传备份][${groupId}] file.url() 不可用，已自动使用接口获取下载 URL（仅提示一次）`)
          }
          return url.trim()
        }
        throw new Error('通过接口获取下载 URL 失败')
      }

      const baseRemotePath = buildRemotePathForItem(item, targetDir, flat)
      let remotePath = baseRemotePath
      let remoteDir = normalizePosixPath(path.posix.dirname(remotePath))

      const parseFileNameForVersioning = (fileName: string) => {
        const raw = String(fileName ?? '').trim()
        const dot = raw.lastIndexOf('.')
        const hasExt = dot > 0 && dot < raw.length - 1
        const namePart = hasExt ? raw.slice(0, dot) : raw
        const ext = hasExt ? raw.slice(dot) : ''

        const match = namePart.match(/^(.*)_v(\d+)$/i)
        const base = (match?.[1] ? match[1] : namePart) || namePart || 'file'
        const start = match?.[2] ? Math.max(1, Math.floor(Number(match[2])) + 1) : 1
        return { base, ext, start }
      }

      const isTargetAlreadyExistsError = (error: unknown) => {
        const msg = formatErrorMessage(error)
        return /\b409\b/.test(msg)
          || /already exists|file exists|EEXIST|object already exists/i.test(msg)
          || /已存在|存在同名|文件已存在|同名文件/i.test(msg)
          || /code=409\b/i.test(msg)
      }

      const versioning = (() => {
        const dirPath = normalizePosixPath(path.posix.dirname(baseRemotePath))
        const baseName = path.posix.basename(baseRemotePath)
        const parsed = parseFileNameForVersioning(baseName)
        let current = parsed.start
        const max = parsed.start + 30 - 1
        const used = new Set<string>([baseRemotePath])

        const next = () => {
          while (current <= max) {
            const suffix = `_v${current}`
            current++
            const candidateName = safePathSegment(`${parsed.base}${suffix}${parsed.ext}`)
            const candidatePath = normalizePosixPath(path.posix.join(dirPath, candidateName))
            if (used.has(candidatePath)) continue
            used.add(candidatePath)
            return candidatePath
          }
        }

        return { next }
      })()

      const state = readGroupSyncState(groupId)

      const pickNextRemotePath = () => {
        let nextPath = versioning.next()
        while (nextPath && state.files?.[nextPath]?.fileId) nextPath = versioning.next()
        return nextPath
      }

      // 如果目标路径已存在（历史已备份），自动改名避免覆盖
      if (state.files?.[remotePath]?.fileId) {
        const nextPath = pickNextRemotePath()
        if (!nextPath) throw new Error(`目标端已存在同名文件，且自动改名（_v1..）仍失败：${remotePath}`)
        logger.info(`[群上传备份][${groupId}] 目标已存在，改名重试: ${remotePath} -> ${nextPath}`)
        remotePath = nextPath
        remoteDir = normalizePosixPath(path.posix.dirname(remotePath))
      }

      const auth = buildOpenListAuthHeader(username, password)
      const fileTimeoutSec = typeof targetCfg?.fileTimeoutSec === 'number'
        ? targetCfg.fileTimeoutSec
        : (typeof defaults?.fileTimeoutSec === 'number' ? defaults.fileTimeoutSec : 600)
      const safeFileTimeoutSec = Math.min(
        MAX_FILE_TIMEOUT_SEC,
        Math.max(MIN_FILE_TIMEOUT_SEC, Math.floor(fileTimeoutSec || 0)),
      )

      const rateDown = Math.max(0, Math.floor(typeof (targetCfg as any)?.downloadLimitKbps === 'number' ? (targetCfg as any).downloadLimitKbps : (typeof defaults?.downloadLimitKbps === 'number' ? defaults.downloadLimitKbps : 0)))
      const rateUp = Math.max(0, Math.floor(typeof (targetCfg as any)?.uploadLimitKbps === 'number' ? (targetCfg as any).uploadLimitKbps : (typeof defaults?.uploadLimitKbps === 'number' ? defaults.uploadLimitKbps : 0)))
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

      logger.info(`[群上传备份][${groupId}] 开始 ${name} (${fid}) -> ${remotePath}`)

      let lastError: unknown
      const attempts = Math.max(0, Math.floor(retryTimes) || 0) + 1
      for (let attempt = 1; attempt <= attempts; attempt++) {
        try {
          await dirEnsurer.ensureDir(remoteDir)
          const url = await resolveUrl()

          while (true) {
            // 如果候选目标路径在 state 中已存在，继续递增版本号，确保写入新文件
            while (state.files?.[remotePath]?.fileId) {
              const nextPath = pickNextRemotePath()
              if (!nextPath) throw new Error(`目标端已存在同名文件，且自动改名（_v1..）仍失败：${remotePath}`)
              logger.info(`[群上传备份][${groupId}] 目标已存在，改名重试: ${remotePath} -> ${nextPath}`)
              remotePath = nextPath
              remoteDir = normalizePosixPath(path.posix.dirname(remotePath))
            }

            const targetUrl = `${davBaseUrl}${encodePathForUrl(remotePath)}`

            try {
              await downloadAndUploadByWebDav({
                sourceUrl: url,
                targetUrl,
                auth,
                timeoutMs: transferTimeoutMs,
                rateLimitBytesPerSec: effectiveRateLimitBytesPerSec || undefined,
                expectedSize: size,
              })
              break
            } catch (error) {
              if (!isTargetAlreadyExistsError(error)) throw error

              const nextPath = versioning.next()
              if (!nextPath) {
                throw new Error(`目标端已存在同名文件，且自动改名（_v1..）仍失败：${remotePath}`)
              }

              logger.info(`[群上传备份][${groupId}] 目标已存在，改名重试: ${remotePath} -> ${nextPath}`)
              remotePath = nextPath
              remoteDir = normalizePosixPath(path.posix.dirname(remotePath))
              continue
            }
          }

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
  }
}
