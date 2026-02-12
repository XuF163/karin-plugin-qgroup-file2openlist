import path from 'node:path'
import { dir } from '@/dir'
import { config } from '@/utils'
import { karin, logger } from 'node-karin'
import { runWithAdaptiveConcurrency, runWithConcurrency } from '@/model/shared/concurrency'
import { formatErrorMessage } from '@/model/shared/errors'
import { encodePathForUrl, normalizePosixPath, safePathSegment } from '@/model/shared/path'
import {
  buildOpenListAuthHeader,
  buildOpenListDavBaseUrl,
  createWebDavDirEnsurer,
  downloadAndUploadByWebDav,
  webdavHeadExists,
} from '@/model/openlist'
import type { ExportError, ExportedGroupFile, GroupFileSyncStateV1, SyncMode } from './types'
import { collectAllGroupFiles, resolveGroupFileUrl } from './qgroup'
import { readGroupSyncState, withGroupSyncLock, writeGroupSyncState } from './state'

const MAX_FILE_TIMEOUT_SEC = 3000
const MIN_FILE_TIMEOUT_SEC = 10
const DEFAULT_PROGRESS_REPORT_EVERY = 10
const MAX_TRANSFER_CONCURRENCY = 5

const createAsyncLimiter = (concurrency: number) => {
  const limit = Math.max(1, Math.floor(concurrency) || 1)
  let active = 0
  const queue: Array<() => void> = []

  const acquire = async () => {
    if (active < limit) {
      active++
      return
    }
    await new Promise<void>((resolve) => queue.push(resolve))
  }

  const release = () => {
    const next = queue.shift()
    if (next) {
      next()
      return
    }
    active = Math.max(0, active - 1)
  }

  return async <T>(fn: () => Promise<T>): Promise<T> => {
    await acquire()
    try {
      return await fn()
    } finally {
      release()
    }
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
      '开始同步群文件到 OpenList，请稍候..',
      `- 群号：${groupId}`,
      `- 目标目录：${finalTargetDir}`,
      `- 模式：${mode === 'incremental' ? '增量' : '全量'}`,
      `- 保留目录结构：${flat ? '否' : '是'}`,
      `- 并发：URL ${urlConcurrency} / 传输 ${transferConcurrency}`,
    ].join('\n'))

    const list = await collectAllGroupFiles(bot, groupId, folderId)

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

    // 增量同步：额外检查 OpenList 目标端是否已存在同路径文件（HEAD）。
    // - 相比 PROPFIND：避免目录文件过多时响应体过大导致内存飙升
    // - 代价：每个文件一个 HEAD 请求（可通过 state + 并发控制减轻）
    if (mode === 'incremental' && needSync.length) {
      const existsResults = new Array<boolean>(needSync.length).fill(false)
      const existsConcurrency = 10
      await runWithConcurrency(needSync, existsConcurrency, async ({ remotePath }, index) => {
        existsResults[index] = await webdavHeadExists({
          davBaseUrl,
          auth,
          filePath: remotePath,
          timeoutMs: webdavTimeoutMs,
        })
      })

      needSync = needSync.filter((_it, index) => {
        if (existsResults[index]) {
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

    report && await report('将按需解析下载 URL（避免批量预取导致 URL 过期/触发限流）')

    const urlErrors: ExportError[] = []
    const syncErrors: Array<{ path: string, fileId: string, message: string }> = []
    const urlOkSet = new Set<string>()
    let okCount = 0

    const shouldRefreshUrl = (message: string) => {
      return /401|403|404|URL已过期|url已过期|url可能已失效|需要重新获取|下载超时/.test(message)
    }

    const resolveUrlWithLimit = createAsyncLimiter(Math.max(1, Math.floor(urlConcurrency) || 1))
    const urlResolveOptions = {
      retries: 2,
      delayMs: retryDelayMs > 0 ? Math.floor(retryDelayMs) : undefined,
      maxDelayMs: 20_000,
    }

    const ensureItemUrl = async (item: ExportedGroupFile, remotePath: string) => {
      const existing = typeof item.url === 'string' ? item.url.trim() : ''
      if (existing) {
        urlOkSet.add(remotePath)
        return existing
      }

      try {
        const url = await resolveUrlWithLimit(() =>
          resolveGroupFileUrl(bot, groupContact, groupId, item, urlResolveOptions),
        )
        const cleaned = typeof url === 'string' ? url.trim() : ''
        if (!cleaned) throw new Error('返回空 URL')
        item.url = cleaned
        urlOkSet.add(remotePath)
        return cleaned
      } catch (error) {
        throw new Error(`URL获取失败: ${formatErrorMessage(error)}`)
      }
    }

    const transferOne = async (sourceUrl: string, targetUrl: string, expectedSize?: number) => {
      await downloadAndUploadByWebDav({
        sourceUrl,
        targetUrl,
        auth,
        timeoutMs: transferTimeoutMs,
        rateLimitBytesPerSec: effectiveRateLimitBytesPerSec || undefined,
        expectedSize,
      })
    }

    report && await report('开始下载并上传到 OpenList，请稍候..')

    const transferInitial = Math.min(MAX_TRANSFER_CONCURRENCY, Math.max(1, Math.floor(transferConcurrency) || 1))
    const adaptiveTransfer = effectiveRateLimitBytesPerSec <= 0

    const transferFn = async ({ item, remotePath }: typeof needSync[number], index: number) => {
      logger.info(`[群文件同步][${groupId}] 同步中(${index + 1}/${needSync.length}): ${item.path}`)

      const remoteDir = normalizePosixPath(path.posix.dirname(remotePath))
      const targetUrl = `${davBaseUrl}${encodePathForUrl(remotePath)}`

      let lastError: unknown
      let succeeded = false
      const attempts = Math.max(0, Math.floor(retryTimes) || 0) + 1

      for (let attempt = 1; attempt <= attempts; attempt++) {
        try {
          await dirEnsurer.ensureDir(remoteDir)

          const currentUrl = await ensureItemUrl(item, remotePath)

          await transferOne(currentUrl, targetUrl, item.size)

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
              item.url = undefined
            }

            const delay = Math.max(0, Math.floor(retryDelayMs) || 0) * Math.pow(2, attempt - 1)
            if (delay > 0) await new Promise(resolve => setTimeout(resolve, delay))
            continue
          }
        }
      }

      if (!succeeded && lastError) {
        const message = formatErrorMessage(lastError)
        if (message.startsWith('URL获取失败:')) {
          urlErrors.push({ fileId: item.fileId, path: item.path, message })
        } else {
          syncErrors.push({
            path: item.path,
            fileId: item.fileId,
            message,
          })
        }
      }

      if (safeProgressReportEvery > 0 && (index + 1) % safeProgressReportEvery === 0) {
        report && await report(`同步进度：${index + 1}/${needSync.length}（成功 ${okCount}）`)
      }
    }

    if (adaptiveTransfer) {
      logger.info(`[群文件同步][${groupId}] 未配置限速，将自适应调整传输并发（最多 ${MAX_TRANSFER_CONCURRENCY}）`)
      await runWithAdaptiveConcurrency(needSync, {
        initial: transferInitial,
        max: MAX_TRANSFER_CONCURRENCY,
        fn: transferFn,
        onAdjust: (current, reason) => {
          logger.info(`[群文件同步][${groupId}] 自适应调整传输并发=${current} (${reason})`)
        },
      })
    } else {
      await runWithConcurrency(needSync, transferInitial, transferFn)
    }

    state.lastSyncAt = Date.now()
    writeGroupSyncState(groupId, state)

    const failCount = needSync.length - okCount
    report && await report(`同步完成：成功 ${okCount} / 失败 ${failCount}（增量跳过 ${skipped}）`)

    if (syncErrors.length) {
      const preview = syncErrors.slice(0, 5).map((it) => `${it.path} (${it.fileId})\n${it.message}`).join('\n\n')
      report && await report(`失败示例（前5条）：\n${preview}`)
    }

    if (urlErrors.length) {
      const preview = urlErrors.slice(0, 5).map((it) => `${it.path ?? ''} (${it.fileId ?? ''})\n${it.message}`).join('\n\n')
      report && await report(`URL获取失败示例（前5条）：\n${preview}`)
    }

    return { total: limitedList.length, skipped, urlOk: urlOkSet.size, ok: okCount, fail: failCount }
  })
}
