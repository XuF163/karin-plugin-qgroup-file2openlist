import path from 'node:path'
import { config } from '@/utils'
import { getAllBot, logger } from 'node-karin'
import { syncGroupFilesToOpenListCore } from '@/model/groupFiles'
import type { SyncMode } from '@/model/groupFiles/types'

const NIGHTLY_MODE: SyncMode = 'incremental'
const NIGHTLY_FILE_TIMEOUT_SEC = 3000

const normalizeMode = (value: unknown, fallback: SyncMode): SyncMode => {
  const v = String(value ?? '').trim().toLowerCase()
  if (v === 'full' || v === '全量') return 'full'
  if (v === 'incremental' || v === '增量' || v === 'inc') return 'incremental'
  return fallback
}

const parseCronField = (field: string, min: number, max: number) => {
  const trimmed = String(field ?? '').trim()
  if (!trimmed || trimmed === '*' || trimmed === '?') return null

  const allowed = new Set<number>()
  const parts = trimmed.split(',').map(p => p.trim()).filter(Boolean)

  for (const part of parts) {
    const [rangePart, stepPart] = part.split('/')
    const step = stepPart ? Number(stepPart) : 1
    if (!Number.isFinite(step) || step <= 0) return null

    const applyRange = (start: number, end: number) => {
      const s = Math.max(min, start)
      const e = Math.min(max, end)
      for (let v = s; v <= e; v += step) allowed.add(v)
    }

    if (rangePart === '*' || rangePart === '?') {
      applyRange(min, max)
      continue
    }

    const rangeMatch = rangePart.match(/^(\d+)-(\d+)$/)
    if (rangeMatch) {
      applyRange(Number(rangeMatch[1]), Number(rangeMatch[2]))
      continue
    }

    const single = Number(rangePart)
    if (Number.isFinite(single)) applyRange(single, single)
  }

  return allowed
}

export const cronMatches = (expr: string, date: Date) => {
  const parts = String(expr ?? '').trim().split(/\s+/).filter(Boolean)
  if (parts.length !== 5 && parts.length !== 6) return false

  const fields = parts.length === 6 ? parts : ['0', ...parts]

  const sec = date.getSeconds()
  const min = date.getMinutes()
  const hour = date.getHours()
  const dom = date.getDate()
  const month = date.getMonth() + 1
  const dowRaw = date.getDay()
  const dow = dowRaw === 0 ? 0 : dowRaw

  const allowedSec = parseCronField(fields[0], 0, 59)
  const allowedMin = parseCronField(fields[1], 0, 59)
  const allowedHour = parseCronField(fields[2], 0, 23)
  const allowedDom = parseCronField(fields[3], 1, 31)
  const allowedMonth = parseCronField(fields[4], 1, 12)
  const allowedDow = parseCronField(fields[5], 0, 7)

  const match = (allowed: Set<number> | null, value: number) => !allowed || allowed.has(value)
  const dowOk = !allowedDow || allowedDow.has(dow) || (dow === 0 && allowedDow.has(7))

  return match(allowedSec, sec)
    && match(allowedMin, min)
    && match(allowedHour, hour)
    && match(allowedDom, dom)
    && match(allowedMonth, month)
    && dowOk
}

export const parseTimeWindows = (value: string) => {
  const raw = String(value ?? '').trim()
  if (!raw) return []
  const items = raw.split(',').map(s => s.trim()).filter(Boolean)
  const windows: Array<{ start: number, end: number }> = []

  for (const item of items) {
    const match = item.match(/^(\d{1,2}):(\d{2})-(\d{1,2}):(\d{2})$/)
    if (!match) continue
    const sh = Number(match[1]); const sm = Number(match[2])
    const eh = Number(match[3]); const em = Number(match[4])
    if (![sh, sm, eh, em].every(n => Number.isFinite(n))) continue
    if (sh < 0 || sh > 23 || eh < 0 || eh > 23) continue
    if (sm < 0 || sm > 59 || em < 0 || em > 59) continue
    const start = sh * 60 + sm
    const end = eh * 60 + em

    if (start === end) continue
    if (end > start) {
      windows.push({ start, end })
    } else {
      windows.push({ start, end: 24 * 60 })
      windows.push({ start: 0, end })
    }
  }

  return windows
}

export const isNowAllowed = (timeWindows: unknown, now: Date) => {
  const windows = parseTimeWindows(String(timeWindows ?? ''))
  if (!windows.length) return true
  const minutes = now.getHours() * 60 + now.getMinutes()
  return windows.some(w => minutes >= w.start && minutes < w.end)
}

const lastTriggered = new Map<string, number>()

const runScheduledSync = async (groupId: string) => {
  const cfg = config()
  const defaults = cfg.groupSyncDefaults ?? {}
  const targets = Array.isArray(cfg.groupSyncTargets) ? cfg.groupSyncTargets : []
  const target = targets.find((it: any) => String(it?.groupId) === String(groupId))
  if (!target) return
  if (target?.enabled === false) return
  if (target?.schedule?.enabled !== true) return

  const mode = normalizeMode(target?.mode, normalizeMode(defaults?.mode, 'incremental'))
  const targetDir = String(target?.targetDir ?? '').trim()
    ? String(target.targetDir)
    : path.posix.join(String(cfg.openlistTargetDir ?? '/'), String(groupId))

  const folderId = String(target?.sourceFolderId ?? '').trim() ? String(target.sourceFolderId) : undefined
  const maxFiles = typeof target?.maxFiles === 'number' ? target.maxFiles : undefined

  const urlConcurrency = typeof target?.urlConcurrency === 'number'
    ? target.urlConcurrency
    : (typeof defaults?.urlConcurrency === 'number' ? defaults.urlConcurrency : 3)

  const transferConcurrency = typeof target?.transferConcurrency === 'number'
    ? target.transferConcurrency
    : (typeof defaults?.transferConcurrency === 'number' ? defaults.transferConcurrency : 3)

  const fileTimeoutSec = typeof target?.fileTimeoutSec === 'number'
    ? target.fileTimeoutSec
    : (typeof defaults?.fileTimeoutSec === 'number' ? defaults.fileTimeoutSec : 600)

  const retryTimes = typeof target?.retryTimes === 'number'
    ? target.retryTimes
    : (typeof defaults?.retryTimes === 'number' ? defaults.retryTimes : 2)

  const retryDelayMs = typeof target?.retryDelayMs === 'number'
    ? target.retryDelayMs
    : (typeof defaults?.retryDelayMs === 'number' ? defaults.retryDelayMs : 1500)

  const progressReportEvery = typeof target?.progressReportEvery === 'number'
    ? target.progressReportEvery
    : (typeof defaults?.progressReportEvery === 'number' ? defaults.progressReportEvery : 10)

  const downloadLimitKbps = typeof target?.downloadLimitKbps === 'number'
    ? target.downloadLimitKbps
    : (typeof defaults?.downloadLimitKbps === 'number' ? defaults.downloadLimitKbps : 0)

  const uploadLimitKbps = typeof target?.uploadLimitKbps === 'number'
    ? target.uploadLimitKbps
    : (typeof defaults?.uploadLimitKbps === 'number' ? defaults.uploadLimitKbps : 0)

  const flat = typeof target?.flat === 'boolean' ? target.flat : Boolean(defaults?.flat ?? false)

  const bots = getAllBot()
  if (!bots.length) {
    logger.warn(`[群文件定时同步][${groupId}] 未找到可用Bot，跳过`)
    return
  }

  let lastError: unknown
  for (const bot of bots) {
    try {
      await syncGroupFilesToOpenListCore({
        bot,
        groupId: String(groupId),
        folderId,
        maxFiles,
        flat,
        targetDir,
        mode,
        urlConcurrency: Math.max(1, Math.floor(urlConcurrency) || 1),
        transferConcurrency: Math.max(1, Math.floor(transferConcurrency) || 1),
        fileTimeoutSec: Math.max(10, Math.floor(fileTimeoutSec) || 10),
        retryTimes: Math.max(0, Math.floor(retryTimes) || 0),
        retryDelayMs: Math.max(0, Math.floor(retryDelayMs) || 0),
        progressReportEvery: Math.max(0, Math.floor(progressReportEvery) || 0),
        downloadLimitKbps: Math.max(0, Math.floor(downloadLimitKbps) || 0),
        uploadLimitKbps: Math.max(0, Math.floor(uploadLimitKbps) || 0),
        report: (msg) => logger.info(`[群文件定时同步][${groupId}] ${msg}`),
      })
      return
    } catch (error: any) {
      const msg = error instanceof Error ? error.message : String(error)
      if (msg.includes('正在进行中')) {
        logger.info(`[群文件定时同步][${groupId}] 同步任务正在进行中，跳过`)
        return
      }
      lastError = error
      continue
    }
  }

  if (lastError) logger.error(lastError)
}

const uploadBackupEnabledForTarget = (target: any) => {
  if (target?.uploadBackup === true) return true
  return ['true', '1', 'on'].includes(String(target?.uploadBackup ?? '').trim().toLowerCase())
}

/**
 * 夜间自动备份：按固定策略对“已开启 uploadBackup 的群”做一次增量同步。
 * - 由 `src/apps/scheduler.ts` 定时触发（默认每天 02:00）
 * - 固定：增量模式 + 单文件超时 3000s
 */
export const runNightlyGroupBackup = async () => {
  const cfg = config()
  if (cfg?.scheduler?.enabled === false) return
  if (cfg?.scheduler?.groupSync?.enabled === false) return

  const defaults = cfg.groupSyncDefaults ?? {}
  const targets = Array.isArray(cfg.groupSyncTargets) ? cfg.groupSyncTargets : []
  const now = new Date()

  const nightlyTargets = targets.filter((t: any) => {
    const groupId = String(t?.groupId ?? '').trim()
    if (!groupId) return false
    if (t?.enabled === false) return false
    return uploadBackupEnabledForTarget(t)
  })

  if (!nightlyTargets.length) return

  logger.info(`[夜间备份][群] 开始：${nightlyTargets.length} 个群（模式=${NIGHTLY_MODE} 超时=${NIGHTLY_FILE_TIMEOUT_SEC}s）`)

  for (const target of nightlyTargets) {
    const groupId = String((target as any)?.groupId ?? '').trim()
    if (!groupId) continue

    if (!isNowAllowed((target as any)?.timeWindows, now)) {
      logger.info(`[夜间备份][群][${groupId}] 不在时段内，跳过`)
      continue
    }

    const folderId = String((target as any)?.sourceFolderId ?? '').trim() ? String((target as any).sourceFolderId) : undefined
    const maxFiles = typeof (target as any)?.maxFiles === 'number' ? (target as any).maxFiles : undefined

    const urlConcurrency = typeof (target as any)?.urlConcurrency === 'number'
      ? (target as any).urlConcurrency
      : (typeof defaults?.urlConcurrency === 'number' ? defaults.urlConcurrency : 3)

    const transferConcurrency = typeof (target as any)?.transferConcurrency === 'number'
      ? (target as any).transferConcurrency
      : (typeof defaults?.transferConcurrency === 'number' ? defaults.transferConcurrency : 3)

    const retryTimes = typeof (target as any)?.retryTimes === 'number'
      ? (target as any).retryTimes
      : (typeof defaults?.retryTimes === 'number' ? defaults.retryTimes : 2)

    const retryDelayMs = typeof (target as any)?.retryDelayMs === 'number'
      ? (target as any).retryDelayMs
      : (typeof defaults?.retryDelayMs === 'number' ? defaults.retryDelayMs : 1500)

    const progressReportEvery = typeof (target as any)?.progressReportEvery === 'number'
      ? (target as any).progressReportEvery
      : (typeof defaults?.progressReportEvery === 'number' ? defaults.progressReportEvery : 10)

    const downloadLimitKbps = typeof (target as any)?.downloadLimitKbps === 'number'
      ? (target as any).downloadLimitKbps
      : (typeof defaults?.downloadLimitKbps === 'number' ? defaults.downloadLimitKbps : 0)

    const uploadLimitKbps = typeof (target as any)?.uploadLimitKbps === 'number'
      ? (target as any).uploadLimitKbps
      : (typeof defaults?.uploadLimitKbps === 'number' ? defaults.uploadLimitKbps : 0)

    const flat = typeof (target as any)?.flat === 'boolean' ? (target as any).flat : Boolean(defaults?.flat ?? false)

    const targetDir = String((target as any)?.targetDir ?? '').trim()
      ? String((target as any).targetDir)
      : path.posix.join(String(cfg.openlistTargetDir ?? '/'), groupId)

    const bots = getAllBot()
    if (!bots.length) {
      logger.warn(`[夜间备份][群][${groupId}] 未找到可用Bot，跳过`)
      continue
    }

    logger.info(`[夜间备份][群][${groupId}] 开始同步`)

    let lastError: unknown
    for (const bot of bots) {
      try {
        await syncGroupFilesToOpenListCore({
          bot,
          groupId,
          folderId,
          maxFiles,
          flat,
          targetDir,
          mode: NIGHTLY_MODE,
          urlConcurrency: Math.max(1, Math.floor(urlConcurrency) || 1),
          transferConcurrency: Math.max(1, Math.floor(transferConcurrency) || 1),
          fileTimeoutSec: NIGHTLY_FILE_TIMEOUT_SEC,
          retryTimes: Math.max(0, Math.floor(retryTimes) || 0),
          retryDelayMs: Math.max(0, Math.floor(retryDelayMs) || 0),
          progressReportEvery: Math.max(0, Math.floor(progressReportEvery) || 0),
          downloadLimitKbps: Math.max(0, Math.floor(downloadLimitKbps) || 0),
          uploadLimitKbps: Math.max(0, Math.floor(uploadLimitKbps) || 0),
          report: (msg) => logger.info(`[夜间备份][群][${groupId}] ${msg}`),
        })
        logger.info(`[夜间备份][群][${groupId}] 完成`)
        lastError = undefined
        break
      } catch (error: any) {
        const msg = error instanceof Error ? error.message : String(error)
        if (msg.includes('正在进行中')) {
          logger.info(`[夜间备份][群][${groupId}] 同步任务正在进行中，跳过`)
          lastError = undefined
          break
        }
        lastError = error
        continue
      }
    }

    if (lastError) {
      logger.error(lastError)
      logger.info(`[夜间备份][群][${groupId}] 失败（已记录日志），继续下一个群`)
    }
  }

  logger.info('[夜间备份][群] 结束')
}

/**
 * 供 apps 层的 `karin.task` 调用：每秒检查一次各群的 schedule cron，并触发对应同步任务。
 * - 为避免同一秒内重复触发，对每个 (groupId, cron) 记录触发秒级时间戳。
 */
export const runGroupFileSyncSchedulerTick = async () => {
  const cfg = config()
  if (cfg?.scheduler?.enabled === false) return
  if (cfg?.scheduler?.groupSync?.enabled === false) return
  const targets = Array.isArray(cfg.groupSyncTargets) ? cfg.groupSyncTargets : []
  const now = new Date()
  const stamp = Math.floor(now.getTime() / 1000)

  for (const target of targets) {
    const groupId = String((target as any)?.groupId ?? '').trim()
    if (!groupId) continue
    if ((target as any)?.enabled === false) continue
    if ((target as any)?.schedule?.enabled !== true) continue

    const cron = String((target as any)?.schedule?.cron ?? '').trim()
    if (!cron) continue

    if (!cronMatches(cron, now)) continue

    const key = `${groupId}:${cron}`
    if (lastTriggered.get(key) === stamp) continue
    lastTriggered.set(key, stamp)

    if (!isNowAllowed((target as any)?.timeWindows, now)) {
      logger.info(`[群文件定时同步][${groupId}] 命中cron但不在时段内，跳过`)
      continue
    }

    void runScheduledSync(groupId)
  }
}

