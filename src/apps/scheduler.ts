import { karin, logger } from 'node-karin'
import { config } from '@/utils'
import { runNightlyGroupBackup } from '@/model/groupSync'
import { runNightlyOpltBackup } from '@/model/oplt'

const DEFAULT_SCHEDULER_CRON = '0 0 2 * * *'

const normalizeTickCron = (value: unknown) => {
  const raw = String(value ?? '').trim()
  if (!raw) return DEFAULT_SCHEDULER_CRON
  const parts = raw.split(/\s+/).filter(Boolean)
  const normalized = (() => {
    if (parts.length === 6) return parts.join(' ')
    if (parts.length === 5) return `0 ${parts.join(' ')}`
    return ''
  })()

  // 兼容旧版本的 tick 默认值（每秒触发），避免升级后高频执行夜间备份。
  if (normalized === '* * * * * *') return DEFAULT_SCHEDULER_CRON
  if (normalized) return normalized
  return DEFAULT_SCHEDULER_CRON
}

const getTickCron = () => {
  try {
    return normalizeTickCron((config() as any)?.scheduler?.tickCron)
  } catch (error) {
    logger.error(error)
    return DEFAULT_SCHEDULER_CRON
  }
}

/**
 * 夜间自动备份统一调度器（默认每天 02:00）：
 * - 群文件：对已开启 uploadBackup 的群做一次增量同步
 * - oplts：对所有用户的 oplts 做一次增量备份
 *
 * 固定策略：先群后 oplts，统一增量，单文件超时 3000s（不再由用户命令配置）。
 */
let running = false
export const scheduler = karin.task(
  '夜间自动备份调度器',
  getTickCron(),
  async () => {
    try {
      const cfg = config()
      if (cfg?.scheduler?.enabled === false) return
    } catch (error) {
      logger.error(error)
      return
    }

    if (running) {
      logger.info('[夜间备份] 上一次任务仍在进行中，已跳过本次触发')
      return
    }

    running = true
    logger.info('[夜间备份] 触发')
    try {
      try {
        await runNightlyGroupBackup()
      } catch (error) {
        logger.error(error)
      }

      try {
        await runNightlyOpltBackup()
      } catch (error) {
        logger.error(error)
      }
    } finally {
      running = false
      logger.info('[夜间备份] 结束')
    }
  },
  { log: false, type: 'skip' },
)
