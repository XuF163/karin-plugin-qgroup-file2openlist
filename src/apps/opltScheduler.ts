import { karin, logger } from 'node-karin'
import { cronMatches } from '@/model/groupSync/scheduler'
import { backupOpenListToOpenListCore } from '@/model/openlist'
import {
  DEFAULT_OPLT_NIGHTLY_APPEND_HOST_DIR,
  DEFAULT_OPLT_NIGHTLY_CRON,
  DEFAULT_OPLT_NIGHTLY_MODE,
  DEFAULT_OPLT_NIGHTLY_TRANSPORT,
  readOpltData,
  resolveOpltMapping,
  writeOpltData,
} from '@/model/oplt'
import { formatErrorMessage } from '@/model/shared/errors'
import type { OpenListBackupTransport, SyncMode } from '@/model/groupFiles/types'

const lastTriggered = new Map<string, number>()
let queue: Promise<void> = Promise.resolve()

const runOpltNightlyForUser = async (userKey: string) => {
  const data = readOpltData()
  const user = data.users[userKey]
  if (!user) return

  const nightly = user.nightly
  if (!nightly || nightly.enabled !== true) return

  const mode: SyncMode = (nightly.mode === 'full' || nightly.mode === 'incremental') ? nightly.mode : DEFAULT_OPLT_NIGHTLY_MODE
  const transport: OpenListBackupTransport = (nightly.transport === 'api' || nightly.transport === 'webdav' || nightly.transport === 'auto')
    ? nightly.transport
    : DEFAULT_OPLT_NIGHTLY_TRANSPORT
  const appendHostDir = typeof nightly.appendHostDir === 'boolean' ? nightly.appendHostDir : DEFAULT_OPLT_NIGHTLY_APPEND_HOST_DIR

  if (!user.oplts.length) {
    user.nightly = {
      ...nightly,
      lastRunAt: Date.now(),
      lastResult: { ok: 0, skipped: 0, fail: 0 },
    }
    writeOpltData(data)
    logger.info(`[oplt夜间][${userKey}] oplts 为空，跳过`)
    return
  }

  logger.info(`[oplt夜间][${userKey}] 开始：${user.oplts.length} 条（${mode}/${transport}）`)

  let sumOk = 0
  let sumSkipped = 0
  let sumFail = 0

  for (let i = 0; i < user.oplts.length; i++) {
    const item = user.oplts[i]
    const idx = i + 1
    try {
      const { sourceBaseUrl, srcDir, toDir } = resolveOpltMapping({ left: item.left, right: item.right })
      logger.info(`[oplt夜间][${userKey}][${idx}] ${sourceBaseUrl}${srcDir === '/' ? '' : srcDir} -> ${toDir}`)
      const res = await backupOpenListToOpenListCore({
        sourceBaseUrl,
        srcDir,
        toDir,
        mode,
        transport,
        appendHostDir,
      })
      sumOk += res.ok
      sumSkipped += res.skipped
      sumFail += res.fail
    } catch (error: any) {
      const msg = formatErrorMessage(error)
      if (msg.includes('正在进行中')) {
        logger.info(`[oplt夜间][${userKey}][${idx}] 任务正在进行中，跳过`)
        continue
      }
      logger.error(`[oplt夜间][${userKey}][${idx}] 失败：${msg}`)
      sumFail += 1
    }
  }

  user.nightly = {
    ...nightly,
    lastRunAt: Date.now(),
    lastResult: { ok: sumOk, skipped: sumSkipped, fail: sumFail },
  }
  writeOpltData(data)

  logger.info(`[oplt夜间][${userKey}] 完成：ok=${sumOk} skip=${sumSkipped} fail=${sumFail}`)
}

/**
 * 每秒 tick：检查每个用户的 nightly.cron，命中则串行触发一次备份。
 */
export const opltsNightlyScheduler = karin.task(
  'oplt夜间备份调度器',
  '* * * * * *',
  () => {
    const data = readOpltData()
    const now = new Date()
    const stamp = Math.floor(now.getTime() / 1000)

    for (const [userKey, user] of Object.entries(data.users)) {
      if (!user?.nightly || user.nightly.enabled !== true) continue

      const cron = String(user.nightly.cron ?? DEFAULT_OPLT_NIGHTLY_CRON).trim() || DEFAULT_OPLT_NIGHTLY_CRON
      if (!cronMatches(cron, now)) continue

      const key = `${userKey}:${cron}`
      if (lastTriggered.get(key) === stamp) continue
      lastTriggered.set(key, stamp)

      queue = queue
        .then(() => runOpltNightlyForUser(userKey))
        .catch((error) => logger.error(error))
    }
  },
  { log: false, type: 'skip' },
)

