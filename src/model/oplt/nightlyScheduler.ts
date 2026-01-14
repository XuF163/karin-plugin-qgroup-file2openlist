import { logger } from 'node-karin'
import { config } from '@/utils'
import { backupOpenListToOpenListCore } from '@/model/openlist'
import {
  DEFAULT_OPLT_NIGHTLY_APPEND_HOST_DIR,
  DEFAULT_OPLT_NIGHTLY_TRANSPORT,
  readOpltData,
  writeOpltData,
} from './store'
import { resolveOpltMapping } from './parse'
import { formatErrorMessage } from '@/model/shared/errors'
import type { OpenListBackupTransport, SyncMode } from '@/model/groupFiles/types'

const NIGHTLY_MODE: SyncMode = 'incremental'
const NIGHTLY_TIMEOUT_SEC = 3000

const runOpltNightlyForUser = async (data: ReturnType<typeof readOpltData>, userKey: string) => {
  const user = data.users[userKey]
  if (!user) return

  const nightly = user.nightly ?? {}

  const transport: OpenListBackupTransport = (nightly.transport === 'api' || nightly.transport === 'webdav' || nightly.transport === 'auto')
    ? nightly.transport
    : DEFAULT_OPLT_NIGHTLY_TRANSPORT
  const appendHostDir = typeof nightly.appendHostDir === 'boolean' ? nightly.appendHostDir : DEFAULT_OPLT_NIGHTLY_APPEND_HOST_DIR

  if (!user.oplts.length) {
    user.nightly = {
      ...nightly,
      enabled: true,
      lastRunAt: Date.now(),
      lastResult: { ok: 0, skipped: 0, fail: 0 },
    }
    logger.info(`[oplt夜间][${userKey}] oplts 为空，跳过`)
    return
  }

  logger.info(`[oplt夜间][${userKey}] 开始：${user.oplts.length} 条（${NIGHTLY_MODE}/${transport} timeout=${NIGHTLY_TIMEOUT_SEC}s）`)

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
        mode: NIGHTLY_MODE,
        transport,
        appendHostDir,
        timeoutSec: NIGHTLY_TIMEOUT_SEC,
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
    enabled: true,
    lastRunAt: Date.now(),
    lastResult: { ok: sumOk, skipped: sumSkipped, fail: sumFail },
  }

  logger.info(`[oplt夜间][${userKey}] 完成：ok=${sumOk} skip=${sumSkipped} fail=${sumFail}`)
}

/**
 * 夜间自动备份：对所有用户的 oplts 做一次备份（固定：增量 + timeout=3000s）。
 * - 由 `src/apps/scheduler.ts` 定时触发（默认每天 02:00）
 */
export const runNightlyOpltBackup = async () => {
  const cfg = config()
  if (cfg?.scheduler?.enabled === false) return
  if (cfg?.scheduler?.opltNightly?.enabled === false) return

  const data = readOpltData()
  const userKeys = Object.keys(data.users)
  if (!userKeys.length) return

  logger.info(`[夜间备份][oplt] 开始：${userKeys.length} 个用户（模式=${NIGHTLY_MODE} 超时=${NIGHTLY_TIMEOUT_SEC}s）`)

  for (const userKey of userKeys) {
    try {
      await runOpltNightlyForUser(data, userKey)
      writeOpltData(data)
    } catch (error) {
      logger.error(error)
    }
  }

  logger.info('[夜间备份][oplt] 结束')
}
