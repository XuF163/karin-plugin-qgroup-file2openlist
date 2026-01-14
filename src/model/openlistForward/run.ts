import { formatErrorMessage } from '@/model/shared/errors'
import { backupOpenListToOpenListCore } from '@/model/openlist'
import { updateOpenListForwardRule } from './store'
import type { OpenListForwardRule, SyncMode } from './types'

const active = new Set<string>()

export const runOpenListForwardRule = async (params: {
  rule: OpenListForwardRule
  mode?: SyncMode
  report?: (message: string) => Promise<void> | void
}) => {
  const rule = params.rule
  const key = String(rule.id)
  if (active.has(key)) throw new Error('该规则正在执行中，请稍后再试。')
  active.add(key)

  const startedAt = Date.now()
  try {
    const mode: SyncMode = params.mode ?? (rule.mode ?? 'full')

    const res = await backupOpenListToOpenListCore({
      sourceBaseUrl: rule.sourceBaseUrl,
      // 二期：允许源端账号（可选）
      sourceUsername: rule.sourceUsername,
      sourcePassword: rule.sourcePassword,
      srcDir: rule.srcDir,
      toDir: rule.toDir,
      concurrency: rule.concurrency,
      scanConcurrency: rule.scanConcurrency,
      perPage: rule.perPage,
      timeoutSec: rule.timeoutSec,
      mode,
      transport: rule.transport,
      report: params.report,
    })

    updateOpenListForwardRule(rule.id, {
      lastRunAt: Date.now(),
      lastResult: res,
    })

    return {
      ok: res.ok,
      skipped: res.skipped,
      fail: res.fail,
      ms: Date.now() - startedAt,
    }
  } catch (error) {
    updateOpenListForwardRule(rule.id, {
      lastRunAt: Date.now(),
      lastResult: { ok: 0, skipped: 0, fail: 1 },
    })
    throw new Error(`规则执行失败：${formatErrorMessage(error)}`)
  } finally {
    active.delete(key)
  }
}

