/**
 * 并发控制工具。
 * - 为了避免“重构改变行为”，这里保持与原实现一致。
 */

import { formatErrorMessage } from './errors'

export const runWithConcurrency = async <T>(
  items: T[],
  concurrency: number,
  fn: (item: T, index: number) => Promise<void>,
) => {
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

export const runWithAdaptiveConcurrency = async <T>(
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
