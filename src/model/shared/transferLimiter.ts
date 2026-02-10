import { logger } from 'node-karin'
import { config } from '@/utils'

type Waiter = {
  resolve: () => void
  label?: string
  enqueuedAt: number
}

let active = 0
const queue: Waiter[] = []
let lastQueueLogAt = 0

const clampInt = (value: number, min: number, max: number) => Math.min(max, Math.max(min, Math.floor(value)))

let cachedLimit = 1
let cachedAt = 0
const CACHE_MS = 1_000

/**
 * 获取全局传输并发上限。
 * - <=0：不限制
 * - 未配置：默认 1（串行，最省内存）
 */
export const getGlobalTransferConcurrencyLimit = () => {
  const now = Date.now()
  if (now - cachedAt < CACHE_MS) return cachedLimit
  cachedAt = now

  try {
    const cfg: any = config()
    const raw = cfg?.resourceLimits?.transferConcurrency
    if (raw === undefined || raw === null || raw === '') return (cachedLimit = 1)

    const n = typeof raw === 'number' ? raw : Number(raw)
    if (!Number.isFinite(n)) return (cachedLimit = 1)
    if (n <= 0) return (cachedLimit = Number.POSITIVE_INFINITY)
    return (cachedLimit = clampInt(n, 1, 50))
  } catch {
    return (cachedLimit = 1)
  }
}

const logQueue = () => {
  if (!queue.length) return
  const now = Date.now()
  if (now - lastQueueLogAt < 30_000) return
  lastQueueLogAt = now

  const limit = getGlobalTransferConcurrencyLimit()
  const limitText = Number.isFinite(limit) ? String(limit) : '无限制'
  const first = queue[0]
  const waitedMs = first ? Math.max(0, now - first.enqueuedAt) : 0
  logger.warn(`[传输限流] 队列中=${queue.length} 运行中=${active} 上限=${limitText} 最早等待=${waitedMs}ms${first?.label ? ` label=${first.label}` : ''}`)
}

const drain = () => {
  const limit = getGlobalTransferConcurrencyLimit()
  while (active < limit && queue.length) {
    active++
    queue.shift()!.resolve()
  }
  logQueue()
}

const acquire = async (label?: string) => {
  const limit = getGlobalTransferConcurrencyLimit()
  if (active < limit && queue.length === 0) {
    active++
    return
  }

  await new Promise<void>((resolve) => {
    queue.push({ resolve, label, enqueuedAt: Date.now() })
    drain()
  })
}

const release = () => {
  active = Math.max(0, active - 1)
  drain()
}

/**
 * 将“单个文件”的下载+上传视为一个全局传输单元，按配置进行并发限制。
 * - 用于避免同时上传/下载多个文件导致内存占用过高。
 */
export const withGlobalTransferLimit = async <T>(label: string, fn: () => Promise<T>): Promise<T> => {
  await acquire(label)
  try {
    return await fn()
  } finally {
    release()
  }
}
