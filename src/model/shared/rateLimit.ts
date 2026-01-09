import { Transform } from 'node:stream'
import { setTimeout as sleep } from 'node:timers/promises'

/**
 * 为流创建一个简单限速 Transform。
 * - bytesPerSec <= 0 时返回 null
 */
export const createThrottleTransform = (bytesPerSec: number) => {
  const limit = Math.floor(bytesPerSec || 0)
  if (!Number.isFinite(limit) || limit <= 0) return null

  let nextTime = Date.now()
  const msPerByte = 1000 / limit

  return new Transform({
    transform(chunk, _enc, cb) {
      void (async () => {
        const size = Buffer.isBuffer(chunk) ? chunk.length : Buffer.byteLength(chunk)
        const now = Date.now()
        const start = Math.max(now, nextTime)
        nextTime = start + size * msPerByte
        const waitMs = start - now
        if (waitMs > 0) await sleep(waitMs)
        cb(null, chunk)
      })().catch((err) => cb(err as any))
    },
  })
}

