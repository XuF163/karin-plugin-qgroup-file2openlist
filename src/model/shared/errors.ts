/**
 * 通用错误处理工具。
 * - 尽量把各种 unknown error 统一转换成可读信息，便于日志与回复。
 */

export const formatErrorMessage = (error: unknown) => {
  if (error instanceof Error) {
    const base = error.message || String(error)
    const cause: any = (error as any).cause
    if (cause) {
      const causeMsg = cause instanceof Error ? cause.message : String(cause)
      const causeCode = typeof cause === 'object' && cause && 'code' in cause ? String((cause as any).code) : ''
      const extra = [causeCode, causeMsg].filter(Boolean).join(' ')
      if (extra && extra !== base) return `${base} (${extra})`
    }
    return base
  }
  return String(error)
}

export const isAbortError = (error: unknown) => {
  return Boolean(error && typeof error === 'object' && 'name' in (error as any) && (error as any).name === 'AbortError')
}

/**
 * 主动读取并丢弃响应体，避免在大量请求场景下因未消费 body 导致连接/内存占用累积。
 */
export const drainResponseBody = async (res: Response) => {
  try {
    const body = res.body
    if (!body) return

    const reader = body.getReader()
    while (true) {
      const { done } = await reader.read()
      if (done) break
    }
  } catch {
    try {
      await res.body?.cancel()
    } catch {}
  }
}

export const fetchTextSafely = async (res: Response, maxLen = 500) => {
  try {
    const maxBytes = Math.max(0, Math.floor(maxLen) || 0)
    if (!maxBytes) {
      await res.body?.cancel()
      return ''
    }

    const body = res.body
    if (!body) return ''

    const reader = body.getReader()
    const chunks: Uint8Array[] = []
    let total = 0

    while (total < maxBytes) {
      const { value, done } = await reader.read()
      if (done) break
      if (!value || value.byteLength <= 0) continue

      const remaining = maxBytes - total
      if (value.byteLength <= remaining) {
        chunks.push(value)
        total += value.byteLength
        continue
      }

      chunks.push(value.slice(0, remaining))
      total += remaining
      break
    }

    try {
      await reader.cancel()
    } catch {}

    if (!chunks.length) return ''
    return Buffer.concat(chunks.map(c => Buffer.from(c))).toString('utf8')
  } catch {
    try {
      await res.body?.cancel()
    } catch {}
    return ''
  }
}
