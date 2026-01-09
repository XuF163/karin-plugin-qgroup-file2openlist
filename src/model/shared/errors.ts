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

export const fetchTextSafely = async (res: Response, maxLen = 500) => {
  try {
    const text = await res.text()
    return text.slice(0, maxLen)
  } catch {
    return ''
  }
}
