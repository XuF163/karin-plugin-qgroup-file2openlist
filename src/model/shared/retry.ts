type RetryOptions = {
  retries: number
  delaysMs?: number[]
  isRetryable?: (error: unknown) => boolean
}

export const retryAsync = async <T>(
  fn: () => Promise<T>,
  options: RetryOptions,
) => {
  const retries = Math.max(0, Math.floor(options.retries) || 0)
  const delaysMs = options.delaysMs?.length ? options.delaysMs : [300, 900, 2_000]
  const isRetryable = options.isRetryable ?? (() => true)

  let lastError: unknown
  for (let attempt = 0; attempt <= retries; attempt++) {
    try {
      return await fn()
    } catch (error) {
      lastError = error
      if (attempt >= retries || !isRetryable(error)) throw error
      const delay = delaysMs[Math.min(attempt, delaysMs.length - 1)]
      await new Promise(resolve => setTimeout(resolve, delay))
    }
  }

  throw lastError
}
