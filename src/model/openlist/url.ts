/**
 * OpenList URL / 鉴权相关工具。
 */

const trimTrailingSlashes = (value: string) => String(value ?? '').trim().replace(/\/+$/, '')

export const buildOpenListDavBaseUrl = (baseUrl: string) => {
  const normalized = trimTrailingSlashes(baseUrl)
  if (!normalized) return ''
  return `${normalized}/dav`
}

export const buildOpenListApiBaseUrl = (baseUrl: string) => {
  const normalized = trimTrailingSlashes(baseUrl)
  if (!normalized) return ''
  return `${normalized}/api`
}

export const isSameOriginUrl = (a: string, b: string) => {
  try {
    return new URL(a).origin === new URL(b).origin
  } catch {
    return false
  }
}

/**
 * OpenList API token 只应在同源 raw_url 请求中携带，避免跨域泄露。
 */
export const buildOpenListRawUrlAuthHeaders = (params: { rawUrl: string, baseUrl: string, token: string }) => {
  const { rawUrl, baseUrl, token } = params
  if (!token) return undefined
  if (!isSameOriginUrl(rawUrl, baseUrl)) return undefined
  return { Authorization: token }
}

/**
 * OpenList WebDAV BasicAuth Header
 */
export const buildOpenListAuthHeader = (username: string, password: string) => {
  const user = String(username ?? '')
  const pass = String(password ?? '')
  const token = Buffer.from(`${user}:${pass}`).toString('base64')
  return `Basic ${token}`
}

