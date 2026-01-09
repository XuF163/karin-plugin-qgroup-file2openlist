/**
 * 与 OpenList/WebDAV/文件路径相关的通用工具。
 * - OpenList / WebDAV 通常使用 POSIX 风格路径：/a/b/c
 */

export const normalizePosixPath = (
  inputPath: string,
  { ensureLeadingSlash = true, stripTrailingSlash = true } = {},
) => {
  let value = String(inputPath ?? '').trim().replaceAll('\\', '/')
  value = value.replace(/\/+/g, '/')
  if (!value) value = '/'
  if (ensureLeadingSlash && !value.startsWith('/')) value = `/${value}`
  if (stripTrailingSlash && value.length > 1) value = value.replace(/\/+$/, '')
  return value
}

export const safePathSegment = (input: string) => {
  const value = String(input ?? '')
    .replaceAll('\0', '')
    .replaceAll('\\', '_')
    .replaceAll('/', '_')
    .trim()
  if (!value || value === '.' || value === '..') return 'unnamed'
  return value
}

export const encodePathForUrl = (posixPath: string) => {
  const normalized = normalizePosixPath(posixPath)
  const segments = normalized.split('/').filter(Boolean).map(encodeURIComponent)
  return `/${segments.join('/')}`
}

