import { normalizePosixPath } from '@/model/shared/path'

const stripHashAndQuery = (value: string) => {
  const idxHash = value.indexOf('#')
  const idxQuery = value.indexOf('?')
  const idx = Math.min(idxHash === -1 ? Infinity : idxHash, idxQuery === -1 ? Infinity : idxQuery)
  return idx === Infinity ? value : value.slice(0, idx)
}

const normalizeMaybeHttpUrl = (value: string) => {
  const trimmed = stripHashAndQuery(String(value ?? '').trim().replaceAll('\\', '/'))
  if (!trimmed) return ''
  if (/^https?:\/\//i.test(trimmed)) return trimmed
  return `https://${trimmed}`
}

const normalizeMaybePosixPath = (value: string) => normalizePosixPath(String(value ?? '').trim().replaceAll('\\', '/'))

export const resolveOpltMapping = (params: { left: string, right: string }) => {
  const leftRaw = String(params.left ?? '').trim()
  const rightRaw = String(params.right ?? '').trim()
  if (!leftRaw) throw new Error('A 不能为空')
  if (!rightRaw) throw new Error('B 不能为空')

  const leftUrl = normalizeMaybeHttpUrl(leftRaw)
  let sourceBaseUrl: string
  let srcDir: string
  try {
    const u = new URL(leftUrl)
    sourceBaseUrl = u.origin
    srcDir = normalizeMaybePosixPath(u.pathname || '/')
  } catch {
    throw new Error(`A 不是有效的 URL：${leftRaw}`)
  }

  let toDir: string
  if (/^https?:\/\//i.test(rightRaw)) {
    try {
      const u = new URL(normalizeMaybeHttpUrl(rightRaw))
      toDir = normalizeMaybePosixPath(u.pathname || '/')
    } catch {
      throw new Error(`B 不是有效的 URL：${rightRaw}`)
    }
  } else {
    toDir = normalizeMaybePosixPath(rightRaw)
  }

  return { sourceBaseUrl, srcDir, toDir }
}

