import fs from 'node:fs'
import path from 'node:path'
import { randomUUID } from 'node:crypto'
import { pipeline } from 'node:stream/promises'
import { dir } from '@/dir'
import { ensureDir } from '@/model/shared/fsJson'
import { config } from '@/utils'

const DEFAULT_LARGE_FILE_SPOOL_THRESHOLD_MB = 200
const MAX_THRESHOLD_MB = 50_000

let cachedThresholdBytes = DEFAULT_LARGE_FILE_SPOOL_THRESHOLD_MB * 1024 * 1024
let cachedAt = 0
const CACHE_MS = 1_000

const clampInt = (value: number, min: number, max: number) => Math.min(max, Math.max(min, Math.floor(value)))

export const parseContentLengthHeader = (value: string | null) => {
  const raw = String(value ?? '').trim()
  if (!raw) return undefined
  const n = Number(raw)
  if (!Number.isFinite(n)) return undefined
  const v = Math.floor(n)
  if (v <= 0) return undefined
  return v
}

/**
 * 大文件落盘阈值（字节）。
 * - 默认 200MB
 * - 配置：resourceLimits.largeFileSpoolThresholdMB
 *   - <=0：禁用落盘（始终流式转发）
 */
export const getLargeFileSpoolThresholdBytes = () => {
  const now = Date.now()
  if (now - cachedAt < CACHE_MS) return cachedThresholdBytes
  cachedAt = now

  try {
    const cfg: any = config()
    const raw = cfg?.resourceLimits?.largeFileSpoolThresholdMB
    if (raw === undefined || raw === null || raw === '') {
      cachedThresholdBytes = DEFAULT_LARGE_FILE_SPOOL_THRESHOLD_MB * 1024 * 1024
      return cachedThresholdBytes
    }

    const n = typeof raw === 'number' ? raw : Number(raw)
    if (!Number.isFinite(n)) {
      cachedThresholdBytes = DEFAULT_LARGE_FILE_SPOOL_THRESHOLD_MB * 1024 * 1024
      return cachedThresholdBytes
    }

    if (n <= 0) {
      cachedThresholdBytes = 0
      return cachedThresholdBytes
    }

    const mb = clampInt(n, 1, MAX_THRESHOLD_MB)
    cachedThresholdBytes = mb * 1024 * 1024
    return cachedThresholdBytes
  } catch {
    cachedThresholdBytes = DEFAULT_LARGE_FILE_SPOOL_THRESHOLD_MB * 1024 * 1024
    return cachedThresholdBytes
  }
}

export const shouldSpoolToDisk = (sizeBytes: number | undefined) => {
  const threshold = getLargeFileSpoolThresholdBytes()
  if (!threshold) return false
  if (typeof sizeBytes !== 'number' || !Number.isFinite(sizeBytes)) return false
  return Math.floor(sizeBytes) >= threshold
}

const safeFileName = (value: string) => {
  return String(value ?? '')
    .replaceAll('\0', '')
    .replace(/[^\w.-]+/g, '_')
    .replace(/^_+/, '')
    .slice(0, 50) || 'transfer'
}

export const getTransferTmpDir = () => {
  const tmpDir = path.join(dir.DataDir, 'transfer-tmp')
  ensureDir(tmpDir)
  return tmpDir
}

export const createTransferTempFilePath = (prefix: string) => {
  const tmpDir = getTransferTmpDir()
  const name = `${safeFileName(prefix)}-${Date.now()}-${randomUUID()}.tmp`
  return path.join(tmpDir, name)
}

export const streamToFile = async (readable: NodeJS.ReadableStream, filePath: string, options?: { signal?: AbortSignal }) => {
  await pipeline(readable, fs.createWriteStream(filePath), { signal: options?.signal })
}

export const safeUnlink = async (filePath: string) => {
  try {
    await fs.promises.unlink(filePath)
  } catch {}
}

