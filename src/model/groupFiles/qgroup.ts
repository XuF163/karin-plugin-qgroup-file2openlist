import { setTimeout as sleep } from 'node:timers/promises'
import { logger } from 'node-karin'
import { formatErrorMessage } from '@/model/shared/errors'
import type { ExportedGroupFile } from './types'

const pickFirstString = (...values: unknown[]) => {
  for (const value of values) {
    if (typeof value === 'string' && value) return value
  }
}

const pickFirstNumber = (...values: unknown[]) => {
  for (const value of values) {
    if (typeof value === 'number' && Number.isFinite(value)) return value
    if (typeof value === 'string' && value && Number.isFinite(Number(value))) return Number(value)
  }
}

export const normalizeGroupFileRelativePath = (input: string) => {
  return String(input ?? '')
    .replaceAll('\0', '')
    .replaceAll('\\', '/')
    .trim()
    .replace(/^\/+/, '')
    .replace(/\/+$/, '')
}

export const getGroupFileListCompat = async (bot: any, groupId: string, folderId?: string) => {
  const groupNum = Number(groupId)
  if (Number.isFinite(groupNum)) {
    const onebot = bot?._onebot
    if (!folderId && typeof onebot?.getGroupRootFiles === 'function') {
      try {
        const res = await onebot.getGroupRootFiles(groupNum)
        return {
          files: Array.isArray(res?.files) ? res.files : [],
          folders: Array.isArray(res?.folders) ? res.folders : [],
        }
      } catch (error) {
        logger.debug(`[群文件导出] onebot.getGroupRootFiles 调用失败，将尝试其它接口: ${formatErrorMessage(error)}`)
      }
    }

    if (folderId && typeof onebot?.getGroupFilesByFolder === 'function') {
      try {
        const res = await onebot.getGroupFilesByFolder(groupNum, folderId)
        return {
          files: Array.isArray(res?.files) ? res.files : [],
          folders: Array.isArray(res?.folders) ? res.folders : [],
        }
      } catch (error) {
        logger.debug(`[群文件导出] onebot.getGroupFilesByFolder 调用失败，将尝试其它接口: ${formatErrorMessage(error)}`)
      }
    }
  }

  if (typeof bot?.getGroupFileList === 'function') {
    try {
      const res = await bot.getGroupFileList(groupId, folderId)
      return {
        files: Array.isArray(res?.files) ? res.files : [],
        folders: Array.isArray(res?.folders) ? res.folders : [],
      }
    } catch (error) {
      logger.debug(`[群文件导出] getGroupFileList 调用失败，将尝试 OneBot 扩展: ${String(error)}`)
    }
  }

  if (!Number.isFinite(groupNum)) {
    throw new Error('群号无法转换为 number，且当前适配器不支持 getGroupFileList')
  }

  if (!folderId && typeof bot?.getGroupRootFiles === 'function') {
    const res = await bot.getGroupRootFiles(groupNum)
    return {
      files: Array.isArray(res?.files) ? res.files : [],
      folders: Array.isArray(res?.folders) ? res.folders : [],
    }
  }

  if (folderId && typeof bot?.getGroupFilesByFolder === 'function') {
    const res = await bot.getGroupFilesByFolder(groupNum, folderId)
    return {
      files: Array.isArray(res?.files) ? res.files : [],
      folders: Array.isArray(res?.folders) ? res.folders : [],
    }
  }

  throw new Error('当前适配器不支持获取群文件列表（getGroupFileList / getGroupRootFiles / getGroupFilesByFolder 均不可用）')
}

type ResolveGroupFileUrlOptions = {
  retries?: number
  delayMs?: number
  maxDelayMs?: number
}

const isRetryableResolveGroupFileUrlError = (error: unknown) => {
  const msg = formatErrorMessage(error)

  // 明确不会通过重试解决的问题
  if (/缺少 fileId|群号无法转换为 number|未找到可用接口/.test(msg)) return false
  if (/不存在|not found|file not found/i.test(msg)) return false

  // 常见的临时性/限流类错误（NapCat/OneBot/网络抖动）
  return /请求错误|sendApi|限流|频繁|风控|rate|limit|429|Too Many Requests|getFileUrl|nc_getFile|getGroupFileUrl|超时|timeout|ECONNRESET|ETIMEDOUT|EAI_AGAIN|ENOTFOUND|ECONNREFUSED|UND_ERR|socket hang up/i.test(msg)
}

const resolveGroupFileUrlOnce = async (bot: any, contact: any, groupId: string, file: ExportedGroupFile) => {
  if (!file.fileId) throw new Error('缺少 fileId')

  const reasons: string[] = []

  if (typeof bot?.getFileUrl === 'function') {
    try {
      const url = await bot.getFileUrl(contact, file.fileId)
      if (typeof url === 'string' && url) return url
      reasons.push('getFileUrl 返回空值')
    } catch (error: any) {
      reasons.push(`getFileUrl: ${error?.message ?? String(error)}`)
    }
  }

  const groupNum = Number(groupId)
  if (!Number.isFinite(groupNum)) {
    throw new Error(reasons[0] ?? '群号无法转换为 number')
  }

  const onebot = bot?._onebot
  if (typeof onebot?.nc_getFile === 'function') {
    try {
      const res = await onebot.nc_getFile(file.fileId)
      if (typeof res?.url === 'string' && res.url) return res.url
      reasons.push('nc_getFile 返回空值')
    } catch (error: any) {
      reasons.push(`nc_getFile: ${error?.message ?? String(error)}`)
    }
  }

  if (typeof onebot?.getGroupFileUrl === 'function') {
    try {
      const res = await onebot.getGroupFileUrl(groupNum, file.fileId, file.busid)
      if (typeof res?.url === 'string' && res.url) return res.url
      reasons.push('onebot.getGroupFileUrl 返回空值')
    } catch (error: any) {
      reasons.push(`onebot.getGroupFileUrl: ${error?.message ?? String(error)}`)
    }

    try {
      const res = await onebot.getGroupFileUrl(groupNum, file.fileId)
      if (typeof res?.url === 'string' && res.url) return res.url
    } catch {}
  }

  if (typeof bot?.getGroupFileUrl === 'function') {
    try {
      const res = await bot.getGroupFileUrl(groupNum, file.fileId, file.busid)
      if (typeof res?.url === 'string' && res.url) return res.url
      reasons.push('getGroupFileUrl 返回空值')
    } catch (error: any) {
      reasons.push(`getGroupFileUrl: ${error?.message ?? String(error)}`)
    }

    try {
      const res = await bot.getGroupFileUrl(groupNum, file.fileId)
      if (typeof res?.url === 'string' && res.url) return res.url
    } catch (error: any) {
      reasons.push(`getGroupFileUrl(no busid): ${error?.message ?? String(error)}`)
    }
  }

  throw new Error(reasons[0] ?? '无法获取下载 URL（未找到可用接口）')
}

export const resolveGroupFileUrl = async (
  bot: any,
  contact: any,
  groupId: string,
  file: ExportedGroupFile,
  options?: ResolveGroupFileUrlOptions,
) => {
  const retries = Math.max(0, Math.floor(options?.retries ?? 2))
  const delayMs = Math.max(0, Math.floor(options?.delayMs ?? 1200))
  const maxDelayMs = Math.max(delayMs, Math.floor(options?.maxDelayMs ?? 15_000))

  let lastError: unknown
  for (let attempt = 0; attempt <= retries; attempt++) {
    try {
      return await resolveGroupFileUrlOnce(bot, contact, groupId, file)
    } catch (error) {
      lastError = error
      if (attempt >= retries) break
      if (!isRetryableResolveGroupFileUrlError(error)) break

      const backoff = delayMs * Math.pow(2, attempt)
      const capped = Math.min(maxDelayMs, backoff)
      const jittered = Math.floor(capped * (0.8 + Math.random() * 0.4))
      if (jittered > 0) await sleep(jittered)
    }
  }

  if (lastError) throw lastError
  throw new Error('无法获取下载 URL')
}

export const collectAllGroupFiles = async (bot: any, groupId: string, startFolderId?: string) => {
  const files: ExportedGroupFile[] = []
  const visitedFolders = new Set<string>()

  const walk = async (folderId: string | undefined, prefix: string) => {
    if (folderId) {
      if (visitedFolders.has(folderId)) return
      visitedFolders.add(folderId)
    }

    const { files: rawFiles, folders: rawFolders } = await getGroupFileListCompat(bot, groupId, folderId)

    for (const raw of rawFiles) {
      const fileId = pickFirstString(raw?.fid, raw?.file_id, raw?.fileId, raw?.id)
      const name = pickFirstString(raw?.name, raw?.file_name, raw?.fileName) ?? (fileId ? `file-${fileId}` : 'unknown-file')
      const filePath = prefix ? `${prefix}/${name}` : name

      files.push({
        path: filePath,
        fileId: fileId ?? '',
        name,
        size: pickFirstNumber(raw?.size, raw?.file_size, raw?.fileSize),
        uploadTime: pickFirstNumber(raw?.uploadTime, raw?.upload_time),
        uploaderId: pickFirstString(raw?.uploadId, raw?.uploader, raw?.uploader_id),
        uploaderName: pickFirstString(raw?.uploadName, raw?.uploader_name),
        md5: pickFirstString(raw?.md5),
        sha1: pickFirstString(raw?.sha1),
        sha3: pickFirstString(raw?.sha3),
        busid: pickFirstNumber(raw?.busid, raw?.busId),
      })
    }

    for (const raw of rawFolders) {
      const folderId = pickFirstString(raw?.id, raw?.folder_id, raw?.folderId)
      if (!folderId) continue
      const folderName = pickFirstString(raw?.name, raw?.folder_name, raw?.folderName) ?? folderId
      const nextPrefix = prefix ? `${prefix}/${folderName}` : folderName
      await walk(folderId, nextPrefix)
    }
  }

  await walk(startFolderId, '')
  return files
}

const locateGroupFileById = async (
  bot: any,
  groupId: string,
  fileId: string,
  options?: { timeoutMs?: number, maxFolders?: number, expectedName?: string, expectedSize?: number },
) => {
  const timeoutMs = Math.max(1000, Math.floor(options?.timeoutMs ?? 12_000))
  const maxFolders = Math.max(1, Math.floor(options?.maxFolders ?? 2000))
  const expectedName = String(options?.expectedName ?? '').trim()
  const expectedSize = typeof options?.expectedSize === 'number' && Number.isFinite(options.expectedSize)
    ? Math.max(0, Math.floor(options.expectedSize))
    : undefined

  const start = Date.now()
  const visited = new Set<string>()
  const queued = new Set<string>()
  const stack: Array<{ folderId?: string, prefix: string }> = [{ folderId: undefined, prefix: '' }]
  let scanned = 0

  let bestCandidate: { path: string, name: string, busid?: number, uploadTime?: number } | undefined

  while (stack.length) {
    if (Date.now() - start > timeoutMs) return bestCandidate
    if (scanned >= maxFolders) return bestCandidate

    const current = stack.pop()!
    const folderId = current.folderId
    const prefix = current.prefix

    if (folderId) {
      if (visited.has(folderId)) continue
      visited.add(folderId)
    }

    scanned++
    const { files: rawFiles, folders: rawFolders } = await getGroupFileListCompat(bot, groupId, folderId)

    for (const raw of rawFiles) {
      const id = pickFirstString(raw?.fid, raw?.file_id, raw?.fileId, raw?.id)
      const busid = pickFirstNumber(raw?.busid, raw?.busId)

      if (!id || String(id) !== String(fileId)) continue

      const name = pickFirstString(raw?.name, raw?.file_name, raw?.fileName) ?? (id ? `file-${id}` : 'unknown-file')
      const filePath = prefix ? `${prefix}/${name}` : name

      return {
        path: filePath,
        name,
        busid,
      }
    }

    if (expectedName) {
      for (const raw of rawFiles) {
        const id = pickFirstString(raw?.fid, raw?.file_id, raw?.fileId, raw?.id)
        if (!id) continue

        const name = pickFirstString(raw?.name, raw?.file_name, raw?.fileName) ?? (id ? `file-${id}` : 'unknown-file')
        if (String(name).trim() !== expectedName) continue

        const size = pickFirstNumber(raw?.size, raw?.file_size, raw?.fileSize)
        if (typeof expectedSize === 'number') {
          if (typeof size !== 'number' || !Number.isFinite(size) || Math.floor(size) !== expectedSize) continue
        }

        const uploadTime = pickFirstNumber(raw?.uploadTime, raw?.upload_time)
        const busid = pickFirstNumber(raw?.busid, raw?.busId)
        const filePath = prefix ? `${prefix}/${name}` : name

        if (!bestCandidate) {
          bestCandidate = { path: filePath, name, busid, uploadTime }
          continue
        }

        const bestTime = typeof bestCandidate.uploadTime === 'number' ? bestCandidate.uploadTime : -1
        const currentTime = typeof uploadTime === 'number' ? uploadTime : -1
        if (currentTime > bestTime) bestCandidate = { path: filePath, name, busid, uploadTime }
      }
    }

    for (const raw of rawFolders) {
      const id = pickFirstString(raw?.id, raw?.folder_id, raw?.folderId)
      if (!id) continue
      if (visited.has(id) || queued.has(id)) continue

      queued.add(id)
      const folderName = pickFirstString(raw?.name, raw?.folder_name, raw?.folderName) ?? id
      const nextPrefix = prefix ? `${prefix}/${folderName}` : folderName
      stack.push({ folderId: id, prefix: nextPrefix })
    }
  }

  return bestCandidate
}

export const locateGroupFileByIdWithRetry = async (
  bot: any,
  groupId: string,
  fileId: string,
  options?: {
    retries?: number
    delayMs?: number
    timeoutMs?: number
    maxFolders?: number
    expectedName?: string
    expectedSize?: number
  },
) => {
  const retries = Math.max(0, Math.floor(options?.retries ?? 2))
  const delayMs = Math.max(0, Math.floor(options?.delayMs ?? 800))

  let lastError: unknown
  for (let attempt = 0; attempt <= retries; attempt++) {
    try {
      const found = await locateGroupFileById(bot, groupId, fileId, options)
      if (found?.path) return found
    } catch (error) {
      lastError = error
    }

    if (attempt < retries && delayMs > 0) await sleep(delayMs)
  }

  if (lastError) throw lastError
}

