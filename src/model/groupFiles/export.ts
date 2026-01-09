import fs from 'node:fs'
import path from 'node:path'
import { dir } from '@/dir'
import { time } from '@/utils'
import { ensureDir } from '@/model/shared/fsJson'
import { runWithConcurrency } from '@/model/shared/concurrency'
import { formatErrorMessage } from '@/model/shared/errors'
import type { ExportError, ExportedGroupFile } from './types'
import { collectAllGroupFiles, resolveGroupFileUrl } from './qgroup'

const csvEscape = (value: unknown) => {
  const str = String(value ?? '')
  if (/[",\n]/.test(str)) return `"${str.replaceAll('"', '""')}"`
  return str
}

const writeExportFile = (format: 'json' | 'csv', outPath: string, payload: any, list: ExportedGroupFile[]) => {
  ensureDir(path.dirname(outPath))

  if (format === 'json') {
    fs.writeFileSync(outPath, JSON.stringify(payload, null, 2), 'utf8')
    return
  }

  const header = ['path', 'name', 'fileId', 'size', 'uploadTime', 'uploaderId', 'uploaderName', 'md5', 'sha1', 'sha3', 'url', 'busid']
  const rows = [header.join(',')]
  for (const item of list) {
    rows.push([
      csvEscape(item.path),
      csvEscape(item.name),
      csvEscape(item.fileId),
      csvEscape(item.size ?? ''),
      csvEscape(item.uploadTime ?? ''),
      csvEscape(item.uploaderId ?? ''),
      csvEscape(item.uploaderName ?? ''),
      csvEscape(item.md5 ?? ''),
      csvEscape(item.sha1 ?? ''),
      csvEscape(item.sha3 ?? ''),
      csvEscape(item.url ?? ''),
      csvEscape(item.busid ?? ''),
    ].join(','))
  }
  fs.writeFileSync(outPath, rows.join('\n'), 'utf8')
}

export const exportGroupFilesToDisk = async (params: {
  bot: any
  contact: any
  groupId: string
  folderId?: string
  maxFiles?: number
  withUrl: boolean
  urlConcurrency: number
  format: 'json' | 'csv'
}) => {
  const { bot, contact, groupId, folderId, maxFiles, withUrl, urlConcurrency, format } = params

  const urlErrors: ExportError[] = []
  const list = await collectAllGroupFiles(bot, groupId, folderId)

  const limitedList = typeof maxFiles === 'number' && Number.isFinite(maxFiles) && maxFiles > 0
    ? list.slice(0, Math.floor(maxFiles))
    : list

  if (withUrl) {
    await runWithConcurrency(limitedList, Math.max(1, Math.floor(urlConcurrency) || 1), async (item) => {
      try {
        item.url = await resolveGroupFileUrl(bot, contact, groupId, item)
      } catch (error: any) {
        urlErrors.push({
          fileId: item.fileId,
          path: item.path,
          message: formatErrorMessage(error),
        })
      }
    })
  }

  const exportDir = path.join(dir.karinPath, 'data', 'group-files-export')
  const exportName = `group-files-${groupId}-${time('YYYYMMDD-HHmmss')}.${format}`
  const exportPath = path.join(exportDir, exportName)

  const payload = {
    type: 'group-files-export',
    groupId,
    exportedAt: Date.now(),
    exportedAtText: time('YYYY-MM-DD HH:mm:ss'),
    folderId: folderId || undefined,
    maxFiles: maxFiles || undefined,
    total: limitedList.length,
    withUrl,
    urlErrorsCount: urlErrors.length,
    list: limitedList,
  }

  writeExportFile(format, exportPath, payload, limitedList)

  return {
    list,
    limitedList,
    exportDir,
    exportName,
    exportPath,
    urlErrors,
  }
}

