import path from 'node:path'
import { pathToFileURL } from 'node:url'
import { config } from '@/utils'
import { exportGroupFilesToDisk, syncGroupFilesToOpenListCore } from '@/model/groupFiles'
import { normalizePosixPath } from '@/model/shared/path'
import { formatErrorMessage } from '@/model/shared/errors'
import { karin, logger } from 'node-karin'

type SyncMode = 'full' | 'incremental'

const MAX_FILE_TIMEOUT_SEC = 3000
const MIN_FILE_TIMEOUT_SEC = 10
const DEFAULT_PROGRESS_REPORT_EVERY = 10
const FIXED_SYNC_MODE: SyncMode = 'incremental'

const buildUploadFileCandidates = (filePath: string) => {
  const normalized = filePath.replaceAll('\\', '/')
  const candidates = [
    filePath,
    normalized,
  ]

  try {
    candidates.push(pathToFileURL(filePath).href)
  } catch {}

  if (/^[a-zA-Z]:\//.test(normalized)) {
    candidates.push(`file:///${normalized}`)
  }

  return [...new Set(candidates.filter(Boolean))]
}

const parseArgs = (text: string) => {
  const raw = text.trim()
  const tokens = raw ? raw.split(/\s+/).filter(Boolean) : []
  const format: 'json' | 'csv' = /(^|\s)(--csv|csv)(\s|$)/i.test(raw) ? 'csv' : 'json'
  const withUrl = !/(^|\s)(--no-url|--nourl|no-url|nourl)(\s|$)/i.test(raw)
  const urlOnly = /(^|\s)(--url-only|--urlonly|url-only|urlonly)(\s|$)/i.test(raw)
  const sendFile = /(^|\s)(--send-file|--sendfile|send-file|sendfile)(\s|$)/i.test(raw)

  let groupId: string | undefined
  for (let i = 0; i < tokens.length; i++) {
    const token = tokens[i]
    const nextToken = tokens[i + 1]

    if (/^--(group|gid|groupid)$/i.test(token) && nextToken && /^\d+$/.test(nextToken)) {
      groupId = nextToken
      break
    }

    const assignMatch = token.match(/^(group|gid|groupid)=(\d+)$/i)
    if (assignMatch) {
      groupId = assignMatch[2]
      break
    }
  }

  if (!groupId) {
    for (let i = 0; i < tokens.length; i++) {
      const token = tokens[i]
      const prevToken = tokens[i - 1]
      if (!/^\d+$/.test(token)) continue
      if (prevToken && /^--(folder|max|concurrency|group|gid|groupid|to|timeout)$/i.test(prevToken)) continue
      groupId = token
      break
    }
  }

  const folderMatch = raw.match(/--folder\s+(\S+)/i) ?? raw.match(/(^|\s)folder=(\S+)/i)
  const folderId = folderMatch ? folderMatch[folderMatch.length - 1] : undefined

  const maxMatch = raw.match(/--max\s+(\d+)/i) ?? raw.match(/(^|\s)max=(\d+)/i)
  const maxFiles = maxMatch ? Number(maxMatch[maxMatch.length - 1]) : undefined

  const concurrencyMatch = raw.match(/--concurrency\s+(\d+)/i) ?? raw.match(/(^|\s)concurrency=(\d+)/i)
  const concurrency = concurrencyMatch ? Number(concurrencyMatch[concurrencyMatch.length - 1]) : undefined
  const concurrencySpecified = Boolean(concurrencyMatch)

  const help = /(^|\s)(--help|-h|help|\?)(\s|$)/i.test(raw)

  return { groupId, format, withUrl, urlOnly, sendFile, folderId, maxFiles, concurrency, concurrencySpecified, help }
}

const parseSyncArgs = (text: string) => {
  const raw = text.trim()
  const base = parseArgs(text)

  const toMatch = raw.match(/--to\s+(\S+)/i) ?? raw.match(/(^|\s)to=(\S+)/i)
  const to = toMatch ? toMatch[toMatch.length - 1] : undefined
  const toSpecified = Boolean(toMatch)

  const flatFlag = /(^|\s)(--flat|flat)(\s|$)/i.test(raw)
  const keepFlag = /(^|\s)(--keep|keep)(\s|$)/i.test(raw)
  const flatSpecified = flatFlag || keepFlag
  const flat = flatFlag ? true : keepFlag ? false : undefined

  return {
    groupId: base.groupId,
    folderId: base.folderId,
    maxFiles: base.maxFiles,
    concurrency: base.concurrency,
    concurrencySpecified: base.concurrencySpecified,
    flat,
    flatSpecified,
    to,
    toSpecified,
    help: base.help,
  }
}

const helpText = [
  '群文件导出用法：',
  '- 请私聊发送：#导出群文件 <群号> [参数]',
  '- 示例：#导出群文件 123456',
  '- #导出群文件 123456 --no-url：只导出列表，不解析 URL',
  '- #导出群文件 123456 --url-only：仅输出 URL（更方便复制）',
  '- #导出群文件 123456 --csv：导出为 CSV（默认 JSON）',
  '- #导出群文件 123456 --folder <id>：从指定文件夹开始导出',
  '- #导出群文件 123456 --max <n>：最多导出 n 条文件记录',
  '- #导出群文件 123456 --concurrency <n>：解析 URL 并发数（默认 3）',
  '- #导出群文件 123456 --send-file：尝试发送导出文件（依赖协议端支持）',
  '提示：下载 URL 通常有时效，过期后需重新导出。',
].join('\n')

export const exportGroupFiles = karin.command(/^#?(导出群文件|群文件导出)(.*)$/i, async (e) => {
  if (!e.isPrivate) return false

  const argsText = e.msg.replace(/^#?(导出群文件|群文件导出)/i, '')
  const { groupId, format, withUrl, urlOnly, sendFile, folderId, maxFiles, concurrency, help } = parseArgs(argsText)
  if (help) {
    await e.reply(helpText)
    return true
  }

  if (!groupId) {
    await e.reply(`缺少群号参数\n\n${helpText}`)
    return true
  }

  const groupContact = karin.contactGroup(groupId)

  await e.reply(`开始导出群文件列表，请稍候..\n- 群号：${groupId}\n- 格式：${format}\n- 包含URL：${withUrl ? '是' : '否'}`)

  let result: Awaited<ReturnType<typeof exportGroupFilesToDisk>>
  try {
    const urlConcurrency = typeof concurrency === 'number' && Number.isFinite(concurrency) && concurrency > 0 ? concurrency : 3
    result = await exportGroupFilesToDisk({
      bot: e.bot,
      contact: groupContact,
      groupId,
      folderId,
      maxFiles,
      withUrl,
      urlConcurrency,
      format,
    })
  } catch (error: any) {
    logger.error(error)
    await e.reply(`导出失败：${formatErrorMessage(error)}`)
    return true
  }

  const { limitedList, exportPath, exportName, urlErrors } = result

  const errorsByFileId = new Map<string, string>()
  for (const err of urlErrors) {
    if (!err.fileId) continue
    if (!errorsByFileId.has(err.fileId)) errorsByFileId.set(err.fileId, err.message)
  }

  await e.reply([
    '导出完成：',
    `- 总数：${limitedList.length}`,
    `- URL获取失败：${urlErrors.length}`,
    `- 文件：${exportPath}`,
  ].join('\n'))

  const compactError = (message: string) => message.replace(/\s+/g, ' ').slice(0, 120)
  const preview = limitedList.slice(0, 20)
  const lines = preview.map((item, index) => {
    if (urlOnly) return `${index + 1}. ${item.url ?? ''}`.trim()
    const errMsg = errorsByFileId.get(item.fileId)
    if (item.url) return `${index + 1}. ${item.path}\n${item.url}`
    return `${index + 1}. ${item.path}\n(获取URL失败) fileId=${item.fileId}${errMsg ? `\n原因：${compactError(errMsg)}` : ''}`
  })

  const chunks: string[] = []
  const maxChunkLen = 1500
  let buf = ''
  for (const line of lines) {
    const next = buf ? `${buf}\n\n${line}` : line
    if (next.length > maxChunkLen) {
      if (buf) chunks.push(buf)
      buf = line
    } else {
      buf = next
    }
  }
  if (buf) chunks.push(buf)

  const maxMessages = 10
  for (const chunk of chunks.slice(0, maxMessages)) {
    await e.reply(chunk)
  }
  if (limitedList.length > preview.length) {
    await e.reply(`（已省略 ${limitedList.length - preview.length} 条，可使用 --max 调整）`)
  } else if (chunks.length > maxMessages) {
    await e.reply('（消息过长，已省略后续内容；可使用 --max 减少条数）')
  }

  if (sendFile && typeof e.bot?.uploadFile === 'function') {
    const candidates = buildUploadFileCandidates(exportPath)
    for (const fileParam of candidates) {
      try {
        await e.bot.uploadFile(e.contact, fileParam, exportName)
        break
      } catch {}
    }
  }

  return true
}, {
  priority: 9999,
  log: true,
  name: '导出群文件',
  permission: 'all',
})

const syncHelpText = [
  '群文件同步到 OpenList 用法：',
  '- 私聊：#同步群文件 <群号> [参数]',
  '- 注意：默认仅私聊响应（群聊不会触发该指令）',
  '- 示例：#同步群文件 123456',
  '- #同步群文件 123456 --to /目标目录：上传到指定目录（默认使用配置 openlistTargetDir）',
  '- #同步群文件 123456 --flat：不保留群文件夹结构，全部平铺到目标目录',
  '- #同步群文件 123456 --keep：强制保留目录结构（覆盖群配置 flat）',
  '- #同步群文件 123456 --folder <id>：从指定文件夹开始',
  '- #同步群文件 123456 --max <n>：最多处理 n 个文件',
  '- #同步群文件 123456 --concurrency <n>：并发数（会覆盖群配置的并发）',
  `- 固定策略：mode=${FIXED_SYNC_MODE} 单文件超时=${MAX_FILE_TIMEOUT_SEC}s（不再通过命令配置）`,
  '前置：请先在配置文件填写 openlistBaseUrl/openlistUsername/openlistPassword。',
].join('\n')

const getGroupSyncTarget = (cfg: any, groupId: string) => {
  const list = cfg?.groupSyncTargets
  if (!Array.isArray(list)) return undefined
  return list.find((it: any) => String(it?.groupId) === String(groupId))
}

export const syncGroupFilesToOpenList = karin.command(/^#?(同步群文件|群文件同步)(.*)$/i, async (e) => {
  if (!e.isPrivate) return false

  const argsText = e.msg.replace(/^#?(同步群文件|群文件同步)/i, '')
  const {
    groupId: parsedGroupId,
    folderId: parsedFolderId,
    maxFiles: parsedMaxFiles,
    concurrency,
    concurrencySpecified,
    flat,
    flatSpecified,
    to,
    toSpecified,
    help,
  } = parseSyncArgs(argsText)
  if (help) {
    await e.reply(syncHelpText)
    return true
  }

  const cfg = config()
  const groupId = parsedGroupId ?? (e.isGroup ? e.groupId : undefined)
  if (!groupId) {
    await e.reply(`缺少群号参数\n\n${syncHelpText}`)
    return true
  }

  const defaults = cfg.groupSyncDefaults ?? {}
  const targetCfg = getGroupSyncTarget(cfg, groupId)

  const mode = FIXED_SYNC_MODE

  const urlC = concurrencySpecified
    ? (typeof concurrency === 'number' ? concurrency : 3)
    : (typeof targetCfg?.urlConcurrency === 'number' ? targetCfg.urlConcurrency : (typeof defaults?.urlConcurrency === 'number' ? defaults.urlConcurrency : 3))

  const transferC = concurrencySpecified
    ? (typeof concurrency === 'number' ? concurrency : 3)
    : (typeof targetCfg?.transferConcurrency === 'number' ? targetCfg.transferConcurrency : (typeof defaults?.transferConcurrency === 'number' ? defaults.transferConcurrency : 3))

  const retryTimes = typeof targetCfg?.retryTimes === 'number'
    ? targetCfg.retryTimes
    : (typeof defaults?.retryTimes === 'number' ? defaults.retryTimes : 2)

  const retryDelayMs = typeof targetCfg?.retryDelayMs === 'number'
    ? targetCfg.retryDelayMs
    : (typeof defaults?.retryDelayMs === 'number' ? defaults.retryDelayMs : 1500)

  const progressEvery = typeof targetCfg?.progressReportEvery === 'number'
    ? targetCfg.progressReportEvery
    : (typeof defaults?.progressReportEvery === 'number' ? defaults.progressReportEvery : DEFAULT_PROGRESS_REPORT_EVERY)

  const downloadLimitKbps = typeof targetCfg?.downloadLimitKbps === 'number'
    ? targetCfg.downloadLimitKbps
    : (typeof defaults?.downloadLimitKbps === 'number' ? defaults.downloadLimitKbps : 0)

  const uploadLimitKbps = typeof targetCfg?.uploadLimitKbps === 'number'
    ? targetCfg.uploadLimitKbps
    : (typeof defaults?.uploadLimitKbps === 'number' ? defaults.uploadLimitKbps : 0)

  const targetDir = normalizePosixPath(
    toSpecified
      ? (to ?? '')
      : (String(targetCfg?.targetDir ?? '').trim() || path.posix.join(String(cfg.openlistTargetDir ?? '/'), String(groupId))),
  )

  const finalFlat = flatSpecified
    ? Boolean(flat)
    : (typeof targetCfg?.flat === 'boolean' ? targetCfg.flat : Boolean(defaults?.flat ?? false))

  const folderId = parsedFolderId ?? targetCfg?.sourceFolderId
  const maxFiles = typeof parsedMaxFiles === 'number' ? parsedMaxFiles : targetCfg?.maxFiles

  try {
    await syncGroupFilesToOpenListCore({
      bot: e.bot,
      groupId,
      folderId,
      maxFiles,
      flat: Boolean(finalFlat),
      targetDir,
      mode,
      urlConcurrency: Math.max(1, Math.floor(urlC) || 1),
      transferConcurrency: Math.max(1, Math.floor(transferC) || 1),
      fileTimeoutSec: MAX_FILE_TIMEOUT_SEC,
      retryTimes: Math.max(0, Math.floor(retryTimes) || 0),
      retryDelayMs: Math.max(0, Math.floor(retryDelayMs) || 0),
      progressReportEvery: Math.max(0, Math.floor(progressEvery) || 0),
      downloadLimitKbps: Math.max(0, Math.floor(downloadLimitKbps) || 0),
      uploadLimitKbps: Math.max(0, Math.floor(uploadLimitKbps) || 0),
      report: (msg) => e.reply(msg),
    })
  } catch (error: any) {
    logger.error(error)
    await e.reply(formatErrorMessage(error))
    return true
  }

  return true
}, {
  priority: 9999,
  log: true,
  name: '同步群文件到OpenList',
  permission: 'all',
})
