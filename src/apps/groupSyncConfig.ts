import fs from 'node:fs'
import path from 'node:path'
import { dir } from '@/dir'
import { config } from '@/utils'
import { karin, logger } from 'node-karin'

type SyncMode = 'full' | 'incremental'

interface GroupSyncTarget {
  groupId: string
  enabled?: boolean
  sourceFolderId?: string
  targetDir?: string
  mode?: SyncMode
  flat?: boolean
  /** 监听群文件上传并自动备份到 OpenList */
  uploadBackup?: boolean
  maxFiles?: number
  urlConcurrency?: number
  transferConcurrency?: number
  fileTimeoutSec?: number
  retryTimes?: number
  retryDelayMs?: number
  timeWindows?: string
  schedule?: {
    enabled?: boolean
    cron?: string
  }
}

const readJsonSafe = (filePath: string): any => {
  try {
    if (!fs.existsSync(filePath)) return {}
    const raw = fs.readFileSync(filePath, 'utf8')
    return raw ? JSON.parse(raw) : {}
  } catch (error) {
    logger.error(error)
    return {}
  }
}

const writeJsonSafe = (filePath: string, data: unknown) => {
  fs.mkdirSync(path.dirname(filePath), { recursive: true })
  fs.writeFileSync(filePath, JSON.stringify(data, null, 2), 'utf8')
}

const getConfigFilePath = () => path.join(dir.ConfigDir, 'config.json')
const getDefaultConfigPath = () => path.join(dir.defConfigDir, 'config.json')

const normalizePosixPath = (inputPath: string, { ensureLeadingSlash = true, stripTrailingSlash = true } = {}) => {
  let value = String(inputPath ?? '').trim().replaceAll('\\', '/')
  value = value.replace(/\/+/g, '/')
  if (!value) value = '/'
  if (ensureLeadingSlash && !value.startsWith('/')) value = `/${value}`
  if (stripTrailingSlash && value.length > 1) value = value.replace(/\/+$/, '')
  return value
}

const parseBoolean = (value: string | undefined) => {
  const v = String(value ?? '').trim().toLowerCase()
  if (!v) return undefined
  if (['1', 'true', 'yes', 'y', 'on', '开启', '开', '是'].includes(v)) return true
  if (['0', 'false', 'no', 'n', 'off', '关闭', '关', '否'].includes(v)) return false
  return undefined
}

const parseIntSafe = (value: string | undefined) => {
  if (value == null) return undefined
  const n = Number(value)
  if (!Number.isFinite(n)) return undefined
  return Math.floor(n)
}

const parseMode = (value: string | undefined): SyncMode | undefined => {
  const v = String(value ?? '').trim().toLowerCase()
  if (!v) return undefined
  if (v === 'full' || v === '全量') return 'full'
  if (v === 'incremental' || v === '增量') return 'incremental'
  return undefined
}

const formatTargetLine = (t: GroupSyncTarget) => {
  const mode = t.mode ?? '-'
  const enabled = (t.enabled ?? true) ? '启用' : '停用'
  const cron = t.schedule?.enabled ? (t.schedule?.cron || '(未填cron)') : '关闭'
  const targetDir = t.targetDir ? t.targetDir : '(默认)'
  const uploadBackup = (t.uploadBackup ?? false) ? 'on' : 'off'
  return `- ${t.groupId} | ${enabled} | ${mode} | uploadBackup:${uploadBackup} | 计划:${cron} | 目录:${targetDir}`
}

const upsertTarget = (cfg: any, groupId: string, patch: Partial<GroupSyncTarget>) => {
  const list = getTargets(cfg)
  const next = [...list]
  const index = next.findIndex(it => String(it.groupId) === String(groupId))
  const base: GroupSyncTarget = index >= 0 ? next[index] : ensureTargetWithDefaults(groupId, cfg)
  const merged: GroupSyncTarget = {
    ...base,
    ...patch,
    groupId: String(groupId),
  }
  if (index >= 0) next[index] = merged
  else next.push(merged)
  return next
}

const ensureTargetWithDefaults = (groupId: string, cfg: any): GroupSyncTarget => {
  const defaults = cfg.groupSyncDefaults ?? {}
  const baseTargetDir = normalizePosixPath(String(cfg.openlistTargetDir ?? '/'))
  return {
    groupId,
    enabled: true,
    mode: (defaults.mode === 'full' ? 'full' : 'incremental'),
    flat: Boolean(defaults.flat ?? false),
    urlConcurrency: parseIntSafe(String(defaults.urlConcurrency ?? '')) ?? 3,
    transferConcurrency: parseIntSafe(String(defaults.transferConcurrency ?? '')) ?? 3,
    fileTimeoutSec: parseIntSafe(String(defaults.fileTimeoutSec ?? '')) ?? 600,
    retryTimes: parseIntSafe(String(defaults.retryTimes ?? '')) ?? 2,
    retryDelayMs: parseIntSafe(String(defaults.retryDelayMs ?? '')) ?? 1500,
    targetDir: normalizePosixPath(path.posix.join(baseTargetDir, String(groupId))),
    schedule: { enabled: false, cron: '' },
  }
}

const updateConfigFile = (updater: (cfg: any) => any) => {
  const configPath = getConfigFilePath()
  const current = readJsonSafe(configPath)
  const def = readJsonSafe(getDefaultConfigPath())
  const merged = { ...def, ...current }
  const next = updater({ ...merged })
  writeJsonSafe(configPath, next)
  return next
}

const getTargets = (cfg: any): GroupSyncTarget[] => {
  const list = cfg?.groupSyncTargets
  return Array.isArray(list) ? list : []
}

const cfgHelp = [
  '群同步配置用法（建议在 WebUI 配置更完整）：',
  '- #群同步配置 列表',
  '- #群同步配置 <群号> 查看',
  '- #群同步配置 <群号> 添加',
  '- #群同步配置 <群号> 删除',
  '- #群同步配置 <群号> 启用 / 停用',
  '- #群同步配置 <群号> 模式 全量|增量',
  '- #群同步配置 <群号> 目录 /挂载/QQ群文件/123456',
  '- #群同步配置 <群号> 平铺 开|关',
  '- #群同步配置 <群号> uploadBackup on|off（监听群文件上传自动备份）',
  '- #群同步配置 <群号> 并发 <n>（下载/上传并发）',
  '- #群同步配置 <群号> url并发 <n>（解析URL并发）',
  '- #群同步配置 <群号> 超时 <sec>（单文件超时）',
  '- #群同步配置 <群号> 重试 <n>',
  '- #群同步配置 <群号> 时段 00:00-06:00,23:00-23:59（空=不限制）',
  '- #群同步配置 <群号> 计划 <cron>（例：0 0 3 * * *）',
  '- #群同步配置 <群号> 计划 开启|关闭',
].join('\n')

export const groupSyncConfig = karin.command(/^#?(群同步配置|同步群配置|群文件同步配置)(.*)$/i, async (e) => {
  if (!e.isPrivate) return false

  const raw = e.msg.replace(/^#?(群同步配置|同步群配置|群文件同步配置)/i, '').trim()
  if (!raw || /^(help|帮助|\?)$/i.test(raw)) {
    await e.reply(cfgHelp)
    return true
  }

  const tokens = raw.split(/\s+/).filter(Boolean)
  const first = tokens[0]?.toLowerCase()

  const isList = ['列表', 'list', 'ls'].includes(first)
  if (isList) {
    const cfg = config()
    const targets = getTargets(cfg)
    if (!targets.length) {
      await e.reply('当前没有配置任何同步目标群。可在 WebUI 或用：#群同步配置 <群号> 添加')
      return true
    }
    const lines = targets.map(formatTargetLine)
    await e.reply(['同步目标群列表：', ...lines].join('\n'))
    return true
  }

  const actionWords = new Map<string, string>([
    ['查看', 'show'], ['详情', 'show'], ['show', 'show'], ['get', 'show'],
    ['添加', 'add'], ['新增', 'add'], ['add', 'add'], ['create', 'add'],
    ['删除', 'remove'], ['移除', 'remove'], ['remove', 'remove'], ['del', 'remove'], ['delete', 'remove'],
    ['启用', 'enable'], ['开启', 'enable'], ['enable', 'enable'], ['on', 'enable'],
    ['停用', 'disable'], ['关闭', 'disable'], ['disable', 'disable'], ['off', 'disable'],
    ['模式', 'mode'], ['策略', 'mode'], ['mode', 'mode'],
    ['目录', 'dir'], ['目标目录', 'dir'], ['dir', 'dir'], ['to', 'dir'],
    ['平铺', 'flat'], ['flat', 'flat'],
    ['上传备份', 'uploadBackup'], ['uploadBackup', 'uploadBackup'], ['uploadbackup', 'uploadBackup'],
    ['并发', 'concurrency'], ['线程', 'concurrency'], ['concurrency', 'concurrency'], ['threads', 'concurrency'],
    ['url并发', 'urlConcurrency'], ['url线程', 'urlConcurrency'], ['urlconcurrency', 'urlConcurrency'],
    ['超时', 'timeout'], ['timeout', 'timeout'],
    ['重试', 'retry'], ['retry', 'retry'],
    ['时段', 'timeWindows'], ['时间窗', 'timeWindows'], ['time', 'timeWindows'], ['window', 'timeWindows'],
    ['计划', 'schedule'], ['定时', 'schedule'], ['cron', 'schedule'], ['schedule', 'schedule'],
  ])

  let actionToken: string | undefined
  let groupId: string | undefined
  let rest: string[] = []

  if (/^\d+$/.test(tokens[0] ?? '')) {
    groupId = tokens[0]
    actionToken = tokens[1]
    rest = tokens.slice(2)
  } else if (actionWords.has(tokens[0] ?? '') && /^\d+$/.test(tokens[1] ?? '')) {
    actionToken = tokens[0]
    groupId = tokens[1]
    rest = tokens.slice(2)
  } else if (e.isGroup && actionWords.has(tokens[0] ?? '')) {
    groupId = e.groupId
    actionToken = tokens[0]
    rest = tokens.slice(1)
  } else if (e.isGroup && /^\d+$/.test(tokens[0] ?? '') === false) {
    groupId = e.groupId
    actionToken = tokens[0]
    rest = tokens.slice(1)
  }

  if (!groupId || !/^\d+$/.test(groupId)) {
    await e.reply(`缺少群号参数。\n\n${cfgHelp}`)
    return true
  }

  const action = actionWords.get(actionToken ?? '') ?? ''
  if (!action) {
    await e.reply(`未知操作：${actionToken ?? ''}\n\n${cfgHelp}`)
    return true
  }

  if (action === 'show') {
    const cfg = config()
    const t = getTargets(cfg).find(it => String(it.groupId) === String(groupId))
    if (!t) {
      await e.reply(`未找到群 ${groupId} 的配置，可先添加：#群同步配置 ${groupId} 添加`)
      return true
    }
    await e.reply(['群同步配置：', formatTargetLine(t), `\n完整配置文件：${getConfigFilePath()}`].join('\n'))
    return true
  }

  if (action === 'add') {
    const cfg = config()
    const defaultsTarget = ensureTargetWithDefaults(groupId, cfg)
    updateConfigFile((next) => {
      const list = getTargets(next)
      if (list.some(it => String(it.groupId) === String(groupId))) return next
      next.groupSyncTargets = [...list, defaultsTarget]
      return next
    })
    await e.reply(`已添加群 ${groupId} 的同步配置（默认启用）。`)
    return true
  }

  if (action === 'remove') {
    const before = config()
    const beforeList = getTargets(before)
    if (!beforeList.some(it => String(it.groupId) === String(groupId))) {
      await e.reply(`群 ${groupId} 未配置，无需删除。`)
      return true
    }
    updateConfigFile((next) => {
      next.groupSyncTargets = getTargets(next).filter(it => String(it.groupId) !== String(groupId))
      return next
    })
    await e.reply(`已删除群 ${groupId} 的同步配置。`)
    return true
  }

  if (action === 'enable' || action === 'disable') {
    const enabled = action === 'enable'
    updateConfigFile((next) => {
      next.groupSyncTargets = upsertTarget(next, groupId!, { enabled })
      return next
    })
    await e.reply(`群 ${groupId} 已${enabled ? '启用' : '停用'}自动同步。`)
    return true
  }

  if (action === 'mode') {
    const mode = parseMode(rest[0])
    if (!mode) {
      await e.reply('模式参数错误，请使用：全量/增量 或 full/incremental')
      return true
    }
    updateConfigFile((next) => {
      next.groupSyncTargets = upsertTarget(next, groupId!, { mode })
      return next
    })
    await e.reply(`群 ${groupId} 同步模式已设置为：${mode === 'full' ? '全量' : '增量'}`)
    return true
  }

  if (action === 'dir') {
    const dirValue = normalizePosixPath(rest.join(' ').trim() || '')
    if (!dirValue || dirValue === '/') {
      await e.reply('目录参数为空或非法。')
      return true
    }
    updateConfigFile((next) => {
      next.groupSyncTargets = upsertTarget(next, groupId!, { targetDir: dirValue })
      return next
    })
    await e.reply(`群 ${groupId} 目标目录已设置为：${dirValue}`)
    return true
  }

  if (action === 'flat') {
    const bool = parseBoolean(rest[0])
    if (typeof bool === 'undefined') {
      await e.reply('平铺参数错误，请使用：开/关 或 true/false')
      return true
    }
    updateConfigFile((next) => {
      next.groupSyncTargets = upsertTarget(next, groupId!, { flat: bool })
      return next
    })
    await e.reply(`群 ${groupId} 平铺上传已设置为：${bool ? '开' : '关'}`)
    return true
  }

  if (action === 'uploadBackup') {
    const bool = parseBoolean(rest[0])
    if (typeof bool === 'undefined') {
      await e.reply('uploadBackup 参数错误，请使用：on/off 或 开/关 或 true/false')
      return true
    }
    updateConfigFile((next) => {
      next.groupSyncTargets = upsertTarget(next, groupId!, { uploadBackup: bool })
      return next
    })
    await e.reply(`群 ${groupId} uploadBackup 已设置为：${bool ? 'on' : 'off'}`)
    return true
  }

  if (action === 'concurrency' || action === 'urlConcurrency') {
    const value = parseIntSafe(rest[0])
    if (!value || value <= 0) {
      await e.reply('并发参数错误，请输入大于0的整数。')
      return true
    }

    const patch = action === 'concurrency'
      ? { transferConcurrency: value }
      : { urlConcurrency: value }

    updateConfigFile((next) => {
      next.groupSyncTargets = upsertTarget(next, groupId!, patch)
      return next
    })
    await e.reply(`群 ${groupId} 已设置${action === 'concurrency' ? '下载/上传' : '解析URL'}并发：${value}`)
    return true
  }

  if (action === 'timeout') {
    const sec = parseIntSafe(rest[0])
    if (!sec || sec <= 0) {
      await e.reply('超时参数错误，请输入大于0的整数（秒）。')
      return true
    }
    const capped = Math.min(3000, sec)
    updateConfigFile((next) => {
      next.groupSyncTargets = upsertTarget(next, groupId!, { fileTimeoutSec: capped })
      return next
    })
    await e.reply(`群 ${groupId} 单文件超时已设置为：${capped}s${sec !== capped ? '（已按上限3000s截断）' : ''}`)
    return true
  }

  if (action === 'retry') {
    const n = parseIntSafe(rest[0])
    if (typeof n === 'undefined' || n < 0) {
      await e.reply('重试次数参数错误，请输入 >=0 的整数。')
      return true
    }
    updateConfigFile((next) => {
      next.groupSyncTargets = upsertTarget(next, groupId!, { retryTimes: n })
      return next
    })
    await e.reply(`群 ${groupId} 重试次数已设置为：${n}`)
    return true
  }

  if (action === 'timeWindows') {
    const win = rest.join(' ').trim()
    updateConfigFile((next) => {
      next.groupSyncTargets = upsertTarget(next, groupId!, { timeWindows: win })
      return next
    })
    await e.reply(`群 ${groupId} 同步时段已设置为：${win || '(不限制)'}`)
    return true
  }

  if (action === 'schedule') {
    const value = rest.join(' ').trim()
    const bool = parseBoolean(rest[0])
    const cron = bool == null ? value : ''

    if (!value) {
      await e.reply('计划参数不能为空。')
      return true
    }

    updateConfigFile((next) => {
      const existing = getTargets(next).find(it => String(it.groupId) === String(groupId))
      const base = existing ?? ensureTargetWithDefaults(groupId!, next)
      const schedule = { ...(base.schedule ?? {} as any) }

      if (typeof bool === 'boolean') {
        schedule.enabled = bool
      } else {
        schedule.cron = cron
        schedule.enabled = true
      }

      next.groupSyncTargets = upsertTarget(next, groupId!, { schedule })
      return next
    })

    await e.reply(`群 ${groupId} 定时计划已更新：${value}`)
    return true
  }

  await e.reply(cfgHelp)
  return true
}, {
  name: '群同步配置',
  log: true,
  priority: 9999,
  permission: 'master',
})
