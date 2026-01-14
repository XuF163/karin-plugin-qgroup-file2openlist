import path from 'node:path'
import { dir } from '@/dir'
import { config } from '@/utils'
import { ensurePluginResources } from '@/utils/resources'
import { backupOpenListToOpenListCore } from '@/model/openlist'
import { normalizePosixPath } from '@/model/shared/path'
import {
  DEFAULT_OPLT_NIGHTLY_APPEND_HOST_DIR,
  DEFAULT_OPLT_NIGHTLY_TRANSPORT,
  resolveOpltMapping,
  readOpltData,
  withOpltUser,
  writeOpltData,
} from '@/model/oplt'
import { formatErrorMessage } from '@/model/shared/errors'
import { readJsonSafe, writeJsonSafe } from '@/model/shared/fsJson'
import { karin, logger, render, segment } from 'node-karin'
import type { OpenListBackupTransport, SyncMode } from '@/model/groupFiles/types'

const FIXED_BACKUP_MODE: SyncMode = 'incremental'
const FIXED_BACKUP_TIMEOUT_SEC = 3000
const DEFAULT_NIGHTLY_CRON = '0 0 2 * * *'

const formatDateTime = (date: Date) => {
  try {
    return date.toLocaleString('zh-CN', {
      year: 'numeric',
      month: '2-digit',
      day: '2-digit',
      hour: '2-digit',
      minute: '2-digit',
      hour12: false,
    })
  } catch {
    return date.toISOString()
  }
}

const pickId = (...values: unknown[]) => {
  for (const v of values) {
    if (typeof v === 'string' && v.trim()) return v.trim()
    if (typeof v === 'number' && Number.isFinite(v)) return String(Math.floor(v))
  }
  return undefined
}

const getUserKey = (e: any) => pickId(
  e?.userId,
  e?.user_id,
  e?.sender?.user_id,
  e?.sender?.userId,
  e?.sender?.id,
  e?.fromId,
  e?.from_id,
  e?.contact?.id,
  e?.contact?.userId,
  e?.contact?.user_id,
) ?? 'global'

const normalizeText = (value: unknown) => String(value ?? '').trim()

const truthyFlag = (value: unknown) => {
  if (value === true) return true
  const v = String(value ?? '').trim().toLowerCase()
  return ['1', 'true', 'yes', 'y', 'on', '开启', '开', '是'].includes(v)
}

type GroupSyncTarget = {
  groupId: string
  enabled?: boolean
  targetDir?: string
  uploadBackup?: boolean
}

const getTargets = (cfg: any): GroupSyncTarget[] => Array.isArray(cfg?.groupSyncTargets) ? cfg.groupSyncTargets : []

const getUploadBackupTargets = (cfg: any) => getTargets(cfg).filter((t) => {
  if (!t?.groupId) return false
  if (t.enabled === false) return false
  return truthyFlag((t as any).uploadBackup)
})

const getConfigFilePath = () => path.join(dir.ConfigDir, 'config.json')
const getDefaultConfigPath = () => path.join(dir.defConfigDir, 'config.json')

const updateConfigFile = (updater: (cfg: any) => any) => {
  const configPath = getConfigFilePath()
  const current = readJsonSafe(configPath)
  const def = readJsonSafe(getDefaultConfigPath())
  const merged = { ...def, ...current }
  const next = updater({ ...merged })
  writeJsonSafe(configPath, next)
  return next
}

const opHelpText = [
  '群文件功能（来自 docs/command.md）：',
  '- #我的备份：查看已开启上传自动备份的群 + oplts 列表',
  '- #添加备份群<群号>：开启该群上传自动备份（uploadBackup=on）',
  '- #删除群备份 <序号>：按序号关闭该群上传自动备份（uploadBackup=off）',
  '- #添加oplt <A> <B>：添加一条 oplts 记录（支持 URL，会自动提取目录路径）',
  '- #删除oplt <序号>：删除一条 oplts 记录',
  `- #oplt备份 <序号|全部>：手动执行 oplts 备份（固定 ${FIXED_BACKUP_MODE} + timeout=${FIXED_BACKUP_TIMEOUT_SEC}s，可选 --auto/--api/--webdav）`,
  '- #oplt夜间 查看：查看夜间自动备份状态（由统一调度器管理，不支持通过命令修改）',
  '- #op帮助：显示本帮助',
  '',
  '提示：更完整的配置建议用 #群同步配置 或 WebUI。',
].join('\n')

export const opHelp = karin.command(/^#?op帮助$/i, async (e) => {
  if (!e.isPrivate) return false
  await e.reply(opHelpText)
  return true
}, {
  priority: 9999,
  log: true,
  name: 'op帮助',
  permission: 'all',
})

export const myBackup = karin.command(/^#?我的备份(\s+.*)?$/i, async (e) => {
  if (!e.isPrivate) return false

  const argsText = String(e.msg ?? '').replace(/^#?我的备份/i, '').trim()
  const forceText = /(^|\s)(--text|text)(\s|$)/i.test(argsText)

  const cfg = config()
  const groups = getUploadBackupTargets(cfg)

  const userKey = getUserKey(e)
  const data = readOpltData()
  const user = withOpltUser(data, userKey)

  const groupsPreviewLimit = 8
  const opltsPreviewLimit = 8

  const nightlyLine = (() => {
    const nightly: any = (user as any)?.nightly
    const globalEnabled = (cfg as any)?.scheduler?.enabled !== false && (cfg as any)?.scheduler?.opltNightly?.enabled !== false
    const cron = String((cfg as any)?.scheduler?.tickCron ?? DEFAULT_NIGHTLY_CRON).trim() || DEFAULT_NIGHTLY_CRON
    const transport: OpenListBackupTransport = (nightly?.transport === 'api' || nightly?.transport === 'webdav' || nightly?.transport === 'auto')
      ? nightly.transport
      : DEFAULT_OPLT_NIGHTLY_TRANSPORT
    const hostDir = typeof nightly?.appendHostDir === 'boolean' ? nightly.appendHostDir : DEFAULT_OPLT_NIGHTLY_APPEND_HOST_DIR
    const last = typeof nightly?.lastRunAt === 'number' && Number.isFinite(nightly.lastRunAt) ? formatDateTime(new Date(nightly.lastRunAt)) : '-'
    const lastResult = nightly?.lastResult && typeof nightly.lastResult === 'object'
      ? `ok=${nightly.lastResult.ok ?? '-'} skip=${nightly.lastResult.skipped ?? '-'} fail=${nightly.lastResult.fail ?? '-'}`
      : '-'

    if (!globalEnabled) return '夜间：OFF（全局已关闭）'
    return `夜间：ON  cron=${cron}  ${FIXED_BACKUP_MODE}/${transport}  timeout=${FIXED_BACKUP_TIMEOUT_SEC}s  hostDir=${hostDir ? 'on' : 'off'}\nlast=${last}  ${lastResult}`
  })()

  const groupsText = (() => {
    if (!groups.length) return '（空）\n可用：#添加备份群123'
    const lines: string[] = []
    const baseTargetDir = normalizePosixPath(String(cfg.openlistTargetDir ?? '/'))
    const preview = groups.slice(0, groupsPreviewLimit)
    for (let i = 0; i < preview.length; i++) {
      const t = preview[i]
      const targetDir = t.targetDir ? normalizePosixPath(String(t.targetDir)) : normalizePosixPath(path.posix.join(baseTargetDir, String(t.groupId)))
      lines.push(`${i + 1}. 群 ${t.groupId} -> ${targetDir}`)
    }
    if (groups.length > groupsPreviewLimit) lines.push(`...（还有 ${groups.length - groupsPreviewLimit} 个，建议用 #群同步配置 列表 / WebUI 查看）`)
    return lines.join('\n')
  })()

  const opltsText = (() => {
    const lines: string[] = [nightlyLine, '']
    if (!user.oplts.length) {
      lines.push('（空）')
      lines.push('可用：#添加oplt A B')
      return lines.join('\n')
    }
    const preview = user.oplts.slice(0, opltsPreviewLimit)
    for (let i = 0; i < preview.length; i++) {
      const it = preview[i]
      lines.push(`${i + 1}. ${it.left} -> ${it.right}`)
    }
    if (user.oplts.length > opltsPreviewLimit) lines.push(`...（还有 ${user.oplts.length - opltsPreviewLimit} 条）`)
    return lines.join('\n')
  })()

  const fallbackText = [
    '我的备份：',
    '',
    '备份群（uploadBackup=on）：',
    ...(() => {
      if (!groups.length) return ['- （空）可用：#添加备份群<群号>']
      const baseTargetDir = normalizePosixPath(String(cfg.openlistTargetDir ?? '/'))
      return groups.map((t, i) => {
        const targetDir = t.targetDir ? normalizePosixPath(String(t.targetDir)) : normalizePosixPath(path.posix.join(baseTargetDir, String(t.groupId)))
        return `- ${i + 1}. 群 ${t.groupId} -> ${targetDir}`
      })
    })(),
    '',
    'oplt（A B）：',
    ...(() => {
      const lines: string[] = [`- ${nightlyLine.replaceAll('\n', ' ')}`]
      if (!user.oplts.length) return [...lines, '- （空）可用：#添加oplt <A> <B>']
      return [...lines, ...user.oplts.map((it, i) => `- ${i + 1}. ${it.left} ${it.right}`)]
    })(),
  ].join('\n')

  if (forceText) {
    await e.reply(fallbackText)
    return true
  }

  try {
    await ensurePluginResources()
    const html = path.join(dir.defResourcesDir, 'template', 'myBackup.html')

    const img = await render.render({
      name: 'my-backup',
      encoding: 'base64',
      file: html,
      type: 'png',
      data: {
        name: dir.name,
        version: dir.version,
        generatedAt: formatDateTime(new Date()),
        groupsCount: groups.length,
        groupsText,
        opltsCount: user.oplts.length,
        opltsText,
      },
      setViewport: {
        width: 540,
        height: 960,
        deviceScaleFactor: 2,
      },
      pageGotoParams: {
        waitUntil: 'networkidle2',
      },
    }) as string

    await e.reply(segment.image(`base64://${img}`))
    return true
  } catch (error: any) {
    logger.error(error)
    await e.reply(fallbackText)
    return true
  }
}, {
  priority: 9999,
  log: true,
  name: '我的备份',
  permission: 'all',
})

export const addBackupGroup = karin.command(/^#?添加备份群(.*)$/i, async (e) => {
  if (!e.isPrivate) return false

  const rest = String(e.msg ?? '').replace(/^#?添加备份群/i, '').trim()
  const groupId = rest.match(/\d+/)?.[0]
  if (!groupId) {
    await e.reply('用法：#添加备份群<群号>\n示例：#添加备份群123456')
    return true
  }

  try {
    updateConfigFile((next) => {
      const targets = getTargets(next)
      const idx = targets.findIndex((it: any) => String(it?.groupId) === String(groupId))
      const base = idx >= 0 ? (targets[idx] ?? {}) : {}
      const defaults = next.groupSyncDefaults ?? {}
      const baseTargetDir = normalizePosixPath(String(next.openlistTargetDir ?? '/'))
      const ensureTargetDir = () => normalizePosixPath(path.posix.join(baseTargetDir, String(groupId)))

      const merged: any = {
        ...base,
        groupId: String(groupId),
        enabled: true,
        uploadBackup: true,
      }

      if (!merged.targetDir) merged.targetDir = ensureTargetDir()
      if (!merged.mode && (defaults.mode === 'full' || defaults.mode === 'incremental')) merged.mode = defaults.mode
      if (typeof merged.flat !== 'boolean' && typeof defaults.flat === 'boolean') merged.flat = defaults.flat

      const nextTargets = [...targets]
      if (idx >= 0) nextTargets[idx] = merged
      else nextTargets.push(merged)
      next.groupSyncTargets = nextTargets
      return next
    })

    await e.reply(`已添加备份群：${groupId}\n- uploadBackup: on`)
    return true
  } catch (error: any) {
    logger.error(error)
    await e.reply(`操作失败：${error?.message ?? String(error)}`)
    return true
  }
}, {
  priority: 9999,
  log: true,
  name: '添加备份群',
  permission: 'master',
})

export const removeBackupGroup = karin.command(/^#?删除群备份(.*)$/i, async (e) => {
  if (!e.isPrivate) return false

  const rest = String(e.msg ?? '').replace(/^#?删除群备份/i, '').trim()
  const index = Number(rest.match(/\d+/)?.[0] ?? NaN)
  if (!Number.isFinite(index) || index <= 0) {
    await e.reply('用法：#删除群备份 <序号>\n示例：#删除群备份 1\n提示：先用 #我的备份 查看序号')
    return true
  }

  const cfg = config()
  const list = getUploadBackupTargets(cfg)
  const target = list[Math.floor(index) - 1]
  if (!target) {
    await e.reply(`序号超出范围：${index}\n提示：先用 #我的备份 查看序号`)
    return true
  }

  const groupId = String(target.groupId)
  try {
    updateConfigFile((next) => {
      const targets = getTargets(next)
      const idx = targets.findIndex((it: any) => String(it?.groupId) === groupId)
      if (idx < 0) return next
      const updated = { ...(targets[idx] as any), uploadBackup: false }
      const nextTargets = [...targets]
      nextTargets[idx] = updated
      next.groupSyncTargets = nextTargets
      return next
    })

    await e.reply(`已关闭备份群：${groupId}\n- uploadBackup: off`)
    return true
  } catch (error: any) {
    logger.error(error)
    await e.reply(`操作失败：${error?.message ?? String(error)}`)
    return true
  }
}, {
  priority: 9999,
  log: true,
  name: '删除群备份',
  permission: 'master',
})

export const addOplt = karin.command(/^#?添加oplt(.*)$/i, async (e) => {
  if (!e.isPrivate) return false

  const rest = String(e.msg ?? '').replace(/^#?添加oplt/i, '').trim()
  const tokens = rest.split(/\s+/).filter(Boolean)
  const left = normalizeText(tokens[0])
  const right = normalizeText(tokens[1])
  if (!left || !right) {
    await e.reply('用法：#添加oplt <A> <B>\n示例：#添加oplt a.com/xxxx/ b.com')
    return true
  }

  try {
    const resolved = resolveOpltMapping({ left, right })
    const normalizedLeft = resolved.srcDir === '/' ? resolved.sourceBaseUrl : `${resolved.sourceBaseUrl}${resolved.srcDir}`
    const normalizedRight = resolved.toDir

    const userKey = getUserKey(e)
    const data = readOpltData()
    const user = withOpltUser(data, userKey)

    const exists = user.oplts.some((it) => it.left === normalizedLeft && it.right === normalizedRight)
    if (!exists) user.oplts.push({ left: normalizedLeft, right: normalizedRight })
    writeOpltData(data)

    const normalizedHint = (left !== normalizedLeft || right !== normalizedRight) ? '\n（已识别并归一化输入）' : ''
    await e.reply(exists ? '该 oplts 已存在（未重复添加）' : `已添加 oplts：${normalizedLeft} -> ${normalizedRight}${normalizedHint}`)
    return true
  } catch (error: any) {
    logger.error(error)
    await e.reply(`操作失败：${error?.message ?? String(error)}`)
    return true
  }
}, {
  priority: 9999,
  log: true,
  name: '添加oplt',
  permission: 'master',
})

export const removeOplt = karin.command(/^#?删除oplt(.*)$/i, async (e) => {
  if (!e.isPrivate) return false

  const rest = String(e.msg ?? '').replace(/^#?删除oplt/i, '').trim()
  const index = Number(rest.match(/\d+/)?.[0] ?? NaN)
  if (!Number.isFinite(index) || index <= 0) {
    await e.reply('用法：#删除oplt <序号>\n示例：#删除oplt 2\n提示：先用 #我的备份 查看序号')
    return true
  }

  try {
    const userKey = getUserKey(e)
    const data = readOpltData()
    const user = withOpltUser(data, userKey)

    const removed = user.oplts.splice(Math.floor(index) - 1, 1)[0]
    if (!removed) {
      await e.reply(`序号超出范围：${index}\n提示：先用 #我的备份 查看序号`)
      return true
    }

    writeOpltData(data)
    await e.reply(`已删除 oplts：${removed.left} ${removed.right}`)
    return true
  } catch (error: any) {
    logger.error(error)
    await e.reply(`操作失败：${error?.message ?? String(error)}`)
    return true
  }
}, {
  priority: 9999,
  log: true,
  name: '删除oplt',
  permission: 'master',
})

const pickFlagValue = (raw: string, names: string[]) => {
  for (const name of names) {
    const m = raw.match(new RegExp(`--${name}\\\\s+(\\\\S+)`, 'i')) ?? raw.match(new RegExp(`(^|\\\\s)${name}=(\\\\S+)`, 'i'))
    if (m) return m[m.length - 1]
  }
  return undefined
}

const parseOpltBackupArgs = (text: string) => {
  const raw = String(text ?? '').trim()
  const tokens = raw ? raw.split(/\s+/).filter(Boolean) : []

  const help = /(^|\s)(--help|-h|help|\?)(\s|$)/i.test(raw)
  const first = tokens[0]

  const selection = (() => {
    if (!first) return undefined
    if (/^(all|全部)$/i.test(first)) return 'all' as const
    const n = Number(first)
    if (Number.isFinite(n) && n > 0) return Math.floor(n)
    return undefined
  })()

  const transportApi = /(^|\s)(--api)(\s|$)/i.test(raw)
  const transportWebDav = /(^|\s)(--webdav|--dav)(\s|$)/i.test(raw)
  const transportAuto = /(^|\s)(--auto)(\s|$)/i.test(raw)
  const transport: OpenListBackupTransport | undefined = transportApi ? 'api' : transportWebDav ? 'webdav' : transportAuto ? 'auto' : undefined

  const host = /(^|\s)(--host|--append-host)(\s|$)/i.test(raw)
  const noHost = /(^|\s)(--no-host|--nohost)(\s|$)/i.test(raw)
  const appendHostDir = host ? true : noHost ? false : undefined

  const sourceUsername = pickFlagValue(raw, ['user', 'username'])
  const sourcePassword = pickFlagValue(raw, ['pass', 'password'])

  return { help, selection, transport, appendHostDir, sourceUsername, sourcePassword }
}

const opltsBackupHelpText = [
  'oplt 手动备份用法：',
  '- #oplt备份 1',
  '- #oplt备份 全部',
  '- #oplt备份 1 --auto | --api | --webdav',
  '- 可选：--host（在目标目录下追加 /<sourceHost>；默认不追加）',
  '- 可选：--user <u> --pass <p>（源端需要登录时）',
  '提示：先用 #我的备份 查看序号',
].join('\n')

export const opltsBackup = karin.command(/^#?oplt备份(.*)$/i, async (e) => {
  if (!e.isPrivate) return false

  const argsText = String(e.msg ?? '').replace(/^#?oplt备份/i, '').trim()
  const { help, selection, transport, appendHostDir, sourceUsername, sourcePassword } = parseOpltBackupArgs(argsText)

  const userKey = getUserKey(e)
  const data = readOpltData()
  const user = withOpltUser(data, userKey)
  if (!user.oplts.length) {
    await e.reply('oplt 列表为空：先用 #添加oplt <A> <B> 添加，再用 #我的备份 查看。')
    return true
  }

  if (help || selection == null) {
    await e.reply(opltsBackupHelpText)
    return true
  }

  const list = selection === 'all'
    ? user.oplts.map((it, idx) => ({ index: idx + 1, it }))
    : [{ index: selection, it: user.oplts[selection - 1] }].filter((x) => Boolean(x.it))

  if (!list.length) {
    await e.reply(`序号超出范围：${String(selection)}\n提示：先用 #我的备份 查看序号`)
    return true
  }

  const finalAppendHostDir = typeof appendHostDir === 'boolean' ? appendHostDir : false
  const effectiveTransport: OpenListBackupTransport = transport ?? 'auto'

  await e.reply([
    '开始执行 oplts 备份：',
    `- 条目：${selection === 'all' ? `全部（${list.length}）` : String(selection)}`,
    `- mode：${FIXED_BACKUP_MODE}`,
    `- timeout：${FIXED_BACKUP_TIMEOUT_SEC}s`,
    `- transport：${effectiveTransport}`,
    `- appendHostDir：${finalAppendHostDir ? 'on' : 'off'}`,
  ].join('\n'))

  let sumOk = 0
  let sumSkipped = 0
  let sumFail = 0

  for (const { index, it } of list) {
    try {
      const { sourceBaseUrl, srcDir, toDir } = resolveOpltMapping({ left: it.left, right: it.right })
      await e.reply([
        `【oplt ${index}】`,
        `源：${sourceBaseUrl}${srcDir === '/' ? '' : srcDir}`,
        `目标：${toDir}`,
      ].join('\n'))

      const res = await backupOpenListToOpenListCore({
        sourceBaseUrl,
        sourceUsername: sourceUsername ? String(sourceUsername) : undefined,
        sourcePassword: sourcePassword ? String(sourcePassword) : undefined,
        srcDir,
        toDir,
        mode: FIXED_BACKUP_MODE,
        transport: effectiveTransport,
        appendHostDir: finalAppendHostDir,
        timeoutSec: FIXED_BACKUP_TIMEOUT_SEC,
        report: (msg) => e.reply(`【oplt ${index}】${msg}`),
      })

      sumOk += res.ok
      sumSkipped += res.skipped
      sumFail += res.fail
    } catch (error: any) {
      logger.error(error)
      await e.reply(`【oplt ${index}】失败：${formatErrorMessage(error)}`)
      sumFail += 1
    }
  }

  await e.reply(`oplt 备份完成：成功 ${sumOk}，跳过 ${sumSkipped}，失败 ${sumFail}`)
  return true
}, {
  priority: 9999,
  log: true,
  name: 'oplt备份',
  permission: 'master',
})

const parseOpltNightlyArgs = (text: string) => {
  const raw = String(text ?? '').trim()
  const tokens = raw ? raw.split(/\s+/).filter(Boolean) : []

  const help = /(^|\s)(--help|-h|help|\?)(\s|$)/i.test(raw)
  const actionRaw = tokens[0] ?? ''
  const action = (() => {
    const v = String(actionRaw).trim().toLowerCase()
    if (!v) return 'view' as const
    if (['查看', '状态', 'view', 'list', 'status'].includes(v)) return 'view' as const
    if (['开启', '开', 'on', 'enable', 'start'].includes(v)) return 'enable' as const
    if (['关闭', '关', 'off', 'disable', 'stop'].includes(v)) return 'disable' as const
    return 'unknown' as const
  })()

  const transportApi = /(^|\s)(--api)(\s|$)/i.test(raw)
  const transportWebDav = /(^|\s)(--webdav|--dav)(\s|$)/i.test(raw)
  const transportAuto = /(^|\s)(--auto)(\s|$)/i.test(raw)
  const transport: OpenListBackupTransport | undefined = transportApi ? 'api' : transportWebDav ? 'webdav' : transportAuto ? 'auto' : undefined

  const host = /(^|\s)(--host|--append-host)(\s|$)/i.test(raw)
  const noHost = /(^|\s)(--no-host|--nohost)(\s|$)/i.test(raw)
  const appendHostDir = host ? true : noHost ? false : undefined

  return { help, action, transport, appendHostDir }
}

const opltsNightlyHelpText = [
  'oplt 夜间自动备份：',
  '- #oplt夜间 查看',
  '说明：夜间自动备份由统一调度器管理，默认每天 02:00 触发（先群后 oplts），固定增量 + 单文件超时 3000s，不支持通过命令修改。',
  '可选（仅影响显示/兼容旧数据）：--auto/--api/--webdav  --host/--no-host',
].join('\n')

export const opltsNightly = karin.command(/^#?oplt夜间(.*)$/i, async (e) => {
  if (!e.isPrivate) return false

  const argsText = String(e.msg ?? '').replace(/^#?oplt夜间/i, '').trim()
  const { help, action, transport, appendHostDir } = parseOpltNightlyArgs(argsText)

  const userKey = getUserKey(e)
  const data = readOpltData()
  const user = withOpltUser(data, userKey)

  const nightly = user.nightly ?? {}
  const cfg = config()
  const globalEnabled = (cfg as any)?.scheduler?.enabled !== false && (cfg as any)?.scheduler?.opltNightly?.enabled !== false
  const currentCron = String((cfg as any)?.scheduler?.tickCron ?? DEFAULT_NIGHTLY_CRON).trim() || DEFAULT_NIGHTLY_CRON
  const currentTransport: OpenListBackupTransport = (nightly.transport === 'api' || nightly.transport === 'webdav' || nightly.transport === 'auto')
    ? nightly.transport
    : DEFAULT_OPLT_NIGHTLY_TRANSPORT
  const currentHostDir = typeof nightly.appendHostDir === 'boolean' ? nightly.appendHostDir : DEFAULT_OPLT_NIGHTLY_APPEND_HOST_DIR

  if (help || action === 'unknown') {
    await e.reply(opltsNightlyHelpText)
    return true
  }

  if (action === 'view') {
    const last = nightly.lastRunAt ? formatDateTime(new Date(nightly.lastRunAt)) : '-'
    const lastResult = nightly.lastResult ? `ok=${nightly.lastResult.ok} skip=${nightly.lastResult.skipped} fail=${nightly.lastResult.fail}` : '-'
    await e.reply([
      'oplt 夜间备份：',
      `- 状态：${globalEnabled ? 'ON' : 'OFF'}`,
      `- cron：${currentCron}`,
      `- mode：${FIXED_BACKUP_MODE}`,
      `- timeout：${FIXED_BACKUP_TIMEOUT_SEC}s`,
      `- transport：${currentTransport}`,
      `- hostDir：${currentHostDir ? 'on' : 'off'}`,
      `- last：${last}`,
      `- result：${lastResult}`,
    ].join('\n'))
    return true
  }

  if (action === 'enable' || action === 'disable') {
    const nextTransport: OpenListBackupTransport = transport ?? currentTransport
    const nextHostDir = typeof appendHostDir === 'boolean' ? appendHostDir : currentHostDir
    user.nightly = { ...nightly, transport: nextTransport, appendHostDir: nextHostDir }
    writeOpltData(data)

    await e.reply([
      '夜间自动备份已由统一调度器管理，不支持通过命令开/关或修改 cron/mode/timeout。',
      `- 全局状态：${globalEnabled ? 'ON' : 'OFF'}`,
      `- cron：${currentCron}`,
      `- mode：${FIXED_BACKUP_MODE}`,
      `- timeout：${FIXED_BACKUP_TIMEOUT_SEC}s`,
      `- transport：${nextTransport}`,
      `- hostDir：${nextHostDir ? 'on' : 'off'}`,
      '',
      '如需关闭：请在配置文件设置 config.scheduler.opltNightly.enabled=false（或关闭 config.scheduler.enabled）。',
    ].join('\n'))
    return true
  }

  await e.reply(opltsNightlyHelpText)
  return true
}, {
  priority: 9999,
  log: true,
  name: 'oplt夜间',
  permission: 'master',
})
