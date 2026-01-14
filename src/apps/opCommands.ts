import path from 'node:path'
import { dir } from '@/dir'
import { config } from '@/utils'
import { ensurePluginResources } from '@/utils/resources'
import { normalizePosixPath } from '@/model/shared/path'
import { readJsonSafe, writeJsonSafe } from '@/model/shared/fsJson'
import { karin, logger, render, segment } from 'node-karin'

type OpltItem = { left: string, right: string }
type OpltDataV1 = {
  version: 1
  users: Record<string, { oplts: OpltItem[] }>
}

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

const getOpltDataPath = () => path.join(dir.DataDir, 'op-commands.json')

const normalizeText = (value: unknown) => String(value ?? '').trim()

const readOpltData = (): OpltDataV1 => {
  const raw = readJsonSafe(getOpltDataPath())
  const users = raw && typeof raw === 'object' && typeof raw.users === 'object' && raw.users ? raw.users : {}
  return { version: 1, users }
}

const writeOpltData = (data: OpltDataV1) => writeJsonSafe(getOpltDataPath(), data)

const withOpltUser = (data: OpltDataV1, userKey: string) => {
  const key = String(userKey || 'global')
  const user = data.users[key]
  if (user && Array.isArray(user.oplts)) return user
  const next = { oplts: [] as OpltItem[] }
  data.users[key] = next
  return next
}

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
  '- #添加oplt <A> <B>：添加一条 oplts 记录（原样保存）',
  '- #删除oplt <序号>：删除一条 oplts 记录',
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
    if (!user.oplts.length) return '（空）\n可用：#添加oplt A B'
    const lines: string[] = []
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
      if (!user.oplts.length) return ['- （空）可用：#添加oplt <A> <B>']
      return user.oplts.map((it, i) => `- ${i + 1}. ${it.left} ${it.right}`)
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
    const userKey = getUserKey(e)
    const data = readOpltData()
    const user = withOpltUser(data, userKey)

    const exists = user.oplts.some((it) => it.left === left && it.right === right)
    if (!exists) user.oplts.push({ left, right })
    writeOpltData(data)

    await e.reply(exists ? '该 oplts 已存在（未重复添加）' : `已添加 oplts：${left} ${right}`)
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
