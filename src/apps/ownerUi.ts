import { karin, logger, segment } from 'node-karin'
import { dir } from '@/dir'
import { readGroupSyncState } from '@/model/groupFiles/state'
import { formatErrorMessage } from '@/model/shared/errors'
import { normalizePosixPath } from '@/model/shared/path'
import { renderUiPngBase64 } from '@/model/ui/render'
import { bindBackupGroup, setGroupUploadBackup, unbindBackupGroup } from '@/model/groupSync/bindings'
import { addOpenListForwardRule, deleteOpenListForwardRule, getOpenListForwardRule, listOpenListForwardRules } from '@/model/openlistForward/store'
import { runOpenListForwardRule } from '@/model/openlistForward/run'
import { readMergedConfig } from '@/model/shared/pluginConfig'
import type { SyncMode } from '@/model/groupFiles/types'

type Img = string | string[]

const formatDateTime = (date: Date) => {
  try {
    return date.toLocaleString('zh-CN', {
      year: 'numeric',
      month: '2-digit',
      day: '2-digit',
      hour: '2-digit',
      minute: '2-digit',
      second: '2-digit',
      hour12: false,
    })
  } catch {
    return date.toISOString()
  }
}

const replyImages = async (e: any, images: Img) => {
  const list = Array.isArray(images) ? images : [images]
  for (const img of list) {
    await e.reply(segment.image(`base64://${img}`))
  }
}

const parseHelp = (raw: string) => /(^|\s)(--help|-h|help|\?)(\s|$)/i.test(String(raw ?? '').trim())

const pickFirstNumberToken = (raw: string) => {
  const text = String(raw ?? '')
  const match = text.match(/(\d{5,})/)
  return match ? match[1] : undefined
}

const pickFlag = (raw: string, names: string[]) => {
  const text = String(raw ?? '').trim()
  return names.some((n) => new RegExp(`(^|\\s)(--${n}|${n})(\\s|$)`, 'i').test(text))
}

const pickFlagValue = (raw: string, names: string[]) => {
  const text = String(raw ?? '')
  for (const name of names) {
    const m = text.match(new RegExp(`--${name}\\s+(\\S+)`, 'i')) ?? text.match(new RegExp(`(^|\\s)${name}=(\\S+)`, 'i'))
    if (m) return m[m.length - 1]
  }
}

const pickMode = (raw: string, fallback: SyncMode): SyncMode => {
  const text = String(raw ?? '').toLowerCase()
  if (/(^|\s)(--full|full)(\s|$)/.test(text)) return 'full'
  if (/(^|\s)(--inc|--incremental|inc|incremental)(\s|$)/.test(text)) return 'incremental'
  return fallback
}

const modeLabel = (mode: SyncMode) => (mode === 'incremental' ? '增量' : '全量')

const transportLabel = (t: string) => {
  const v = String(t ?? '').toLowerCase()
  if (v === 'api') return 'API'
  if (v === 'webdav' || v === 'dav') return 'WebDAV'
  return 'AUTO'
}

const buildActionCard = async (params: {
  title: string
  status: 'ok' | 'warn' | 'bad'
  statusText?: string
  subtitle?: string
  sections?: Array<{
    title: string
    hint?: string
    rows?: Array<{ k: string, v: string, mono?: boolean }>
    lines?: Array<{ text: string, mono?: boolean }>
  }>
  footerRight?: string
}) => {
  const img = await renderUiPngBase64({
    template: 'ui/action-result.html',
    name: 'qgroup-owner-action',
    data: {
      ...params,
      generatedAt: formatDateTime(new Date()),
    },
    viewport: { width: 540, height: 960, deviceScaleFactor: 2 },
    multiPage: 960,
  }) as Img
  return img
}

/**
 * #群文件面板
 */
export const ownerPanel = karin.command(/^#?群文件面板(?:\s+(\d+))?(\s+.*)?$/i, async (e) => {
  if (!e.isPrivate) return false

  const help = parseHelp(e.msg)
  if (help) {
    const img = await buildActionCard({
      title: '群文件面板帮助',
      status: 'ok',
      sections: [
        {
          title: '常用命令',
          lines: [
            { text: '#群文件面板', mono: true },
            { text: '#绑定备份群 123456 --to /目标目录', mono: true },
            { text: '#解绑备份群 123456', mono: true },
            { text: '#开启群文件监听 123456', mono: true },
            { text: '#关闭群文件监听 123456', mono: true },
            { text: '#添加op转发 http://127.0.0.1:5244 --src / --to /backup', mono: true },
            { text: '#op转发 列表 | 查看 <id> | 执行 <id> | 删除 <id>', mono: true },
          ],
        },
      ],
      footerRight: '#群文件面板',
    })
    await replyImages(e, img)
    return true
  }

  try {
    const cfg: any = readMergedConfig()
    const openlistBaseUrl = String(cfg.openlistBaseUrl ?? '').trim()
    const openlistUsername = String(cfg.openlistUsername ?? '').trim()
    const openlistTargetDir = String(cfg.openlistTargetDir ?? '').trim()

    const targets = Array.isArray(cfg.groupSyncTargets) ? cfg.groupSyncTargets : []
    const groups = targets.map((t: any) => {
      const groupId = String(t?.groupId ?? '').trim()
      const enabled = t?.enabled !== false
      const uploadBackup = Boolean(t?.uploadBackup === true || ['true', '1', 'on'].includes(String(t?.uploadBackup ?? '').trim().toLowerCase()))
      const mode: SyncMode = 'incremental'
      const targetDir = String(t?.targetDir ?? '').trim()

      const state = groupId ? readGroupSyncState(groupId) : undefined
      const lastSyncAtText = state?.lastSyncAt ? formatDateTime(new Date(state.lastSyncAt)) : ''

      return {
        groupId,
        enabled,
        uploadBackup,
        mode,
        modeLabel: modeLabel(mode),
        targetDir,
        lastSyncAtText,
      }
    }).filter((g: any) => g.groupId)

    groups.sort((a: any, b: any) => Number(b.enabled) - Number(a.enabled) || String(a.groupId).localeCompare(String(b.groupId)))

    const rules = listOpenListForwardRules().map((r) => {
      const lastRunAtText = r.lastRunAt ? formatDateTime(new Date(r.lastRunAt)) : ''
      const lastResultText = r.lastResult ? `ok=${r.lastResult.ok} skip=${r.lastResult.skipped} fail=${r.lastResult.fail}` : ''
      const mode = (r.mode ?? 'full') as SyncMode
      const transport = String(r.transport ?? 'auto')
      return {
        id: r.id,
        name: r.name ?? '',
        sourceBaseUrl: r.sourceBaseUrl,
        srcDir: r.srcDir ?? '/',
        toDir: r.toDir ?? '',
        mode,
        modeLabel: modeLabel(mode),
        transport,
        transportLabel: transportLabel(transport),
        lastRunAtText,
        lastResultText,
      }
    })

    const quickCommands = [
      '#绑定备份群 123456 --to /目标目录',
      '#解绑备份群 123456',
      '#开启群文件监听 123456',
      '#关闭群文件监听 123456',
      '#添加op转发 http://127.0.0.1:5244 --src / --to /backup',
      '#op转发 列表',
      '#op转发 查看 0000abc',
      '#op转发 执行 0000abc --full',
      '#op转发 删除 0000abc',
    ]

    const img = await renderUiPngBase64({
      template: 'ui/panel.html',
      name: 'qgroup-owner-panel',
      data: {
        pluginName: dir.name,
        pluginVersion: dir.version,
        generatedAt: formatDateTime(new Date()),
        openlistBaseUrl,
        openlistUsername,
        openlistTargetDir,
        groups,
        rules,
        quickCommands,
      },
      viewport: { width: 540, height: 960, deviceScaleFactor: 2 },
      multiPage: 960,
    }) as Img

    await replyImages(e, img)
    return true
  } catch (error: any) {
    logger.error(error)
    await e.reply(`面板生成失败：${formatErrorMessage(error)}`)
    return true
  }
}, {
  priority: 9999,
  log: true,
  name: '群文件面板（主人）',
  permission: 'master',
})

/**
 * #绑定备份群 <groupId> [--to <dir>] [--flat|--keep]
 */
export const bindGroup = karin.command(/^#?绑定备份群(.*)$/i, async (e) => {
  if (!e.isPrivate) return false

  const argsText = e.msg.replace(/^#?绑定备份群/i, '')
  if (parseHelp(argsText)) {
    const img = await buildActionCard({
      title: '绑定备份群帮助',
      status: 'ok',
      sections: [
        {
          title: '用法',
          lines: [
            { text: '#绑定备份群 <群号> [--to /目标目录] [--flat|--keep]', mono: true },
            { text: '绑定后会自动开启群文件上传监听（uploadBackup=on）', mono: false },
          ],
        },
      ],
      footerRight: '#绑定备份群',
    })
    await replyImages(e, img)
    return true
  }

  const groupId = pickFirstNumberToken(argsText)
  if (!groupId) {
    const img = await buildActionCard({
      title: '绑定失败',
      status: 'bad',
      statusText: '参数缺失',
      sections: [{ title: '原因', lines: [{ text: '缺少群号：#绑定备份群 123456', mono: true }] }],
      footerRight: '#绑定备份群',
    })
    await replyImages(e, img)
    return true
  }

  const to = pickFlagValue(argsText, ['to'])
  const flat = pickFlag(argsText, ['flat']) ? true : pickFlag(argsText, ['keep']) ? false : undefined
  const mode: SyncMode = 'incremental'

  try {
    const res = bindBackupGroup({
      groupId,
      targetDir: to ? normalizePosixPath(to) : undefined,
      mode,
      flat,
    })

    const target = res.after
    const img = await buildActionCard({
      title: '绑定成功',
      status: 'ok',
      sections: [
        {
          title: '群信息',
          rows: [
            { k: '群号', v: groupId, mono: true },
            { k: '监听', v: 'on（uploadBackup）', mono: true },
          ],
        },
        {
          title: '生效策略',
          rows: [
            { k: '模式', v: modeLabel(mode), mono: false },
            { k: '目标目录', v: String(target?.targetDir ?? ''), mono: true },
            { k: '平铺', v: String(target?.flat ?? false), mono: true },
          ],
        },
        {
          title: '快捷操作',
          lines: [
            { text: `#同步群文件 ${groupId}`, mono: true },
            { text: `#关闭群文件监听 ${groupId}`, mono: true },
            { text: `#解绑备份群 ${groupId}`, mono: true },
          ],
        },
      ],
      footerRight: '#群文件面板',
    })

    await replyImages(e, img)
    return true
  } catch (error: any) {
    logger.error(error)
    const img = await buildActionCard({
      title: '绑定失败',
      status: 'bad',
      statusText: '异常',
      sections: [{ title: '错误', lines: [{ text: formatErrorMessage(error) }] }],
      footerRight: '#绑定备份群',
    })
    await replyImages(e, img)
    return true
  }
}, {
  priority: 9999,
  log: true,
  name: '绑定备份群（主人）',
  permission: 'master',
})

/**
 * #解绑备份群 <groupId>
 */
export const unbindGroup = karin.command(/^#?解绑备份群(.*)$/i, async (e) => {
  if (!e.isPrivate) return false

  const argsText = e.msg.replace(/^#?解绑备份群/i, '')
  const groupId = pickFirstNumberToken(argsText)
  if (!groupId) {
    const img = await buildActionCard({
      title: '解绑失败',
      status: 'bad',
      statusText: '参数缺失',
      sections: [{ title: '原因', lines: [{ text: '缺少群号：#解绑备份群 123456', mono: true }] }],
      footerRight: '#解绑备份群',
    })
    await replyImages(e, img)
    return true
  }

  try {
    const { removed } = unbindBackupGroup(groupId)
    const img = await buildActionCard({
      title: '解绑完成',
      status: removed ? 'ok' : 'warn',
      statusText: removed ? '已删除' : '未找到',
      sections: [
        {
          title: '结果',
          rows: [
            { k: '群号', v: groupId, mono: true },
            { k: '状态', v: removed ? '已删除群配置（groupSyncTargets）' : '该群未配置，无需删除', mono: false },
          ],
        },
        {
          title: '下一步',
          lines: [
            { text: '#群文件面板', mono: true },
            { text: `#绑定备份群 ${groupId}`, mono: true },
          ],
        },
      ],
      footerRight: '#群文件面板',
    })
    await replyImages(e, img)
    return true
  } catch (error: any) {
    logger.error(error)
    const img = await buildActionCard({
      title: '解绑失败',
      status: 'bad',
      statusText: '异常',
      sections: [{ title: '错误', lines: [{ text: formatErrorMessage(error) }] }],
      footerRight: '#解绑备份群',
    })
    await replyImages(e, img)
    return true
  }
}, {
  priority: 9999,
  log: true,
  name: '解绑备份群（主人）',
  permission: 'master',
})

/**
 * #开启群文件监听 <groupId>
 */
export const enableUploadBackup = karin.command(/^#?开启群文件监听(.*)$/i, async (e) => {
  if (!e.isPrivate) return false

  const argsText = e.msg.replace(/^#?开启群文件监听/i, '')
  const groupId = pickFirstNumberToken(argsText)
  if (!groupId) {
    const img = await buildActionCard({
      title: '开启失败',
      status: 'bad',
      statusText: '参数缺失',
      sections: [{ title: '原因', lines: [{ text: '缺少群号：#开启群文件监听 123456', mono: true }] }],
      footerRight: '#开启群文件监听',
    })
    await replyImages(e, img)
    return true
  }

  try {
    const res = setGroupUploadBackup({ groupId, enabled: true, ensureExists: true })
    const target = res.after
    const img = await buildActionCard({
      title: '已开启群文件监听',
      status: 'ok',
      sections: [
        {
          title: '群信息',
          rows: [
            { k: '群号', v: groupId, mono: true },
            { k: '监听', v: 'on（uploadBackup）', mono: true },
            { k: '目录', v: String(target?.targetDir ?? ''), mono: true },
          ],
        },
        {
          title: '快捷操作',
          lines: [
            { text: `#关闭群文件监听 ${groupId}`, mono: true },
            { text: `#解绑备份群 ${groupId}`, mono: true },
          ],
        },
      ],
      footerRight: '#群文件面板',
    })
    await replyImages(e, img)
    return true
  } catch (error: any) {
    logger.error(error)
    const img = await buildActionCard({
      title: '开启失败',
      status: 'bad',
      statusText: '异常',
      sections: [{ title: '错误', lines: [{ text: formatErrorMessage(error) }] }],
      footerRight: '#开启群文件监听',
    })
    await replyImages(e, img)
    return true
  }
}, {
  priority: 9999,
  log: true,
  name: '开启群文件监听（主人）',
  permission: 'master',
})

/**
 * #关闭群文件监听 <groupId>
 */
export const disableUploadBackup = karin.command(/^#?关闭群文件监听(.*)$/i, async (e) => {
  if (!e.isPrivate) return false

  const argsText = e.msg.replace(/^#?关闭群文件监听/i, '')
  const groupId = pickFirstNumberToken(argsText)
  if (!groupId) {
    const img = await buildActionCard({
      title: '关闭失败',
      status: 'bad',
      statusText: '参数缺失',
      sections: [{ title: '原因', lines: [{ text: '缺少群号：#关闭群文件监听 123456', mono: true }] }],
      footerRight: '#关闭群文件监听',
    })
    await replyImages(e, img)
    return true
  }

  try {
    const existing = getGroupSyncTargetFromMerged(groupId)
    if (!existing) {
      const img = await buildActionCard({
        title: '未找到群配置',
        status: 'warn',
        statusText: '未绑定',
        sections: [
          { title: '提示', lines: [{ text: `该群未绑定：#绑定备份群 ${groupId}`, mono: true }] },
        ],
        footerRight: '#群文件面板',
      })
      await replyImages(e, img)
      return true
    }

    const res = setGroupUploadBackup({ groupId, enabled: false, ensureExists: false })
    const target = res.after ?? existing
    const img = await buildActionCard({
      title: '已关闭群文件监听',
      status: 'ok',
      sections: [
        {
          title: '群信息',
          rows: [
            { k: '群号', v: groupId, mono: true },
            { k: '监听', v: 'off（uploadBackup）', mono: true },
            { k: '目录', v: String(target?.targetDir ?? ''), mono: true },
          ],
        },
        {
          title: '快捷操作',
          lines: [
            { text: `#开启群文件监听 ${groupId}`, mono: true },
            { text: `#解绑备份群 ${groupId}`, mono: true },
          ],
        },
      ],
      footerRight: '#群文件面板',
    })
    await replyImages(e, img)
    return true
  } catch (error: any) {
    logger.error(error)
    const img = await buildActionCard({
      title: '关闭失败',
      status: 'bad',
      statusText: '异常',
      sections: [{ title: '错误', lines: [{ text: formatErrorMessage(error) }] }],
      footerRight: '#关闭群文件监听',
    })
    await replyImages(e, img)
    return true
  }
}, {
  priority: 9999,
  log: true,
  name: '关闭群文件监听（主人）',
  permission: 'master',
})

const getGroupSyncTargetFromMerged = (groupId: string) => {
  const cfg: any = readMergedConfig()
  const list = Array.isArray(cfg.groupSyncTargets) ? cfg.groupSyncTargets : []
  return list.find((it: any) => String(it?.groupId) === String(groupId))
}

/**
 * #添加op转发 <sourceBaseUrl> [--src /] [--to /] [--name xxx] [--user u] [--pass p] [--inc|--full] [--auto|--api|--webdav]
 */
export const addOpForward = karin.command(/^#?添加op转发(.*)$/i, async (e) => {
  if (!e.isPrivate) return false

  const argsText = e.msg.replace(/^#?添加op转发/i, '').trim()
  if (parseHelp(argsText)) {
    const img = await buildActionCard({
      title: '添加op转发帮助',
      status: 'ok',
      sections: [
        {
          title: '用法',
          lines: [
            { text: '#添加op转发 <源OpenListBaseUrl> [--src /] [--to /backup] [--name xxx]', mono: true },
            { text: '#添加op转发 http://127.0.0.1:5244 --src / --to /backup --full', mono: true },
          ],
        },
        {
          title: '说明',
          lines: [
            { text: '目的端固定为插件配置 openlistBaseUrl（单目的端）' },
            { text: '默认只保存规则，手动触发：#op转发 执行 <id>（默认全量）' },
          ],
        },
      ],
      footerRight: '#添加op转发',
    })
    await replyImages(e, img)
    return true
  }

  const firstUrl = (argsText.split(/\s+/).filter(Boolean)[0] ?? '').trim()
  if (!/^https?:\/\//i.test(firstUrl)) {
    const img = await buildActionCard({
      title: '添加失败',
      status: 'bad',
      statusText: '参数缺失',
      sections: [
        { title: '原因', lines: [{ text: '缺少源 OpenList 地址，例如：#添加op转发 http://127.0.0.1:5244', mono: true }] },
      ],
      footerRight: '#添加op转发',
    })
    await replyImages(e, img)
    return true
  }

  // 兼容用户误输入“两个URL”的场景：本期目的端固定，第二个URL会提示忽略
  const secondUrl = (argsText.split(/\s+/).filter(Boolean)[1] ?? '').trim()
  const hasSecondUrl = /^https?:\/\//i.test(secondUrl)

  const rest = argsText.slice(firstUrl.length).trim()

  const srcDir = pickFlagValue(rest, ['src', 'srcDir']) ?? '/'
  const toDir = pickFlagValue(rest, ['to', 'toDir'])
  const name = pickFlagValue(rest, ['name'])
  const sourceUsername = pickFlagValue(rest, ['user', 'username'])
  const sourcePassword = pickFlagValue(rest, ['pass', 'password'])
  const mode = pickMode(rest, 'full')
  const transport = pickFlag(rest, ['api']) ? 'api' : pickFlag(rest, ['webdav', 'dav']) ? 'webdav' : 'auto'

  const concurrency = pickFlagValue(rest, ['concurrency']) ? Number(pickFlagValue(rest, ['concurrency'])) : undefined
  const scanConcurrency = pickFlagValue(rest, ['scan', 'scanConcurrency', 'scan_concurrency']) ? Number(pickFlagValue(rest, ['scan', 'scanConcurrency', 'scan_concurrency'])) : undefined
  const perPage = pickFlagValue(rest, ['per-page', 'perpage', 'page-size', 'per_page']) ? Number(pickFlagValue(rest, ['per-page', 'perpage', 'page-size', 'per_page'])) : undefined
  const timeoutSec = pickFlagValue(rest, ['timeout']) ? Number(pickFlagValue(rest, ['timeout'])) : undefined

  try {
    const rule = addOpenListForwardRule({
      sourceBaseUrl: firstUrl,
      sourceUsername: sourceUsername || undefined,
      sourcePassword: sourcePassword || undefined,
      srcDir,
      toDir,
      name: name || undefined,
      mode,
      transport,
      concurrency: typeof concurrency === 'number' && Number.isFinite(concurrency) ? concurrency : undefined,
      scanConcurrency: typeof scanConcurrency === 'number' && Number.isFinite(scanConcurrency) ? scanConcurrency : undefined,
      perPage: typeof perPage === 'number' && Number.isFinite(perPage) ? perPage : undefined,
      timeoutSec: typeof timeoutSec === 'number' && Number.isFinite(timeoutSec) ? timeoutSec : undefined,
    })

    const cfg: any = readMergedConfig()
    const targetBaseUrl = String(cfg.openlistBaseUrl ?? '').trim()

    const img = await buildActionCard({
      title: '已添加 OP 转发规则',
      status: 'ok',
      sections: [
        {
          title: '规则',
          rows: [
            { k: 'ruleId', v: rule.id, mono: true },
            { k: '名称', v: rule.name ?? '-', mono: false },
            { k: '源', v: rule.sourceBaseUrl, mono: true },
            { k: '源鉴权', v: sourceUsername ? '已配置' : '未配置(guest)', mono: false },
            { k: 'src', v: String(rule.srcDir ?? '/'), mono: true },
            { k: 'to', v: String(rule.toDir ?? '(默认 openlistTargetDir)'), mono: true },
            { k: '模式', v: modeLabel(mode), mono: false },
            { k: '传输', v: transportLabel(transport), mono: false },
          ],
        },
        {
          title: '目的端（固定）',
          rows: [
            { k: 'BaseUrl', v: targetBaseUrl || '(未配置)', mono: true },
          ],
        },
        {
          title: '快捷操作',
          lines: [
            { text: `#op转发 查看 ${rule.id}`, mono: true },
            { text: `#op转发 执行 ${rule.id} --full`, mono: true },
            { text: `#op转发 删除 ${rule.id}`, mono: true },
          ],
        },
        ...(hasSecondUrl ? [{
          title: '提示',
          lines: [{ text: '你输入了两个URL：本期目的端固定为配置 openlistBaseUrl，第二个URL已忽略。' }],
        }] : []),
      ],
      footerRight: '#op转发',
    })
    await replyImages(e, img)
    return true
  } catch (error: any) {
    logger.error(error)
    const img = await buildActionCard({
      title: '添加失败',
      status: 'bad',
      statusText: '异常',
      sections: [{ title: '错误', lines: [{ text: formatErrorMessage(error) }] }],
      footerRight: '#添加op转发',
    })
    await replyImages(e, img)
    return true
  }
}, {
  priority: 9999,
  log: true,
  name: '添加OP转发（主人）',
  permission: 'master',
})

/**
 * #op转发 列表|查看|执行|删除 ...
 */
export const opForward = karin.command(/^#?op转发(.*)$/i, async (e) => {
  if (!e.isPrivate) return false

  const argsText = e.msg.replace(/^#?op转发/i, '').trim()
  const tokens = argsText.split(/\s+/).filter(Boolean)
  const action = (tokens[0] ?? '').toLowerCase()

  if (!action || action === '列表' || action === 'list') {
    try {
      const rules = listOpenListForwardRules()
      const lines = rules.map((r) => {
        const name = r.name ? ` · ${r.name}` : ''
        const mode = modeLabel((r.mode ?? 'full') as SyncMode)
        const transport = transportLabel(String(r.transport ?? 'auto'))
        const last = r.lastRunAt ? formatDateTime(new Date(r.lastRunAt)) : '-'
        const result = r.lastResult ? `ok=${r.lastResult.ok} skip=${r.lastResult.skipped} fail=${r.lastResult.fail}` : '-'
        return `${r.id}${name}\n源: ${r.sourceBaseUrl}\n${mode} ${transport} | last=${last} | ${result}`
      })

      const img = await buildActionCard({
        title: 'OP 转发规则列表',
        status: 'ok',
        sections: [
          {
            title: `共 ${rules.length} 条`,
            lines: lines.length
              ? lines.map(text => ({ text }))
              : [{ text: '暂无规则：#添加op转发 http://127.0.0.1:5244', mono: true }],
          },
        ],
        footerRight: '#op转发 列表',
      })
      await replyImages(e, img)
      return true
    } catch (error: any) {
      logger.error(error)
      await e.reply(formatErrorMessage(error))
      return true
    }
  }

  if (action === '查看' || action === 'view') {
    const id = tokens[1]
    const rule = id ? getOpenListForwardRule(id) : undefined
    if (!rule) {
      const img = await buildActionCard({
        title: '查看失败',
        status: 'bad',
        statusText: '未找到',
        sections: [{ title: '提示', lines: [{ text: '用法：#op转发 查看 <ruleId>', mono: true }] }],
        footerRight: '#op转发 查看',
      })
      await replyImages(e, img)
      return true
    }

    const img = await buildActionCard({
      title: 'OP 转发规则详情',
      status: 'ok',
      sections: [
        {
          title: '规则',
          rows: [
            { k: 'ruleId', v: rule.id, mono: true },
            { k: '名称', v: rule.name ?? '-', mono: false },
            { k: '源', v: rule.sourceBaseUrl, mono: true },
            { k: '源鉴权', v: rule.sourceUsername ? '已配置' : '未配置(guest)', mono: false },
            { k: 'src', v: String(rule.srcDir ?? '/'), mono: true },
            { k: 'to', v: String(rule.toDir ?? '(默认 openlistTargetDir)'), mono: true },
            { k: '模式', v: modeLabel((rule.mode ?? 'full') as SyncMode), mono: false },
            { k: '传输', v: transportLabel(String(rule.transport ?? 'auto')), mono: false },
            { k: '并发', v: String(rule.concurrency ?? 3), mono: true },
            { k: '扫描并发', v: String(rule.scanConcurrency ?? 20), mono: true },
            { k: 'perPage', v: String(rule.perPage ?? 1000), mono: true },
            { k: 'timeout', v: `${rule.timeoutSec ?? 600}s`, mono: true },
          ],
        },
        {
          title: '最近执行',
          rows: [
            { k: 'lastRunAt', v: rule.lastRunAt ? formatDateTime(new Date(rule.lastRunAt)) : '-', mono: true },
            { k: 'lastResult', v: rule.lastResult ? `ok=${rule.lastResult.ok} skip=${rule.lastResult.skipped} fail=${rule.lastResult.fail}` : '-', mono: true },
          ],
        },
        {
          title: '快捷操作',
          lines: [
            { text: `#op转发 执行 ${rule.id} --full`, mono: true },
            { text: `#op转发 删除 ${rule.id}`, mono: true },
          ],
        },
      ],
      footerRight: '#op转发',
    })
    await replyImages(e, img)
    return true
  }

  if (action === '删除' || action === 'del' || action === 'delete') {
    const id = tokens[1]
    if (!id) {
      const img = await buildActionCard({
        title: '删除失败',
        status: 'bad',
        statusText: '参数缺失',
        sections: [{ title: '用法', lines: [{ text: '用法：#op转发 删除 <ruleId>', mono: true }] }],
        footerRight: '#op转发 删除',
      })
      await replyImages(e, img)
      return true
    }

    const { removed } = deleteOpenListForwardRule(id)
    const img = await buildActionCard({
      title: '删除完成',
      status: removed ? 'ok' : 'warn',
      statusText: removed ? '已删除' : '未找到',
      sections: [
        {
          title: '结果',
          rows: [
            { k: 'ruleId', v: String(id), mono: true },
            { k: '状态', v: removed ? '已删除该规则' : '该 ruleId 不存在', mono: false },
          ],
        },
        {
          title: '下一步',
          lines: [
            { text: '#op转发 列表', mono: true },
            { text: '#群文件面板', mono: true },
          ],
        },
      ],
      footerRight: '#op转发',
    })
    await replyImages(e, img)
    return true
  }

  if (action === '执行' || action === 'run') {
    const id = tokens[1]
    if (!id) {
      const img = await buildActionCard({
        title: '执行失败',
        status: 'bad',
        statusText: '参数缺失',
        sections: [{ title: '用法', lines: [{ text: '用法：#op转发 执行 <ruleId> [--inc|--full]', mono: true }] }],
        footerRight: '#op转发 执行',
      })
      await replyImages(e, img)
      return true
    }

    const rule = getOpenListForwardRule(id)
    if (!rule) {
      const img = await buildActionCard({
        title: '执行失败',
        status: 'bad',
        statusText: '未找到',
        sections: [{ title: '提示', lines: [{ text: '#op转发 列表', mono: true }] }],
        footerRight: '#op转发 执行',
      })
      await replyImages(e, img)
      return true
    }

    const forcedMode = pickMode(argsText, 'full')

    const startCard = await buildActionCard({
      title: '任务已启动',
      status: 'ok',
      sections: [
        {
          title: '执行信息',
          rows: [
            { k: 'ruleId', v: rule.id, mono: true },
            { k: '源', v: rule.sourceBaseUrl, mono: true },
            { k: '模式', v: modeLabel(forcedMode), mono: false },
            { k: '传输', v: transportLabel(String(rule.transport ?? 'auto')), mono: false },
          ],
        },
        {
          title: '提示',
          lines: [
            { text: '执行过程中会有少量文本进度提示；结束后会发送图片总结。' },
          ],
        },
      ],
      footerRight: '#op转发 执行',
    })
    await replyImages(e, startCard)

    try {
      const result = await runOpenListForwardRule({
        rule,
        mode: forcedMode,
        report: (msg) => e.reply(msg),
      })

      const doneCard = await buildActionCard({
        title: '执行完成',
        status: result.fail ? 'warn' : 'ok',
        statusText: result.fail ? '部分失败' : '成功',
        sections: [
          {
            title: '结果',
            rows: [
              { k: 'ruleId', v: rule.id, mono: true },
              { k: 'ok', v: String(result.ok), mono: true },
              { k: 'skipped', v: String(result.skipped), mono: true },
              { k: 'fail', v: String(result.fail), mono: true },
              { k: '耗时', v: `${Math.floor(result.ms / 1000)}s`, mono: true },
            ],
          },
          {
            title: '下一步',
            lines: [
              { text: `#op转发 查看 ${rule.id}`, mono: true },
              { text: '#群文件面板', mono: true },
            ],
          },
        ],
        footerRight: '#op转发',
      })
      await replyImages(e, doneCard)
      return true
    } catch (error: any) {
      logger.error(error)
      const errCard = await buildActionCard({
        title: '执行失败',
        status: 'bad',
        statusText: '异常',
        sections: [{ title: '错误', lines: [{ text: formatErrorMessage(error) }] }],
        footerRight: '#op转发 执行',
      })
      await replyImages(e, errCard)
      return true
    }
  }

  const unknown = await buildActionCard({
    title: '未知子命令',
    status: 'warn',
    statusText: 'help',
    sections: [
      {
        title: '可用子命令',
        lines: [
          { text: '#op转发 列表', mono: true },
          { text: '#op转发 查看 <ruleId>', mono: true },
          { text: '#op转发 执行 <ruleId> [--full|--inc]', mono: true },
          { text: '#op转发 删除 <ruleId>', mono: true },
        ],
      },
    ],
    footerRight: '#op转发',
  })
  await replyImages(e, unknown)
  return true
}, {
  priority: 9999,
  log: true,
  name: 'OP转发（主人）',
  permission: 'master',
})
