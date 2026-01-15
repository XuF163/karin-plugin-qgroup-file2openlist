import { karin, logger } from 'node-karin'
import { backupOpenListToOpenListCore } from '@/model/openlist'
import { handleGroupFileUploadedAutoBackup } from '@/model/groupFiles'
import { buildActionCard, replyImages } from '@/model/ui/actionCard'
import { config } from '@/utils'
import { formatErrorMessage } from '@/model/shared/errors'
import type { OpenListBackupTransport, SyncMode } from '@/model/groupFiles/types'

const FIXED_BACKUP_MODE: SyncMode = 'incremental'
const FIXED_TIMEOUT_SEC = 3000

const parseBackupOpenListArgs = (text: string) => {
  const raw = text.trim()
  const tokens = raw ? raw.split(/\s+/).filter(Boolean) : []
  const help = /(^|\s)(--help|-h|help|\?)(\s|$)/i.test(raw)

  const first = tokens[0]
  const sourceBaseUrl = first && /^https?:\/\//i.test(first) ? first : undefined
  const restRaw = sourceBaseUrl ? raw.slice(first.length).trim() : raw

  const srcMatch = restRaw.match(/--src\s+(\S+)/i) ?? restRaw.match(/(^|\s)src=(\S+)/i)
  const srcDir = srcMatch ? srcMatch[srcMatch.length - 1] : undefined

  const toMatch = restRaw.match(/--to\s+(\S+)/i) ?? restRaw.match(/(^|\s)to=(\S+)/i)
  const toDir = toMatch ? toMatch[toMatch.length - 1] : undefined

  const maxMatch = restRaw.match(/--max\s+(\d+)/i) ?? restRaw.match(/(^|\s)max=(\d+)/i)
  const maxFiles = maxMatch ? Number(maxMatch[maxMatch.length - 1]) : undefined

  const concurrencyMatch = restRaw.match(/--concurrency\s+(\d+)/i) ?? restRaw.match(/(^|\s)concurrency=(\d+)/i)
  const concurrency = concurrencyMatch ? Number(concurrencyMatch[concurrencyMatch.length - 1]) : undefined

  const scanMatch = restRaw.match(/--scan(?:-concurrency)?\s+(\d+)/i)
    ?? restRaw.match(/(^|\s)scan=(\d+)/i)
    ?? restRaw.match(/(^|\s)scanConcurrency=(\d+)/i)
    ?? restRaw.match(/(^|\s)scan_concurrency=(\d+)/i)
  const scanConcurrency = scanMatch ? Number(scanMatch[scanMatch.length - 1]) : undefined

  const perPageMatch = restRaw.match(/--per-page\s+(\d+)/i)
    ?? restRaw.match(/--perpage\s+(\d+)/i)
    ?? restRaw.match(/--page-size\s+(\d+)/i)
    ?? restRaw.match(/(^|\s)per[_-]?page=(\d+)/i)
    ?? restRaw.match(/(^|\s)pageSize=(\d+)/i)
    ?? restRaw.match(/(^|\s)per_page=(\d+)/i)
  const perPage = perPageMatch ? Number(perPageMatch[perPageMatch.length - 1]) : undefined

  const transportApi = /(^|\s)(--api)(\s|$)/i.test(restRaw)
  const transportWebDav = /(^|\s)(--webdav|--dav)(\s|$)/i.test(restRaw)
  const transportAuto = /(^|\s)(--auto)(\s|$)/i.test(restRaw)
  const transport: OpenListBackupTransport | undefined = transportApi ? 'api' : transportWebDav ? 'webdav' : transportAuto ? 'auto' : undefined

  return {
    sourceBaseUrl,
    srcDir,
    toDir,
    maxFiles,
    concurrency,
    scanConcurrency,
    perPage,
    transport,
    help,
  }
}

const openListToOpenListHelpText = [
  'OpenList -> OpenList 备份用法：',
  '- 私聊：#备份oplist [源OpenList地址] [参数]',
  '- 示例：#备份oplist https://pan.example.com',
  `- 固定策略：mode=${FIXED_BACKUP_MODE} 单文件超时=${FIXED_TIMEOUT_SEC}s（不再通过命令配置）`,
  '- #备份oplist https://pan.example.com --api',
  '- #备份oplist https://pan.example.com --webdav',
  '- #备份oplist https://pan.example.com --concurrency 3',
  '- #备份oplist https://pan.example.com --scan 30 --per-page 2000',
  '提示：目标端使用 openlistBaseUrl/openlistUsername/openlistPassword（与群文件同步共用）。',
  '提示：传输默认 auto（源端下载偏向 API，目标端上传偏向 WebDAV；失败会回退）。',
  '说明：会在目标目录下创建子目录（源 OpenList 域名，"." 替换为 "_"）。',
].join('\n')

export const backupOpenListToOpenList = karin.command(/^#?备份oplist(.*)$/i, async (e) => {
  if (!e.isPrivate) return false

  const argsText = e.msg.replace(/^#?备份oplist/i, '')
  const {
    sourceBaseUrl,
    srcDir,
    toDir,
    maxFiles,
    concurrency,
    scanConcurrency,
    perPage,
    transport,
    help,
  } = parseBackupOpenListArgs(argsText)

  if (help || !sourceBaseUrl) {
    try {
      const img = await buildActionCard({
        title: '备份oplist 帮助',
        status: 'ok',
        subtitle: '私聊 · 图片说明',
        sections: [
          {
            title: '说明',
            lines: openListToOpenListHelpText.split('\n').map((text) => ({
              text,
              mono: /(^|\s)#/.test(text),
            })),
          },
        ],
        footerRight: '#备份oplist',
      })
      await replyImages(e, img)
    } catch (error) {
      logger.error(error)
      await e.reply(openListToOpenListHelpText)
    }
    return true
  }

  try {
    const cfg = config()
    const targetBaseUrl = String(cfg.openlistBaseUrl ?? '').trim() || '-'

    const startedAt = Date.now()
    try {
      const startCard = await buildActionCard({
        title: '备份任务已启动',
        status: 'ok',
        subtitle: 'OpenList → OpenList（单次）',
        sections: [
          {
            title: '执行信息',
            rows: [
              { k: '源', v: String(sourceBaseUrl), mono: true },
              { k: '源目录', v: String(srcDir ?? '/'), mono: true },
              { k: '目标', v: targetBaseUrl, mono: true },
              { k: '目标目录', v: String(toDir ?? '(默认 openlistTargetDir)'), mono: true },
              { k: 'mode', v: String(FIXED_BACKUP_MODE), mono: true },
              { k: 'timeout', v: `${FIXED_TIMEOUT_SEC}s`, mono: true },
              { k: 'transport', v: String(transport ?? 'auto'), mono: true },
            ],
          },
          {
            title: '提示',
            lines: [
              { text: '执行过程中不发送文本进度；完成后发送结果图。' },
            ],
          },
        ],
        footerRight: '#备份oplist',
      })
      await replyImages(e, startCard)
    } catch (error) {
      logger.error(error)
    }

    const res = await backupOpenListToOpenListCore({
      sourceBaseUrl,
      srcDir,
      toDir,
      maxFiles,
      concurrency,
      timeoutSec: FIXED_TIMEOUT_SEC,
      scanConcurrency,
      perPage,
      mode: FIXED_BACKUP_MODE,
      transport,
    })

    const elapsed = Math.max(0, Math.floor((Date.now() - startedAt) / 1000))
    try {
      const doneCard = await buildActionCard({
        title: '备份完成',
        status: res.fail ? 'warn' : 'ok',
        statusText: res.fail ? '部分失败' : '成功',
        subtitle: 'OpenList → OpenList（单次）',
        sections: [
          {
            title: '结果',
            rows: [
              { k: 'ok', v: String(res.ok), mono: true },
              { k: 'skipped', v: String(res.skipped), mono: true },
              { k: 'fail', v: String(res.fail), mono: true },
              { k: '耗时', v: `${elapsed}s`, mono: true },
            ],
          },
          {
            title: '下一步',
            lines: [
              { text: '#我的备份', mono: true },
            ],
          },
        ],
        footerRight: '#备份oplist',
      })
      await replyImages(e, doneCard)
    } catch (error) {
      logger.error(error)
      await e.reply(`备份完成：成功 ${res.ok}，跳过 ${res.skipped}，失败 ${res.fail}`)
    }
    return true
  } catch (error: any) {
    logger.error(error)
    const msg = formatErrorMessage(error)
    try {
      const errCard = await buildActionCard({
        title: '备份失败',
        status: 'bad',
        statusText: '异常',
        subtitle: 'OpenList → OpenList（单次）',
        sections: [
          {
            title: '错误',
            lines: [{ text: msg }],
          },
        ],
        footerRight: '#备份oplist',
      })
      await replyImages(e, errCard)
    } catch (renderError) {
      logger.error(renderError)
      await e.reply(msg)
    }
    return true
  }
}, {
  priority: 9999,
  log: true,
  name: 'OpenList备份到对端OpenList',
  permission: 'all',
})

/**
 * 群文件上传事件：自动备份到 OpenList（由 WebUI/配置开关控制）。
 */
export const groupFileUploadedAutoBackup = karin.accept('notice.groupFileUploaded', (e, next) => {
  try {
    handleGroupFileUploadedAutoBackup(e)
  } finally {
    next()
  }
}, { log: false, name: '群文件上传自动备份' })
