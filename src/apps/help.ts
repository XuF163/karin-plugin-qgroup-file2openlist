import path from 'node:path'
import { dir } from '@/dir'
import { ensurePluginResources } from '@/utils/resources'
import { karin, logger, render, segment } from 'node-karin'

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

/**
 * 帮助指令（图片）
 * 触发指令：#群文件帮助
 */
export const helpImage = karin.command(/^#?(群文件帮助|同步群文件帮助|openlist帮助|op帮助)$/i, async (e) => {
  if (!e.isPrivate) return false

  try {
    await ensurePluginResources()
    const html = path.join(dir.defResourcesDir, 'template', 'help.html')

    const img = await render.render({
      name: 'qgroup-help',
      encoding: 'base64',
      file: html,
      type: 'png',
      data: {
        name: dir.name,
        version: dir.version,
        generatedAt: formatDateTime(new Date()),
      },
      setViewport: {
        /** 竖版输出 1080x1920（通过 2x DPR 达到更清晰的字体/边缘） */
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
    await e.reply(`帮助图渲染失败：${error?.message ?? String(error)}`)
    return true
  }
}, {
  priority: 9999,
  log: true,
  name: '群文件帮助',
  permission: 'all',
})
