import { segment } from 'node-karin'
import { renderUiPngBase64 } from './render'

export type Img = string | string[]

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

export const buildActionCard = async (params: {
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
  footerLeft?: string
  footerRight?: string
  viewport?: { width: number, height: number, deviceScaleFactor: number }
  multiPage?: number | boolean
  name?: string
}): Promise<Img> => {
  return await renderUiPngBase64({
    template: 'ui/action-result.html',
    name: params.name ?? 'qgroup-action',
    data: {
      ...params,
      generatedAt: formatDateTime(new Date()),
    },
    viewport: params.viewport ?? { width: 540, height: 960, deviceScaleFactor: 2 },
    multiPage: typeof params.multiPage === 'undefined' ? 960 : params.multiPage,
  }) as Img
}

export const replyImages = async (e: any, images: Img) => {
  const list = Array.isArray(images) ? images : [images]
  for (const img of list) {
    await e.reply(segment.image(`base64://${img}`))
  }
}

