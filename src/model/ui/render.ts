import path from 'node:path'
import { dir } from '@/dir'
import { ensurePluginResources } from '@/utils/resources'
import { render } from 'node-karin'

export type UiViewport = { width: number, height: number, deviceScaleFactor: number }

const DEFAULT_VIEWPORT: UiViewport = { width: 540, height: 960, deviceScaleFactor: 2 }

export const resolveUiTemplatePath = (templateRel: string) => {
  const rel = String(templateRel ?? '').replaceAll('\\', '/').replace(/^\/+/, '')
  return path.join(dir.defResourcesDir, 'template', rel)
}

export const renderUiPngBase64 = async (params: {
  template: string
  data?: Record<string, any>
  viewport?: Partial<UiViewport>
  multiPage?: number | boolean
  fullPage?: boolean
  name?: string
}) => {
  await ensurePluginResources()

  const viewport: UiViewport = {
    ...DEFAULT_VIEWPORT,
    ...(params.viewport ?? {}),
  }

  const file = resolveUiTemplatePath(params.template)
  const name = String(params.name ?? 'qgroup-ui')

  return await render.render({
    name,
    file,
    type: 'png',
    encoding: 'base64',
    data: params.data ?? {},
    setViewport: viewport,
    fullPage: params.fullPage,
    multiPage: params.multiPage,
    pageGotoParams: {
      waitUntil: 'networkidle2',
    },
  }) as any
}

