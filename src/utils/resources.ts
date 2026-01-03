import fs from 'node:fs'
import path from 'node:path'
import { dir } from '@/dir'
import { createPluginDir, getAllFilesSync } from 'node-karin'

let ensurePromise: Promise<void> | null = null

/**
 * 将 npm 包内置 resources 目录初始化到 Karin 的 `@karinjs/<plugin>/resources`。
 * - 只复制“缺失的文件”，不覆盖用户自定义内容
 * - 兼容插件更新时新增资源
 */
export const ensurePluginResources = async () => {
  if (ensurePromise) return ensurePromise

  ensurePromise = (async () => {
    const sourceDir = path.join(dir.pluginDir, 'resources')
    const targetDir = dir.defResourcesDir

    if (!fs.existsSync(sourceDir)) return

    await createPluginDir(dir.name, ['resources'])

    const files = getAllFilesSync(sourceDir, { returnType: 'rel' })
    for (const rel of files) {
      const normalizedRel = rel.replaceAll('\\', '/')
      const shouldOverwrite = normalizedRel.startsWith('template/')
      const sourcePath = path.join(sourceDir, rel)
      const targetPath = path.join(targetDir, rel)

      if (fs.existsSync(targetPath) && !shouldOverwrite) continue

      fs.mkdirSync(path.dirname(targetPath), { recursive: true })
      fs.copyFileSync(sourcePath, targetPath)
    }
  })().catch((error) => {
    ensurePromise = null
    throw error
  })

  return ensurePromise
}
