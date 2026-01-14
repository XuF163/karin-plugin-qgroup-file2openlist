import path from 'node:path'
import { dir } from '@/dir'
import { readJsonSafe, writeJsonSafe } from './fsJson'

export type PluginConfigData = Record<string, any>

export const getRuntimeConfigPath = () => path.join(dir.ConfigDir, 'config.json')
export const getDefaultConfigPath = () => path.join(dir.defConfigDir, 'config.json')

export const readDefaultConfig = <T extends PluginConfigData = PluginConfigData>() => {
  return readJsonSafe(getDefaultConfigPath()) as T
}

export const readRuntimeConfig = <T extends PluginConfigData = PluginConfigData>() => {
  return readJsonSafe(getRuntimeConfigPath()) as T
}

/**
 * 读取“默认配置 + 运行时配置”的浅合并结果。
 * - 对象：运行时覆盖默认
 * - 数组：整体替换
 */
export const readMergedConfig = <T extends PluginConfigData = PluginConfigData>() => {
  const def = readDefaultConfig<T>()
  const current = readRuntimeConfig<T>()
  return { ...def, ...current } as T
}

/**
 * 更新运行时配置文件（`@karinjs/<plugin>/config/config.json`）。
 * - updater 可选择“直接修改 draft”或“返回新对象”
 * - 默认会先读默认配置与当前配置并合并，确保新增字段不丢失
 */
export const updateRuntimeConfig = <T extends PluginConfigData = PluginConfigData>(
  updater: (draft: T) => T | void,
) => {
  const configPath = getRuntimeConfigPath()
  const def = readDefaultConfig<T>()
  const current = readRuntimeConfig<T>()
  const merged = { ...def, ...current } as T

  const draft = { ...merged } as T
  const result = updater(draft)
  const next = (result ?? draft) as T

  writeJsonSafe(configPath, next)
  return next
}

