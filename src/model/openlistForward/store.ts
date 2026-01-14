import crypto from 'node:crypto'
import { normalizePosixPath } from '@/model/shared/path'
import { readMergedConfig, updateRuntimeConfig } from '@/model/shared/pluginConfig'
import type { OpenListForwardRule, OpenListBackupTransport, SyncMode } from './types'

const normalizeBaseUrl = (value: string) => String(value ?? '').trim().replace(/\/+$/, '')

const genId = () => {
  const n = crypto.randomBytes(4).readUInt32BE(0)
  return n.toString(36).padStart(7, '0')
}

const asRules = (cfg: any): OpenListForwardRule[] => {
  const list = cfg?.openlistForwardRules
  return Array.isArray(list) ? list as OpenListForwardRule[] : []
}

const normalizeMode = (value: unknown, fallback: SyncMode): SyncMode => {
  const v = String(value ?? '').trim().toLowerCase()
  if (v === 'full' || v === '全量') return 'full'
  if (v === 'incremental' || v === '增量' || v === 'inc') return 'incremental'
  return fallback
}

const normalizeTransport = (value: unknown, fallback: OpenListBackupTransport): OpenListBackupTransport => {
  const v = String(value ?? '').trim().toLowerCase()
  if (v === 'api') return 'api'
  if (v === 'webdav' || v === 'dav') return 'webdav'
  if (v === 'auto') return 'auto'
  return fallback
}

export const listOpenListForwardRules = () => {
  const cfg: any = readMergedConfig()
  const rules = asRules(cfg)
  return rules.slice().sort((a, b) => (b.updatedAt ?? b.createdAt ?? 0) - (a.updatedAt ?? a.createdAt ?? 0))
}

export const getOpenListForwardRule = (id: string) => {
  const key = String(id ?? '').trim()
  if (!key) return undefined
  return listOpenListForwardRules().find(r => String(r.id) === key)
}

export const addOpenListForwardRule = (params: {
  sourceBaseUrl: string
  sourceUsername?: string
  sourcePassword?: string
  srcDir?: string
  toDir?: string
  name?: string
  mode?: SyncMode
  transport?: OpenListBackupTransport
  concurrency?: number
  scanConcurrency?: number
  perPage?: number
  timeoutSec?: number
}) => {
  const sourceBaseUrl = normalizeBaseUrl(params.sourceBaseUrl)
  if (!sourceBaseUrl) throw new Error('缺少源 OpenList 地址')

  const now = Date.now()
  const id = genId()

  const rule: OpenListForwardRule = {
    id,
    name: params.name ? String(params.name).trim() : undefined,
    sourceBaseUrl,
    sourceUsername: params.sourceUsername ? String(params.sourceUsername).trim() : undefined,
    sourcePassword: params.sourcePassword ? String(params.sourcePassword) : undefined,
    srcDir: params.srcDir ? normalizePosixPath(String(params.srcDir)) : '/',
    toDir: params.toDir ? normalizePosixPath(String(params.toDir)) : undefined,
    mode: params.mode ? normalizeMode(params.mode, 'full') : 'full',
    transport: params.transport ? normalizeTransport(params.transport, 'auto') : 'auto',
    concurrency: typeof params.concurrency === 'number' ? Math.max(1, Math.floor(params.concurrency)) : undefined,
    scanConcurrency: typeof params.scanConcurrency === 'number' ? Math.max(1, Math.floor(params.scanConcurrency)) : undefined,
    perPage: typeof params.perPage === 'number' ? Math.max(1, Math.floor(params.perPage)) : undefined,
    timeoutSec: typeof params.timeoutSec === 'number' ? Math.max(1, Math.floor(params.timeoutSec)) : undefined,
    createdAt: now,
    updatedAt: now,
  }

  updateRuntimeConfig<any>((cfg) => {
    const rules = asRules(cfg)
    cfg.openlistForwardRules = [...rules, rule]
    return cfg
  })

  return rule
}

export const deleteOpenListForwardRule = (id: string) => {
  const key = String(id ?? '').trim()
  if (!key) throw new Error('缺少 ruleId')

  let removed: OpenListForwardRule | undefined
  updateRuntimeConfig<any>((cfg) => {
    const rules = asRules(cfg)
    cfg.openlistForwardRules = rules.filter((r) => {
      const match = String(r.id) === key
      if (match) removed = { ...r }
      return !match
    })
    return cfg
  })

  return { removed }
}

export const updateOpenListForwardRule = (id: string, patch: Partial<OpenListForwardRule>) => {
  const key = String(id ?? '').trim()
  if (!key) throw new Error('缺少 ruleId')

  let before: OpenListForwardRule | undefined
  let after: OpenListForwardRule | undefined

  updateRuntimeConfig<any>((cfg) => {
    const rules = asRules(cfg)
    const idx = rules.findIndex(r => String(r.id) === key)
    if (idx < 0) return cfg

    before = { ...rules[idx] }
    const merged: OpenListForwardRule = {
      ...rules[idx],
      ...patch,
      id: key,
      updatedAt: Date.now(),
    }

    // normalize fields if provided
    merged.sourceBaseUrl = normalizeBaseUrl(merged.sourceBaseUrl)
    if (merged.srcDir) merged.srcDir = normalizePosixPath(merged.srcDir)
    if (merged.toDir) merged.toDir = normalizePosixPath(merged.toDir)
    if (merged.mode) merged.mode = normalizeMode(merged.mode, 'full')
    if (merged.transport) merged.transport = normalizeTransport(merged.transport, 'auto')

    after = merged
    const next = [...rules]
    next[idx] = merged
    cfg.openlistForwardRules = next
    return cfg
  })

  return { before, after: after ?? getOpenListForwardRule(key) }
}

