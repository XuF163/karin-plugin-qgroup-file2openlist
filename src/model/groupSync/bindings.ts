import path from 'node:path'
import { normalizePosixPath } from '@/model/shared/path'
import { readMergedConfig, updateRuntimeConfig } from '@/model/shared/pluginConfig'

export type SyncMode = 'full' | 'incremental'

export type GroupSyncTarget = {
  groupId: string
  enabled?: boolean
  sourceFolderId?: string
  targetDir?: string
  mode?: SyncMode
  flat?: boolean
  maxFiles?: number
  urlConcurrency?: number
  transferConcurrency?: number
  fileTimeoutSec?: number
  retryTimes?: number
  retryDelayMs?: number
  progressReportEvery?: number
  downloadLimitKbps?: number
  uploadLimitKbps?: number
  uploadBackup?: boolean
  timeWindows?: string
  schedule?: { enabled?: boolean, cron?: string }
}

const asTargets = (cfg: any): GroupSyncTarget[] => {
  const list = cfg?.groupSyncTargets
  return Array.isArray(list) ? list as GroupSyncTarget[] : []
}

const normalizeMode = (value: unknown, fallback: SyncMode): SyncMode => {
  const v = String(value ?? '').trim().toLowerCase()
  if (v === 'full' || v === '全量') return 'full'
  if (v === 'incremental' || v === '增量' || v === 'inc') return 'incremental'
  return fallback
}

const ensureTargetWithDefaults = (cfg: any, groupId: string): GroupSyncTarget => {
  const defaults = cfg?.groupSyncDefaults ?? {}
  const baseTargetDir = normalizePosixPath(String(cfg?.openlistTargetDir ?? '/'))
  return {
    groupId: String(groupId),
    enabled: true,
    mode: normalizeMode(defaults?.mode, 'incremental'),
    flat: Boolean(defaults?.flat ?? false),
    urlConcurrency: typeof defaults?.urlConcurrency === 'number' ? defaults.urlConcurrency : 3,
    transferConcurrency: typeof defaults?.transferConcurrency === 'number' ? defaults.transferConcurrency : 3,
    fileTimeoutSec: typeof defaults?.fileTimeoutSec === 'number' ? defaults.fileTimeoutSec : 600,
    retryTimes: typeof defaults?.retryTimes === 'number' ? defaults.retryTimes : 2,
    retryDelayMs: typeof defaults?.retryDelayMs === 'number' ? defaults.retryDelayMs : 1500,
    progressReportEvery: typeof defaults?.progressReportEvery === 'number' ? defaults.progressReportEvery : 10,
    downloadLimitKbps: typeof defaults?.downloadLimitKbps === 'number' ? defaults.downloadLimitKbps : 0,
    uploadLimitKbps: typeof defaults?.uploadLimitKbps === 'number' ? defaults.uploadLimitKbps : 0,
    targetDir: normalizePosixPath(path.posix.join(baseTargetDir, String(groupId))),
    schedule: { enabled: false, cron: '' },
  }
}

export const getGroupSyncTarget = (groupId: string) => {
  const cfg: any = readMergedConfig()
  const targets = asTargets(cfg)
  return targets.find(t => String(t.groupId) === String(groupId))
}

export const bindBackupGroup = (params: {
  groupId: string
  targetDir?: string
  mode?: SyncMode
  flat?: boolean
}) => {
  const groupId = String(params.groupId ?? '').trim()
  if (!groupId) throw new Error('缺少群号')

  let before: GroupSyncTarget | undefined
  let after: GroupSyncTarget | undefined

  updateRuntimeConfig<any>((cfg) => {
    const targets = asTargets(cfg)
    const idx = targets.findIndex(t => String(t.groupId) === groupId)
    before = idx >= 0 ? { ...targets[idx] } : undefined

    const base = before ? { ...before } : ensureTargetWithDefaults(cfg, groupId)
    const patch: Partial<GroupSyncTarget> = {
      enabled: true,
      uploadBackup: true,
    }

    if (typeof params.flat === 'boolean') patch.flat = params.flat
    if (params.mode) patch.mode = params.mode
    if (typeof params.targetDir === 'string') {
      const v = normalizePosixPath(params.targetDir.trim() || '')
      if (v && v !== '/') patch.targetDir = v
    }

    after = { ...base, ...patch, groupId }

    const next = [...targets]
    if (idx >= 0) next[idx] = after
    else next.push(after)

    cfg.groupSyncTargets = next
    return cfg
  })

  return { before, after: after ?? getGroupSyncTarget(groupId) }
}

export const unbindBackupGroup = (groupIdInput: string) => {
  const groupId = String(groupIdInput ?? '').trim()
  if (!groupId) throw new Error('缺少群号')

  let removed: GroupSyncTarget | undefined
  updateRuntimeConfig<any>((cfg) => {
    const targets = asTargets(cfg)
    const next = targets.filter((t) => {
      const match = String(t.groupId) === groupId
      if (match) removed = { ...t }
      return !match
    })
    cfg.groupSyncTargets = next
    return cfg
  })

  return { removed }
}

export const setGroupUploadBackup = (params: { groupId: string, enabled: boolean, ensureExists?: boolean }) => {
  const groupId = String(params.groupId ?? '').trim()
  if (!groupId) throw new Error('缺少群号')

  let before: GroupSyncTarget | undefined
  let after: GroupSyncTarget | undefined

  updateRuntimeConfig<any>((cfg) => {
    const targets = asTargets(cfg)
    const idx = targets.findIndex(t => String(t.groupId) === groupId)
    before = idx >= 0 ? { ...targets[idx] } : undefined

    if (idx < 0 && !params.ensureExists) {
      after = undefined
      return cfg
    }

    const base = before ? { ...before } : ensureTargetWithDefaults(cfg, groupId)
    after = {
      ...base,
      enabled: before?.enabled ?? true,
      uploadBackup: params.enabled,
      groupId,
    }

    const next = [...targets]
    if (idx >= 0) next[idx] = after
    else next.push(after)
    cfg.groupSyncTargets = next
    return cfg
  })

  return { before, after: after ?? getGroupSyncTarget(groupId) }
}

