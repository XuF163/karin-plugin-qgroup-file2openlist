import path from 'node:path'
import { dir } from '@/dir'
import { readJsonSafe, writeJsonSafe } from '@/model/shared/fsJson'
import type { GroupFileSyncStateV1 } from './types'

const getGroupSyncStatePath = (groupId: string) => path.join(
  dir.DataDir,
  'group-file-sync-state',
  `${String(groupId)}.json`,
)

export const readGroupSyncState = (groupId: string): GroupFileSyncStateV1 => {
  const raw = readJsonSafe(getGroupSyncStatePath(groupId))
  if (raw && typeof raw === 'object' && raw.version === 1 && raw.files && typeof raw.files === 'object') {
    return {
      version: 1,
      groupId: String(raw.groupId ?? groupId),
      updatedAt: typeof raw.updatedAt === 'number' ? raw.updatedAt : Date.now(),
      lastSyncAt: typeof raw.lastSyncAt === 'number' ? raw.lastSyncAt : undefined,
      files: raw.files as GroupFileSyncStateV1['files'],
    }
  }

  return {
    version: 1,
    groupId: String(groupId),
    updatedAt: Date.now(),
    files: {},
  }
}

export const writeGroupSyncState = (groupId: string, state: GroupFileSyncStateV1) => {
  const next: GroupFileSyncStateV1 = {
    version: 1,
    groupId: String(groupId),
    updatedAt: Date.now(),
    lastSyncAt: typeof state.lastSyncAt === 'number' ? state.lastSyncAt : undefined,
    files: state.files ?? {},
  }
  writeJsonSafe(getGroupSyncStatePath(groupId), next)
}

const activeGroupSync = new Set<string>()

/**
 * 同一群只允许一个同步任务（避免重复跑导致大量重复上传/资源占用）。
 */
export const withGroupSyncLock = async <T>(groupId: string, fn: () => Promise<T>) => {
  const key = String(groupId)
  if (activeGroupSync.has(key)) throw new Error('该群同步任务正在进行中，请稍后再试。')
  activeGroupSync.add(key)
  try {
    return await fn()
  } finally {
    activeGroupSync.delete(key)
  }
}

