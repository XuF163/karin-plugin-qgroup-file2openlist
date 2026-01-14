import path from 'node:path'
import { dir } from '@/dir'
import { readJsonSafe, writeJsonSafe } from '@/model/shared/fsJson'
import type { OpenListBackupTransport, SyncMode } from '@/model/groupFiles/types'
import type { OpltDataV1, OpltNightly, OpltUserData } from './types'

export const DEFAULT_OPLT_NIGHTLY_CRON = '0 0 2 * * *'
export const DEFAULT_OPLT_NIGHTLY_MODE: SyncMode = 'incremental'
export const DEFAULT_OPLT_NIGHTLY_TRANSPORT: OpenListBackupTransport = 'auto'
export const DEFAULT_OPLT_NIGHTLY_APPEND_HOST_DIR = false

export const getOpltDataPath = () => path.join(dir.DataDir, 'op-commands.json')

const asObject = (value: unknown): Record<string, any> | undefined => {
  if (!value || typeof value !== 'object') return undefined
  return value as any
}

const normalizeNightly = (value: unknown): OpltNightly | undefined => {
  const obj = asObject(value)
  if (!obj) return undefined
  const mode = obj.mode === 'full' || obj.mode === 'incremental' ? obj.mode : undefined
  const transport: OpenListBackupTransport | undefined = obj.transport === 'api' || obj.transport === 'webdav' || obj.transport === 'auto'
    ? obj.transport
    : undefined
  const lastResult = obj.lastResult && typeof obj.lastResult === 'object' ? obj.lastResult as any : undefined
  return {
    enabled: typeof obj.enabled === 'boolean' ? obj.enabled : undefined,
    cron: typeof obj.cron === 'string' ? obj.cron : undefined,
    mode,
    transport,
    appendHostDir: typeof obj.appendHostDir === 'boolean' ? obj.appendHostDir : undefined,
    lastRunAt: typeof obj.lastRunAt === 'number' && Number.isFinite(obj.lastRunAt) ? obj.lastRunAt : undefined,
    lastResult: lastResult && typeof lastResult.ok === 'number' && typeof lastResult.skipped === 'number' && typeof lastResult.fail === 'number'
      ? { ok: lastResult.ok, skipped: lastResult.skipped, fail: lastResult.fail }
      : undefined,
  }
}

const normalizeUserData = (value: unknown): OpltUserData => {
  const obj = asObject(value)
  const oplts = Array.isArray(obj?.oplts)
    ? obj!.oplts
      .filter((it) => it && typeof it === 'object' && typeof (it as any).left === 'string' && typeof (it as any).right === 'string')
      .map((it) => ({ left: String((it as any).left), right: String((it as any).right) }))
    : []
  return {
    oplts,
    nightly: normalizeNightly(obj?.nightly),
  }
}

export const readOpltData = (): OpltDataV1 => {
  const raw = readJsonSafe(getOpltDataPath())
  const usersRaw = asObject(raw)?.users
  const users: Record<string, OpltUserData> = {}
  if (usersRaw && typeof usersRaw === 'object') {
    for (const [k, v] of Object.entries(usersRaw)) {
      users[String(k)] = normalizeUserData(v)
    }
  }
  return { version: 1, users }
}

export const writeOpltData = (data: OpltDataV1) => writeJsonSafe(getOpltDataPath(), data)

export const withOpltUser = (data: OpltDataV1, userKey: string) => {
  const key = String(userKey || 'global')
  const user = data.users[key]
  if (user && Array.isArray(user.oplts)) return user
  const next = normalizeUserData(user)
  data.users[key] = next
  return next
}
