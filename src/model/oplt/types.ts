import type { OpenListBackupTransport, SyncMode } from '@/model/groupFiles/types'

export type OpltItem = { left: string, right: string }

export type OpltNightly = {
  enabled?: boolean
  cron?: string
  mode?: SyncMode
  transport?: OpenListBackupTransport
  appendHostDir?: boolean
  lastRunAt?: number
  lastResult?: { ok: number, skipped: number, fail: number }
}

export type OpltUserData = { oplts: OpltItem[], nightly?: OpltNightly }

export type OpltDataV1 = {
  version: 1
  users: Record<string, OpltUserData>
}

