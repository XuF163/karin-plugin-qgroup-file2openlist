export type SyncMode = 'full' | 'incremental'
export type OpenListBackupTransport = 'auto' | 'webdav' | 'api'

export type OpenListForwardRule = {
  id: string
  name?: string

  sourceBaseUrl: string
  sourceUsername?: string
  sourcePassword?: string

  srcDir?: string
  toDir?: string

  mode?: SyncMode
  transport?: OpenListBackupTransport

  concurrency?: number
  scanConcurrency?: number
  perPage?: number
  timeoutSec?: number

  lastRunAt?: number
  lastResult?: { ok: number, skipped: number, fail: number }
  createdAt?: number
  updatedAt?: number
}

