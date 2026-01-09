export type SyncMode = 'full' | 'incremental'
export type OpenListBackupTransport = 'auto' | 'webdav' | 'api'

export interface ExportedGroupFile {
  path: string
  fileId: string
  name: string
  size?: number
  uploadTime?: number
  uploaderId?: string
  uploaderName?: string
  md5?: string
  sha1?: string
  sha3?: string
  url?: string
  busid?: number
}

export interface ExportError {
  fileId?: string
  path?: string
  message: string
}

export interface GroupFileSyncStateV1 {
  version: 1
  groupId: string
  updatedAt: number
  lastSyncAt?: number
  files: Record<string, {
    fileId?: string
    size?: number
    uploadTime?: number
    md5?: string
    sha1?: string
    syncedAt: number
  }>
}

