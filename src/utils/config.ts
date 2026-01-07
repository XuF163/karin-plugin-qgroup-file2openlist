import { dir } from '@/dir'
import {
  watch,
  logger,
  filesByExt,
  copyConfigSync,
  requireFileSync,
} from 'node-karin'

export interface Config {
  /** 一言API */
  yiyanApi: string
  /** OpenList 基础地址 例如 http://127.0.0.1:5244 */
  openlistBaseUrl: string
  /** OpenList 用户名（用于 WebDAV BasicAuth） */
  openlistUsername: string
  /** OpenList 密码（用于 WebDAV BasicAuth） */
  openlistPassword: string
  /** OpenList 目标目录（WebDAV 路径，例：/QQ群文件） */
  openlistTargetDir: string

  /** OpenList å¤‡ä»½ï¼šç›®æ ‡ OpenList åŸºç¡€åœ°å€ï¼ˆå¯¹ç«¯ï¼‰ */
  /** OpenList å¤‡ä»½ï¼šç›®æ ‡ OpenList ç”¨æˆ·åï¼ˆå¯¹ç«¯ï¼Œç”¨äºŽ WebDAV BasicAuthï¼‰ */
  /** OpenList å¤‡ä»½ï¼šç›®æ ‡ OpenList å¯†ç ï¼ˆå¯¹ç«¯ï¼Œç”¨äºŽ WebDAV BasicAuthï¼‰ */
  /** OpenList å¤‡ä»½ï¼šæºç›®å½•ï¼ˆå½“å‰ OpenList çš„ WebDAV è·¯å¾„ï¼‰ */
  /** OpenList å¤‡ä»½ï¼šç›®æ ‡æ ¹ç›®å½•ï¼ˆå¯¹ç«¯ OpenList çš„ WebDAV è·¯å¾„ï¼‰ */
  /** OpenList å¤‡ä»½ï¼šé»˜è®¤æ¨¡å¼ */
  /** OpenList å¤‡ä»½ï¼šé»˜è®¤å¹¶å‘æ•° */
  /** OpenList å¤‡ä»½ï¼šé»˜è®¤å•æ–‡ä»¶è¶…æ—¶ç§’æ•° */
  /** OpenList -> OpenList 备份传输方式：auto/webdav/api */

  /** 群文件同步：默认策略 */
  groupSyncDefaults?: {
    /** 同步模式 */
    mode?: 'full' | 'incremental'
    /** 是否平铺上传（不保留目录结构） */
    flat?: boolean
    /** 解析 URL 并发 */
    urlConcurrency?: number
    /** 下载+上传 并发（多线程） */
    transferConcurrency?: number
    /** 单文件超时秒数（下载+上传总超时） */
    fileTimeoutSec?: number
    /** 单文件失败重试次数 */
    retryTimes?: number
    /** 重试基础延迟毫秒（指数退避） */
    retryDelayMs?: number
    /** 同步进度消息间隔（每N个文件发送一次，0=关闭） */
    progressReportEvery?: number
    /** 下载限速（KB/s），0或空=不限制 */
    downloadLimitKbps?: number
    /** 上传限速（KB/s），0或空=不限制 */
    uploadLimitKbps?: number
  }

  /** 群文件同步：目标群配置 */
  groupSyncTargets?: Array<{
    /** 群号 */
    groupId: string
    /** 是否启用该群自动同步 */
    enabled?: boolean

    /** 从群文件的指定文件夹开始（可选） */
    sourceFolderId?: string

    /** OpenList 目标目录（为空则使用 openlistTargetDir/<groupId>） */
    targetDir?: string

    /** 同步模式（覆盖 groupSyncDefaults.mode） */
    mode?: 'full' | 'incremental'
    /** 是否平铺上传（覆盖 groupSyncDefaults.flat） */
    flat?: boolean
    /** 最多同步多少个文件（0/空=不限制） */
    maxFiles?: number

    /** 解析 URL 并发（覆盖 groupSyncDefaults.urlConcurrency） */
    urlConcurrency?: number
    /** 下载+上传 并发（覆盖 groupSyncDefaults.transferConcurrency） */
    transferConcurrency?: number

    /** 单文件超时秒数（覆盖 groupSyncDefaults.fileTimeoutSec） */
    fileTimeoutSec?: number
    /** 单文件失败重试次数（覆盖 groupSyncDefaults.retryTimes） */
    retryTimes?: number
    /** 重试基础延迟毫秒（覆盖 groupSyncDefaults.retryDelayMs） */
    retryDelayMs?: number
    /** 同步进度消息间隔（覆盖 groupSyncDefaults.progressReportEvery，0=关闭） */
    progressReportEvery?: number
    /** 下载限速（KB/s），0或空=不限制（覆盖 groupSyncDefaults.downloadLimitKbps） */
    downloadLimitKbps?: number
    /** 上传限速（KB/s），0或空=不限制（覆盖 groupSyncDefaults.uploadLimitKbps） */
    uploadLimitKbps?: number

    /** 监听群文件上传并自动备份到 OpenList */
    uploadBackup?: boolean

    /** 同步时段控制（仅用于定时任务/自动同步），例：00:00-06:00,23:00-23:59；空=不限制 */
    timeWindows?: string

    /** 定时同步计划 */
    schedule?: {
      /** 是否启用定时同步 */
      enabled?: boolean
      /** cron 表达式（秒 分 时 日 月 周） */
      cron?: string
    }
  }>
}

/**
 * @description 初始化配置文件
 */
copyConfigSync(dir.defConfigDir, dir.ConfigDir, ['.json'])

/**
 * @description 配置文件
 */
export const config = () => {
  const cfg = requireFileSync(`${dir.ConfigDir}/config.json`, { force: true })
  const def = requireFileSync(`${dir.defConfigDir}/config.json`, { force: true })
  return { ...def, ...cfg }
}

/**
 * @description 监听配置文件
 */
setTimeout(() => {
  const list = filesByExt(dir.ConfigDir, '.json', 'abs')
  list.forEach(file => watch(file, (old, now) => {
    logger.info([
      'QAQ: 检测到配置文件更新',
      `这是旧数据: ${old}`,
      `这是新数据: ${now}`,
    ].join('\n'))
  }))
}, 2000)
