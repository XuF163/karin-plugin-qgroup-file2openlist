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

  /** 资源限制（用于生产环境控制内存/带宽） */
  resourceLimits?: {
    /**
     * 全局传输并发上限（所有“下载+上传/复制”共享）
     * - <=0：不限制
     * - 未配置：默认 1（完全串行，最省内存）
     */
    transferConcurrency?: number
  }

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

  /** OpenList -> OpenList 转发规则（多源站，单目的端由 openlistBaseUrl 决定） */
  openlistForwardRules?: Array<{
    /** 规则ID */
    id: string
    /** 别名 */
    name?: string

    /** 源 OpenList baseUrl */
    sourceBaseUrl: string
    /** 源端用户名（可选，未填则按 guest 访问） */
    sourceUsername?: string
    /** 源端密码（可选） */
    sourcePassword?: string

    /** 源目录（默认 /） */
    srcDir?: string
    /** 目标根目录（默认 openlistTargetDir；最终会按源域名创建子目录） */
    toDir?: string

    /** 复制模式（默认 full） */
    mode?: 'full' | 'incremental'
    /** 传输方式（默认 auto） */
    transport?: 'auto' | 'webdav' | 'api'

    /** 复制并发（默认 3） */
    concurrency?: number
    /** 扫描并发（默认 20） */
    scanConcurrency?: number
    /** API per_page（默认 1000） */
    perPage?: number
    /** 单文件超时秒数（默认 600） */
    timeoutSec?: number

    lastRunAt?: number
    lastResult?: { ok: number, skipped: number, fail: number }
    createdAt?: number
    updatedAt?: number
  }>

  /**
   * 定时任务统一调度配置（群文件定时同步 / oplts 夜间备份）
   * - enabled=false：关闭所有定时任务触发（不影响手动命令/事件）
   * - tickCron：调度器 cron（默认每天 02:00）
   */
  scheduler?: {
    enabled?: boolean
    /** 调度器 cron（建议 6 段：秒 分 时 日 月 周） */
    tickCron?: string
    /** 群文件夜间备份全局开关（对 uploadBackup=on 的群执行一次同步） */
    groupSync?: { enabled?: boolean }
    /** oplts 夜间自动备份全局开关（#oplt夜间 仅查看状态） */
    opltNightly?: { enabled?: boolean }
  }
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
