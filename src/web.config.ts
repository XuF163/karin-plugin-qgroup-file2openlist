import fs from 'node:fs'
import path from 'node:path'
import { components, defineConfig, logger } from 'node-karin'
import { dir } from './dir'

type WebConfigData = Record<string, any>
type SyncMode = 'full' | 'incremental'
const MAX_FILE_TIMEOUT_SEC = 3000
type AccordionProCell = { key?: string, value?: any }

const readJsonSafe = (filePath: string) => {
  try {
    if (!fs.existsSync(filePath)) return {}
    const raw = fs.readFileSync(filePath, 'utf8')
    return raw ? JSON.parse(raw) : {}
  } catch (error) {
    logger.error(error)
    return {}
  }
}

const writeJsonSafe = (filePath: string, data: unknown) => {
  fs.mkdirSync(path.dirname(filePath), { recursive: true })
  fs.writeFileSync(filePath, JSON.stringify(data, null, 2), 'utf8')
}

const getConfigFilePath = () => path.join(dir.ConfigDir, 'config.json')
const getDefaultConfigPath = () => path.join(dir.defConfigDir, 'config.json')

const getMergedConfig = () => {
  const cfg = readJsonSafe(getConfigFilePath())
  const def = readJsonSafe(getDefaultConfigPath())
  return { ...def, ...cfg }
}

const asString = (value: unknown) => (typeof value === 'string' ? value : value == null ? '' : String(value))
const asBoolean = (value: unknown) => {
  if (typeof value === 'boolean') return value
  if (typeof value === 'number') return value !== 0
  if (typeof value === 'string') {
    const v = value.trim().toLowerCase()
    if (!v) return false
    if (['1', 'true', 'yes', 'y', 'on', '开启', '开', '是'].includes(v)) return true
    if (['0', 'false', 'no', 'n', 'off', '关闭', '关', '否'].includes(v)) return false
  }
  return false
}

const asInt = (value: unknown, fallback: number) => {
  if (typeof value === 'number' && Number.isFinite(value)) return Math.floor(value)
  if (typeof value === 'string') {
    const trimmed = value.trim()
    if (!trimmed) return fallback
    const n = Number(trimmed)
    if (Number.isFinite(n)) return Math.floor(n)
  }
  return fallback
}

const asOptionalInt = (value: unknown) => {
  if (typeof value === 'number' && Number.isFinite(value)) return Math.floor(value)
  if (typeof value === 'string') {
    const trimmed = value.trim()
    if (!trimmed) return undefined
    const n = Number(trimmed)
    if (Number.isFinite(n)) return Math.floor(n)
  }
  return undefined
}

const unwrapAccordionValue = (value: unknown) => {
  if (!value || typeof value !== 'object') return value
  const v = value as AccordionProCell
  return Object.prototype.hasOwnProperty.call(v, 'value') ? v.value : value
}

const apInput = (value: unknown) => ({ key: 'input', value: asString(value) })
const apSwitch = (value: unknown) => ({ key: 'switch', value: asBoolean(value) })
const apSelect = (value: unknown) => ({ key: 'select', value: asString(value) })

const pickMode = (value: unknown, fallback: SyncMode): SyncMode => {
  const v = asString(value).trim().toLowerCase()
  if (v === 'full' || v === '全量') return 'full'
  if (v === 'incremental' || v === '增量') return 'incremental'
  return fallback
}

const clampInt = (value: number, min: number, max: number) => Math.min(max, Math.max(min, value))

const arrayFrom = (value: unknown) => {
  if (Array.isArray(value)) return value
  if (value == null) return []
  return [value]
}

const getFormRowValue = <T>(form: WebConfigData, key: string, index: number): T | undefined => {
  const raw = (form as any)?.[key]
  if (Array.isArray(raw)) return raw[index] as T
  return (index === 0 ? raw : undefined) as T | undefined
}

const extractAccordionRows = (value: unknown): Array<Record<string, any>> => {
  if (Array.isArray(value)) return value as Array<Record<string, any>>
  if (value && typeof value === 'object') {
    const obj: any = value as any
    const candidates = [obj.rows, obj.data, obj.value, obj.items]
    for (const it of candidates) {
      if (Array.isArray(it)) return it as Array<Record<string, any>>
    }
  }
  return []
}

export default defineConfig<WebConfigData>({
  info: {
    id: dir.name,
    name: '群文件工具',
    version: dir.version,
    description: dir.pkg?.description ?? '',
  },
  components: () => {
    const cfg = getMergedConfig()

    const defaults = cfg.groupSyncDefaults ?? {}
    const targets = Array.isArray(cfg.groupSyncTargets) ? cfg.groupSyncTargets : []
    const targetsData = targets.map((t: any) => {
      const groupId = asString(t?.groupId ?? '').trim()
      const title = groupId ? `群 ${groupId}` : '目标群'

      return {
        title,
        subtitle: '\u200b',
        gst_groupId: groupId,
        gst_enabled: Boolean(t?.enabled ?? true),
        gst_sourceFolderId: asString(t?.sourceFolderId ?? ''),
        gst_targetDir: asString(t?.targetDir ?? ''),
        gst_mode: pickMode(t?.mode, pickMode(defaults?.mode, 'incremental')),
        gst_flat: (typeof t?.flat === 'boolean') ? t.flat : Boolean(defaults?.flat ?? false),
        gst_maxFiles: asString(t?.maxFiles ?? ''),
        gst_urlConcurrency: asString(t?.urlConcurrency ?? ''),
        gst_transferConcurrency: asString(t?.transferConcurrency ?? ''),
        gst_fileTimeoutSec: asString(t?.fileTimeoutSec ?? ''),
        gst_retryTimes: asString(t?.retryTimes ?? ''),
        gst_retryDelayMs: asString(t?.retryDelayMs ?? ''),
        gst_progressReportEvery: asString(t?.progressReportEvery ?? ''),
        gst_downloadLimitKbps: asString(t?.downloadLimitKbps ?? ''),
        gst_uploadLimitKbps: asString(t?.uploadLimitKbps ?? ''),
        gst_timeWindows: asString(t?.timeWindows ?? ''),
        gst_scheduleEnabled: Boolean(t?.schedule?.enabled ?? false),
        gst_scheduleCron: asString(t?.schedule?.cron ?? ''),
      }
    })

    const compactInput = {
      size: 'sm',
      variant: 'bordered',
      radius: 'sm',
      color: 'default',
      labelPlacement: 'outside',
      fullWidth: true,
      isRequired: false,
      disableAnimation: true,
      className: 'w-full',
      componentClassName: 'w-full',
    } as const

    const compactNumber = {
      ...compactInput,
      rules: [] as any[],
    } as const

    const compactSelect = {
      size: 'sm',
      variant: 'bordered',
      radius: 'sm',
      color: 'default',
      labelPlacement: 'outside',
      isRequired: false,
      disableAnimation: true,
      className: '!max-w-none w-full !p-0',
      componentClassName: 'w-full',
    } as const

    const compactSwitch = {
      size: 'sm',
      color: 'default',
      disableAnimation: true,
      className: 'w-full',
    } as const

    const grid2 = 'grid grid-cols-1 sm:grid-cols-2 gap-2 p-1'

    return [
      components.accordion.create('groupFilesTool', {
        title: '群文件工具配置',
        variant: 'bordered',
        selectionMode: 'multiple',
        isCompact: true,
        showDivider: true,
        fullWidth: true,
        children: [
          components.accordionItem.create('openlist', {
            title: 'OpenList',
            subtitle: '\u200b',
            isCompact: true,
            className: grid2,
            children: [
              components.input.string('openlistBaseUrl', {
                ...compactInput,
                className: `sm:col-span-2 ${compactInput.className}`,
                label: 'OpenList 地址',
                description: '例如：http://127.0.0.1:5244',
                defaultValue: cfg.openlistBaseUrl ?? 'http://127.0.0.1:5244',
                placeholder: 'http://127.0.0.1:5244',
                isClearable: true,
              }),
              components.input.string('openlistUsername', {
                ...compactInput,
                label: 'OpenList 用户名',
                description: '用于 WebDAV BasicAuth 登录',
                defaultValue: cfg.openlistUsername ?? '',
                isClearable: true,
              }),
              components.input.create('openlistPassword', {
                ...compactInput,
                label: 'OpenList 密码',
                description: '用于 WebDAV BasicAuth 登录（会保存到配置文件）',
                defaultValue: cfg.openlistPassword ?? '',
                type: 'password',
                isClearable: true,
              }),
              components.input.string('openlistTargetDir', {
                ...compactInput,
                className: `sm:col-span-2 ${compactInput.className}`,
                label: 'OpenList 默认目标目录',
                description: 'WebDAV 目标目录，例如：/挂载目录/QQ群文件',
                defaultValue: cfg.openlistTargetDir ?? '/',
                placeholder: '/挂载目录/QQ群文件',
                isClearable: true,
              }),
            ],
          }),
          components.accordionItem.create('defaults', {
            title: '群同步默认策略',
            subtitle: '\u200b',
            isCompact: true,
            className: grid2,
            children: [
              components.select.create('groupSyncDefaults_mode', {
                ...compactSelect,
                label: '默认同步模式',
                description: '全量=每次都同步；增量=跳过已同步文件（依赖本地状态）',
                defaultValue: pickMode(defaults?.mode, 'incremental'),
                items: [
                  components.select.createItem('incremental', { value: 'incremental', label: '增量 (incremental)' }),
                  components.select.createItem('full', { value: 'full', label: '全量 (full)' }),
                ],
              }),
              components.switch.options('groupSyncDefaults_flat', {
                ...compactSwitch,
                label: '默认平铺上传',
                startText: '保留目录结构',
                endText: '平铺上传',
                defaultSelected: Boolean(defaults?.flat ?? false),
              }),
              components.input.number('groupSyncDefaults_urlConcurrency', {
                ...compactNumber,
                label: '默认解析URL并发',
                defaultValue: asString(defaults?.urlConcurrency ?? 3),
                isClearable: true,
                rules: [{ min: 1, max: 5, error: '并发范围 1-5' }],
              }),
              components.input.number('groupSyncDefaults_transferConcurrency', {
                ...compactNumber,
                label: '默认下载/上传并发（多线程）',
                defaultValue: asString(defaults?.transferConcurrency ?? 3),
                isClearable: true,
                rules: [{ min: 1, max: 5, error: '并发范围 1-5' }],
              }),
              components.input.number('groupSyncDefaults_fileTimeoutSec', {
                ...compactNumber,
                className: `sm:col-span-2 ${compactInput.className}`,
                label: '默认单文件超时（秒）',
                description: `仅影响单个文件的下载+上传总超时（最大 ${MAX_FILE_TIMEOUT_SEC}s）`,
                defaultValue: asString(defaults?.fileTimeoutSec ?? 600),
                isClearable: true,
                rules: [{ min: 10, max: MAX_FILE_TIMEOUT_SEC, error: `范围 10-${MAX_FILE_TIMEOUT_SEC}` }],
              }),
              components.input.number('groupSyncDefaults_retryTimes', {
                ...compactNumber,
                label: '默认失败重试次数',
                defaultValue: asString(defaults?.retryTimes ?? 2),
                isClearable: true,
                rules: [{ min: 0, max: 1000, error: '范围 0-1000' }],
              }),
              components.input.number('groupSyncDefaults_retryDelayMs', {
                ...compactNumber,
                className: `sm:col-span-2 ${compactInput.className}`,
                label: '默认重试延迟（ms）',
                description: '指数退避的基础延迟',
                defaultValue: asString(defaults?.retryDelayMs ?? 1500),
                isClearable: true,
                rules: [{ min: 0, max: 3600000, error: '范围 0-3600000' }],
              }),
              components.input.number('groupSyncDefaults_progressReportEvery', {
                ...compactNumber,
                className: `sm:col-span-2 ${compactInput.className}`,
                label: '默认进度消息间隔（每N个文件）',
                description: '0=关闭进度消息（仅影响聊天回执，不影响日志）',
                defaultValue: asString(defaults?.progressReportEvery ?? 10),
                isClearable: true,
                rules: [{ min: 0, max: 100000, error: '范围 0-100000' }],
              }),
              components.input.number('groupSyncDefaults_downloadLimitKbps', {
                ...compactNumber,
                className: `sm:col-span-2 ${compactInput.className}`,
                label: '默认下载限速（KB/s）',
                description: '0=不限制；不配置限速时会自动调整并发（最大5）以尽量跑满带宽',
                defaultValue: asString(defaults?.downloadLimitKbps ?? 0),
                isClearable: true,
                rules: [{ min: 0, max: 10000000, error: '范围 0-10000000' }],
              }),
              components.input.number('groupSyncDefaults_uploadLimitKbps', {
                ...compactNumber,
                className: `sm:col-span-2 ${compactInput.className}`,
                label: '默认上传限速（KB/s）',
                description: '0=不限制；不配置限速时会自动调整并发（最大5）以尽量跑满带宽',
                defaultValue: asString(defaults?.uploadLimitKbps ?? 0),
                isClearable: true,
                rules: [{ min: 0, max: 10000000, error: '范围 0-10000000' }],
              }),
            ],
          }),
          ...(false ? [components.accordionItem.create('targets', {
            title: '同步对象群配置',
            subtitle: '\u200b',
            isCompact: true,
            children: [
              components.accordionPro.create('groupSyncTargets', targetsData, {
                title: '同步对象群',
                variant: 'bordered',
                selectionMode: 'single',
                isCompact: true,
                showDivider: true,
                fullWidth: true,
                children: {
                  title: '目标群',
                  subtitle: '\u200b',
                  isCompact: true,
                  className: grid2,
                  children: [
                    components.input.string('gst_groupId', {
                      ...compactInput,
                      className: `sm:col-span-2 ${compactInput.className}`,
                      label: '群号',
                      placeholder: '123456',
                      isClearable: true,
                    }),
                    components.switch.options('gst_enabled', {
                      ...compactSwitch,
                      label: '启用自动同步',
                      defaultSelected: true,
                    }),
                    components.select.create('gst_mode', {
                      ...compactSelect,
                      label: '同步模式',
                      defaultValue: pickMode(defaults?.mode, 'incremental'),
                      items: [
                        components.select.createItem('incremental', { value: 'incremental', label: '增量' }),
                        components.select.createItem('full', { value: 'full', label: '全量' }),
                      ],
                    }),
                    components.input.string('gst_targetDir', {
                      ...compactInput,
                      className: `sm:col-span-2 ${compactInput.className}`,
                      label: '目标目录（可选）',
                      description: '为空则使用 openlistTargetDir/<群号>',
                      placeholder: '/挂载目录/QQ群文件/123456',
                      isClearable: true,
                    }),
                    components.input.string('gst_sourceFolderId', {
                      ...compactInput,
                      className: `sm:col-span-2 ${compactInput.className}`,
                      label: '起始文件夹ID（可选）',
                      description: '从群文件的指定文件夹开始导出/同步',
                      isClearable: true,
                    }),
                    components.switch.options('gst_flat', {
                      ...compactSwitch,
                      label: '平铺上传',
                      startText: '保留结构',
                      endText: '平铺',
                      defaultSelected: Boolean(defaults?.flat ?? false),
                    }),
                    components.input.number('gst_maxFiles', {
                      ...compactNumber,
                      label: '最多同步文件数（0=不限制）',
                      defaultValue: '',
                      isClearable: true,
                      rules: [{ min: 0, max: 1000000, error: '范围 0-1000000' }],
                    }),
                    components.input.number('gst_urlConcurrency', {
                      ...compactNumber,
                      label: '解析URL并发（可选）',
                      defaultValue: '',
                      isClearable: true,
                      rules: [{ min: 0, max: 5, error: '范围 0-5' }],
                    }),
                    components.input.number('gst_transferConcurrency', {
                      ...compactNumber,
                      label: '下载/上传并发（可选）',
                      defaultValue: '',
                      isClearable: true,
                      rules: [{ min: 0, max: 5, error: '范围 0-5' }],
                    }),
                    components.input.number('gst_fileTimeoutSec', {
                      ...compactNumber,
                      label: '单文件超时（秒，可选）',
                      description: `最大 ${MAX_FILE_TIMEOUT_SEC}s`,
                      defaultValue: '',
                      isClearable: true,
                      rules: [{ min: 0, max: MAX_FILE_TIMEOUT_SEC, error: `范围 0-${MAX_FILE_TIMEOUT_SEC}` }],
                    }),
                    components.input.number('gst_retryTimes', {
                      ...compactNumber,
                      label: '重试次数（可选）',
                      defaultValue: '',
                      isClearable: true,
                      rules: [{ min: 0, max: 1000, error: '范围 0-1000' }],
                    }),
                    components.input.number('gst_retryDelayMs', {
                      ...compactNumber,
                      label: '重试延迟（ms，可选）',
                      defaultValue: '',
                      isClearable: true,
                      rules: [{ min: 0, max: 3600000, error: '范围 0-3600000' }],
                    }),
                    components.input.number('gst_progressReportEvery', {
                      ...compactNumber,
                      label: '进度消息间隔（每N个文件，可选）',
                      description: '0=关闭（仅影响聊天回执，不影响日志）',
                      defaultValue: '',
                      isClearable: true,
                      rules: [{ min: 0, max: 100000, error: '范围 0-100000' }],
                    }),
                    components.input.number('gst_downloadLimitKbps', {
                      ...compactNumber,
                      label: '下载限速（KB/s，可选）',
                      description: '0=不限制；不配置限速时会自动调整并发（最大5）以尽量跑满带宽',
                      defaultValue: '',
                      isClearable: true,
                      rules: [{ min: 0, max: 10000000, error: '范围 0-10000000' }],
                    }),
                    components.input.number('gst_uploadLimitKbps', {
                      ...compactNumber,
                      label: '上传限速（KB/s，可选）',
                      description: '0=不限制；不配置限速时会自动调整并发（最大5）以尽量跑满带宽',
                      defaultValue: '',
                      isClearable: true,
                      rules: [{ min: 0, max: 10000000, error: '范围 0-10000000' }],
                    }),
                    components.input.string('gst_timeWindows', {
                      ...compactInput,
                      className: `sm:col-span-2 ${compactInput.className}`,
                      label: '同步时段（定时任务）',
                      description: '例：00:00-06:00,23:00-23:59；空=不限制',
                      placeholder: '00:00-06:00,23:00-23:59',
                      isClearable: true,
                    }),
                    components.switch.options('gst_scheduleEnabled', {
                      ...compactSwitch,
                      label: '启用定时同步',
                      defaultSelected: false,
                    }),
                    components.input.string('gst_scheduleCron', {
                      ...compactInput,
                      className: `sm:col-span-2 ${compactInput.className}`,
                      label: '定时 Cron',
                      description: '6段/5段均可，例如：0 */10 * * * *（每10分钟）',
                      placeholder: '0 */10 * * * *',
                      isClearable: true,
                    }),
                  ],
                },
              }),
            ],
          })] : []),
        ],
      }),
      components.accordionPro.create('groupSyncTargets', targetsData, {
        className: 'mt-2',
        title: '同步对象群配置',
        variant: 'bordered',
        selectionMode: 'single',
        isCompact: true,
        showDivider: true,
        fullWidth: true,
        children: {
          title: '目标群',
          subtitle: '\u200b',
          isCompact: true,
          className: grid2,
          children: [
            components.input.string('gst_groupId', {
              ...compactInput,
              className: `sm:col-span-2 ${compactInput.className}`,
              label: '群号',
              placeholder: '123456',
              isClearable: true,
            }),
            components.switch.options('gst_enabled', {
              ...compactSwitch,
              label: '启用自动同步',
              defaultSelected: true,
            }),
            components.select.create('gst_mode', {
              ...compactSelect,
              label: '同步模式',
              defaultValue: pickMode(defaults?.mode, 'incremental'),
              items: [
                components.select.createItem('incremental', { value: 'incremental', label: '增量' }),
                components.select.createItem('full', { value: 'full', label: '全量' }),
              ],
            }),
            components.input.string('gst_targetDir', {
              ...compactInput,
              className: `sm:col-span-2 ${compactInput.className}`,
              label: '目标目录（可选）',
              description: '为空则使用 openlistTargetDir/<群号>',
              placeholder: '/挂载目录/QQ群文件/123456',
              isClearable: true,
            }),
            components.input.string('gst_sourceFolderId', {
              ...compactInput,
              className: `sm:col-span-2 ${compactInput.className}`,
              label: '起始文件夹ID（可选）',
              description: '从群文件的指定文件夹开始递归同步',
              isClearable: true,
            }),
            components.switch.options('gst_flat', {
              ...compactSwitch,
              label: '平铺上传',
              startText: '保留结构',
              endText: '平铺',
              defaultSelected: Boolean(defaults?.flat ?? false),
            }),
            components.input.number('gst_maxFiles', {
              ...compactNumber,
              label: '最多同步文件数（0=不限）',
              defaultValue: '',
              isClearable: true,
              rules: [{ min: 0, max: 1000000, error: '范围 0-1000000' }],
            }),
            components.input.number('gst_urlConcurrency', {
              ...compactNumber,
              label: '解析URL并发（可选）',
              defaultValue: '',
              isClearable: true,
              rules: [{ min: 0, max: 5, error: '范围 0-5' }],
            }),
            components.input.number('gst_transferConcurrency', {
              ...compactNumber,
              label: '下载/上传并发（可选）',
              defaultValue: '',
              isClearable: true,
              rules: [{ min: 0, max: 5, error: '范围 0-5' }],
            }),
            components.input.number('gst_fileTimeoutSec', {
              ...compactNumber,
              label: '单文件超时（秒，可选）',
              description: `最多 ${MAX_FILE_TIMEOUT_SEC}s`,
              defaultValue: '',
              isClearable: true,
              rules: [{ min: 0, max: MAX_FILE_TIMEOUT_SEC, error: `范围 0-${MAX_FILE_TIMEOUT_SEC}` }],
            }),
            components.input.number('gst_retryTimes', {
              ...compactNumber,
              label: '重试次数（可选）',
              defaultValue: '',
              isClearable: true,
              rules: [{ min: 0, max: 1000, error: '范围 0-1000' }],
            }),
            components.input.number('gst_retryDelayMs', {
              ...compactNumber,
              label: '重试延迟（ms，可选）',
              defaultValue: '',
              isClearable: true,
              rules: [{ min: 0, max: 3600000, error: '范围 0-3600000' }],
            }),
            components.input.number('gst_progressReportEvery', {
              ...compactNumber,
              label: '进度消息间隔（每N个文件，可选）',
              description: '0=关闭（仅影响聊天回执，不影响日志）',
              defaultValue: '',
              isClearable: true,
              rules: [{ min: 0, max: 100000, error: '范围 0-100000' }],
            }),
            components.input.number('gst_downloadLimitKbps', {
              ...compactNumber,
              label: '下载限速（KB/s，可选）',
              description: '0=不限制；不配置限速时会自动调整并发（最多 5）以尽量跑满带宽',
              defaultValue: '',
              isClearable: true,
              rules: [{ min: 0, max: 10000000, error: '范围 0-10000000' }],
            }),
            components.input.number('gst_uploadLimitKbps', {
              ...compactNumber,
              label: '上传限速（KB/s，可选）',
              description: '0=不限制；不配置限速时会自动调整并发（最多 5）以尽量跑满带宽',
              defaultValue: '',
              isClearable: true,
              rules: [{ min: 0, max: 10000000, error: '范围 0-10000000' }],
            }),
            components.input.string('gst_timeWindows', {
              ...compactInput,
              className: `sm:col-span-2 ${compactInput.className}`,
              label: '同步时段（定时任务）',
              description: '例：00:00-06:00,23:00-23:59；空=不限制',
              placeholder: '00:00-06:00,23:00-23:59',
              isClearable: true,
            }),
            components.switch.options('gst_scheduleEnabled', {
              ...compactSwitch,
              label: '启用定时同步',
              defaultSelected: false,
            }),
            components.input.string('gst_scheduleCron', {
              ...compactInput,
              className: `sm:col-span-2 ${compactInput.className}`,
              label: '定时 Cron',
              description: '6段/5段均可，例：0 */10 * * * *（每10分钟）',
              placeholder: '0 */10 * * * *',
              isClearable: true,
            }),
          ],
        },
      }),
    ]
  },
  save: (form) => {
    try {
      const flattenedForm = (() => {
        if (!form || typeof form !== 'object') return form as any
        const groupFilesTool = (form as any).groupFilesTool
        if (!Array.isArray(groupFilesTool)) return form as any
        const merged: Record<string, any> = {}
        for (const item of groupFilesTool) {
          if (!item || typeof item !== 'object') continue
          Object.assign(merged, item)
        }
        return { ...merged, ...form }
      })()

      const configPath = getConfigFilePath()
      const current = readJsonSafe(configPath)
      const def = readJsonSafe(getDefaultConfigPath())

      const next: Record<string, any> = {
        ...def,
        ...current,
      }

      next.openlistBaseUrl = asString(flattenedForm.openlistBaseUrl ?? next.openlistBaseUrl)
      next.openlistUsername = asString(flattenedForm.openlistUsername ?? next.openlistUsername)
      next.openlistPassword = asString(flattenedForm.openlistPassword ?? next.openlistPassword)
      next.openlistTargetDir = asString(flattenedForm.openlistTargetDir ?? next.openlistTargetDir)

      const baseDefaults = next.groupSyncDefaults ?? {}
      next.groupSyncDefaults = {
        ...baseDefaults,
        mode: pickMode(flattenedForm.groupSyncDefaults_mode, pickMode(baseDefaults?.mode, 'incremental')),
        flat: (typeof flattenedForm.groupSyncDefaults_flat === 'undefined') ? Boolean(baseDefaults?.flat ?? false) : asBoolean(flattenedForm.groupSyncDefaults_flat),
        urlConcurrency: asInt(flattenedForm.groupSyncDefaults_urlConcurrency, asInt(baseDefaults?.urlConcurrency, 3)),
        transferConcurrency: asInt(flattenedForm.groupSyncDefaults_transferConcurrency, asInt(baseDefaults?.transferConcurrency, 3)),
        fileTimeoutSec: clampInt(asInt(flattenedForm.groupSyncDefaults_fileTimeoutSec, asInt(baseDefaults?.fileTimeoutSec, 600)), 10, MAX_FILE_TIMEOUT_SEC),
        retryTimes: asInt(flattenedForm.groupSyncDefaults_retryTimes, asInt(baseDefaults?.retryTimes, 2)),
        retryDelayMs: asInt(flattenedForm.groupSyncDefaults_retryDelayMs, asInt(baseDefaults?.retryDelayMs, 1500)),
        progressReportEvery: clampInt(asInt(flattenedForm.groupSyncDefaults_progressReportEvery, asInt(baseDefaults?.progressReportEvery, 10)), 0, 100000),
        downloadLimitKbps: clampInt(asInt(flattenedForm.groupSyncDefaults_downloadLimitKbps, asInt(baseDefaults?.downloadLimitKbps, 0)), 0, 10000000),
        uploadLimitKbps: clampInt(asInt(flattenedForm.groupSyncDefaults_uploadLimitKbps, asInt(baseDefaults?.uploadLimitKbps, 0)), 0, 10000000),
      }

      const normalizedTargets: any[] = []

      const rows = extractAccordionRows((flattenedForm as any).groupSyncTargets)

      if (rows.length) {
        for (const row of rows) {
          const groupId = asString(unwrapAccordionValue(row?.gst_groupId) ?? '').trim()
          if (!groupId) continue

          const enabled = asBoolean(unwrapAccordionValue(row?.gst_enabled) ?? true)
          const mode = pickMode(unwrapAccordionValue(row?.gst_mode), next.groupSyncDefaults?.mode ?? 'incremental')
          const targetDir = asString(unwrapAccordionValue(row?.gst_targetDir) ?? '').trim()
          const sourceFolderId = asString(unwrapAccordionValue(row?.gst_sourceFolderId) ?? '').trim()
          const flat = asBoolean(unwrapAccordionValue(row?.gst_flat) ?? next.groupSyncDefaults?.flat ?? false)
          const maxFiles = asInt(unwrapAccordionValue(row?.gst_maxFiles), 0)
          const urlConcurrency = asInt(unwrapAccordionValue(row?.gst_urlConcurrency), 0)
          const transferConcurrency = asInt(unwrapAccordionValue(row?.gst_transferConcurrency), 0)
          const fileTimeoutSec = clampInt(asInt(unwrapAccordionValue(row?.gst_fileTimeoutSec), 0), 0, MAX_FILE_TIMEOUT_SEC)
          const retryTimes = asOptionalInt(unwrapAccordionValue(row?.gst_retryTimes))
          const retryDelayMs = asOptionalInt(unwrapAccordionValue(row?.gst_retryDelayMs))
          const progressReportEvery = asOptionalInt(unwrapAccordionValue(row?.gst_progressReportEvery))
          const downloadLimitKbps = asOptionalInt(unwrapAccordionValue(row?.gst_downloadLimitKbps))
          const uploadLimitKbps = asOptionalInt(unwrapAccordionValue(row?.gst_uploadLimitKbps))
          const timeWindows = asString(unwrapAccordionValue(row?.gst_timeWindows) ?? '').trim()
          const scheduleEnabled = asBoolean(unwrapAccordionValue(row?.gst_scheduleEnabled) ?? false)
          const scheduleCron = asString(unwrapAccordionValue(row?.gst_scheduleCron) ?? '').trim()

          const target: Record<string, any> = {
            groupId,
            enabled,
            mode,
            flat,
          }

          if (sourceFolderId) target.sourceFolderId = sourceFolderId
          if (targetDir) target.targetDir = targetDir
          if (maxFiles > 0) target.maxFiles = maxFiles
          if (urlConcurrency > 0) target.urlConcurrency = urlConcurrency
          if (transferConcurrency > 0) target.transferConcurrency = transferConcurrency
          if (fileTimeoutSec > 0) target.fileTimeoutSec = fileTimeoutSec
          if (typeof retryTimes === 'number' && retryTimes >= 0) target.retryTimes = retryTimes
          if (typeof retryDelayMs === 'number' && retryDelayMs >= 0) target.retryDelayMs = retryDelayMs
          if (typeof progressReportEvery === 'number' && progressReportEvery >= 0) target.progressReportEvery = clampInt(progressReportEvery, 0, 100000)
          if (typeof downloadLimitKbps === 'number' && downloadLimitKbps >= 0) target.downloadLimitKbps = clampInt(downloadLimitKbps, 0, 10000000)
          if (typeof uploadLimitKbps === 'number' && uploadLimitKbps >= 0) target.uploadLimitKbps = clampInt(uploadLimitKbps, 0, 10000000)
          if (timeWindows) target.timeWindows = timeWindows
          if (scheduleEnabled || scheduleCron) target.schedule = { enabled: scheduleEnabled, cron: scheduleCron }

          normalizedTargets.push(target)
        }
      } else {
        const container = ((flattenedForm as any).groupSyncTargets && typeof (flattenedForm as any).groupSyncTargets === 'object')
          ? (flattenedForm as any).groupSyncTargets as WebConfigData
          : flattenedForm

        const groupIds = arrayFrom((container as any).gst_groupId).map(v => asString(unwrapAccordionValue(v)).trim())
        for (let index = 0; index < groupIds.length; index++) {
          const groupId = groupIds[index]
          if (!groupId) continue

          const enabled = asBoolean(unwrapAccordionValue(getFormRowValue(container, 'gst_enabled', index)) ?? true)
          const mode = pickMode(unwrapAccordionValue(getFormRowValue(container, 'gst_mode', index)), next.groupSyncDefaults?.mode ?? 'incremental')
          const targetDir = asString(unwrapAccordionValue(getFormRowValue(container, 'gst_targetDir', index)) ?? '').trim()
          const sourceFolderId = asString(unwrapAccordionValue(getFormRowValue(container, 'gst_sourceFolderId', index)) ?? '').trim()
          const flat = asBoolean(unwrapAccordionValue(getFormRowValue(container, 'gst_flat', index)) ?? next.groupSyncDefaults?.flat ?? false)
          const maxFiles = asInt(unwrapAccordionValue(getFormRowValue(container, 'gst_maxFiles', index)), 0)
          const urlConcurrency = asInt(unwrapAccordionValue(getFormRowValue(container, 'gst_urlConcurrency', index)), 0)
          const transferConcurrency = asInt(unwrapAccordionValue(getFormRowValue(container, 'gst_transferConcurrency', index)), 0)
          const fileTimeoutSec = clampInt(asInt(unwrapAccordionValue(getFormRowValue(container, 'gst_fileTimeoutSec', index)), 0), 0, MAX_FILE_TIMEOUT_SEC)
          const retryTimes = asOptionalInt(unwrapAccordionValue(getFormRowValue(container, 'gst_retryTimes', index)))
          const retryDelayMs = asOptionalInt(unwrapAccordionValue(getFormRowValue(container, 'gst_retryDelayMs', index)))
          const progressReportEvery = asOptionalInt(unwrapAccordionValue(getFormRowValue(container, 'gst_progressReportEvery', index)))
          const downloadLimitKbps = asOptionalInt(unwrapAccordionValue(getFormRowValue(container, 'gst_downloadLimitKbps', index)))
          const uploadLimitKbps = asOptionalInt(unwrapAccordionValue(getFormRowValue(container, 'gst_uploadLimitKbps', index)))
          const timeWindows = asString(unwrapAccordionValue(getFormRowValue(container, 'gst_timeWindows', index)) ?? '').trim()
          const scheduleEnabled = asBoolean(unwrapAccordionValue(getFormRowValue(container, 'gst_scheduleEnabled', index)) ?? false)
          const scheduleCron = asString(unwrapAccordionValue(getFormRowValue(container, 'gst_scheduleCron', index)) ?? '').trim()

          const target: Record<string, any> = {
            groupId,
            enabled,
            mode,
            flat,
          }

          if (sourceFolderId) target.sourceFolderId = sourceFolderId
          if (targetDir) target.targetDir = targetDir
          if (maxFiles > 0) target.maxFiles = maxFiles
          if (urlConcurrency > 0) target.urlConcurrency = urlConcurrency
          if (transferConcurrency > 0) target.transferConcurrency = transferConcurrency
          if (fileTimeoutSec > 0) target.fileTimeoutSec = fileTimeoutSec
          if (typeof retryTimes === 'number' && retryTimes >= 0) target.retryTimes = retryTimes
          if (typeof retryDelayMs === 'number' && retryDelayMs >= 0) target.retryDelayMs = retryDelayMs
          if (typeof progressReportEvery === 'number' && progressReportEvery >= 0) target.progressReportEvery = clampInt(progressReportEvery, 0, 100000)
          if (typeof downloadLimitKbps === 'number' && downloadLimitKbps >= 0) target.downloadLimitKbps = clampInt(downloadLimitKbps, 0, 10000000)
          if (typeof uploadLimitKbps === 'number' && uploadLimitKbps >= 0) target.uploadLimitKbps = clampInt(uploadLimitKbps, 0, 10000000)
          if (timeWindows) target.timeWindows = timeWindows
          if (scheduleEnabled || scheduleCron) target.schedule = { enabled: scheduleEnabled, cron: scheduleCron }

          normalizedTargets.push(target)
        }
      }

      const unique = new Map<string, any>()
      for (const item of normalizedTargets) {
        unique.set(String(item.groupId), item)
      }
      next.groupSyncTargets = [...unique.values()]

      writeJsonSafe(configPath, next)
      return { success: true, message: '配置保存成功' }
    } catch (error: any) {
      logger.error(error)
      return { success: false, message: error?.message ?? String(error) }
    }
  },
})
