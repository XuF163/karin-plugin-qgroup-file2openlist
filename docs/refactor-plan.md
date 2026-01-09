# 重构规划：`karin-plugin-qgroup-file2openlist`（src 目录）

## 目标

- `src/apps/` 只保留“事件监听 / 命令匹配 / 参数解析 / 回复消息”的薄层代码。
- 业务逻辑集中到 `src/model/`：OpenList 登录、WebDAV/API 上传、群文件列表与 URL 解析、同步状态、并发/重试等。
- 降低单文件体积与耦合度：拆分模块、明确依赖方向（apps → model → shared）。
- 增加必要注释：解释“为什么这么做”、关键边界条件（超时、并发、重试、回退策略、路径规范化）。

## 当前落地（已完成）

### 新目录结构（已创建）

```
src/
  apps/
    groupFiles.ts              # 群文件导出 + 同步（薄封装）
    groupFiles.backup.ts       # OpenList->OpenList 备份 + 上传自动备份（薄封装）

  model/
    shared/
      errors.ts                # formatErrorMessage / isAbortError / fetchTextSafely
      concurrency.ts           # runWithConcurrency / runWithAdaptiveConcurrency（保持原算法）
      retry.ts                 # retryAsync（保持原算法）
      path.ts                  # normalizePosixPath / safePathSegment / encodePathForUrl
      fsJson.ts                # readJsonSafe / writeJsonSafe / ensureDir
      rateLimit.ts             # createThrottleTransform

    openlist/
      url.ts                   # baseUrl 规范化、BasicAuth、同源 header
      api.ts                   # /auth/login、/fs/list、/fs/get(raw_url)、/fs/mkdir、/fs/put 等
      webdav.ts                # PROPFIND/HEAD/MKCOL/PUT 以及 WebDAV 复制工具
      backup.ts                # OpenList -> OpenList 备份（扫描/存在性/复制/回退）
      index.ts                 # barrel exports

    groupFiles/
      types.ts                 # ExportedGroupFile / SyncMode / 状态类型
      state.ts                 # 同步状态读写 + 任务锁
      qgroup.ts                # 群文件列表兼容层 + URL 解析 + locate by id
      export.ts                # 导出逻辑（写 JSON/CSV、并发解析 URL）
      syncToOpenList.ts        # 同步核心（增量/全量、并发/重试、进度回调）
      uploadAutoBackup.ts      # 上传事件自动备份（串行队列 + 状态落盘 + WebDAV 上传）
      index.ts                 # barrel exports

    groupSync/
      configCommand.ts         # 群同步配置命令核心（读写配置）
      scheduler.ts             # 定时同步调度（cron/timeWindow 解析 + 触发同步）
      index.ts                 # barrel exports
```

### 行为与依赖方向

- `apps` 负责：
  - 命令正则匹配、参数解析（`--max/--folder/--concurrency/...`）
  - 调用 `model` 并把进度/结果通过 `e.reply()` 输出
  - 不再承载 OpenList API/WebDAV/群文件遍历等核心逻辑
- `model` 负责：
  - OpenList API/WebDAV 通讯、限速、重试
  - 群文件列表/URL 解析兼容（不同协议端能力差异）
  - 同步状态（`data/group-file-sync-state/<groupId>.json`）与互斥锁
  - 同步/导出等业务流程

### 已验证

- 本地构建通过：`npm run build`

## 后续重构阶段（已完成）

### Phase 2（已完成）：把 `groupFiles.backup.ts` 的业务继续下沉到 `src/model`

目标：让 `src/apps/groupFiles.backup.ts` 只保留“参数解析 + 调用 model + 回复”。

建议拆分：

- `src/model/openlist/backup.ts`
  - `scanOpenListFiles()`：扫描目录树并产出文件清单（支持 API/WebDAV 两种 list）
  - `copyOpenListFile()`：根据（sourceTransport,targetTransport）选择最合适的传输方式
  - `ensureTargetDir()`：WebDAV / API mkdir 以及 auto fallback 策略
  - `existsOnTarget()`：增量模式下的存在性判断（HEAD 或 /fs/get）

### Phase 3（已完成）：上传自动备份下沉

建议拆分：

- `src/model/groupFiles/uploadAutoBackup.ts`
  - `handleGroupFileUploaded()`：输入（event, cfg）输出（是否执行/执行结果）
  - 复用 `groupFiles/state.ts`（状态落盘）与 `openlist/webdav.ts`（上传）

### Phase 4（已完成）：配置与调度整理

- 把 `src/apps/groupSyncConfig.ts` / `src/apps/groupSyncScheduler.ts` 中的：
  - `normalizePosixPath`、cron/timeWindow 解析等公共逻辑抽到 `src/model/groupSync/*`
- 保持 apps 中只剩：注册 `karin.command`/`karin.task`，调用 model 的纯函数/服务函数。

## 约定（建议写到代码里并持续执行）

- 命名：
  - `apps/*`：以“动作/触发器”命名（export/sync/backup/accept）
  - `model/*`：以“领域能力”命名（openlist/qgroup/sync/state/shared）
- 错误信息：
  - `model` 抛出语义化 Error（适合直接 reply）
  - `apps` 统一 `logger.error` + `e.reply(formatErrorMessage(err))`
- 网络边界：
  - 所有 fetch / WebDAV 操作只出现在 `src/model/openlist/*`
- 兼容性：
  - 群文件能力差异（OneBot/适配器）集中在 `src/model/groupFiles/qgroup.ts`
