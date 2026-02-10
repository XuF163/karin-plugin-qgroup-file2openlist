# Karin TypeScript 插件开发模板

## 📖 目录

- [前言](#前言)
- [群文件导出](#群文件导出)
- [同步到 OpenList](#同步到-openlist)
- [主人管理（图片面板）](#主人管理图片面板)
- [OpenList → OpenList 转发](#openlist--openlist-转发)
- [快速开始](#快速开始)
- [详细开发流程](#详细开发流程)
- [常见问题与建议](#常见问题与建议)
- [贡献与反馈](#贡献与反馈)

---

## 前言

TypeScript 插件开发流程现在更加简单，无需手动克隆模板仓库，只需一条命令即可快速开始！

> TypeScript 编写 → 编译为 JS → 发布 NPM 包 → 用户安装

---

## 群文件导出

在私聊中发送（默认所有人可用，可在 `src/apps/groupFiles.ts` 调整权限）：

- `#导出群文件 <群号>`：递归导出群文件列表（JSON，包含下载 URL）
- `#导出群文件 <群号> --csv`：导出 CSV
- `#导出群文件 <群号> --no-url`：只导出列表，不解析 URL
- `#导出群文件 <群号> --url-only`：仅输出 URL（更方便复制）
- `#导出群文件 <群号> --folder <id>`：从指定文件夹开始导出
- `#导出群文件 <群号> --max <n>`：最多导出 n 条文件记录
- `#导出群文件 <群号> --concurrency <n>`：解析 URL 并发数（默认 3）
- `#导出群文件 <群号> --send-file`：尝试发送导出文件（依赖协议端支持）

提示：下载 URL 通常有时效，过期后需重新导出。

## 同步到 OpenList

前置配置：`config/config.json`（会自动复制到 `@karinjs/<插件名>/config/config.json`）

- `openlistBaseUrl`：例如 `http://127.0.0.1:5244`
- `openlistUsername` / `openlistPassword`：用于 WebDAV BasicAuth 登录
- `openlistTargetDir`：目标目录（例：`/挂载目录/QQ群文件`）
- `resourceLimits.transferConcurrency`：全局传输并发上限（所有下载+上传共享；生产环境建议 1，避免同时传输吃满内存；<=0 不限制）
- `groupSyncDefaults`：群同步默认策略（并发、单文件超时、重试等）
- `groupSyncTargets`：同步对象群配置（每群可单独覆盖目录/并发/同步时段等）

私聊命令：

- `#同步群文件 <群号>`：下载群文件并通过 OpenList WebDAV 上传到目标目录
- `#同步群文件 <群号> --to /目标目录`：覆盖配置里的目标目录
- `#同步群文件 <群号> --flat`：不保留群文件夹结构，全部平铺上传
- `#同步群文件 <群号> --max <n>` / `--folder <id>` / `--concurrency <n>`：同导出命令

固定策略：同步模式为增量（incremental），单文件超时 3000 秒（不再通过命令配置）。

群聊命令（简化）：

- 默认不响应（所有指令请私聊触发，避免群内刷屏）

群同步配置命令（主人权限）：

- `#群同步配置 列表`
- `#群同步配置 <群号> 添加/删除/查看`
- `#群同步配置 <群号> 启用/停用`
- `#群同步配置 <群号> 时段 00:00-06:00,23:00-23:59`：限制定时同步时间段（空=不限制）

说明：夜间自动备份默认每天 02:00 触发（先群后 oplts），固定增量 + 单文件超时 3000s；不再支持通过 `#群同步配置` 配置 cron/mode/timeout。

## 主人管理（图片面板）

仅主人私聊可用：

- `#群文件面板`：输出管理总览面板（群绑定/监听/OP转发规则摘要/快捷命令）
- `#绑定备份群 <群号> [--to /目标目录] [--flat|--keep]`：写入群配置并默认开启群文件上传监听
- `#解绑备份群 <群号>`：删除该群配置
- `#开启群文件监听 <群号>` / `#关闭群文件监听 <群号>`：切换 uploadBackup 监听开关

## OpenList → OpenList 转发

用于“多源站 → 单目的端（固定为配置 openlistBaseUrl）”的转发规则（仅主人私聊可用）：

- `#添加op转发 <源OpenListBaseUrl> [--src /] [--to /backup] [--name xxx] [--user u] [--pass p] [--full|--inc] [--auto|--api|--webdav]`
- `#op转发 列表`
- `#op转发 查看 <ruleId>`
- `#op转发 执行 <ruleId>`（默认全量）
- `#op转发 删除 <ruleId>`

## 🚀 快速开始
