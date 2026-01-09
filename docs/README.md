# 文档索引（`karin-plugin-qgroup-file2openlist`）

这份 `docs/` 目录下的文档用于 **开发交接**：让后续维护者能在最短时间内理解本插件“做什么 / 怎么跑 / 配置在哪里 / 关键逻辑在哪 / 常见坑怎么排”。

## 推荐阅读顺序

1. `docs/开发文档.md`（主文档，包含：现状、功能、配置、目录结构、核心流程、排障）
2. `docs/refactor-plan.md`（已完成的重构规划与目录分层说明，偏实现侧）
3. `docs/二期工程计划.md`（主人私聊图片交互增强：命令/配置/模板/里程碑）

## 你可能最关心的三个入口

- **配置文件位置**：运行时为 `@karinjs/<插件包名>/config/config.json`（见 `src/dir.ts`、`src/utils/config.ts`）
- **核心指令入口**：`src/apps/groupFiles.ts`、`src/apps/groupSyncConfig.ts`、`src/apps/groupFiles.backup.ts`
- **核心业务逻辑**：`src/model/groupFiles/*`、`src/model/openlist/*`、`src/model/groupSync/*`
