# Karin TypeScript 插件开发模板

## 📖 目录

- [前言](#前言)
- [群文件导出](#群文件导出)
- [同步到 OpenList](#同步到-openlist)
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
- `groupSyncDefaults`：群同步默认策略（增量/全量、并发、单文件超时、重试等）
- `groupSyncTargets`：同步对象群配置（每群可单独覆盖策略/目录/并发/定时计划/同步时段）

私聊命令：

- `#同步群文件 <群号>`：下载群文件并通过 OpenList WebDAV 上传到目标目录
- `#同步群文件 <群号> --to /目标目录`：覆盖配置里的目标目录
- `#同步群文件 <群号> --flat`：不保留群文件夹结构，全部平铺上传
- `#同步群文件 <群号> --max <n>` / `--folder <id>` / `--concurrency <n>`：同导出命令
- `#同步群文件 <群号> --full/--inc`：强制全量/增量（覆盖群配置）

群聊命令（简化）：

- `#同步群文件`：默认同步本群（建议先在 WebUI 配置 `groupSyncTargets`）

群同步配置命令（主人权限）：

- `#群同步配置 列表`
- `#群同步配置 <群号> 添加/删除/查看`
- `#群同步配置 <群号> 启用/停用`
- `#群同步配置 <群号> 模式 全量|增量`
- `#群同步配置 <群号> 计划 <cron>`：设置定时同步（例：`0 0 3 * * *`）
- `#群同步配置 <群号> 时段 00:00-06:00,23:00-23:59`：限制定时同步时间段（空=不限制）

## 🚀 快速开始

```bash
pnpm create karin
```

- 按提示选择“ts插件开发模板”即可自动初始化项目。
- 进入新建的项目目录，继续开发。

---

## 详细开发流程

1. **一键创建项目**

   ```bash
   pnpm create karin
   ```

   - 选择“ts插件开发模板”
   - 填写你的插件名称（会自动作为 package.json 的 name）
   - 其余信息按提示填写

2. **安装依赖**

   ```bash
   pnpm install
   ```

3. **开发与调试**

   - 启动开发命令：
     ```bash
     pnpm dev
     ```
   - 编写你的插件代码于 `src/` 目录。
   - 编译输出：
     ```bash
     pnpm build
     ```
   - 调试编译之后的代码：
     ```bash
     pnpm app
     ```
   - 本地调试建议：
     - 可用 `pnpm link --global` 进行全局软链测试。
     - 或在 karin 根目录用 `pnpm add ../your-plugin-repo -w` 进行本地依赖测试。

4. **配置 NPM 秘钥**

   > 用于自动化发布，建议开启 2FA。

   1. 注册 [npmjs](https://www.npmjs.com/) 账号。
   2. 进入 `Access Tokens`，新建 `Classic Token`，类型选 `Automation`。
   3. 复制生成的 Token。
   4. 打开你的 GitHub 仓库 → Settings → Secrets and variables → Actions。
   5. 新建 `NPM_TOKEN`，粘贴 Token。
   6. 允许 GitHub Actions 创建和批准 PR（Settings → Actions）。

5. **设置包信息**

   > 包名必须唯一，建议先在 [npm](https://www.npmjs.com/) 搜索确认。

   - 初始化时填写的插件名会自动作为 package.json 的 name，无需手动修改。
   - 其他如 `author`、`description`、`homepage`、`bugs.url`、`repository` 可在 package.json 中补充完善。
   - **CI 配置无需再手动修改 package-name，已自动同步。**

6. **自动化发布**

   > 推送代码后，GitHub Actions 会自动编译并发布到 npm。

   - 常规开发流程：
     1. `git add . && git commit -m "feat: ..." && git push`
     2. 等待 CI 自动发布
     3. 发布成功后可在 npm 页面看到新版本

7. **安装与验证**

   - 在 karin 根目录下安装你的插件：
     ```bash
     pnpm add your-package-name -w
     ```
   - 验证插件是否生效，可查看 karin 启动日志或相关功能。

---

## 💡 常见问题与建议

- **Q: 发布失败怎么办？**
  - 检查 NPM_TOKEN 是否配置正确，权限是否足够。
  - 包名是否唯一，未被占用。
  - Actions 日志可定位具体报错。
- **Q: 如何本地调试插件？**
  - 推荐用 `pnpm link` 或本地依赖安装。
- **Q: 如何贡献代码？**
  - 欢迎 PR，建议先提 issue 讨论。

---

## 贡献与反馈

- 有任何建议或问题，欢迎在 [Issues](https://github.com/KarinJS/karin-plugin-template-ts/issues) 提出。
- 也可加入官方交流群交流经验。
