## 群文件功能

- `#我的备份`：查看已开启上传自动备份的群（uploadBackup=on）+ oplts 列表（图片，支持 `--text` 文本回退）。
- `#添加备份群<群号>`：开启该群上传自动备份（主人权限）。
- `#删除群备份 <序号>`：按序号关闭该群上传自动备份（主人权限）。

### oplts（OpenList → OpenList 订阅）

- `#添加oplt <A> <B>`：添加一条 oplts。
  - A：源 OpenList 地址（URL，可带路径表示源目录）。
  - B：目标目录（/path 或 URL，若是 URL 会自动提取 pathname）。
- `#删除oplt <序号>`：删除一条 oplts（主人权限）。
- `#oplt备份 <序号|全部> [参数]`：手动触发 oplts 备份（主人权限）。
  - 参数：`--inc|--full`、`--auto|--api|--webdav`、`--host|--no-host`、`--user <u> --pass <p>`。
- `#oplt夜间 查看|开启|关闭 [参数]`：夜间自动备份 oplts（主人权限）。
  - 示例：`#oplt夜间 开启 0 0 3 * * * --inc --auto`
  - 参数：`--inc|--full`、`--auto|--api|--webdav`、`--host|--no-host`。

- `#op帮助`：展开所有命令介绍（文本）。
- `#群文件帮助`：帮助图（图片）。

## OpenList → OpenList（单次）

- `#备份oplist https://pan.example.com --src / --to /backup --inc --auto`
