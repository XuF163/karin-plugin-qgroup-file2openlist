import { karin } from 'node-karin'
import { handleGroupSyncConfigCommand } from '@/model/groupSync'

/**
 * 群同步配置命令入口。
 * - apps 层只负责命令匹配与权限配置
 * - 具体处理逻辑在 model 中，便于复用与测试
 */
export const groupSyncConfig = karin.command(
  /^#?(群同步配置|同步群配置|群文件同步配置)(.*)$/i,
  handleGroupSyncConfigCommand,
  {
    name: '群同步配置',
    log: true,
    priority: 9999,
    permission: 'master',
  },
)

