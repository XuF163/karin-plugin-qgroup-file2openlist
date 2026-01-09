import { karin } from 'node-karin'
import { runGroupFileSyncSchedulerTick } from '@/model/groupSync'

export const groupFileSyncScheduler = karin.task(
  '群文件同步调度器',
  '* * * * * *',
  runGroupFileSyncSchedulerTick,
  { log: false, type: 'skip' },
)
