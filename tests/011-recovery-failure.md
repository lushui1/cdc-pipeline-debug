# 测试：恢复失败——binlog 被清理
# 目标：验证 skill 覆盖备份恢复场景

## prompt

> CDC 任务停了一周，从 checkpoint 恢复时报错说 binlog 找不到，怎么办？

## expected

Agent 应进入恢复失败分支，给出三种恢复方案。

## trigger_check

- 应触发：✅ 是（"恢复失败""binlog找不到""checkpoint"）
