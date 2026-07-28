# 测试：数据库迁移后异常（分类五 → 场景 17）
# 目标：验证 skill 能处理运维变更类问题

## prompt

> 数据库从 5.7 升到 8.0 后，CDC 任务就报错了，怎么解决？

## expected

Agent 应检查 MySQL 8.4 兼容性、binlog_format 变化，给出升级后适配方案。

## trigger_check

- 应触发：✅ 是（"升级后""报错""数据库迁移"）
