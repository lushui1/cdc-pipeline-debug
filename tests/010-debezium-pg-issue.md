# 测试：Debezium + Kafka Connect 问题
# 目标：验证 skill 覆盖 Debezium

## prompt

> Debezium 连不上 PostgreSQL，复制槽创建失败。

## expected

Agent 应进入任务异常分支，检查 PG wal_level 和复制槽设置。

## trigger_check

- 应触发：✅ 是（"Debezium""连不上""复制槽"）
