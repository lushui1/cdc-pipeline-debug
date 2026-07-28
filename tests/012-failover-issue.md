# 测试：主从切换后异常（场景 18）
# 目标：验证 skill 覆盖高可用场景

## prompt

> MySQL 主库挂了切换到从库之后，CDC 数据就对不上了。

## expected

Agent 应检查 GTID 是否开启，给出高可用配置方案。

## trigger_check

- 应触发：✅ 是（"主从切换""数据对不上"）
