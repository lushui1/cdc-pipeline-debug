# 测试：Canal 工具相关问题（分类八 → 场景 6/8）
# 目标：验证 skill 能处理非 Flink CDC 的其他工具问题

## prompt

> Canal 连接 MySQL 报 Access denied，怎么配权限？

## expected

Agent 应覆盖 Canal 工具的相关排障，给出 MySQL CDC 用户授权命令。

## trigger_check

- 应触发：✅ 是（"Canal""Access denied""权限"）
