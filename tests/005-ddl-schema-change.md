# 测试：源表加字段目标库没有（分类四 → 场景 14）
# 目标：验证 skill 能处理 DDL 相关描述

## prompt

> 源 MySQL 表加了两个字段，但 Doris 目标表一直没出现，怎么处理？

## expected

Agent 应区分 Flink SQL 模式（不支持自动 DDL）和 CDC YAML 模式（可开启 Schema Evolution），分别给出方案。

## trigger_check

- 应触发：✅ 是（"加字段""目标表没出现""DDL"）
