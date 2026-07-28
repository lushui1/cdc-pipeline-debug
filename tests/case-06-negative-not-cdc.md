# 测试用例 06：不应触发 — 非 CDC 问题

## prompt

> 帮我写一个Flink的WordCount程序。

## expected

Agent 不应该用 CDC 技能，应该走通用编程能力。

## trigger_check

- 应触发：❌ 否（与 CDC/增量同步/数据管道无关）
