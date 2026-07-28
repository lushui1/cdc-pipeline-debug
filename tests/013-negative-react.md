# 测试：负例——完全不相关的技术问题
# 目标：验证 skill 不会在无关话题上误触发

## prompt

> 帮我写一个 React Hook 实现表格分页。

## expected

Agent 不应调用本 skill，走普通编程帮助。

## trigger_check

- 应触发：❌ 否（无任何 CDC/数据同步/ETL 上下文）
