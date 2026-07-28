# 测试用例 04：显式触发 — 整库同步配置

## prompt

> 我要把10张MySQL表CDC同步到Doris，Flink SQL每张表配一个任务太麻烦了，有没有一次性搞定的方案？

## expected

Agent 应该：
1. 推荐 Flink CDC 3.0 YAML Pipeline 方案
2. 给出 YAML 配置示例
3. 列出踩坑点：server-id、正则匹配、Schema Evolution、checkpoint

## trigger_check

- 应触发：✅ 是（包含"CDC""整库同步""Flink CDC YAML"等关键词）
