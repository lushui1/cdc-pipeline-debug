# 测试用例 02：显式触发 — CDC 实时数据乱序

## prompt

> Flink CDC从MySQL同步到Doris，运单状态"已发货→已完成"后，Doris里还是"已发货"，CDC日志看过了没问题，Flink日志也没问题，到底哪出问题了？

## expected

Agent 应该：
1. 识别出这是 CDC 乱序问题（一切正常但数据不对）
2. 确认目标库类型（Doris）
3. 检查是否启用了 Sequence Column
4. 给出 ALTER TABLE 启用 Sequence Column 的 SQL
5. 提示 Doris 2.0.14 的 Bug 和升级建议

## trigger_check

- 应触发：✅ 是（包含"CDC""Doris""Sequence Column"等关键词）
