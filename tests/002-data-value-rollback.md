# 测试：数据值回退 + 日志正常（分类一 → 场景 2）
# 目标：验证 skill 能处理"所有日志正常但数据不对"这个最隐蔽的问题

## prompt

> CDC 日志正常，Flink 日志正常，Doris 日志正常，但订单状态从已支付到已完成后，查 Doris 还是已支付。

## expected

Agent 应识别为乱序覆盖问题，引导检查 Sequence Column。

## trigger_check

- 应触发：✅ 是（"日志正常但"+数据不对+"状态回退"）
