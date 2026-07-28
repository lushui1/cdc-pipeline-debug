# 测试：延迟高/性能慢（分类三 → 场景 11）
# 目标：验证 skill 能处理性能相关描述

## prompt

> CDC 同步延迟越来越高，现在差 2 小时了，追不上。

## expected

Agent 应进入性能问题分支，检查 Flink WebUI 背压状态。

## trigger_check

- 应触发：✅ 是（"延迟越来越高""追不上""差+小时"）
