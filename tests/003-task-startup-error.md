# 测试：启动就报错（分类二 → 场景 6）
# 目标：验证 skill 能引导排查启动类错误

## prompt

> Flink CDC 任务提交就报错，启动不起来。

## expected

Agent 应引导用户查看错误信息，对照错误类型表逐行排查。

## trigger_check

- 应触发：✅ 是（"启动就报错""起不来"）
