#!/bin/bash
# 诊断：检查 checkpoint 记录的 binlog 位点是否仍然可用
# 用法：Agent 从 Flink WebUI 或日志中获取 checkpoint 位点，对比 MySQL 可用 binlog

cat << 'EOF'
【步骤 1：获取 checkpoint 位点】
可以从以下来源获取：
  a) Flink WebUI → Checkpoints → 点击某个 checkpoint → 查看 "Binlog offset on checkpoint"
  b) Flink 日志: grep "Binlog offset on checkpoint" flink-*.log
  c) 如果用户有 savepoint，直接问用户

【步骤 2：检查 MySQL 可用 binlog】
SHOW BINARY LOGS;

【步骤 3：对比】
如果 checkpoint 记录的文件名在 SHOW BINARY LOGS 结果中 → 位点可用 ✅
如果不在 → binlog 已被清理 ❌
EOF

echo ""
echo "=== 恢复方案（按推荐优先级） ==="
echo "1. 如果位点可用 → 从 checkpoint/savepoint 正常恢复"
echo "2. 如果位点被清理，用户接受丢数据 → scan.startup.mode=latest-offset"
echo "3. 如果位点被清理，用户要求完整数据 → scan.startup.mode=initial（重跑全量+增量）"
echo "4. 如果有近似可用的其他位点 → scan.startup.mode=specific-offset + 指定位点"
