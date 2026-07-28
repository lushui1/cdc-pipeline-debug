#!/bin/bash
# 诊断：按小时对比源库与目标库数据量，用于发现时区偏差导致的边界数据错位
# 用法：用户提供源库和目标库的 COUNT 分组查询结果
# Agent 执行：提示用户分别在源库和目标库运行以下 SQL，然后粘贴结果

cat << 'EOF'
【源库执行】
SELECT DATE_FORMAT(create_time, '%Y-%m-%d %H:00:00') AS hour_slot,
       COUNT(*) AS cnt
FROM source_table
WHERE create_time >= NOW() - INTERVAL 48 HOUR
GROUP BY hour_slot
ORDER BY hour_slot;

【目标库执行】
SELECT DATE_FORMAT(create_time, '%Y-%m-%d %H:00:00') AS hour_slot,
       COUNT(*) AS cnt
FROM target_table
WHERE create_time >= NOW() - INTERVAL 48 HOUR
GROUP BY hour_slot
ORDER BY hour_slot;
EOF

echo ""
echo "=== 对比方法 ==="
echo "将两边结果粘贴上来，Agent 自动对比。"
echo "如果每天固定某个小时（通常是凌晨 0-4 点）目标库数据量明显少于源库 → 时区问题"
echo "如果所有小时均匀偏少 → 检查物理删除或切分字段"
