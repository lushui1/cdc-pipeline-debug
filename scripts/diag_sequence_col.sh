#!/bin/bash
# 诊断：检查目标表是否启用了 Sequence Column（Doris 场景）
# 用法：Agent 提示用户连接 Doris 执行以下命令

cat << 'EOF'
【检查 Sequence Column 是否启用】
SET show_hidden_columns = true;
DESC orders;

如果输出包含 __DORIS_SEQUENCE_COL__ 列 → 已启用
如果不包含 → 未启用，需要配置

【检查当前 Sequence Column 定义】
SHOW CREATE TABLE orders;

在 PROPERTIES 中查找：
  function_column.sequence_col = 'update_time'   → 用 update_time 列排序
  function_column.sequence_type = 'DATETIME'     → 用隐藏列 + 类型

【粒度检查】
如果使用了 DATETIME（秒级精度），且业务可能在同秒多次变更 → 需要升级到毫秒或 BIGINT
EOF

echo ""
echo "=== 判断 ==="
echo "Sequence Column 已启用？请在结果中查找 __DORIS_SEQUENCE_COL__"
