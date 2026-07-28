#!/bin/bash
# MySQL CDC 前置条件一键预检脚本
# 运行方式：在 MySQL 上执行以下命令，或 DBA 配合执行
# 输出：每一项 ✅ 或 ❌

echo "========================================"
echo " MySQL CDC 前置条件检查"
echo "========================================"

echo ""
echo "--- 1. binlog ---"
BINLOG=$(mysql -u"$1" -p"$2" -h"${3:-localhost}" -P"${4:-3306}" -NBe "SHOW VARIABLES LIKE 'log_bin'" 2>/dev/null | awk '{print $2}')
if [ "$BINLOG" = "ON" ]; then
  echo "  ✅ log_bin = ON"
else
  echo "  ❌ log_bin = ${BINLOG:-OFF}（需要开启 binlog）"
  echo "     修复: my.cnf 添加 log_bin = mysql-bin, binlog_format = ROW, server_id = 1，重启 MySQL"
fi

FORMAT=$(mysql -u"$1" -p"$2" -h"${3:-localhost}" -P"${4:-3306}" -NBe "SHOW VARIABLES LIKE 'binlog_format'" 2>/dev/null | awk '{print $2}')
if [ "$FORMAT" = "ROW" ]; then
  echo "  ✅ binlog_format = ROW"
else
  echo "  ❌ binlog_format = ${FORMAT:-未设置}（需要 ROW）"
fi

echo ""
echo "--- 2. GTID ---"
GTID=$(mysql -u"$1" -p"$2" -h"${3:-localhost}" -P"${4:-3306}" -NBe "SHOW VARIABLES LIKE 'gtid_mode'" 2>/dev/null | awk '{print $2}')
if [ "$GTID" = "ON" ]; then
  echo "  ✅ gtid_mode = ON（高可用推荐）"
else
  echo "  ⚠️  gtid_mode = ${GTID:-OFF}（建议开启以支持主从切换）"
  echo "     修复: my.cnf 添加 gtid_mode = on, enforce_gtid_consistency = on"
fi

echo ""
echo "--- 3. 超时参数 ---"
TIMEOUT=$(mysql -u"$1" -p"$2" -h"${3:-localhost}" -P"${4:-3306}" -NBe "SHOW VARIABLES LIKE 'wait_timeout'" 2>/dev/null | awk '{print $2}')
echo "  wait_timeout = ${TIMEOUT}（大表快照建议 86400）"
if [ "$TIMEOUT" -lt 86400 ] 2>/dev/null; then
  echo "  ⚠️  wait_timeout 小于 86400，大表快照可能超时"
fi

echo ""
echo "--- 4. binlog 保留 ---"
EXPIRE=$(mysql -u"$1" -p"$2" -h"${3:-localhost}" -P"${4:-3306}" -NBe "SHOW VARIABLES LIKE 'binlog_expire_logs_seconds'" 2>/dev/null | awk '{print $2}')
echo "  binlog保留: ${EXPIRE:-未设置} 秒（建议 ≥ 86400，即 1 天）"

echo ""
echo "--- 5. server-id ---"
SID=$(mysql -u"$1" -p"$2" -h"${3:-localhost}" -P"${4:-3306}" -NBe "SHOW VARIABLES LIKE 'server_id'" 2>/dev/null | awk '{print $2}')
echo "  server_id = ${SID}"

echo ""
echo "--- 6. 连接数 ---"
MAXCONN=$(mysql -u"$1" -p"$2" -h"${3:-localhost}" -P"${4:-3306}" -NBe "SHOW VARIABLES LIKE 'max_connections'" 2>/dev/null | awk '{print $2}')
CURCONN=$(mysql -u"$1" -p"$2" -h"${3:-localhost}" -P"${4:-3306}" -NBe "SELECT COUNT(*) FROM information_schema.processlist" 2>/dev/null)
echo "  max_connections = ${MAXCONN}，当前连接数 ≈ ${CURCONN}"

echo ""
echo "========================================"
echo " 检查完成"
echo "========================================"
echo "说明：带有 ❌ 的项必须先修复再启动 CDC"
echo "带有 ⚠️ 的项建议按场景评估"
echo ""
echo "使用方式：$0 <user> <password> [host] [port]"
