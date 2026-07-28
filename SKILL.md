---
name: cdc-pipeline-debug
description: 基于 Apache Flink CDC 官方文档构建的 CDC 管道症状排查手册。从现象反向定位根因。当用户描述以下任何现象时触发：数据量对不上、数据值回退、精度丢失、时区偏差、任务启动失败、运行中断、恢复失败、OOM、延迟堆积、加字段未同步、改类型写入失败、主从切换异常。不依赖用户具体架构，纯症状驱动。
version: 1.0.0
author: open-anolis
os_support:
  - 通用
tags:
  - 数据中间件
  - CDC
  - Flink CDC
  - 故障排查
  - Debezium
suggested_sig: middleware
contributor_type: personal
---

# CDC 管道故障排查手册

> **基于 Apache Flink CDC 官方文档构建**
> 官方文档：https://nightlies.apache.org/flink/flink-cdc-docs-master/zh/docs/connectors/flink-sources/overview/
>
> 排查引用格式：每条诊断末尾的 `【官网 §X.Y】` 指向官方文档的对应章节，方便你直接查阅原文。

**用法：** 用户描述现象 → 按症状索引定位 → 逐条排查可能原因 → 每个原因包含反向定位 → 确认方法 → 修复 → 验证。

---

## 症状 1：数据量不一致——数仓记录数比业务库少

### 可能原因

#### 原因 1.1：增量切分字段的时区不统一，边界数据被切到前/后一个分区

**反向定位：** 按小时对比两边数据量，发现每天固定某个小时（通常是凌晨整点）开始缺失。

**确认方法：**
```sql
-- 源库和目标库按小时统计，对比差异
SELECT DATE_FORMAT(modified_time, '%Y-%m-%d %H:00:00') AS hour, COUNT(*) AS cnt
FROM source_table WHERE modified_time >= NOW() - INTERVAL 48 HOUR
GROUP BY hour ORDER BY hour;
```

**修复：**
```
增量时间窗口扩宽，前后各加缓冲：
  原：modified_time >= T-1 00:00:00 AND modified_time < T 00:00:00
  改：modified_time >= T-1 00:00:00 - 1h AND modified_time < T 00:00:00 + 1h
少量重复数据由下游 DWD 层去重处理。
【官网 MySQL CDC §连接器选项 → server-time-zone】
```

**验证：** 次日对账差值消失。

---

#### 原因 1.2：增量切分字段不可靠（业务写入的时间戳含 NULL / 未来时间）

**反向定位：** 切分字段来自业务代码写入而非数据库自动维护，可能存在空洞。

**确认方法：**
```sql
SELECT COUNT(*) AS total,
  SUM(CASE WHEN modified_time IS NULL THEN 1 ELSE 0 END) AS nulls,
  SUM(CASE WHEN modified_time > NOW() THEN 1 ELSE 0 END) AS future
FROM source_table;
```
如果存在 NULL 或未来时间戳，说明切分字段不可靠。

**修复：** 改用 binlog 位点或自增 ID 范围做增量切分。Flink CDC 本身不受此问题影响（基于 binlog），此问题主要影响 DataX 等离线工具。

---

#### 原因 1.3：业务存在物理 DELETE，增量同步未捕获

**反向定位：** 目标库存在源库已查不到的记录（"幽灵数据"）。

**确认方法：**
```sql
SELECT COUNT(*) FROM target_table t
LEFT JOIN source_table s ON t.id = s.id
WHERE s.id IS NULL;
```

**修复：**
```
方案一（推荐）：业务改逻辑删除，加 is_deleted 字段
方案二（Flink CDC）：利用 row_kind 元数据字段捕获 DELETE 事件
  CREATE TABLE t (..., operation STRING METADATA FROM 'row_kind' VIRTUAL)
  【官网 MySQL CDC §支持的元数据】
方案三：开启 scan.read-changelog-as-append-only 将所有变更转 INSERT，
       下游通过 row_kind 做逻辑删除 【官网 MySQL CDC §连接器选项】
```

**验证：** 差集查询结果不再增长。

---

#### 原因 1.4：增量快照的分片键不是主键，快照期间并发更新导致行被遗漏或重复

**反向定位：** 检查 `scan.incremental.snapshot.chunk.key-column` 配置。如果使用了非主键列，且该列在快照期间可能被更新，则可能造成数据不一致。

**官方警告：**
> 使用非主键列作为分片键可能会导致数据不一致。【官网 MySQL CDC §连接器选项 → scan.incremental.snapshot.chunk.key-column】

**确认方法：**
```sql
-- 检查当前表的 chunk key 配置（从 Flink DDL 中查看）
-- 如果没有显式配置，默认是主键第一列
SHOW CREATE TABLE target_table;
```

**修复：** 将 chunk key 改为主键第一列（默认值）。无主键表需要：要么加主键，要么关闭增量快照回退到旧模式。`【官网 MySQL CDC §增量快照读取】`

---

## 症状 2：数据值不对——最终值不是最新值

CDC 日志 ✅ → 计算引擎日志 ✅ → 目标库日志 ✅，但目标库存的是旧值。

### 可能原因

#### 原因 2.1：目标库使用"后写入覆盖"语义，CDC 管道内发生乱序

**这是 CDC 管道中最隐蔽的问题。** 同一条记录在毫秒级内发生多次变更，经并行处理后写入顺序被打乱——后发生的变更先入库，先发生的变更后入库，旧值覆盖了新值。Doris Unique Key 模型在没有 Sequence Column 时依赖写入版本号排序，无法感知业务时间顺序。

**确认方法：**
```sql
-- 在源库查该记录的所有变更时间线
SELECT order_id, status, update_time FROM source_db WHERE order_id = 'X';
-- 在目标库查该记录的最终值
SELECT order_id, status, update_time FROM target_table WHERE order_id = 'X';
-- 如果最终值的 update_time 不是最大的那条，说明乱序
```

**修复：**
```
为目标表启用 Sequence Column：
  Doris: ALTER TABLE orders ENABLE FEATURE "SEQUENCE_LOAD"
         WITH PROPERTIES ("function_column.sequence_type" = "DATETIME");
  【Doris 官方文档 §Unique Key 并发更新控制】
```

**验证 Sequence Column 已启用：**
```sql
SET show_hidden_columns = true;
DESC orders;  -- 应包含 __DORIS_SEQUENCE_COL__
```

---

#### 原因 2.2：Sequence Column 使用秒级时间戳，同秒多次变更区分不了

**反向定位：** Sequence Column 定义为 `DATETIME`（秒级精度），但业务上同一秒内可能对同一条记录发生多次状态变更。

**修复：** 改用毫秒级时间戳或自增版本号作为排序列。

---

## 症状 3：数据值不对——数值字段精度丢失

### 可能原因

#### 原因 3.1：DECIMAL 精度超过 38，Flink 最大只支持 DECIMAL(38, s)

MySQL 支持 DECIMAL 精度高达 65，但 Flink 的 DECIMAL 类型精度上限为 38。超过 38 的字段如果映射为 DECIMAL 会被截断。

**确认方法：**
```sql
SELECT COLUMN_NAME, NUMERIC_PRECISION FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 't' AND DATA_TYPE = 'decimal' AND NUMERIC_PRECISION > 38;
```

**修复：** 在 Flink DDL 中将精度 > 38 的字段声明为 STRING，避免精度损失。`【官网 MySQL CDC §数据类型映射】`

---

#### 原因 3.2：BIGINT UNSIGNED 映射为 BIGINT，超出范围

MySQL 的 `BIGINT UNSIGNED` 范围是 0~18446744073709551615，Flink 的 `BIGINT` 是有符号的 -2^63~2^63-1，超出正数范围的数值会变成负数。

**修复：** 映射为 `DECIMAL(20, 0)` 来容纳无符号范围。

---

## 症状 4：数据值不对——时间字段整体偏差固定小时数

### 可能原因

#### 原因 4.1：未设置 server-time-zone，TIMESTAMP 转换默认用了 JVM 时区

MySQL 的 TIMESTAMP 类型在存储时为 UTC，读取时按 `server-time-zone` 转换为当前会话时区。如果未设置，Flink 使用 `ZoneId.systemDefault()`，可能与数据库实际时区不一致。

**确认方法：**
```sql
-- 检查 MySQL 时区
SHOW VARIABLES LIKE 'time_zone';
```

**修复：**
```
在 Flink DDL 中显式设置 server-time-zone：
  'server-time-zone' = 'Asia/Shanghai'
该值应等于数据库服务器的时区。
【官网 MySQL CDC §连接器选项 → server-time-zone】
```

---

## 症状 5：任务启动就报错

### 可能原因

#### 原因 5.1：binlog 未开启或格式不是 ROW（MySQL）

**确认方法：**
```sql
SHOW VARIABLES LIKE 'log_bin';        -- 必须 ON
SHOW VARIABLES LIKE 'binlog_format';   -- 必须 ROW
```

**恢复：** 开启 binlog（需要重启 MySQL）：
```ini
[mysqld]
log_bin = mysql-bin
binlog_format = ROW
server_id = 1
```
`【官网 MySQL CDC §配置 MySQL 服务器】`

---

#### 原因 5.2：CDC 用户权限不足（MySQL）

**确认方法：**
```sql
SHOW GRANTS FOR 'cdc_user'@'%';
```

**恢复：**
```sql
GRANT SELECT, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'cdc_user'@'%';
```
注意：增量快照开启（默认）时不需要 RELOAD 权限。`【官网 MySQL CDC §配置 MySQL 服务器】`

---

#### 原因 5.3：server-id 与已有客户端冲突

每个读 binlog 的 MySQL 客户端必须有唯一 server-id。如果同一个 MySQL 实例上有多个 CDC 任务，必须确保它们的 server-id 不重复。

**确认方法：**
```sql
SHOW PROCESSLIST;   -- 查看各连接的 server-id
```

**修复：**
```
在 Flink DDL 中分配一个不被其他任务占用的范围：
  'server-id' = '5401-5404'     -- 并行度 4
范围大小必须 ≥ 并行度。
【官网 MySQL CDC §注意事项 → 为每个 Reader 设置不同的 Server id】
```

---

#### 原因 5.4：PostgreSQL 复制槽已被占用

**确认方法：**
```sql
SELECT slot_name, active, pid FROM pg_replication_slots;
```

**修复：**
```
用不同的 slot.name。slot 名只能包含小写字母、数字和下划线。
【官网 PostgreSQL CDC §连接器选项 → slot.name】
```

---

#### 原因 5.5：Oracle 未开启归档模式或补充日志

**确认方法：**
```sql
archive log list;                        -- 应显示 Archive Mode
SELECT supplemental_log_data_min FROM v$database;  -- 应返回 YES
```

**修复：**
```sql
-- 开启归档（需重启）
SHUTDOWN IMMEDIATE; STARTUP MOUNT;
ALTER DATABASE ARCHIVELOG; ALTER DATABASE OPEN;
-- 开启补充日志
ALTER DATABASE ADD SUPPLEMENTAL LOG DATA;
```
`【官网 Oracle CDC §Setup Oracle】`

---

#### 原因 5.6：PostgreSQL wal_level 不是 logical

**确认方法：**
```sql
SHOW wal_level;   -- 必须为 logical
```

**修复：**
```ini
# postgresql.conf
wal_level = logical
max_replication_slots = 10
max_wal_senders = 10
```
`【官网 PostgreSQL CDC §设置 PostgreSQL】`

---

## 症状 6：任务运行中周期性断开

### 可能原因

#### 原因 6.1：MySQL wait_timeout / interactive_timeout 过短

大表快照阶段耗时长，连接可能因空闲超时被 MySQL 关闭。

**确认方法：**
```sql
SHOW VARIABLES LIKE 'wait_timeout';
SHOW VARIABLES LIKE 'interactive_timeout';
```

**修复：** 在 MySQL 侧加大超时时间：
```ini
wait_timeout = 86400
interactive_timeout = 86400
```
`【官网 MySQL CDC §注意事项 → 设置 MySQL 会话超时时间】`

---

#### 原因 6.2：心跳未配置，慢变更表的 binlog 位置被清理

不常更新的表，binlog 的提交位置长时间不推进，binlog 文件被清理后 CDC 任务断开后无法恢复。

**确认方法：**
```
查看 heartbeat.interval 配置（默认 30s）。如果被设为 0s 则禁用了心跳。
```

**修复：**
```yaml
'heartbeat.interval' = '30s'   # 默认值，不要禁用
```
`【官网 MySQL CDC §连接器选项 → heartbeat.interval】`

---

## 症状 7：从 checkpoint/savepoint 恢复失败

### 可能原因

#### 原因 7.1：binlog 已被清理，恢复位点不存在

**确认方法：**
```
查看 checkpoint 记录的 binlog 文件名，与 SHOW BINARY LOGS 的结果对比。
如果该文件已被清理，需要重新 initial 或换用 latest-offset 模式。
```

**修复：**
```
方案一：从最新位点开始（可能丢数据）
  'scan.startup.mode' = 'latest-offset'
方案二：重新 initial（全量+增量，完整但慢）
  'scan.startup.mode' = 'initial'
方案三：如果能定位到可用位点
  'scan.startup.mode' = 'specific-offset'
  'scan.startup.specific-offset.file' = 'mysql-bin.000123'
  'scan.startup.specific-offset.pos' = '456789'
  【官网 MySQL CDC §连接器选项 → scan.startup.mode】
```

---

#### 原因 7.2：MongoDB resumeToken 过期

**确认方法：** 恢复报错包含 `resumeToken` 或 `expired`。

**修复：**
```yaml
'heartbeat.interval.ms' = '300000'   # 建议 5 分钟
# 慢变更集合必须设置心跳以持续推送 resumeToken
【官网 MongoDB CDC §连接器选项 → heartbeat.interval.ms】
```

---

## 症状 8：大表快照任务 OOM

### 可能原因

#### 原因 8.1：最后一个 unbounded chunk 过大，TaskManager 内存不足

增量快照算法的最后一个 chunk（unbounded chunk）覆盖剩余所有数据，如果未优先分配，可能全部集中在一个 TaskManager 上。

**修复：**
```yaml
# 开启先分配 unbounded chunk（默认 true）
'scan.incremental.snapshot.unbounded-chunk-first.enabled' = 'true'
```
`【官网 MySQL CDC §连接器选项 → scan.incremental.snapshot.unbounded-chunk-first.enabled】`

如果已开启仍 OOM，增大 TaskManager 内存：
```
taskmanager.memory.process.size: 4g
```

---

#### 原因 8.2：Oracle LogMiner online_catalog 策略内存消耗大

**修复：**
```yaml
'debezium.log.mining.strategy' = 'redo_log_catalog'
```
`【官网 Oracle CDC §连接器选项 → debezium.*】`

---

## 症状 9：同步延迟持续增大（反压）

### 可能原因

#### 原因 9.1：目标库写入能力跟不上源库变更速度

**确认方法：** Flink WebUI → 算子背压状态。Source 正常但 Sink 出现 HIGH backpressure。

**修复：**
```yaml
# 增大并行度
SET 'parallelism.default' = '8';
# 注意同步扩大 server-id 范围

# 增大 checkpoint 间隔，减少写入冲突
SET 'execution.checkpointing.interval' = '30s';
```

---

#### 原因 9.2：快照阶段 binlog 排队是正常行为

**反向定位：** Flink Metrics 显示 `isSnapshotting = true`，说明仍处于全量快照阶段。快照阶段不消费 binlog，binlog 在 MySQL 上堆积是正常的，快照完成后会追上。

**修复：** 无需处理。如果快照太慢影响业务，调大 chunk.size：
```yaml
'scan.incremental.snapshot.chunk.size' = '65536'   # 默认 8096
```

---

## 症状 10：源表结构变更后同步异常

### 10a：源表加了字段，目标库没有

Flink CDC SQL 模式下**不支持自动 DDL 变更同步**。需要手动修改 DDL 后从 savepoint 重启。

CDC YAML 模式下支持 Schema Evolution，但默认关闭：
```yaml
source:
  schema-change.enabled: true
```
注意：字段改名映射为先删后加，数据会丢失。类型缩窄会导致写入失败。`【官网 Flink CDC YAML §整库同步】`

---

### 10b：gh-ost / pt-osc 在线 DDL 导致异常

**修复：**
```yaml
'scan.parse.online.schema.changes.enabled' = 'true'   # 实验性功能
```
`【官网 MySQL CDC §连接器选项 → scan.parse.online.schema.changes.enabled】`

---

## 症状 11：主从切换后 CDC 异常

### 可能原因

#### 原因 11.1：MySQL GTID 未开启

MySQL CDC 通过 GTID 提供高可用支持。集群必须配置 GTID 模式。如果 CDC 监控的是从实例，还需要 `log-slave-updates = 1`。

**确认方法：**
```sql
SHOW VARIABLES LIKE 'gtid_mode';
-- 应为 ON
```

**修复：**
```ini
gtid_mode = on
enforce_gtid_consistency = on
log-slave-updates = 1    # 监控从实例时需要
```
`【官网 MySQL CDC §MySQL 高可用性支持】`

MySQL 集群故障时，只需将 hostname 切换到其他可用服务器，从 checkpoint/savepoint 恢复。建议配置 DNS 或 VIP 实现自动切换。

---

## 附录 A：诊断命令速查

```sql
-- MySQL 全局
SHOW VARIABLES LIKE 'log_bin';                 -- binlog 是否开启
SHOW VARIABLES LIKE 'binlog_format';            -- 必须 ROW
SHOW VARIABLES LIKE 'gtid_mode';                -- 高可用必需
SHOW BINARY LOGS;                               -- 可用 binlog 文件列表
SHOW VARIABLES LIKE 'binlog_expire_logs_seconds'; -- 保留时长
SHOW VARIABLES LIKE 'wait_timeout';             -- 连接超时
SHOW VARIABLES LIKE 'server_id';                -- 本机 server-id
SHOW PROCESSLIST;                               -- 查看所有连接的 server-id

-- PostgreSQL
SELECT slot_name, active, wal_status FROM pg_replication_slots;
SHOW wal_level;

-- Oracle
archive log list;
SELECT supplemental_log_data_min FROM v$database;

-- MongoDB
db.runCommand({ hello: 1 });                    -- 检查副本集状态
```

## 附录 B：Flink CDC 内置监控指标

当作业运行时，可以通过 Flink WebUI 或 REST API 获取以下指标：

| 指标名 | 含义 | 说明 |
|--------|------|------|
| `isSnapshotting` | 是否在快照阶段 | true=全量，false=增量 |
| `isStreamReading` | 是否在增量阶段 | true=已进入增量阶段 |
| `numSnapshotSplitsRemaining` | 剩余待处理的分片数 | 为 0 表示快照完成 |
| `numSnapshotSplitsFinished` | 已完成的分片数 | 持续增长说明正常 |
| `snapshotStartTime` | 快照开始时间 | |
| `snapshotEndTime` | 快照结束时间 | |

指标名格式：`{database}.{table}.{metric_name}`。`【官方指标说明参见连接器各章节】`

## 附录 C：官方文档索引

| 连接器 | 官方文档 |
|--------|----------|
| MySQL CDC | https://nightlies.apache.org/flink/flink-cdc-docs-master/zh/docs/connectors/flink-sources/mysql-cdc/ |
| Oracle CDC | https://nightlies.apache.org/flink/flink-cdc-docs-master/zh/docs/connectors/flink-sources/oracle-cdc/ |
| PostgreSQL CDC | https://nightlies.apache.org/flink/flink-cdc-docs-master/zh/docs/connectors/flink-sources/postgres-cdc/ |
| MongoDB CDC | https://nightlies.apache.org/flink/flink-cdc-docs-master/zh/docs/connectors/flink-sources/mongodb-cdc/ |
| SQL Server CDC | https://nightlies.apache.org/flink/flink-cdc-docs-master/zh/docs/connectors/flink-sources/sqlserver-cdc/ |
| TiDB CDC | https://nightlies.apache.org/flink/flink-cdc-docs-master/zh/docs/connectors/flink-sources/tidb-cdc/ |
| OceanBase CDC | https://nightlies.apache.org/flink/flink-cdc-docs-master/zh/docs/connectors/flink-sources/oceanbase-cdc/ |
| Db2 CDC | https://nightlies.apache.org/flink/flink-cdc-docs-master/zh/docs/connectors/flink-sources/db2-cdc/ |
| Vitess CDC | https://nightlies.apache.org/flink/flink-cdc-docs-master/zh/docs/connectors/flink-sources/vitess-cdc/ |
| 全部连接器总览 | https://nightlies.apache.org/flink/flink-cdc-docs-master/zh/docs/connectors/flink-sources/overview/ |
