---
name: cdc-pipeline-debug
description: CDC 数据管道症状排查手册。从现象反向定位根因。当用户描述以下任何现象时立即触发：数据量对不上、数据值不对、最终值回退、同步延迟、数据重复、精度丢失、乱码、时区差、任务启动失败、运行中断、恢复失败、OOM、加字段没同步、改类型写入失败、删表重建、扩容后异常、改密码报错、迁移集群后起不来、主从切换后异常。无论用户用何种 CDC 工具（Flink CDC / Canal / Debezium / DataX / Kafka Connect），也无论源库或目标库类型，只要现象匹配就介入排查。
version: 1.0.0
author: open-anolis
os_support:
  - 通用
tags:
  - 数据中间件
  - CDC
  - 故障排查
  - Flink CDC
  - Debezium
suggested_sig: middleware
contributor_type: personal
---

# CDC 管道故障排查手册 / CDC Pipeline Symptom Diagnosis

**用法：** 用户描述现象 → 在下面找到匹配的症状章节 → 逐一排查可能原因 → 直到定位根因。

每条症状包含：**现象描述 → 可能原因列表 → 对每个原因：反向定位 → 确认方法 → 修复 → 验证**

---

## 症状 1：数据量不一致——数仓记录数比业务库少

用户发现目标库（数仓/数据湖）中某张表的记录数少于源业务库，出现了"漏数据"。

### 可能原因

#### 原因 1.1：增量时间窗口的时区不对，边界数据被切到前/后一个分区

**反向定位：** 按小时对比源库和目标库的数据量曲线，看看是否是凌晨/边界整点时刻的数据缺失。

**确认方法：**

```sql
-- 源库：按小时统计最近 48 小时的数据量
SELECT DATE_FORMAT(create_time, '%Y-%m-%d %H:00:00') AS hour_slot,
       COUNT(*) AS cnt
FROM source_table
WHERE create_time >= NOW() - INTERVAL 48 HOUR
GROUP BY DATE_FORMAT(create_time, '%Y-%m-%d %H:00:00')
ORDER BY hour_slot;

-- 目标库同样的统计，对比差异
-- 如果每天固定某个小时（通常是凌晨 0-4 点）的数据量差异最大 → 时区问题
```

**修复：**
```
修改增量切分时间窗口，前后各加 1 小时缓冲：
  原逻辑：modified_time >= T-1 00:00 AND modified_time < T 00:00
  改后：  modified_time >= T-1 00:00 - 1h AND modified_time < T 00:00 + 1h
少量重复由下游去重层处理。
```

**验证：** 次日同一时段对比差值消失。

---

#### 原因 1.2：增量切分字段是业务写入的时间戳，本身有空洞或时区混乱

**反向定位：** 源表中存在 `modified_time` 为 NULL、为未来时间、或明显偏离业务时间范围的记录。

**确认方法：**

```sql
-- 检查增量字段是否可靠
SELECT 
  COUNT(*) AS total,
  SUM(CASE WHEN modified_time IS NULL THEN 1 ELSE 0 END) AS null_count,
  SUM(CASE WHEN modified_time > NOW() THEN 1 ELSE 0 END) AS future_count,
  MIN(modified_time) AS earliest,
  MAX(modified_time) AS latest
FROM source_table;
```

如果发现 NULL 或未来时间戳，说明增量字段本身不可靠。

**修复：**
```
改用数据库自动维护的修改时间戳（如果有），或者用 binlog 位点做增量切分。
如果是 DataX 类工具，改用主键自增 ID 范围切分。
```

**验证：** 使用新切分方式重新跑一次增量同步，对比差值。

---

#### 原因 1.3：源表存在物理 DELETE，增量同步只捕获了 INSERT/UPDATE

**反向定位：** 目标库中存在源库已经找不到的记录（数仓多出来的旧数据）。

**确认方法：**

```sql
-- 在主键维度上做差集：数仓有但业务库无 = 被物理删除的残留
SELECT COUNT(*) AS ghost_records
FROM (
  SELECT target.id FROM target_table target
  LEFT JOIN source_table source ON target.id = source.id
  WHERE source.id IS NULL
) t;
```

如果 `ghost_records` > 0，说明业务存在物理删除，增量同步未捕获 DELETE 事件。

**修复：**
```
方案一（推荐）：让业务改为逻辑删除（加 is_deleted 字段，DELETE → UPDATE SET is_deleted=1）
方案二（实时场景）：启用 CDC 的 binlog 监听，捕获 DELETE 事件写入删除日志表
方案三（兜底）：每日全量对比主键，标记被删除的记录
```

**验证：** 修复后再跑一次差集查询，`ghost_records` 不再增长。

---

#### 原因 1.4：快照阶段的分片键列被并发更新，导致部分行被遗漏

**反向定位：** 检查表的分片键（chunk key）是否为主键列。如果不是，检查该列在快照期间是否有被更新。

**确认方法：**

```sql
-- 检查表的 chunk key 配置
-- MySQL CDC：scan.incremental.snapshot.chunk.key-column
-- 如果未显式配置，默认是主键第一列

-- 检查分片列是否有 update 操作
SELECT COUNT(*) FROM source_table
WHERE chunk_key_column != COALESCE(
  (SELECT chunk_key_column FROM source_table_history WHERE ...), chunk_key_column
);
```

如果配了非主键列作为 chunk key，**这本身就可能造成数据遗漏或重复**。

**修复：**
```
将 scan.incremental.snapshot.chunk.key-column 改为主键第一列（默认值）。
如果表没有主键，尝试加一个自增 ID 作为主键。
```

**验证：** 重新做一次全量快照，对比主键集合是否一致。

---

## 症状 2：数据量不一致——数仓记录数比业务库多

用户发现目标库比源库多了记录，出现了"幽灵数据"。

### 可能原因

#### 原因 2.1：业务有物理 DELETE，但增量同步未捕获（同 1.3）

参见症状 1 的 原因 1.3。

---

#### 原因 2.2：增量时间窗口缓冲过大，产生重复记录

**反向定位：** 检查最近几天的增量同步是否存在边界重复——同一条记录的主键在目标库出现两次以上。

**确认方法：**

```sql
-- 检查目标库主键是否重复
SELECT id, COUNT(*) AS dup_count
FROM target_table
GROUP BY id
HAVING COUNT(*) > 1
ORDER BY dup_count DESC
LIMIT 10;
```

**修复：**
```
在 DWD 层或目标表上做幂等写入（UPSERT 语义），以最新时间戳为准。
如果是 DataX 类不支持 UPSERT 的工具，在写入前做一次去重。
```

**验证：** 再次检查主键重复数为 0。

---

#### 原因 2.3：快照阶段与增量阶段的数据重叠

**反向定位：** 任务刚启动时，快照阶段的数据和随后开始的增量阶段数据有重叠。

**确认方法：** 检查任务启动时间附近的数据是否存在整批重复——同一条记录在快照阶段和增量阶段各被消费了一次。

**修复：**
```
Flink CDC 增量快照算法本身保证不重叠（全量完成后等一个完整 checkpoint 才开启 binlog 消费）。
如果出现重叠，检查是否手动修改过 scan.startup.mode 或 debezium.snapshot.mode。
确保不要同时设置 scan.startup.mode 和 debezium.snapshot.mode，两者会冲突。
```

**验证：** 从 savepoint 重启任务后检查。

---

## 症状 3：数据值不对——最终值不是最新值

CDC 日志正常、计算引擎日志正常、目标库日志正常，但目标库中的最终值是旧值，仿佛变更"回退"了。

### 可能原因

#### 原因 3.1：目标库使用"后写入覆盖"语义，CDC 管道内发生乱序

**这是 CDC 管道最隐蔽的问题。** 同一条记录的多次变更在毫秒级到达，并行处理后写入顺序被打乱——后发生的变更先入库，先发生的变更后入库，旧值覆盖了新值。

**反向定位：** 任取一条问题记录，在源库查完整的变更时间线，在目标库查最终值，对比。

**确认方法：**

```sql
-- 源库：查该记录的所有变更（依赖 binlog 或操作日志）
-- 目标库：查该记录的最终值
-- 如果目标库的最终值 = 源库的某个中间值（不是最新值）= 乱序覆盖
```

**修复：**
```
为目标表启用 Sequence Column（顺序列），用业务时间戳或自增版本号作为排序依据。
Doris 示例：
  ALTER TABLE orders ENABLE FEATURE "SEQUENCE_LOAD"
  WITH PROPERTIES ("function_column.sequence_type" = "DATETIME");
Paimon：检查主键定义和 LSM-Tree 的合并顺序。
```

**确认 Sequence Column 已生效：**
```sql
SET show_hidden_columns = true;
DESC orders;
-- 输出应包含 __DORIS_SEQUENCE_COL__ 列
```

**验证：** 手动模拟乱序写入（先发新值后发旧值），检查目标库是否保留了时间戳最大的那条。

---

#### 原因 3.2：Sequence Column 粒度过粗，同秒内多次变更无法区分

**反向定位：** Sequence Column 用的是秒级时间戳 `DATETIME`，但业务上同一秒内可能发生多次变更。

**确认方法：**
```
检查目标表的 Sequence Column 定义：
  如果是 DATETIME 且精度为秒 → 可能存在同秒多次变更覆盖
  检查源库同一秒内对同一条记录是否有多次 update
```

**修复：**
```
将 Sequence Column 改为毫秒级时间戳或自增版本号。
Doris 示例：
  function_column.sequence_type = BIGINT  -- 用自增版本号
  function_column.sequence_col = update_ms  -- 用毫秒字段
```

**验证：** 同秒多变更场景下目标库的值正确。

---

## 症状 4：数据值不对——数值字段精度不对

小数点位错乱、末尾出现多余数字、大数变成负数。

### 可能原因

#### 原因 4.1：DECIMAL 精度 > 38，Flink 最大只支持 DECIMAL(38, s)

**反向定位：** 源表字段类型是 `DECIMAL(65, s)` 或精度超过 38。

**确认方法：**

```sql
-- 源库检查 decimal 字段的精度
SELECT COLUMN_NAME, NUMERIC_PRECISION, NUMERIC_SCALE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'your_table'
  AND DATA_TYPE IN ('decimal', 'numeric')
  AND NUMERIC_PRECISION > 38;
```

**修复：**
```
在 Flink DDL 中将该字段声明为 STRING 类型来接收完整精度，不做截断。
'connector' = 'mysql-cdc',
...
CREATE TABLE t (
  amount STRING,     -- 原来是 DECIMAL(65,2)
  ...
)
```

**验证：** 比对源库和目标库的数值字符串是否一致。

---

#### 原因 4.2：BIGINT UNSIGNED 映射为 BIGINT，超出正数范围

**反向定位：** 源表字段是 `BIGINT UNSIGNED`，目标字段是 `BIGINT`（有符号）。

**确认方法：**

```sql
-- 查找负数值的记录（本应为正数）
SELECT * FROM target_table WHERE id < 0;
```

**修复：**
```
在 Flink DDL 中将该字段映射为 DECIMAL(20, 0) 来容纳无符号范围。
```

**验证：** 修复后检查原来为负数的记录是否变为正数。

---

## 症状 5：数据值不对——时间字段差了几个小时

目标库的时间字段整体偏移了固定的小时数，或者只有边界时间出错。

### 可能原因

#### 原因 5.1：源库时区和目标库时区不一致，TIMESTAMP 转换错误

**反向定位：** 时间偏差是否是固定的整数小时（如差 8 小时、13 小时）。

**确认方法：**

```sql
-- 检查 MySQL 的时区设置
SHOW VARIABLES LIKE 'time_zone';
-- 可能输出：SYSTEM、+08:00、America/Chicago 等

-- 检查 Flink DDL 中的 server-time-zone 配置
-- 'server-time-zone' = 'Asia/Shanghai'
-- 如果不设置，默认用 ZoneId.systemDefault()
```

**修复：**
```
在 Flink DDL 中明确设置 server-time-zone：
  'server-time-zone' = 'Asia/Shanghai'
值应与数据库的时区一致，而非业务期望的时区。
```

**验证：** 修改后对比源库和目标库同一条记录的时间戳。

---

#### 原因 5.2：业务代码写入的时间与数据库时间戳混用

**反向定位：** 同一条记录中有的时间戳是业务代码写入的（如 `order_time`），有的是数据库自动生成的（如 `modified_time`），两者时区不同。

**确认方法：**
```sql
-- 对比两条记录的时间差是否等于时区差值
SELECT 
  order_time,
  modified_time,
  TIMESTAMPDIFF(HOUR, order_time, modified_time) AS hour_diff
FROM source_table
LIMIT 10;
```

如果 `hour_diff` 在一天内的不同时段数值不同，说明时区链有问题。

**修复：**
```
确保增量切分用数据库自动维护的 modified_time，不用业务写入的时间戳。
如必须用业务时间戳，在 Flink ETL 中显式转换时区：CONVERT_TZ(...)
```

**验证：** 同一条记录的多个时间戳之间的逻辑关系正确。

---

## 症状 6：任务启动就报错

任务提交后立即失败，无法启动。

### 可能原因

#### 原因 6.1：binlog / WAL / 归档日志未开启

**反向定位：** 数据库层面未开启变更捕获所需的日志。

**确认方法：**

| 数据库 | 检查命令 | 期望值 |
|--------|----------|--------|
| MySQL | `SHOW VARIABLES LIKE 'log_bin'` | `ON` |
| MySQL | `SHOW VARIABLES LIKE 'binlog_format'` | `ROW` |
| Oracle | `archive log list` | `Archive Mode` |
| PostgreSQL | `SHOW wal_level;` | `logical` |
| MongoDB | `db.version()` + 检查副本集 | ≥ 3.6 + 副本集/分片集群 |

**修复：** 参考各数据库官方文档开启相应日志模式。

**验证：** 再次提交任务，启动成功。

---

#### 原因 6.2：CDC 账号权限不足

**反向定位：** 报错信息中包含 `Access denied`、`Permission denied`、`REPLICATION` 等关键词。

**确认方法：**

```sql
-- MySQL
SHOW GRANTS FOR 'cdc_user'@'%';
-- 必须包含：SELECT, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT

-- PostgreSQL：检查角色
\du
```

**修复：** 授予 CDC 所需的最小权限。

**验证：** 用 CDC 账号手动尝试连接和读取变更日志。

---

#### 原因 6.3：server-id 与已有客户端冲突（MySQL / OceanBase）

**反向定位：** 同一 MySQL 实例上已有其他 CDC 任务在运行，server-id 重复。报错可能包含 `slave id`、`conflict` 等。

**确认方法：**

```sql
SHOW PROCESSLIST;
-- 查看每个连接的 server-id
```

**修复：**
```
在 Flink DDL 中手动指定 server-id 范围：
  'server-id' = '5401-54xx'   -- xx ≥ 并行度
确保范围与同一 MySQL 实例上所有其他 CDC 任务不重叠。
```

**验证：** 任务启动成功，不再报 server-id 冲突。

---

#### 原因 6.4：连接器 JAR 版本与 Flink 版本不匹配

**反向定位：** 报错包含 `ClassNotFoundException`、`NoSuchMethodError`、`Incompatible` 等。

**确认方法：**
```
检查 Flink CDC JAR 版本和 Flink 版本的映射关系（参考技能前置条件章节的版本表）。
例如：flink-sql-connector-mysql-cdc 3.0.x 需要 Flink 1.14+。
```

**修复：** 下载与当前 Flink 版本兼容的 CDC JAR。

**验证：** 启动成功。

---

#### 原因 6.5：Oracle LogMiner 未配置归档或补充日志

**反向定位：** 错误信息涉及 `LOGMINER`、`archivelog`、`supplemental log`。

**确认方法：**
```sql
-- 检查归档模式
archive log list;

-- 检查补充日志
SELECT supplemental_log_data_min FROM v$database;
```

**修复：**
```sql
ALTER DATABASE ARCHIVELOG;
ALTER DATABASE ADD SUPPLEMENTAL LOG DATA;
-- CDB 模式还需：
'debezium.database.pdb.name' = 'your_pdb_name'
```

**验证：** 启动成功。

---

#### 原因 6.6：PostgreSQL 复制槽被占用

**反向定位：** 报错 `replication slot "flink" is active for PID xxxx`。

**确认方法：**
```sql
SELECT slot_name, active, pid FROM pg_replication_slots;
-- 如果 slot_name 已存在且 active = true，说明被其他进程占用
```

**修复：**
```
方案一：用不同的 slot.name 启动新任务
方案二：等旧进程释放（KILL 旧 PID），再用原 slot name
方案三：DROP 旧槽重建
```

**验证：** 启动成功。

---

## 症状 7：任务运行中突然断开，周期性断开

任务能启动，但运行一段时间后自动断开连接，然后重试。

### 可能原因

#### 原因 7.1：MySQL 连接超时参数过短

**反向定位：** 断开发生在前一个事件处理后一段时间（接近 `wait_timeout` 值），然后自动重连。

**确认方法：**
```sql
SHOW VARIABLES LIKE 'wait_timeout';
SHOW VARIABLES LIKE 'interactive_timeout';
-- 默认通常是 28800（8 小时），如果被改为较小值（如 300）则容易超时
```

**修复：**
```ini
# my.cnf
wait_timeout = 86400
interactive_timeout = 86400
```

**验证：** 修改后观察是否仍出现周期性断开。

---

#### 原因 7.2：网络层不稳定（跨机房、SSL、防火墙）

**反向定位：** 断开的间隔不固定，伴随网络延迟抖动。

**确认方法：**
```
检查源库和目标库的网络延迟和丢包率。
如果启用 SSL，检查 SSL 证书是否有效、是否到期。
```

**修复：**
```
在 JDBC 连接中配置 keepalive 和重试参数：
  'connect.timeout' = '30s',
  'connect.max-retries' = '3'
如果跨机房，考虑缩短心跳间隔以保持连接活跃。
```

**验证：** 观察一段时间内是否不再断开。

---

#### 原因 7.3：MySQL 心跳间隔太长，慢变更表导致 binlog 位置被清理

**反向定位：** 表长时间没有写入，断开恢复后报 binlog 位置不存在。

**确认方法：**
```sql
-- 检查 binlog 保留时长
SHOW VARIABLES LIKE 'binlog_expire_logs_seconds';
-- 默认 2592000（30 天），但若磁盘紧张可能被缩短
```

**修复：**
```
确保心跳开启（默认 30s）：
  'heartbeat.interval' = '30s'
不要禁用：'heartbeat.interval' = '0s' ❌
```

**验证：** 慢变更表场景下长时间运行不再断开。

---

## 症状 8：从 checkpoint/savepoint 恢复失败

任务停止后重新从 checkpoint 恢复，报错或数据对不上。

### 可能原因

#### 原因 8.1：binlog / WAL / 归档日志已被清理，恢复位点不存在

**反向定位：** 恢复报错中包含 `binlog` 文件找不到、`GTID` 不存在、`SCN` 过期、`resumeToken` 不存在。

**确认方法：**
```sql
-- MySQL：检查 checkpoint 记录的 binlog 文件是否还存在
SHOW BINARY LOGS;
-- 把 checkpoint 记录的 binlog 文件名与此列表对比

-- 如果 binlog 已被清理 → 需要重新 initial 全量快照
```

**修复：**
```
方案一（快速恢复，可能丢数据）：用 latest-offset 模式从当前最新位点开始
方案二（完整恢复但慢）：用 initial 模式重新做全量+增量
方案三（精确恢复）：如果能定位到可用的历史位点，用 specific-offset 指定
```

**验证：** 恢复后数据一致性检查。

---

#### 原因 8.2：MongoDB resumeToken 过期

**反向定位：** 恢复报错包含 `resumeToken`、`token`、`expired`。

**确认方法：** 检查集合的变更频率和心跳配置。

**修复：**
```
设置较短的 heartbeat 间隔，持续推送 resumeToken：
  'heartbeat.interval.ms' = '300000'  （建议 5 分钟）
慢变更集合强烈建议设置。
```

**验证：** 从 savepoint 恢复成功。

---

#### 原因 8.3：PostgreSQL 复制槽的 LSN 已经被提交

**反向定位：** 恢复后部分数据缺失。

**确认方法：**
```
检查 scan.lsn-commit.checkpoints-num-delay 配置（默认 3）。
如果有多次 checkpoint 提交，最初的 LSN 可能已被提交，旧位点不可恢复。
```

**修复：**
```
增大 checkpoints-num-delay 值，延迟 LSN 提交时间：
  'scan.lsn-commit.checkpoints-num-delay' = '10'
```

**验证：** 从较早的 checkpoint 恢复成功。

---

## 症状 9：同步延迟越来越高（反压）

CDC 管道刚开始正常，运行一段时间后延迟持续增大，差距无法追上。

### 可能原因

#### 原因 9.1：目标库写入能力跟不上源库变更速度

**反向定位：** 源库变更速率恒定，但目标库的写入时延持续增长。Flink WebUI 中出现 `backpressure`。

**确认方法：**
```
Flink WebUI → 查看算子背压状态。
如果 Source 端正常但 Sink 端出现 HIGH backpressure → 目标库写入慢。
```

**修复：**
```
方案一：增加 Flink 并行度
  SET 'parallelism.default' = '8';
  注意需同步扩大 server-id 范围
  
方案二：增大 checkpoint 间隔，减少写入冲突
  SET 'execution.checkpointing.interval' = '30s';
  
方案三：检查目标库的写入瓶颈（磁盘 IO、compaction、导入队列）
```

**验证：** 延迟曲线下降并稳定。

---

#### 原因 9.2：debezium.max.queue.size 太小，事件排队

**反向定位：** Source 端正常，但事件处理端有明显的排队积压。

**确认方法：**
```
检查 Debezium 队列相关配置：
  debezium.max.queue.size（默认 8192）
  debezium.max.batch.size（默认 2048）
如果源库批量更新频繁，队列可能溢出。
```

**修复：**
```yaml
'debezium.max.queue.size' = '16384'
'debezium.max.batch.size' = '4096'
```

**验证：** 延迟下降。

---

#### 原因 9.3：大表快照阶段尚未完成，增量 binlog 排队

**反向定位：** 任务刚刚启动，正处于快照读阶段。

**确认方法：**
```
查看 Flink Metrics：isSnapshotting = true → 还在快照阶段
快照阶段不消费 binlog，binlog 在位点上堆积，快照完成后会追上。
```

**修复：**
```
这是正常行为。如果快照太慢影响业务，调整 chunk.size：
  'scan.incremental.snapshot.chunk.size' = '65536'   （默认 8096，大表调大）
```

**验证：** 快照完成后延迟自动追上。

---

## 症状 10：OOM / 内存溢出

任务运行中或快照阶段报 `OutOfMemoryError`。

### 可能原因

#### 原因 10.1：大表快照时最大分片过大，内存不足

**反向定位：** OOM 发生在快照阶段的最后一个分片（unbounded chunk）。

**确认方法：**
```
检查 Flink TaskManager 的内存配置和表的最大分片大小。
```

**修复：**
```yaml
# 开启先分配 unbounded chunk，降低 OOM 风险（默认 true）
'scan.incremental.snapshot.unbounded-chunk-first.enabled' = 'true'

# 增大 TaskManager 内存
taskmanager.memory.process.size: 4g
```

**验证：** 快照阶段不再 OOM。

---

#### 原因 10.2：debezium.max.queue.size 过大，事件队列占满内存

**反向定位：** 高吞吐场景下队列积压。

**修复：**
```yaml
'debezium.max.queue.size' = '8192'    # 默认值，不需要调太大
'debezium.buffer.max.demand' = '512'  # 控制缓冲区上限
```

**验证：** 内存使用稳定。

---

#### 原因 10.3：Oracle LogMiner 使用 online_catalog 策略内存消耗大

**修复：**
```yaml
# 改用 redo_log_catalog 策略，减少内存但更慢
'debezium.log.mining.strategy' = 'redo_log_catalog'
```

**验证：** OOM 消失。

---

## 症状 11：源表结构变更后同步异常

### 11a：源表加了字段，目标库没有

**反向定位：** 确认 Schema Evolution 是否开启。

**确认方法：**
```sql
-- 检查 Flink DDL 中有无配置 schema change
-- MySQL CDC: 默认不开启
-- CDC YAML: schema-change.enabled 默认 false
```

**修复：**
```
MySQL CDC SQL 模式不支持自动 DDL 同步。
方案一：手动修改 Flink DDL，从 savepoint 重启
方案二：CDC YAML 模式开启 schema-change.enabled
```

---

### 11b：源表改了字段类型，写入失败

**反向定位：** 报错包含 `Type mismatch`、`cannot be cast`、`truncation`。

**确认方法：**
```
对比源表新的字段类型和 Flink DDL 中的字段类型是否兼容。
```

**修复：**
```
方案一：类型缩窄（如 INT→TINYINT）→ 改 Flink DDL 后重启
方案二：字符串长度变短（VARCHAR(200)→VARCHAR(100)）→ 可能丢数据，需评估
方案三：字段改名 → 映射为先删后加，数据丢失，避免自动同步
```

---

### 11c：MySQL gh-ost / pt-osc 在线 DDL 导致同步异常

**反向定位：** 表结构变更后有大量重复或丢失数据。

**修复：**
```yaml
# 开启在线 DDL 解析（实验性）
'scan.parse.online.schema.changes.enabled' = 'true'
```

---

## 症状 12：数据库主从切换后 CDC 异常

源库发生了主从切换（failover）后，CDC 任务数据对不上了。

### 可能原因

#### 原因 12.1：MySQL GTID 未开启，从库的 binlog 位点与主库不一致

**反向定位：** 主从切换后 CDC 仍然追踪旧的 binlog 文件名+位置，但新主库上这些信息不存在。

**确认方法：**
```sql
SHOW VARIABLES LIKE 'gtid_mode';
-- 如果是 OFF → 主从切换后位点必然失效
```

**修复：**
```ini
# MySQL 集群必须开启 GTID
gtid_mode = on
enforce_gtid_consistency = on
# 如果从 CDC 读从库，还需
log-slave-updates = 1
```

**验证：** 主从切换后 CDC 自动从新主库的正确位点继续消费。

---

#### 原因 12.2：PostgreSQL 主从切换后复制槽丢失

**反向定位：** 切换后报复制槽找不到。

**修复：**
```
PostgreSQL CDC 在流复制切换后，逻辑复制槽只存在于原主库。
需要在新主库上创建新复制槽，重新开始 CDC。
建议为 PostgreSQL 配置连接池或 VIP，让 CDC 自动重连到新主。
```

---

## 附录：快速诊断命令集

以下命令可用于排查大多数场景：

```sql
-- MySQL 全局检查
SHOW VARIABLES LIKE 'log_bin';
SHOW VARIABLES LIKE 'binlog_format';
SHOW VARIABLES LIKE 'gtid_mode';
SHOW VARIABLES LIKE 'binlog_expire_logs_seconds';
SHOW VARIABLES LIKE 'server_id';
SHOW VARIABLES LIKE 'wait_timeout';
SHOW BINARY LOGS;

-- PostgreSQL 复制槽检查
SELECT slot_name, slot_type, database, wal_status, active 
FROM pg_replication_slots;

-- Oracle 归档检查
archive log list;
SELECT supplemental_log_data_min FROM v$database;

-- Flink 指标检查（WebUI / REST API）
-- GET /jobs/:jobid/metrics?get=0.isSnapshotting,0.numSnapshotSplitsRemaining
```

## 附录：修复验证清单

修复完成后，按以下顺序验证：

1. **任务状态**：Flink WebUI → Job 状态 → RUNNING
2. **Checkpoint**：最近几个 checkpoint 全部成功
3. **数据量**：源库和目标库主键差 < 0.1%
4. **数据值**：任取 10 条记录逐字段对比
5. **延迟**：端到端延迟在业务容忍范围内
6. **稳定性**：观察 30 分钟无异常断开

---

## 参考文档

- Apache Flink CDC 官方文档：https://nightlies.apache.org/flink/flink-cdc-docs-stable/
- Debezium 文档：https://debezium.io/documentation/reference/stable/
- Doris Sequence Column：https://doris.apache.org/docs/dev/data-operate/update/unique-update-concurrent-control/
