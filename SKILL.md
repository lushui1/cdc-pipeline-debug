---
name: cdc-pipeline-debug
description: 诊断与修复 CDC 增量同步/实时数据管道中的各类数据一致性问题。当用户说"数据对不上""增量同步少了""CDC丢数据""同步延迟""时区导致数据错位""DELETE未捕获""Sequence Column""数据乱序""整库同步配置""Flink CDC""binlog""server-id""GTID""增量快照""checkpoint超时""心跳事件""schema evolution""动态加表""LogMiner""slot.name""LSN""数据类型映射""精度丢失""debezium""快照分片""无主键表""复制槽膨胀""WAL"时立刻触发。即使用户只说"实时数据不准""业务库和数仓对不上""任务启动失败""快照读超时""flink任务重启报错"而上下文是数据管道/ETL/实时数仓，也应该触发。涵盖 Flink CDC 全系列连接器（MySQL、Oracle、PostgreSQL、MongoDB、SQL Server、TiDB、OceanBase、Db2、Vitess）及 DataX、Canal、Debezium 等工具的深度排障。基于 Apache Flink CDC 官方文档体系构建。
version: 1.0.0
author: open-anolis
os_support:
  - 通用
tags:
  - 数据中间件
  - CDC
  - Flink CDC
  - 增量同步
  - 实时数仓
  - ETL
  - Debezium
suggested_sig: middleware
contributor_type: personal
---

# CDC Pipeline Debug / CDC 增量同步管道排障

诊断与修复 CDC 增量同步/实时数据管道中的各类数据一致性问题。基于 [Apache Flink CDC](https://nightlies.apache.org/flink/flink-cdc-docs-master/) 官方文档体系构建，涵盖全部 9 个连接器及 Debezium 引擎层。

---

## 一、前置条件与版本兼容性

排查前先确认以下信息，减少空转：

1. **同步工具与版本**：Flink CDC（精确版本？）、DataX、Canal、Debezium
2. **源库类型与版本**：MySQL 5.7/8.0/8.4+、Oracle 11/12/19/21、PostgreSQL 9.6-14、MongoDB 3.6-7.0、SQL Server 2012-2019、TiDB 5.1-6.0、OceanBase 3.x/4.x、Db2 11.5、Vitess 8.0+/9.0+
3. **目标库类型**：Doris / Paimon / ClickHouse / Kafka / Elasticsearch / Hive
4. **同步模式**：增量同步 / 全量+增量（snapshot + CDC） / CDC 实时流
5. **问题特征**：数据少了 / 数据重复 / 数据错位 / 延迟高 / 任务启动失败 / 快照读超时 / checkpoint 超时 / 连接断开 / 数据类型转换错误 / OOM

### Flink CDC ↔ Flink 版本映射

**先确认版本兼容性**——版本不匹配是很多"莫名其妙"问题的根因：

| Flink CDC 版本 | 兼容 Flink 版本 | 关键变化 |
|---------------|----------------|----------|
| 3.6.* | 1.20.*, 2.2.* | 最新稳定 |
| 3.5.* | 1.19.*, 1.20.* | |
| 3.4.* | 1.19.*, 1.20.* | |
| 3.3.* | 1.18.*, 1.19.*, 1.20.*, 2.1.* | YAML Pipeline 正式可用 |
| 3.2.* | 1.17.*, 1.18.*, 1.19.*, 1.20.* | |
| 3.0.* | 1.14.*, 1.15.*, 1.16.*, 1.17.*, 1.18.* | 增量快照、动态加表 |
| 2.4.* | 1.13.*, 1.14.*, 1.15.*, 1.16.*, 1.17.* | |
| 2.3.* | 1.13.*, 1.14.*, 1.15.*, 1.16.* | |

> ⚠️ 用错版本会直接导致运行时类加载异常或语义不一致。例如 Flink CDC 3.0+ 的增量快照在 Flink 1.13 上无法使用。

### 连接器特性矩阵

不同连接器的能力差异很大，先确认当前连接器的能力边界，避免猜测不存在的特性：

| 连接器 | 无锁读 | 并行读快照 | Exactly-Once | 增量快照 | 动态加表 | 支持无主键表 |
|--------|--------|-----------|-------------|---------|---------|------------|
| **MySQL CDC** | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ |
| **Oracle CDC** | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ |
| **PostgreSQL CDC** | ✅ | ⚠️ 实验性 | ✅ | ⚠️ 默认关闭 | ❌ | ❌ |
| **MongoDB CDC** | ✅ (Change Stream) | ❌ | ✅ | ⚠️ 需 4.0+ | ✅ (3.1.0+) | N/A |
| **SQL Server CDC** | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ |
| **TiDB CDC** | ❌ | ✅ | ✅ | ❌ | ❌ | ❌ |
| **OceanBase CDC** | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ |
| **Db2 CDC** | ✅ | ✅ | ✅ | ✅ | ❌ | ✅ (3.4.0+) |
| **Vitess CDC** | ✅ | ❌ | ✅ | ❌ | ❌ | ❌ |

---

## 二、任务启动失败排查

### A1 连接失败 / 权限不足 / binlog 未开启

**MySQL CDC 前置检查清单：**

```sql
-- 检查 binlog（必须 ON）
SHOW VARIABLES LIKE 'log_bin';
-- 检查 binlog 格式（必须 ROW）
SHOW VARIABLES LIKE 'binlog_format';
-- 检查 binlog 保留天数
SHOW VARIABLES LIKE 'binlog_expire_logs_seconds';
-- 检查 GTID（高可用推荐开启）
SHOW VARIABLES LIKE 'gtid_mode';
-- 检查连接超时设置
SHOW VARIABLES LIKE 'wait_timeout';
SHOW VARIABLES LIKE 'interactive_timeout';
```

CDC 账号最小权限：

```sql
-- 增量快照模式（默认开启）不需要 RELOAD 权限
CREATE USER 'cdc'@'%' IDENTIFIED BY 'password';
GRANT SELECT, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'cdc'@'%';
FLUSH PRIVILEGES;
```

**各数据库 CDC 前置条件速查：**

| 条件 | MySQL | Oracle | PostgreSQL | MongoDB | SQL Server |
|------|-------|--------|------------|---------|------------|
| 日志模式 | `binlog_format=ROW` | `ARCHIVELOG` + `SUPPLEMENTAL LOG` | `wal_level=logical` | 需副本集/分片集群 | 需启用 CDC |
| 额外组件 | MySQL Connector 需手动添加（GPL 协议不兼容） | Oracle JDBC、xdb 需手动添加 | 无 | 无 | 需 SQL Server Agent 运行 |
| 关键权限 | `REPLICATION SLAVE`, `REPLICATION CLIENT` | `LOGMINING`, `SELECT ANY TRANSACTION` | `REPLICATION` 角色 | `changeStream`, `read` | `db_owner` |
| 特殊约束 | 无 | CDB 模式下需配置 `debezium.database.pdb.name` | `slot.name` 需小写字母+数字+下划线 | 需 WiredTiger 引擎、pv1 协议 | 需逐表 `sys.sp_cdc_enable_table` |
| 连接器协议不兼容 | ❌ MySQL Connector (GPLv2) | ❌ Oracle JDBC (FUTC) | ✅ 内置 | ✅ 内置 | ✅ 内置 |

**Oracle CDC 数据库级配置：**

```sql
-- 1. 开启归档模式（需要重启）
SHUTDOWN IMMEDIATE;
STARTUP MOUNT;
ALTER DATABASE ARCHIVELOG;
ALTER DATABASE OPEN;

-- 2. 开启 Supplemental Logging
ALTER DATABASE ADD SUPPLEMENTAL LOG DATA;

-- 3. 表级补充日志（可选）
ALTER TABLE mytable ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
```

**PostgreSQL CDC 配置：**

```ini
# postgresql.conf 必须配置
wal_level = logical
max_replication_slots = 10    # 至少 1
max_wal_senders = 10          # 至少 1
```

### A2 server-id 冲突（MySQL / OceanBase）

每个读 binlog 的客户端必须有唯一 server-id。多个 CDC 任务连接同一 MySQL 实例时必须用范围分配。

**排查命令：**
```sql
SHOW PROCESSLIST;
-- 查看所有连接的 server-id
```

**最佳实践：**

```sql
-- 并行度 = 4，server-id 范围 = 5401-5404
'server-id' = '5401-5404'
-- 范围大小必须 ≥ 并行度
```

默认连接器会在 5400-6400 之间随机生成，但显式指定更可靠。

### A3 大表快照读 checkpoint 超时

**触发条件：** 大表全量快照阶段，数据库连接超时或 checkpoint 超时导致 failover。

**适用连接器：** Oracle CDC、SQL Server CDC（增量快照关闭时）、PostgreSQL CDC（增量快照关闭时）

```sql
-- 必须为这些大表配置宽松的 checkpoint 策略
SET 'execution.checkpointing.interval' = '10min';
SET 'execution.checkpointing.tolerable-failed-checkpoints' = '100';
SET 'restart-strategy' = 'fixed-delay';
SET 'restart-strategy.fixed-delay.attempts' = '2147483647';
```

**MySQL 大表快照需额外配置 MySQL 连接超时：**

```ini
# my.cnf
interactive_timeout = 86400     # 24 小时
wait_timeout = 86400
```

### A4 Vitess 特殊配置

Vitess CDC 通过 VTGate gRPC 读取变更，不直接连 MySQL：

- 需确保 VTGate host 和 gRPC 端口（默认 15991）可达
- 目前仅支持无认证的 gRPC 访问
- `tablet.type` 控制从哪个 MySQL 实例读：`MASTER` / `REPLICA` / `RDONLY`
- 不包含快照功能，只支持增量读取

---

## 三、增量同步数据量对不上（离线增量类）

**典型症状：** 业务库和数仓同一张表对账，记录数不一致。

### 排查步骤

| 步骤 | 操作 | 判断依据 |
|------|------|----------|
| 1. 按时间窗口对账 | 按小时/天分区对比两边记录数 | 如果凌晨边界数据缺失 → 时区问题 |
| 2. 检查时区链 | 源库时区 ↔ 业务代码时区 ↔ 数仓分区时区 | 三个时区不一定相同，转一次不够 |
| 3. 检查物理删除 | 对比全量主键集合，找"数仓有、业务库无"的记录 | Flink CDC 的 changelog 包含 `-D`，DataX 类不处理 DELETE |
| 4. 检查增量字段 | `modified_time` 是数据库自动维护的还是业务写入的 | 业务写入的时间戳可能有时区混乱、空值、未来时间 |

### 解决方案

| 根因 | 修复方案 |
|------|----------|
| 时区不一致 | 增量时间窗口前后各留缓冲（如 1 小时），下游做去重 |
| 物理删除未捕获 | 推业务改逻辑删除；或 binlog 监听 DELETE；或 `scan.read-changelog-as-append-only` + `row_kind` |
| 增量字段不可靠 | 改用 binlog 位点做增量切分 |

---

## 四、CDC 实时数据不一致（核心排障）

### 场景 B1：乱序覆盖——表象正常但数据不对

**典型症状：** CDC 日志 ✅ → 计算引擎日志 ✅ → 目标库日志 ✅ → 但最终值是旧的。

**根因：** 目标库使用 last-write-wins 语义（Doris Unique Key、Paimon LSM-Tree），CDC 管道并行处理后写入顺序被打乱。

#### CDC 四层排查法（必记）

```
问题现象 → 逐层定位 → 根因修复
```

| 特征 | 问题层 | 排查方法 |
|------|--------|----------|
| 任务启动就失败 | 源库层 | binlog/归档/WAL 开启？用户权限？SSL？ |
| 任务偶尔断开 | 网络层 | `wait_timeout` / `interactive_timeout` / keepalive / 跨机房延迟 |
| 处理时报错 | 计算引擎层 | Flink SQL 语法兼容性？并行度设置？checkpoint 配置？ |
| **一切正常但数据不对** | **存储引擎层 ⚠️** | **逐条对比源库和目标库，这是最难排查的一层** |

#### Sequence Column 选择指南

| 粒度 | 适用场景 | 风险 |
|------|----------|------|
| 秒级 update_time | 业务变更频率 < 1次/秒 | 同秒多次变更区分不了 |
| 毫秒级时间戳 | 高频变更场景 | 需源库支持毫秒精度 |
| 自增版本号 | 最严格的正确性要求 | 业务代码需维护版本号 |

**Doris SQL：**
```sql
ALTER TABLE orders ENABLE FEATURE "SEQUENCE_LOAD"
WITH PROPERTIES ("function_column.sequence_type" = "DATETIME");
```

**验证 Sequence Column 是否启用：**
```sql
SET show_hidden_columns = true;
DESC orders;
-- 输出应包含 __DORIS_SEQUENCE_COL__
```

### 场景 B2：增量快照算法原理与参数调优

MySQL CDC 增量快照（incremental snapshot）是 3.0+ 的核心特性，理解其工作原理对排障至关重要：

```
启动 → 表按 chunk key 分片 → 多个 reader 并行读 chunk → 
每个 chunk 完成后做 checkpoint → 全量完成 → 
单个 task 读 binlog（等一个完整 checkpoint 后开始以确保顺序）
```

**核心参数：**

| 参数 | 默认值 | 说明 | 调优场景 |
|------|--------|------|----------|
| `scan.incremental.snapshot.chunk.size` | 8096 行 | 每个分片的行数 | 大表调大（减少分片数），小表调小（减少空转） |
| `scan.snapshot.fetch.size` | 1024 | 每次拉取的最大行数 | 大字段表调大 |
| `scan.incremental.snapshot.chunk.key-column` | 主键第一列 | 分片键 | **非主键列做 chunk key 可能导致数据不一致！见下方警告** |
| `scan.incremental.snapshot.backfill.skip` | false | 是否跳过 backfill | **开启后仅保证 at-least-once，快照阶段变更可能被重放** |
| `scan.incremental.snapshot.unbounded-chunk-first.enabled` | true | 先分配 unbounded chunk | 降低 OOM 风险 |
| `scan.incremental.close-idle-reader.enabled` | false | 快照结束后关闭空闲 reader | 减少资源占用，需 Flink ≥ 1.14 |

**⚠️ 非主键列作为 chunk key 的数据不一致风险：**

当分片键列在快照期间被更新时，同一行记录可能：
1. 被多个分片同时读到（重复）
2. 被遗漏（不在任何分片中）

**示例场景：**
```
表：id(PK), pid(chunk key)
分片0: 1 < pid <= 3
分片1: 3 < pid <= 5

并发读取时，id=0 的 pid 从 2→4 更新
分片0 读到 [id=0, pid=2]
分片1 读到 [id=0, pid=4]
最终值不确定 → 数据不一致
```

**💡 最佳实践：始终用主键列做 chunk key，除非表无主键且你清楚风险。**

### 场景 B3：MySQL 启动模式详解

启动模式决定 CDC 从什么位点开始消费。选错会导致数据缺失或重复。

| 模式 | 行为 | 适用场景 |
|------|------|----------|
| `initial`（默认） | 全量快照 + 增量 binlog | 新建同步作业 |
| `latest-offset` | 只从当前最新位点开始，不读快照 | 只关心未来的变更 |
| `earliest-offset` | 从最早可用 binlog 开始 | 需要全历史变更 |
| `specific-offset` | 从指定 binlog 文件名+位置开始 | 恢复场景（从已知正确位点重放） |
| `timestamp` | 从指定时间戳开始 | 按时间回溯 |
| `snapshot` | 只读快照，不读 binlog | 一次性全量抽取 |

**恢复场景示例：**

```sql
-- 从指定 binlog 位点恢复
'scan.startup.mode' = 'specific-offset',
'scan.startup.specific-offset.file' = 'mysql-bin.000123',
'scan.startup.specific-offset.pos' = '456789'
```

### 场景 B4：Heartbeat 与 binlog 清理

**问题：** 慢变更表的 binlog 位置长时间不更新，binlog 文件被清理后 CDC 恢复失败。

```sql
-- ⚠️ 启用心跳事件（默认 30s），持续推动 binlog 位置
'heartbeat.interval' = '30s'

-- ❌ 不推荐禁用
'heartbeat.interval' = '0s'
```

### 场景 B5：PostgreSQL 复制槽管理

**⚠️ 这是 PostgreSQL CDC 排障中最常见的问题。**

```
PostgreSQL CDC 原理：
逻辑复制槽 → 追踪 WAL 消费位点 → 阻止 WAL 被清理
```

**排障清单：**

| 问题 | 排查方法 | 解决方案 |
|------|----------|----------|
| 复制槽膨胀 | `SELECT * FROM pg_replication_slots;` 检查 `wal_status` | 清理不再使用的槽：`SELECT pg_drop_replication_slot('flink');` |
| WAL 磁盘满 | 复制槽未释放 → WAL 堆积 → 磁盘占满 | 删除无用槽后 VACUUM |
| slot 名冲突 | 同一 PostgreSQL 实例多个 CDC 作业 | 每个作业用不同的 `slot.name` |
| LSN 恢复 | `scan.lsn-commit.checkpoints-num-delay` | 默认延迟 3 个 checkpoint 再提交 LSN |

```sql
-- 查看所有复制槽
SELECT slot_name, slot_type, database, wal_status, active
FROM pg_replication_slots;

-- 手动删除（确认任务已停）
SELECT pg_drop_replication_slot('flink');
```

**PostgreSQL CDC 其他关键参数：**

```yaml
# 变更日志模式
'changelog-mode' = 'all'    # 标准 retract 流 (INSERT/DELETE/UPDATE_BEFORE/UPDATE_AFTER)
'changelog-mode' = 'upsert' # 幂等更新流，用于无 REPLICA IDENTITY FULL 的表

# 解码插件
'decoding.plugin.name' = 'decoderbufs'       # 默认，需 protobuf
'decoding.plugin.name' = 'wal2json'          # JSON 格式输出
'decoding.plugin.name' = 'pgoutput'          # PostgreSQL 原生插件
```

### 场景 B6：MongoDB CDC 特殊排障

**核心技术差异：** MongoDB CDC 使用 **Change Stream**（3.6+）而非 oplog，差异巨大。

| 维度 | Change Stream（MongoDB CDC 使用） | Oplog（Debezium MongoDB 使用） |
|------|----------------------------------|-------------------------------|
| 全文档查找 | ✅ 每次更新返回完整文档 | ❌ 需要手动 lookup |
| 版本兼容 | MongoDB 3.6+ | MongoDB 5+ 格式变更 |
| 协议版本 | 需要 pv1 | 不受限 |

**MongoDB 排障要点：**

```yaml
# 1. ⚠️ 慢变更集合必须设心跳（防止 resumeToken 过期）
'heartbeat.interval.ms' = '300000'

# 2. 快照数据筛选（下推到 MongoDB，提高效率）
'initial.snapshotting.pipeline' = '[ { "$match": { "status": "active" } } ]'

# 3. 增量快照（实验性，MongoDB 4.0+）
'scan.incremental.snapshot.enabled' = 'true'

# 4. 启动模式
'scan.startup.mode' = 'initial'         # 全量+增量
'scan.startup.mode' = 'latest-offset'   # 只从当前开始
'scan.startup.mode' = 'timestamp'       # 从指定时间戳开始
'scan.startup.timestamp-millis' = '1667232000000'
```

**MongoDB 动态加表（3.1.0+）：**
```
1. savepoint 停止作业
2. 更新 collectionList() 添加新集合
3. 从 savepoint 恢复
→ 新集合自动读快照 + 变更流
```

### 场景 B7：Db2 无主键表支持（3.4.0+）

无主键表的 CDC 需要格外注意：

```yaml
# 必须指定 chunk key（非 null 列）
'scan.incremental.snapshot.chunk.key-column' = 'non_null_col'

# 如果 chunk key 列不被更新 → exactly-once
# 如果 chunk key 列被更新 → at-least-once
# 💡 下游做幂等去重来保证最终一致性
```

**建议：** 优先用索引列作为 chunk key 以提高查询性能。

### 场景 B8：CDC YAML Pipeline 整库同步

Flink CDC 3.0+ 的 YAML Pipeline 模式用于多表整库同步，但有几个常见配置陷阱：

```yaml
source:
  type: mysql
  hostname: 10.0.0.1
  port: 3306
  username: cdc_user
  password: "${PASSWORD}"
  tables: db.\.*                     # ⚠️ 会匹配临时表、备份表
  server-id: 5500-5504               # 💡 按并行度手动指定范围

sink:
  type: doris
  fenodes: 10.0.0.2:8030
  username: root
  password: ""

route:
  - source-table: db.\.*
    sink-table: ods.ods_<>           # <> 自动替换为源表名

pipeline:
  name: cdc_pipeline
  parallelism: 4
```

**排查清单：**

| 问题 | 原因 | 解决 |
|------|------|------|
| server-id 冲突 | YAML 任务与旧的 SQL 任务共享 MySQL 实例 | 先停旧任务，或手动指定范围 |
| 同步了不该同步的表 | `\.*` 无差别匹配 | 用 `tables.exclude: db.tmp_.*, db._.*_bak` |
| Schema Evolution 丢数据 | 字段改名映射为先删后加 | 开启但配合监控 |
| Checkpoint 超时 | 目标库写入慢 | 调大 checkpoint 间隔 |
| 多表数据倾斜 | 全局并行度下大表占满 | 考虑独立的 YAML 任务拆分 |

---

## 五、Debezium 透传属性（debezium.*）深度参考

Flink CDC 底层嵌入式 Debezium 引擎的几乎所有行为都通过 `debezium.*` 参数控制。**这是排障中最重要的"暗门"——大部分问题都可以通过透传 Debezium 属性解决。**

### 全局通用

| 属性 | 默认值 | 说明 | 排障场景 |
|------|--------|------|----------|
| `debezium.snapshot.mode` | `initial` | 快照模式 | 需与 `scan.startup.mode` 配合使用，**不要同时设置两者** |
| `debezium.snapshot.lock.timeout.ms` | 10000 | 快照锁超时 | 大表快照锁等待超时 |
| `debezium.snapshot.fetch.size` | 2000 | 快照读取批大小 | 大字段表调大 |
| `debezium.max.queue.size` | 8192 | 事件队列大小 | 高吞吐场景 OOM 或 backpressure |
| `debezium.max.batch.size` | 2048 | 批处理大小 | 吞吐调优 |
| `debezium.poll.interval.ms` | 1000 | 轮询间隔 | 延迟调优 |

### MySQL 专用透传

| 属性 | 默认值 | 说明 | 排障场景 |
|------|--------|------|----------|
| `debezium.snapshot.locking.mode` | `minimal` | 快照锁模式 | `none` 可避免锁但可能不一致，`extended` 保证一致性 |
| `debezium.snapshot.select.statement.overrides` | (none) | 快照 SQL 覆写 | **大数据表只快照需要的数据范围** |
| `debezium.buffer.max.demand` | 512 | 缓冲区上限 | 高并发写入时避免内存溢出 |
| `debezium.source.include.query` | false | 是否包含原始 SQL | 调试时开启 |
| `debezium.event.deserialization.failure.handling.mode` | `warn` | 反序列化失败处理 | `fail` 严格模式，`ignore` 忽略错误事件 |

### PostgreSQL 专用透传

| 属性 | 默认值 | 说明 | 排障场景 |
|------|--------|------|----------|
| `debezium.snapshot.select.statement.overrides` | (none) | 快照 SQL 覆写 | 指定 `debezium.snapshot.select.statement.overrides.[schema].[table]` |
| `debezium.publication.name` | `dbz_publication` | 逻辑发布名称 | 自定义发布名避免冲突 |

### Oracle 专用透传

| 属性 | 默认值 | 说明 | 排障场景 |
|------|--------|------|----------|
| `debezium.database.pdb.name` | (none) | CDB 模式下 PDB 名称 | **CDB 模式必须配置** |
| `debezium.database.history.store.only.monitored.tables.ddl` | false | 只记录监控表的 DDL | 减少历史表大小 |
| `debezium.log.mining.strategy` | `online_catalog` | LogMiner 策略 | `redo_log_catalog` 可减少内存但更慢 |

### 快照数据范围筛选（PostgreSQL 示例）

```sql
-- 仅快照特定范围的数据（而非全表）
'debezium.snapshot.select.statement.overrides' = 'public.orders',
'debezium.snapshot.select.statement.overrides.public.orders' = 
  'SELECT * FROM public.orders WHERE create_time > ''2025-01-01'''
```

---

## 六、数据类型映射排查

数据类型映射错误是"数据不对"但 CDC 日志正常的常见原因。不同连接器的映射规则不同。

### 常见精度丢失场景

| 源类型 | Flink 目标类型 | 问题 | 修复 |
|--------|---------------|------|------|
| MySQL DECIMAL(65, s) | DECIMAL(38, s) | **精度被截断** | 精度 > 38 的列映射为 STRING |
| MySQL BIGINT UNSIGNED | BIGINT | 超出范围变负数 | 映射为 DECIMAL(20, 0) |
| MySQL TIMESTAMP | TIMESTAMP_LTZ | 时区转换错误 | 设置 `server-time-zone` |
| Oracle/MySQL JSON | STRING | JSON 格式丢失空格 | `use.legacy.json.format` 控制 |
| MongoDB 嵌套文档 | ROW / ARRAY<ROW> | 扁平化映射复杂 | 声明完整的嵌套结构 |
| MySQL ENUM | STRING | 枚举值映射 | 自动转字符串 |
| MySQL SET | ARRAY<STRING> | 集合映射 | 自动转字符串数组 |
| Db2 BOOLEAN | BOOLEAN | **CDC 不支持 BOOLEAN 类型** | 用其他类型替代（SQL Replication 限制） |

### 时间类型时区陷阱

```sql
-- MySQL
'server-time-zone' = 'Asia/Shanghai'  -- 控制 TIMESTAMP 类型如何转 STRING

-- 暴露 op_ts 元数据获取数据库层面的变更时间
op_ts TIMESTAMP_LTZ(3) METADATA FROM 'op_ts' VIRTUAL
```

### Decimal 精度完整映射

| 源精度 | Flink 映射 | 建议 |
|--------|-----------|------|
| p ≤ 38 | `DECIMAL(p, s)` | 直接映射 |
| p > 38 | `STRING` | **必须先转 STRING 再处理** |

---

## 七、性能调优指南

### 并行度与 server-id

```yaml
# 并行度原则：每个并行 reader 需要一个唯一 server-id
# server-id 范围 ≥ 并行度
'server-id' = '5401-5408'   # 并行度 8
SET 'parallelism.default' = 8;
```

### Chunk 大小调优

```yaml
# 大表（> 1亿行）：chunk.size 调大
'scan.incremental.snapshot.chunk.size' = '65536'

# 小表（< 100万行）：chunk.size 调小
'scan.incremental.snapshot.chunk.size' = '2048'
```

### Checkpoint 配置

```yaml
# 实时场景（低延迟优先）
SET 'execution.checkpointing.interval' = '10s';
SET 'execution.checkpointing.timeout' = '1min';

# 大表快照场景（避免 failover）
SET 'execution.checkpointing.interval' = '10min';
SET 'execution.checkpointing.tolerable-failed-checkpoints' = '100';
```

### 数据分布均匀性调优

当 chunk key 分布不均匀时（例如自增 ID 大量跳号），可通过分布因子参数优化分片：

```yaml
# 分布因子 = (MAX(id) - MIN(id) + 1) / rowCount
# 值越接近 1 说明分布越均匀
# 值远大于 1 说明数据稀疏，需要用查询方式分片

'chunk-key.even-distribution.factor.lower-bound' = '0.05'
'chunk-key.even-distribution.factor.upper-bound' = '1000.0'
```

---

## 八、Flink 监控指标

CDC Source 内置了 Flink Metrics，用于监控快照和增量阶段的进度：

| 指标 | 类型 | 说明 |
|------|------|------|
| `isSnapshotting` | Gauge | 当前是否在快照阶段 |
| `isStreamReading` | Gauge | 当前是否在增量阶段 |
| `numTablesSnapshotted` | Gauge | 已完成快照的表数量 |
| `numTablesRemaining` | Gauge | 尚未快照的表数量 |
| `numSnapshotSplitsProcessed` | Gauge | 正在处理的分片数 |
| `numSnapshotSplitsRemaining` | Gauge | 剩余分片数 |
| `numSnapshotSplitsFinished` | Gauge | 已完成的分片数 |
| `snapshotStartTime` | Gauge | 快照开始时间 |
| `snapshotEndTime` | Gauge | 快照结束时间 |

**指标名格式：** `{database}.{schema}.{table}.{metric_name}`  
例如 MySQL：`mydb.inventory.products.isSnapshotting`  
例如 Db2：`test_database.test_schema.test_table.numSnapshotSplitsFinished`

**典型运维场景：**
```
# 快照卡住了？查看 numSnapshotSplitsRemaining
# 大表分片过多？检查 numSnapshotSplitsFinished 增长曲线
# 确认当前阶段：isSnapshotting = true → 快照中；isStreamReading = true → 增量中
```

---

## 九、高可用与故障恢复

### MySQL GTID 高可用

```ini
# MySQL 集群配置
gtid_mode = on
enforce_gtid_consistency = on
log-slave-updates = 1    # 监控从实例时需要
```

**故障切换策略：**
| 策略 | 做法 | 优点 |
|------|------|------|
| DNS/VIP | CDC 连接 DNS 名，故障时 DNS 切换 | 无需改配置，零中断 |
| 修改 hostname | savepoint → 改 hostname → 恢复 | 手动可控 |

### 恢复策略矩阵

| 故障场景 | 恢复操作 | 注意事项 |
|----------|----------|----------|
| 任务意外停止 | 从最新 checkpoint/savepoint 恢复 | Exactly-Once 保障 |
| binlog 已被清理 | 用 `initial` 重新快照 | 大表需要长时间窗口 |
| binlog 部分可用 | 用 `specific-offset` 从已知位点恢复 | 需确认位点仍然存在 |
| 数据发现不一致 | 定位不一致范围 → 用 flink 从问题前的位点重放 | ⚠️ 需回滚下游数据 |
| Oracle SCN 过期 | 检查归档日志保留策略 | 延长归档保留时间 |
| PG 复制槽损坏 | 删槽重建 | 可能会导致短暂数据丢失 |
| MongoDB resumeToken 过期 | 设置 `heartbeat.interval.ms` | 从 savepoint 恢复 |

---

## 十、验证标准

| 验证项 | 通过标准 | 检查方法 |
|--------|----------|----------|
| 数据量对账 | 主键差 < 0.1% | `COUNT(*)` 对比 + 差集 `EXCEPT` |
| 快照进度 | `numSnapshotSplitsRemaining = 0` | Flink Metrics |
| 乱序修复 | 目标库最新值 = 源库最新值 | 逐条对比关键变更 |
| 整库完整性 | 无遗漏表、无多余表 | `tables.exclude` 确认 |
| 延迟 | 端到端 < 30s（实时场景） | Flink 延迟 Metrics |
| 心跳 | 心跳间隔内 binlog 位置更新 | `SHOW PROCESSLIST` 或 Metrics |
| 复制槽 | slot 活跃且 WAL 不膨胀 | `pg_replication_slots` 检查 |
| Checkpoint | checkpoint 成功率 > 99% | Flink WebUI / Metrics |

---

## 十一、错误信息速查

| 错误模式 | 常见根因 | 排查方向 |
|----------|----------|----------|
| `Table xxx is not supported by the connector` | 表无主键（MySQL/Oracle/PG/SQLServer） | 添加主键或用 Db2 3.4.0+ 的 chunk key 方案 |
| `Server id is not unique` | 多个 CDC 任务 server-id 冲突 | 用范围分配 `5401-54xx` |
| `Replication slot is active for PID` | PG 复制槽被其他进程占用 | 换 slot.name 或等旧进程释放 |
| `ResumeToken not found` | MongoDB resumeToken 过期 | 设置 heartbeat.interval.ms |
| `No stable position found` | binlog/归档日志已被清理 | 调长保留时间或重新 initial |
| `Column 'xxx' of type 'BOOLEAN' is not supported` | Db2 BOOLEAN 不支持 CDC | 换用 SMALLINT |
| `Decimal precision 65 exceeds max 38` | 精度 > 38 的 DECIMAL | 映射为 STRING |
| `Cannot perform checkpoint during scanning snapshot` | Oracle/SQLServer 快照阶段无法 checkpoint | 配置 toleratble-failed-checkpoints |
| `Schema change cannot be applied` | DDL 不被目标库支持 | 关闭 schema evolution 或手动处理 |
| `Chunk key column updated` | 分片键列在快照期间被更新 | 用主键列做 chunk key |

---

## 十二、示例（Examples）

### 示例 1：增量数据对不上

**用户：** 业务库 10000 单，数仓只有 7000 单。
**排查：** 按小时对账 → 凌晨数据缺失 → 源库 UTC-5，数仓 UTC+8 → 窗口加 1h 缓冲。

### 示例 2：CDC 乱序

**用户：** 订单状态"已支付→已发货→已完成"后，目标库还是"已支付"。
**排查：** CDC ✅ → Flink ✅ → 目标库导入日志显示已完成先到、已支付后到 → 启用 Sequence Column。

### 示例 3：任务启动失败——binlog 未开

**用户：** Flink CDC MySQL 任务启动就报错。
**排查：** `log_bin=OFF` → 开启 `log_bin=ON`, `binlog_format=ROW` → 配置 CDC 用户权限。

### 示例 4：MongoDB 检查点恢复失败

**用户：** 任务停后恢复，报 resumeToken 过期。
**排查：** 慢变更集合未设心跳 → 设置 `heartbeat.interval.ms = 300000` → 从 savepoint 恢复成功。

### 示例 5：PostgreSQL 复制槽导致 WAL 膨胀

**用户：** CDC 任务停了几天，PostgreSQL 磁盘报警 95%。
**排查：** 查看 `pg_replication_slots` → 复制槽未释放 → WAL 堆积 → 删除无用槽后清理 WAL → 设置定时监控。

### 示例 6：Decimal 精度丢失

**用户：** 金额字段同步后小数点后多出乱码。
**排查：** MySQL DECIMAL(65,2) 映射为 Flink DECIMAL(38,2) → 精度截断 → 改为 STRING 映射 → 数据正确。

---

## 十三、参考文档

- `references/cdc-cases.md` — 生产环境 CDC 排障案例集（按需加载）
- Apache Flink CDC 官方文档：https://nightlies.apache.org/flink/flink-cdc-docs-stable/
- Debezium 文档：https://debezium.io/documentation/reference/stable/
- Doris Sequence Column：https://doris.apache.org/docs/dev/data-operate/update/unique-update-concurrent-control/
- Paimon CDC 配置：https://paimon.apache.org/docs/master/engines/flink/
- Flink Metrics：https://nightlies.apache.org/flink/flink-docs-stable/docs/ops/metrics/
