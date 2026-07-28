---
name: cdc-pipeline-debug
description: 诊断与修复 CDC 增量同步/实时数据管道中的各类数据一致性问题。当用户说"数据对不上""增量同步少了""CDC丢数据""同步延迟""时区导致数据错位""DELETE未捕获""Sequence Column""数据乱序""整库同步配置""Flink CDC""binlog""server-id""GTID""增量快照""checkpoint超时""心跳事件""schema evolution""动态加表""LogMiner""slot.name""LSN"时立刻触发。即使用户只说"实时数据不准""业务库和数仓对不上""任务启动失败""快照读超时"而上下文是数据管道/ETL/实时数仓，也应该触发。涵盖 Flink CDC 全系列连接器（MySQL、Oracle、PostgreSQL、MongoDB、SQL Server、TiDB、OceanBase、Db2、Vitess）及 DataX、Canal、Debezium 等工具的排障。
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

诊断与修复 CDC 增量同步/实时数据管道中的各类数据一致性问题。涵盖 Flink CDC 全系列连接器（MySQL、Oracle、PostgreSQL、MongoDB、SQL Server、TiDB、OceanBase、Db2、Vitess）、DataX 类离线增量、Canal/Debezium 等场景。

---

## 一、前置条件

排查前先确认以下信息，减少空转：

1. **同步工具与版本**：Flink CDC（版本？）、DataX、Canal、Debezium？Flink CDC 版本直接决定了功能和已知 bug（详见下方版本映射表）
2. **源库类型与版本**：MySQL 5.7/8.0/8.4+、Oracle 11/12/19/21、PostgreSQL 9.6-14、MongoDB 3.6-7.0、SQL Server 2012-2019、TiDB 5.1-6.0、OceanBase 3.x/4.x 等
3. **目标库类型**：Doris / Paimon / ClickHouse / Kafka / Hive
4. **同步模式**：增量同步 / 全量+增量（snapshot + CDC） / CDC 实时流
5. **问题特征**：数据少了 / 数据重复 / 数据错位 / 延迟高 / 任务启动失败 / 快照读超时 / checkpoint 超时 / 连接断开

### Flink CDC 版本与 Flink 版本映射

排查前先确认版本兼容性——版本不匹配是很多"莫名其妙"问题的根因：

| Flink CDC 版本 | 兼容 Flink 版本 |
|---------------|----------------|
| 3.6.* | 1.20.*, 2.2.* |
| 3.5.* | 1.19.*, 1.20.* |
| 3.4.* | 1.19.*, 1.20.* |
| 3.3.* | 1.18.*, 1.19.*, 1.20.*, 2.1.* |
| 3.2.* | 1.17.*, 1.18.*, 1.19.*, 1.20.* |
| 3.0.* | 1.14.*, 1.15.*, 1.16.*, 1.17.*, 1.18.* |
| 2.4.* | 1.13.*, 1.14.*, 1.15.*, 1.16.*, 1.17.* |

### 连接器特性速查

不同连接器支持的特性差异很大，排查时先确认当前连接器的能力边界：

| 连接器 | 无锁读 | 并行读 | Exactly-Once | 增量快照 | 动态加表 |
|--------|--------|--------|-------------|---------|---------|
| MySQL CDC | ✅ | ✅ | ✅ | ✅ | ✅ |
| Oracle CDC | ✅ | ✅ | ✅ | ✅ | ❌ |
| PostgreSQL CDC | ✅ | ✅（实验性） | ✅ | ⚠️ 默认关闭 | ❌ |
| MongoDB CDC | ✅ | ❌ | ✅ | ⚠️ 需 4.0+ | ✅ |
| SQL Server CDC | ✅ | ✅ | ✅ | ✅ | ❌ |
| TiDB CDC | ❌ | ✅ | ✅ | ❌ | ❌ |
| OceanBase CDC | ✅ | ✅ | ✅ | ✅ | ✅ |

---

## 二、问题分类与排查步骤

### 场景 A：任务启动就失败

#### A1 连接失败 / 权限不足

**MySQL CDC：**

```sql
-- 检查 binlog 是否开启
SHOW VARIABLES LIKE 'log_bin';
-- 检查 binlog 格式（必须为 ROW）
SHOW VARIABLES LIKE 'binlog_format';
-- 检查 GTID 是否开启
SHOW VARIABLES LIKE 'gtid_mode';
```

CDC 账号需要的最小权限：

```sql
GRANT SELECT, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'cdc_user';
-- 注意：增量快照模式（默认开启）不需要 RELOAD 权限
```

排查清单：

| 现象 | MySQL | Oracle | PostgreSQL | MongoDB |
|------|-------|--------|------------|---------|
| 连接拒绝 | 检查 hostname/port 是否正确、MySQL 是否绑定了 127.0.0.1 | 检查 listener 状态、SID 是否正确 | 检查 pg_hba.conf 是否允许远程连接 | 检查认证机制、connection.options |
| 权限不足 | 缺 REPLICATION SLAVE 或 REPLICATION CLIENT | 缺 LOGMINING、SELECT ANY TRANSACTION | 缺 replication 角色 | 缺 changeStream 和 read 权限 |
| binlog 未开 | `log_bin=ON` 且 `binlog_format=ROW` | 需开启归档模式 `ARCHIVELOG` + supplemental logging | 需设置 `wal_level=logical` | 需 3.6+ + 副本集/分片集群 + WiredTiger |
| 驱动缺失 | MySQL Connector 需手动添加（GPL 协议不兼容） | Oracle JDBC 需手动添加（FUTC 协议不兼容） | 包含在连接器中 | 包含在连接器中 |

**Oracle CDC 特殊配置：**
- 必须开启归档模式：`ALTER DATABASE ARCHIVELOG;`
- 必须开启 supplemental logging：`ALTER DATABASE ADD SUPPLEMENTAL LOG DATA;`
- CDB 模式下需额外配置 `debezium.database.pdb.name`
- 需要 `DBMS_LOGMNR` 相关权限

**PostgreSQL CDC 特殊配置：**

```ini
# postgresql.conf 必须配置
wal_level = logical
max_replication_slots = 10  # 至少 1
max_wal_senders = 10        # 至少 1
```

- slot.name 必须符合命名规范（小写字母、数字、下划线）
- 复制槽不自动清理，CDC 任务停止后需要手动删除

**SQL Server CDC 特殊配置：**
- 需要先对数据库启用 CDC，再对表启用
- 通过存储过程 `sys.sp_cdc_enable_table` 逐表开启
- 要求 SQL Server Agent 正在运行

#### A2 server-id 冲突（MySQL / OceanBase）

每个读 binlog 的客户端都必须有唯一的 server-id。如果一个 MySQL 实例被多个 CDC 任务连接，server-id 不能重复。

```sql
-- 查看当前有哪些 server-id 在使用
SHOW PROCESSLIST;
```

**最佳实践：** 用范围分配，范围必须大于并行度：

```sql
-- 并行度 = 4 时，server-id 范围至少 4 个
'server-id' = '5401-5404'
```

默认情况下连接器会在 5400-6400 之间随机生成，但建议明确指定。

#### A3 大表快照读 checkpoint 超时（Oracle / SQL Server）

**Oracle 和 SQL Server（增量快照关闭时）：** 全量快照阶段无法做 checkpoint，因为不存在可恢复的位点。CDC source 会让 checkpoint 等待直到超时，默认触发 failover。

```sql
-- 大表场景必须配置：
SET 'execution.checkpointing.interval' = '10min';
SET 'execution.checkpointing.tolerable-failed-checkpoints' = '100';
SET 'restart-strategy' = 'fixed-delay';
SET 'restart-strategy.fixed-delay.attempts' = '2147483647';
```

---

### 场景 B：增量同步数据量对不上（离线增量类）

**典型症状**：业务库和数仓同一张表对账，记录数不一致。

**排查步骤**：

1. **核对时间窗口**
   - 确认增量切分字段用的是哪个时间戳
   - 确认源库时区、目标库时区、增量切分时区三者是否统一

2. **检查时区偏差**
   - 用源库和目标库各跑一次分区统计，按小时对比差异区间
   - 如果凌晨边界数据丢失，大概率是时区不一致导致的跨天错位
   - MySQL 通过 `server-time-zone` 选项控制 TIMESTAMP 类型转换行为

3. **检查物理删除**
   - 对比业务库和数仓的全量主键集合：数仓有但业务库无的记录 = 被物理删除但未捕获
   - Flink CDC 的 changelog 流包含 `-D`（DELETE）消息，但 DataX 类离线增量只认 `modified_time`
   - 方案：推业务改逻辑删除；或用 binlog 监听 DELETE 事件；或开启 `scan.read-changelog-as-append-only` + `row_kind` 元数据字段做软删

4. **检查增量字段可靠性**
   - 确认增量字段是数据库自动维护的（可靠）还是业务代码写入的（不可靠）

**解决方案**：

| 根因 | 修复方案 |
|------|----------|
| 时区不一致 | 增量时间窗口前后各留缓冲（如 1 小时），下游去重 |
| 物理删除未捕获 | 逻辑删除 / binlog 监听 DELETE / scan.read-changelog-as-append-only |
| 增量字段不可靠 | 改用数据库自增 ID 或 binlog 位点做增量切分 |

---

### 场景 C：CDC 实时数据对不上——乱序覆盖

**典型症状**：CDC 日志正常，计算引擎日志正常，目标库日志正常，但最终值不是最新值。

#### 根因

目标库（如 Doris Unique Key、Paimon LSM-Tree）依赖"后写入覆盖"语义（last-write-wins）。CDC 管道中同一条记录的多次变更在毫秒级到达，并行处理后写入顺序可能被打乱——后发生的变更先入库，先发生的变更后入库，旧值覆盖新值。

这是 CDC 管道的**结构性特征**，不是偶发网络问题。

#### CDC 四层排查法

| 特征 | 问题层 | 排查方法 |
|------|--------|----------|
| 任务启动就失败 | 源库层 | 检查 binlog/归档/WAL 是否开启、权限是否足够 |
| 任务偶尔断开 | 网络层 | 网络监控、SSL 配置、MySQL `wait_timeout` / `interactive_timeout`、keepalive |
| 处理数据时报语法错误 | 计算引擎层 | 检查 Flink SQL 语法兼容性（DATE_SUB、保留字等） |
| 一切正常但数据不对 | 存储引擎层 | 逐条对比源库和目标库数据 |

#### MySQL CDC 增量快照算法的深层理解

MySQL CDC 使用**增量快照（incremental snapshot）** 算法，理解它对排障至关重要：

- 全量阶段：表按 chunk key 分片（默认主键第一列），多个 reader 并行读取各 chunk
- 每个 chunk 读取完成后做 checkpoint（chunk 粒度 checkpoint）
- 全量完成后，**单个 task** 读 binlog，保证全局顺序
- binlog reader 等到所有 chunk 完成并做完一个完整 checkpoint 后才开始消费，确保快照和增量数据顺序正确

相关参数：

| 参数 | 默认值 | 说明 | 排障场景 |
|------|--------|------|----------|
| `scan.incremental.snapshot.enabled` | true | 是否启用增量快照 | 关闭后回退到旧快照机制，不支持并行和 checkpoint |
| `scan.incremental.snapshot.chunk.size` | 8096 行 | 每个 chunk 的行数 | 大表 chunk 太小会导致分片过多 |
| `scan.snapshot.fetch.size` | 1024 | 每次读取的最大条数 | 大字段表调大 |
| `scan.incremental.snapshot.chunk.key-column` | 主键第一列 | 分片键 | 非主键列做分片键可能导致数据不一致！ |
| `scan.incremental.snapshot.backfill.skip` | false | 是否跳过 backfill | **开启后可能导致数据不一致**（仅 at-least-once） |

**警告**：使用非主键列作为 chunk key 可能导致数据不一致——chunk key 在快照读取期间如果被更新，可能被分配到错误的 chunk 或被多次读取/遗漏。

#### Heartbeat 与 binlog 清理

如果表不经常更新，binlog 文件或 GTID 集可能在最后提交位置被清理，CDC 任务重启后找不到位点。

```sql
-- 启用心跳事件（默认 30s 间隔）
'heartbeat.interval' = '30s'

-- 禁用（不推荐）
'heartbeat.interval' = '0s'
```

#### MongoDB CDC 的 Change Stream 机制

MongoDB CDC 使用 Change Stream（3.6+ 特性）而非 oplog：

- Change Stream 提供"查找更新操作的完整文档"功能，返回更新后的多数提交版本
- 需要副本集或分片集群
- 需要 WiredTiger 存储引擎
- 需要副本集协议版本 1（pv1）

MongoDB CDC 的排障要点：

```yaml
# 集合变更缓慢时必须设置心跳
'heartbeat.interval.ms' = '300000'  # 5分钟

# 快照阶段可以用 pipeline 筛选数据
'initial.snapshotting.pipeline' = '[ { "$match": { "status": "active" } } ]'

# 增量快照（实验性）仅支持 MongoDB 4.0+
'scan.incremental.snapshot.enabled' = 'true'
```

MongoDB 的 **动态加表** 功能（3.1.0+）允许在运行中的作业添加新集合：

1. savepoint 停止作业
2. 更新 `collectionList()` 添加新集合
3. 从 savepoint 恢复

#### 解决方案

- Doris：启用 Sequence Column，用 `update_time` 或自增版本号
- Paimon：检查主键定义和 LSM-Tree 顺序
- Flink：控制并行度，避免同一条记录分配到不同实例
- 调整并行度后需重新分配 server-id 范围

##### Sequence Column 选择原则

| 粒度 | 适用场景 | 风险 |
|------|----------|------|
| 秒级 update_time | 业务更新不频繁 | 同秒变更区分不了 |
| 毫秒级时间戳 | 高频变更 | 需源库支持毫秒精度 |
| 自增版本号 | 最严格 | 业务代码需维护版本号 |

---

### 场景 D：MySQL CDC 配置参数排障

以下是与数据准确性直接相关的参数，排障时逐一核对：

#### 启动模式（决定从哪开始读）

```sql
-- initial（默认）：全量快照 + 增量 binlog
-- latest-offset：不读快照，只从当前最新位点开始
-- earliest-offset：从最早可用的 binlog 开始
-- specific-offset：从指定 binlog 文件名+位置开始
-- timestamp：从指定时间戳开始
-- snapshot：只读快照，不读增量
'scan.startup.mode' = 'initial'
```

启动模式选错会导致数据缺失或重复。例如恢复场景应该用 `specific-offset` 而不是 `initial`。

#### 元数据列

MySQL CDC 可以暴露数据行所属的库、表、操作类型和变更时间：

```sql
CREATE TABLE cdc_table (
    db_name STRING METADATA FROM 'database_name' VIRTUAL,
    table_name STRING METADATA FROM 'table_name' VIRTUAL,
    operation_ts TIMESTAMP_LTZ(3) METADATA FROM 'op_ts' VIRTUAL,
    operation STRING METADATA FROM 'row_kind' VIRTUAL, -- +I/-D/-U/+U
    ...
)
```

`row_kind` 元数据可用于在逻辑删除场景下区分操作类型。

#### 在线 DDL 变更

```sql
-- 解析 gh-ost 或 pt-osc 生成的影子表 DDL 事件（实验性）
'scan.parse.online.schema.changes.enabled' = 'true'
```

#### Changelog 转 Append-Only

```sql
'scan.read-changelog-as-append-only.enabled' = 'true'
```

所有变更（INSERT/DELETE/UPDATE_BEFORE/UPDATE_AFTER）都转为 INSERT，配合 `row_kind` 元数据字段，下游可以保存所有明细数据再按 `row_kind` 做逻辑删除。

---

### 场景 E：PostgreSQL CDC 特殊排障

#### 复制槽管理

PostgreSQL CDC 需要逻辑复制槽，这是排障中问题最多的部分：

- `slot.name` 必须唯一，不同表建议用不同 slot name
- 复制槽会阻止 WAL 日志清理，CDC 任务停掉后槽不自动释放，会导致 WAL 膨胀

```sql
-- 查看所有复制槽
SELECT * FROM pg_replication_slots;
-- 手动删除不再使用的槽
SELECT pg_drop_replication_slot('flink');
```

#### 变更日志模式

```yaml
-- all（默认）：标准 retract 流，包含 +I/-D/-U/+U
-- upsert：幂等更新流，仅包含 +I/+U
'changelog-mode' = 'upsert'
```

upsert 模式可用于表有主键但无法设置 replica identity FULL 的场景。

#### LSN 提交延迟

```yaml
-- 延迟 LSN 提交的 checkpoint 数量（默认 3）
-- 确保有足够的历史 LSN 用于恢复
'scan.lsn-commit.checkpoints-num-delay' = '3'
```

#### 解码插件

```yaml
-- decoderbufs（默认）：需要 protobuf 库
-- wal2json：JSON 格式输出
-- wal2json_streaming：流式 JSON 输出
-- pgoutput：PostgreSQL 原生输出插件
'decoding.plugin.name' = 'decoderbufs'
```

---

### 场景 F：Oracle CDC 特殊排障

#### LogMiner 原理

Oracle CDC 基于 LogMiner 技术，它读取在线重做日志和归档日志来捕获变更：

- 需要开启 `ARCHIVELOG` 模式（需重启数据库）
- 归档日志占用大量磁盘空间，需定期清理
- 慢变更表可能导致 LogMiner 持有的归档日志空间无法释放

#### SCN 模式

```yaml
-- initial（默认）：全量快照 + LogMiner
'scan.startup.mode' = 'initial'
-- latest-offset：只从当前 SCN 开始
'scan.startup.mode' = 'latest-offset'
-- specific-offset：从指定 SCN 开始
'scan.startup.mode' = 'specific-offset'
'scan.startup.specific-offset.scn' = '1234567890'
```

#### CDB 与 non-CDB

- CDB（Container Database）模式下需额外配置 `debezium.database.pdb.name`
- PDB（Pluggable Database）级别的表同步

---

### 场景 G：Fiink CDC YAML 整库同步配置

**典型症状**：多表同步任务失败、同步了不该同步的表、Schema 变更导致写入失败。

| 排查步骤 | 具体操作 |
|----------|----------|
| server-id 冲突 | 旧 Flink SQL 任务和 YAML 任务共享同一 MySQL 实例时 server-id 可能重叠。先停旧任务或手动指定范围 |
| 表名映射 | 正则匹配 `\.*` 会同步临时表、备份表。用 `tables.exclude` 排除或显式白名单 |
| Schema Evolution | 默认关闭。开启后字段改名可能丢数据（先删后加），类型缩窄导致写入失败，DDL 变更要配合监控 |
| 并行度与 Checkpoint | 全局并行度下多表可能数据倾斜；checkpoint 间隔需要匹配目标库写入能力 |

YAML 模式 vs SQL 模式的核心差异：

| 维度 | Flink SQL（N 任务） | CDC YAML（1 任务） |
|------|-------------------|-------------------|
| server-id | 手动分配 | 自动分配 |
| Schema Evolution | 不支持 | 支持 |
| 连接信息 | 重复 N 份 | 1 份 |
| 正则匹配 | 不支持 | 支持 |

---

## 三、高可用与恢复

### MySQL GTID 高可用

MySQL CDC 通过 GTID 提供高可用集群支持。MySQL 集群配置要求：

```ini
gtid_mode = on
enforce_gtid_consistency = on
-- 如果监控从实例，还需要
log-slave-updates = 1
```

源库故障后只需将 `hostname` 切换到其他可用服务器，从最新 checkpoint/savepoint 恢复。

建议为 MySQL 集群配置 DNS 或 VIP，故障切换无需改地址。

### 恢复策略

| 恢复场景 | 操作 |
|----------|------|
| 任务意外停止 | 从最新 checkpoint/savepoint 恢复，Exactly-Once 保障 |
| binlog 已被清理 | 重新 initial snapshot（需较大表做全量），或用 latest-offset（丢失历史变更） |
| 数据发现不一致 | 用 specific-offset 回溯到问题发生前的位点重新消费 |
| Oracle SCN 过期 | 检查归档日志保留策略，确保 LogMiner 可访问 |

---

## 四、验证（Verification）

| 验证项 | 通过标准 |
|--------|----------|
| 数据量对账 | 业务库和数仓全量主键差 < 0.1% |
| 分区数据一致性 | 连续 3 天分区数据量曲线形态一致 |
| 乱序修复 | 同一条记录在目标库的最终值 = 源库最新值 |
| 整库同步完整性 | 所有目标表都在同步，无遗漏、无多余表 |
| 延迟监控 | 端到端延迟在业务可接受范围内（实时场景通常 < 30s） |
| 心跳正常 | MySQL 心跳间隔内 binlog 位置持续更新，无清理风险 |
| 复制槽正常（PG） | pg_replication_slots 中 slot 状态正常，WAL 不膨胀 |

大型排查完成后输出排查报告：问题现象 → 排查路径 → 根因 → 修复方案 → 验证结果。

---

## 五、易错点（Pitfalls）

1. **只查 CDC 日志**：CDC 日志正常不代表数据正确——乱序覆盖时 CDC 正常的，问题在目标库写入层
2. **时区只转一次**：源库时区、业务代码时区、数仓时区可能三个都不一样
3. **增量窗口卡太紧**：不加缓冲一定漏边界数据
4. **Sequence Column 粒度过粗**：秒级时间戳同秒多次变更区分不了
5. **server-id 不唯一**：多个 CDC 任务连同一 MySQL 实例，server-id 必须用范围分配
6. **复制槽不清理（PG）**：CDC 停掉后槽不自动释放，WAL 会膨胀到磁盘满
7. **schema evolution 无脑开**：字段改名丢数据、类型缩窄写失败
8. **物理删除被忽略**：增量同步默认不处理 DELETE
9. **心跳不设（MongoDB）**：慢变更集合的 resumeToken 会过期，检查点恢复失败
10. **非主键做 chunk key**：可能导致数据行被多次读取或遗漏
11. **跳过 backfill**：`scan.incremental.snapshot.backfill.skip=true` 会导致数据不一致（仅 at-least-once）
12. **增量快照与旧模式混淆**：旧快照模式不支持并行读和 checkpoint，大表会超时

---

## 六、示例（Examples）

### 示例 1：增量同步数据对不上

**用户输入**：
> 今天业务库开了 10000 单，数仓查出来只有 7000 单，差了 3000 单。增量同步好像有问题。

**排查路径**：
1. 按小时对比两边数据量 → 发现凌晨 2-5 点数仓数据缺失
2. 查时区配置 → 业务库时区 UTC-5，数仓按 UTC+8 分区
3. 修复：增量窗口前后加 1 小时缓冲

### 示例 2：CDC 实时数据乱序

**用户输入**：
> Flink CDC 同步到目标库，订单状态从"已支付→已发货→已完成"后，查目标库还是"已支付"。CDC、Flink 日志都正常。

**排查路径**：
1. 确认 CDC 捕获了全部三次变更 → 已捕获
2. 确认 Flink 输出了全部记录 → 已输出
3. 查目标库导入日志 → "已完成"先入库，"已支付"后入库
4. 修复：启用 Sequence Column，用 `update_time` 作为排序依据

### 示例 3：CDC 任务启动失败——binlog 未开

**用户输入**：
> Flink CDC MySQL 任务启动就报错，连接失败了。

**排查路径**：
1. 检查 MySQL 的 `log_bin` 状态 → 未开启
2. 检查 `binlog_format` → 未设置
3. 修复：开启 binlog（`log_bin=ON`, `binlog_format=ROW`），配置 CDC 用户权限
4. 检查 `server-id` 是否冲突

### 示例 4：MongoDB CDC 检查点恢复失败

**用户输入**：
> MongoDB CDC 任务停了之后恢复，报 resumeToken 过期。

**排查路径**：
1. 检查集合变更频率 → 很慢，几分钟才一条
2. 检查心跳配置 → 未设置 `heartbeat.interval.ms`
3. 修复：设置 `heartbeat.interval.ms = 300000`
4. 验证：从 savepoint 恢复成功

---

## 七、参考文档

- `references/cdc-cases.md` — 生产环境 CDC 排障案例集（按需加载）
- Flink CDC 官方文档：https://nightlies.apache.org/flink/flink-cdc-docs-stable/
- Debezium 文档：https://debezium.io/documentation/reference/stable/
- Doris Sequence Column：https://doris.apache.org/docs/dev/data-operate/update/unique-update-concurrent-control/
