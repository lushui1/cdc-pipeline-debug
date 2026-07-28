---
name: cdc-pipeline-debug
description: 诊断与修复 CDC 增量同步/实时数据管道中的数据一致性问题。当用户说"数据对不上""增量同步少了""数据是旧的""日志正常但数据不对""启动就报错""起不来""精度不对""小数位多了""时间错了""时间差""差几个小时""延迟越来越高""延迟堆积""加了字段目标库没有""加字段未同步""改类型写入失败""丢数据""数据值回退""运行中断""恢复失败""OOM""主从切换异常""binlog""server-id""GTID""Sequence Column""数据类型映射""复制槽""WAL""LogMiner""checkpoint超时""schema evolution""heartbeat""增量快照""chunk key""debezium""快照太慢"时**立刻触发**。即使用户只说"实时数据不准""业务库和数仓对不上""flink任务起来就挂""数据恢复不了""binlog找不到""CDC任务停了"而上下文是数据管道/ETL/实时数仓，也**应该触发**。基于 Apache Flink CDC 官方文档构建。不依赖用户具体架构，描述现象即可用。
version: 1.0.0
author: open-anolis
os_support:
  - Anolis OS 8
  - Anolis OS 23
  - 通用
tags:
  - 数据中间件
  - CDC
  - Flink CDC
  - 故障排查
suggested_sig: middleware
contributor_type: personal
---

# CDC 管道故障排查手册

基于 Apache Flink CDC 官方文档构建的完整症状排查手册。Agent 按以下步骤执行，不要跳过。

> 官方文档总览：https://nightlies.apache.org/flink/flink-cdc-docs-master/zh/docs/connectors/flink-sources/overview/

---

## 一、前置确认

动手前必须收集这 5 项，不清楚就问用户：

```
1. CDC 工具及版本：Flink CDC（精确到 x.y.z）/ DataX / Canal / Debezium
2. 源库类型及版本：MySQL 5.7/8.0/8.4+ / Oracle 11/12/19/21 / PG 9.6-14 / MongoDB 3.6-7.0 / SQL Server 2012-2019
3. 目标库类型：Doris / Paimon / ClickHouse / Kafka / ES / Hive
4. 同步模式：全量+增量（snapshot+CDC）/ 纯增量 / CDC 实时流
5. 一句话描述问题：_______
```

### 版本兼容性速查

Flink CDC 与 Flink 版本不匹配是启动报错的头号原因，先排除：

| CDC 版本 | 兼容 Flink | 关键变化 |
|----------|-----------|----------|
| 3.6.* | 1.20.*, 2.2.* | 最新 |
| 3.3.* | 1.18.* ~ 2.1.* | YAML Pipeline 正式 |
| 3.0.* | 1.14.* ~ 1.18.* | 增量快照、动态加表 |
| 2.4.* | 1.13.* ~ 1.17.* | 旧版稳定 |
【官网 §概述 → Supported Flink Versions】

---

## 二、执行流程

### 步骤 1：判断问题类型

让用户从以下选项中选择（或根据描述直接判断）：

```
A. 数据量不对（多了/少了/重复）
B. 数据值不对（回退/精度/时区/NULL/乱码）
C. 任务异常（25+ 种报错类型）
D. 性能问题（延迟/OOM/反压）
E. 结构变更后异常（DDL/改类型/主从切换）
F. 数据管道完整性（空洞/断流/顺序错乱）
```

---

### 步骤 2：按分支执行

─────────────────────────────────
#### 分支 A：数据量不对
─────────────────────────────────

**子分支 A1：目标库比源库少**

→ 执行脚本 `scripts/diag_compare_count.sh` 按小时对比两端数据量

- **现象 a：凌晨固定小时缺失** → 时区问题。源库时区、业务代码时区、数仓分区时区三处不一致。增量窗口前后各加 1h 缓冲，下游去重。【官网 §MySQL CDC → server-time-zone】
- **现象 b：均匀偏少（所有小时都少）** → 增量切分字段有问题：
  - 检查切分字段是否来自业务代码（有 NULL 或未来时间戳）
  - 修复：改用自增 ID 范围或 binlog 位点切分
- **现象 c：只有特定 ID 段缺失** → 快照分片键（chunk key）在快照期间被更新，行被遗漏。检查 chunk key 是否非主键列。修复：改回主键第一列。【官网 §MySQL CDC → scan.incremental.snapshot.chunk.key-column 警告】
- **现象 d：大表快照完成后有零星缺失** → 检查 `scan.incremental.snapshot.backfill.skip` 是否被设为 true。如果是，关闭它（默认 false）。【官网 §MySQL CDC → backfill.skip】

**子分支 A2：目标库比源库多**

→ 先执行 `LEFT JOIN` 找出"目标库有但源库无"的主键

- **现象 a：存在幽灵记录（目标库多出旧数据）** → 业务有物理 DELETE，但增量同步未捕获 DELETE 事件
  - 方案一：推业务改逻辑删除（加 `is_deleted` 字段）
  - 方案二：开启 `scan.read-changelog-as-append-only=true` + 配合 `row_kind` 元数据做软删（所有变更转 INSERT，下游按 row_kind 判断操作类型）【官网 §MySQL CDC → scan.read-changelog-as-append-only】
  - 方案三：用 binlog 监听工具（Canal/Debezium）单独捕获 DELETE 事件写入删除日志表
- **现象 b：存在主键重复** → 写入端未做幂等
  - `SELECT id, COUNT(*) FROM target GROUP BY id HAVING COUNT(*) > 1` 确认
  - 修复：目标表启用 UPSERT 语义，或以最新时间戳覆盖旧值

**子分支 A3：数据先少后多（波动型）**

→ 检查是否启用了 `debezium.snapshot.select.statement.overrides` 导致数据范围不一致
→ 检查快照阶段和增量阶段是否有重叠窗口
→ 修复：不要同时设置 `scan.startup.mode` 和 `debezium.snapshot.mode`，两者冲突【官网 §Db2 CDC → 启动模式】

─────────────────────────────────
#### 分支 B：数据值不对
─────────────────────────────────

**子分支 B1：最终值不是最新值（最隐蔽的问题）**

**独特特征：** CDC 日志 ✅ → Flink 日志 ✅ → 目标库日志 ✅，但值是旧的——所有日志都正常。

→ **根因**：目标库使用"后写入覆盖"语义（Doris Unique Key / Paimon LSM-Tree），CDC 管道中同一条记录的多次变更经并行处理后写入顺序被打乱。

→ 执行脚本 `scripts/diag_sequence_col.sh` 检查是否启用 Sequence Column

| Sequence Column 状态 | 诊断 | 修复 |
|---------------------|------|------|
| 未启用 | 目标库按写入版本号排序，乱序时旧值覆盖新值 | `ALTER TABLE orders ENABLE FEATURE "SEQUENCE_LOAD" WITH PROPERTIES ("function_column.sequence_type" = "DATETIME")` |
| 已启用但用秒级 DATETIME | 同秒多次变更无法区分 | 改为毫秒时间戳或 BIGINT 自增版本号 |
| 已启用且粒度足够 | 不是乱序问题 → 走其他分支排查 | |

**验证 Sequence Column：** `SET show_hidden_columns=true; DESC orders;` 应看到 `__DORIS_SEQUENCE_COL__`

**子分支 B2：数值精度不对**

| 现象 | 根因 | 确认 | 修复 |
|------|------|------|------|
| 小数末尾多出随机数字 | DECIMAL(65, s) → DECIMAL(38, s) 截断 | `SELECT NUMERIC_PRECISION FROM INFORMATION_SCHEMA.COLUMNS WHERE DATA_TYPE='decimal' AND NUMERIC_PRECISION > 38` | Flink DDL 中映射为 STRING |
| 正数变成负数 | BIGINT UNSIGNED → BIGINT 溢出 | `SELECT * FROM target WHERE id < 0` 找到应为正数的记录 | 映射为 DECIMAL(20, 0) |
| 金额值缩小了 100 倍 | DECIMAL 精度或比例配置错误 | 对比源库和目标库的比例（scale） | 修正 DDL 中的 DECIMAL(p, s) 比例 |
| 浮点数对不上 | FLOAT/DOUBLE 精度有限 | 逐条对比 | 改为 DECIMAL(p, s) 精确类型 |

【官网 §MySQL CDC → 数据类型映射】

**子分支 B3：时间字段差固定小时数**

→ **唯一原因**：MySQL TIMESTAMP 存储为 UTC，读取时按当前会话时区转换。Flink DDL 未设 `server-time-zone`，使用 JVM 默认时区。

```sql
SHOW VARIABLES LIKE 'time_zone';          -- 检查 MySQL 时区
-- 修复：在 Flink DDL 中设 server-time-zone，值必须等于 MySQL 时区
'server-time-zone' = 'Asia/Shanghai'
```

如果多个时间字段偏差不同步，说明有的字段是业务代码写入、有的是数据库自动生成。需统一时间源。【官网 §MySQL CDC → server-time-zone】

**子分支 B4：字符串乱码 / 截断**

| 现象 | 根因 | 修复 |
|------|------|------|
| 中文字符变 `???` | MySQL charset 非 UTF-8 | JDBC URL 加 `?useUnicode=true&characterEncoding=UTF-8` |
| 长文本被截断 | VARCHAR(n) 映射太短 | 改为 STRING 类型 |
| JSON 字段变成 `[Object]` | 嵌套对象未正确映射 | 声明嵌套结构 `ROW<...>` 或直接映射 STRING |
| ENUM 值变成数字索引 | ENUM 映射为 STRING 时某些驱动行为异常 | 源查询中强制 `CAST(enum_col AS CHAR)` |

**子分支 B5：NULL 变成默认值 / 默认值变成 NULL**

→ 检查 `debezium.column.propagate.source.type` 配置是否影响了空值传播
→ 检查目标表的 DEFAULT 约束是否在写入时触发了覆盖
→ 检查 Flink 侧是否有 `COALESCE` / `IFNULL` 转换逻辑

─────────────────────────────────
#### 分支 C：任务异常（最丰富的错误集）
─────────────────────────────────

**子分支 C1：启动就报错——按报错信息逐行试**

→ 按此表排查，每试一行就问用户是否修复：

| # | 报错关键词 | 数据库 | 确认命令 | 深层根因 | 修复 |
|---|-----------|--------|----------|----------|------|
| 1 | `log_bin=OFF` / `binlog not found` | MySQL | `SHOW VARIABLES LIKE 'log_bin'` | binlog 未开启 | `log_bin=ON` + `binlog_format=ROW`，重启 MySQL |
| 2 | `binlog_format` 非 ROW | MySQL | `SHOW VARIABLES LIKE 'binlog_format'` | binlog 格式错误 | 设为 `ROW` |
| 3 | `Access denied` / `REPLICATION` | MySQL | `SHOW GRANTS FOR 'user'@'%'` | CDC 用户无 REPLICATION 权限 | `GRANT SELECT, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.*` |
| 4 | `server-id` 冲突 | MySQL | `SHOW PROCESSLIST` 查看各连接 | 多个 CDC 任务使用相同 server-id | `'server-id'='5401-5404'`，范围 ≥ 并行度 |
| 5 | `ClassNotFoundException` / `NoSuchMethod` | Flink | 查 CDC JAR 版本 ↔ Flink 版本 | JAR 与 Flink 版本不兼容 | 下载匹配版本的 JAR |
| 6 | `Unsupported VERSION` / `8.4` | MySQL | `SELECT VERSION()` | MySQL 8.4 弃用了 `SHOW MASTER STATUS` | Flink CDC 会自动适配，升级到 CDC 3.0+ |
| 7 | `Archive Mode` 未开启 | Oracle | `archive log list` | 未开启归档 | `SHUTDOWN; STARTUP MOUNT; ALTER DATABASE ARCHIVELOG; ALTER DATABASE OPEN;` + `ADD SUPPLEMENTAL LOG DATA` |
| 8 | `wal_level` 非 logical | PG | `SHOW wal_level` | WAL 级别不足 | `wal_level = logical` + `max_replication_slots=10`，重启 PG |
| 9 | `replication slot "x" is active` | PG | `SELECT slot_name, active FROM pg_replication_slots` | 复制槽被另一进程占用 | 换 `slot.name` 或 KILL 旧进程 |
| 10 | `publication "dbz_publication" not found` | PG | `SELECT * FROM pg_publication` | 逻辑发布未创建 | 自动创建，或手动 `CREATE PUBLICATION dbz_publication FOR ALL TABLES` |
| 11 | `resumeToken` / `expired` | MongoDB | 查 `heartbeat.interval.ms` 配置 | resumeToken 过期 | `'heartbeat.interval.ms' = '300000'`（5分钟） |
| 12 | `Can't perform checkpoint` / checkpoint timeout | Oracle/SQL Server/PG | 看 Flink WebUI checkpoint 状态 | 大表快照阶段无法做 checkpoint | `execution.checkpointing.interval=10min`, `tolerable-failed-checkpoints=100` |
| 13 | `RELOAD` 权限 | MySQL | `SHOW GRANTS` | 旧版快照需要 RELOAD | 升级到增量快照（默认开启），或授予 RELOAD【官网 §MySQL CDC → 配置服务器】 |
| 14 | `Not enough memory` / heap space | Flink | 看 Flink 日志中的 OOM 位置 | TaskManager 内存不足 | 增大 `taskmanager.memory.process.size`，或降低并行度 |
| 15 | `BOOLEAN type not supported` | Db2 | 检查表中有无 BOOLEAN 列 | Db2 SQL Replication 不支持 BOOLEAN | 用 SMALLINT 或 CHAR(1) 替代 |
| 16 | `Decimal precision exceeds 38` | Flink | `SELECT NUMERIC_PRECISION FROM INFORMATION_SCHEMA` | 源表精度 > 38 | 映射为 STRING |
| 17 | `Table without primary key` | MySQL | `SHOW CREATE TABLE` | 表无主键 | 增量快照失败 → 加主键，或关闭增量快照【官网 §MySQL CDC → 增量快照读取】 |
| 18 | `Schema change cannot be applied` | CDC YAML | 看 Sink 日志 | DDL 变更不被目标库支持 | 关闭 `schema-change.enabled`，手动处理 DDL |
| 19 | `MongoDB change stream requires replica set` | MongoDB | `rs.status()` | 单节点不支持 change stream | 部署副本集或分片集群【官网 §MongoDB CDC → 可用性】 |
| 20 | `gRPC connection refused` | Vitess | 检查 VTGate 端口（默认 15991） | gRPC 服务不可达 | 确认 VTGate 启动且端口可访问【官网 §Vitess CDC → 设置】 |
| 21 | `hostname in certificate didn't match` | 通用 | 检查 SSL 配置 | SSL 证书主机名不匹配 | `'jdbc.properties.useSSL'='false'`（测试环境）或修正证书 |
| 22 | `Time zone not recognized` | 通用 | 检查 server-time-zone 值 | 时区字符串拼写错误 | 用 `+08:00` 格式而不是 `Asia/Shanghai`（取决于 MySQL 版本） |

**子分支 C2：运行中周期性断开**

→ 先让用户描述断开频率（固定间隔？随机？）

| 模式 | 根因 | 确认 | 修复 |
|------|------|------|------|
| **固定间隔断开**（8h/28800s） | MySQL `wait_timeout` 过短 | `SHOW VARIABLES LIKE 'wait_timeout'` | 设为 86400（24h） |
| **低流量时断开**（表没数据写入时） | 心跳未配置，binlog 位置被清理 | 检查 `heartbeat.interval` | 设 `'heartbeat.interval'='30s'`，不要禁用【官网 §MySQL CDC → heartbeat.interval】 |
| **随机断开** | 网络不稳定/SSL/防火墙 | 检查网络延迟和丢包率 | `'connect.timeout'='30s'`, `'connect.max-retries'='3'`；跨机房用专线 |
| **高峰期断开** | 源库连接数满 / 资源紧张 | `SHOW VARIABLES LIKE 'max_connections'` | 增大连接数或降低 CDC 并行度 |
| **数据量突增时断开** | Debezium 队列溢出 | `debezium.max.queue.size`（默认 8192） | 调大至 16384 \[【官网 §MySQL CDC → debezium.*】 |

**子分支 C3：从 checkpoint/savepoint 恢复失败**

→ 执行脚本 `scripts/diag_binlog_position.sh`

| 恢复报错 | 根因 | 修复方案（按推荐顺序） |
|----------|------|------------------------|
| binlog 文件不存在 | binlog 已被清理，保留期过短 | ① `'scan.startup.mode'='latest-offset'`（从当前开始，可能丢）② `'initial'`（完整重跑）③ 延长 `binlog_expire_logs_seconds` |
| GTID 集合不完整 | GTID 已被清理 | 同上三选一 |
| MongoDB resumeToken 不在 oplog 中 | 恢复间隔太长，oplog 已轮转 | 设 `heartbeat.interval.ms=300000`，缩短检查点间隔 |
| PG WAL 段已被回收 | 复制槽未及时推进，WAL 被清理 | `scan.lsn-commit.checkpoints-num-delay=10`（延迟 LSN 提交）【官网 §PG CDC → scan.lsn-commit】 |
| Oracle SCN 不在归档中 | 归档日志被清理 | 增加 fast_start_mttr_target，延长归档保留 |
| savepoint 格式不兼容 | CDC 版本升级后状态不兼容 | 旧的 savepoint 无法跨版本恢复 → 全量重跑 |
| `The slice has been abandoned` | Db2 日志被回收 | 类似 binlog 清理，需要重新 initial |

**子分支 C4：任务运行正常但数据不对**

→ **三层隔离法**：逐层确认哪一层有问题

```
第 1 层：源库 → 确认数据本身正确
  - 源库直接查询：SELECT * FROM source_table WHERE id = 'xxx'
  - 如果有问题 → 源库本身问题，不是 CDC 的锅

第 2 层：CDC 捕获 → 确认 CDC 捕获了正确的变更
  - 查看 CDC 日志（Flink TaskManager 日志，搜索该 ID 的变更）
  - 查 Debezium JSON 输出：op='c'/'u'/'d' 是否正确
  - 如果 CDC 日志中变更正确 → 源库→CDC 没问题

第 3 层：目标库 → 确认写入正确
  - 查看目标库该记录的最终值和变更时间线
  - 如果目标库值不对但 CDC 捕获正确 → 问题在 计算引擎/Flink/存储引擎
```

**子分支 C5：数据空洞——某个时间段完全没有变更**

→ 检查是否有 CDC 任务重启、暂停、故障转移事件
→ 检查 `debezium.snapshot.mode` 是否被设为 `never`（跳过快照，但可能漏数据）
→ 检查 MySQL `binlog_expire_logs_seconds`——如果短暂停机，binlog 已被清理，回来后有空洞
→ 修复：缩短心跳间隔，增长 binlog 保留期，集群配置 GTID + VIP 自动切换

**子分支 C6：数据量最终一致但顺序不对**

→ 下游消费顺序敏感的场景（如状态机：必须先支付后发货）
→ 源库→Flink CDC 保证单条记录的 binlog 顺序，但多表间或跨分片不保证
→ 如果要求精确顺序：降低并行度到 1（牺牲吞吐），或在下游按业务时间戳排序

─────────────────────────────────
#### 分支 D：性能问题
─────────────────────────────────

**子分支 D1：延迟持续升高**

→ Flink WebUI → 查看算子背压状态

| 背压位置 | 诊断 | 修复 |
|----------|------|------|
| Source HIGH + Sink 正常 | 源库读取太慢 | 检查源库负载、binlog 写入量；增大并行度 |
| Source 正常 + Sink HIGH | 目标库写入慢 | 增大并行度 + server-id 范围；调大 checkpoint 间隔（减少 flush）；检查目标库 compaction |
| 全链路 HIGH | 资源不足 | 增大 TaskManager 内存和 CPU |
| 全链路 LOW（无背压但延迟高） | 检查点超时/频繁失败 | 调大 checkpoint 间隔和超时时间 |

**子分支 D2：快照阶段太慢**

| 症状 | 根因 | 修复 |
|------|------|------|
| 大表单线程快照 | 增量快照关闭（`scan.incremental.snapshot.enabled=false`） | 开启增量快照 + 设并行度 > 1【官网 §MySQL CDC → 增量快照读取】 |
| 分片数过多 | `chunk.size` 太小（默认 8096） | 大表调大：`'scan.incremental.snapshot.chunk.size'='65536'` |
| 分片分布不均 | 主键分布不均匀（如 UUID 为主键） | 指定分布均匀的 chunk key；或设置 chunk meta group size |
| 数据稀疏（id 跳号严重） | `MAX(id)-MIN(id) >> rowCount` | 设 `chunk-key.even-distribution.factor.upper-bound=1000` |

**子分支 D3：OOM**

| 场景 | 根因 | 修复 |
|------|------|------|
| 快照阶段最后一个 chunk 爆内存 | unbounded chunk 过大 | `'scan.incremental.snapshot.unbounded-chunk-first.enabled'='true'`（默认已开）【官网 §MySQL CDC → 连接器选项】 |
| Oracle LogMiner OOM | `log.mining.strategy=online_catalog` 消耗内存 | 改用 `'debezium.log.mining.strategy'='redo_log_catalog'`【官网 §Oracle CDC → debezium.*】 |
| 高吞吐下队列 OOM | `debezium.max.queue.size` 过大（高吞吐场景积压） | 降低队列大小或增大 task 内存 |
| 大字段表 OOM | 单行数据超大（varchar(65535)/blob） | `scan.snapshot.fetch.size` 调小；拆分大字段表 |

─────────────────────────────────
#### 分支 E：结构变更后异常
─────────────────────────────────

**子分支 E1：源表加/删/改字段**

| 场景 | 行为 | 修复 |
|------|------|------|
| Flink SQL 模式加字段 | 不会自动同步 | 手动改 DDL，从 savepoint 重启 |
| CDC YAML 模式加字段 | 默认不同步。`schema-change.enabled=true` 时会同步 | 注意字段改名会先删后加→数据丢失【官网 §CDC YAML → Schema Evolution】 |
| 删字段 | 目标库仍保留 | 手动删目标库对应列 |
| 字段类型缩窄（VARCHAR 200→100） | 写入失败 | 先停 CDC，改目标表结构，再恢复 |
| DECIMAL 精度降低 | 写入失败或截断 | 同上 |
| 字段改名 | 映射为先删后加 → **数据丢失** | 不要自动同步改名，手动处理 |

**子分支 E2：gh-ost / pt-osc 在线 DDL**

这类工具通过影子表 + rename 交换来执行 DDL，CDC 感知到的是：旧表被 DROP，新表被 CREATE。

```yaml
# 尝试解析影子表事件（实验性）
'scan.parse.online.schema.changes.enabled' = 'true'
```
【官网 §MySQL CDC → 连接器选项 → scan.parse.online.schema.changes.enabled】

**子分支 E3：MySQL 8.4+ 兼容性**

MySQL 8.4 的破坏性变更：
- `SHOW MASTER STATUS` → 需改用 `SHOW BINARY LOG STATUS`
- 术语 `slave/master` → `replica/source`
- Flink CDC 3.0+ 自动适配，旧版可能报错

**子分支 E4：数据库迁移/主从切换后异常**

| 场景 | 根本原因 | 方案 |
|------|----------|------|
| **MySQL 主从切换后数据对不上** | GTID 未开启 | 集群配置 `gtid_mode=on` + `enforce_gtid_consistency=on`【官网 §MySQL CDC → MySQL 高可用性支持】 |
| **从 CDC 读从库，切换后 binlog 不一致** | 从库未设 `log-slave-updates=1` | 设 `log-slave-updates=1` |
| **PG 主从切换后复制槽丢失** | 逻辑复制槽不在新主库上 | 在新主库重建复制槽 + 重新 initial |
| **升级 Flink CDC 版本后无法恢复** | savepoint 格式不兼容 | 无法恢复，只能全量重跑 |

**子分支 E5：数据库字符集/排序规则变更**

→ 如 MySQL 从 `utf8mb3` 改为 `utf8mb4`，CDC 会正常同步新数据
→ 但旧数据已用 `utf8mb3` 存储，新旧混合可能出现排序或索引问题
→ 修复：目标表统一字符集后全量重同步

─────────────────────────────────
#### 分支 F：数据管道完整性
─────────────────────────────────

**子分支 F1：数据断流——某时间段完全没有数据流入**

→ 检查 CDC 任务是否有重启/暂停事件：
```sql
-- MySQL 侧检查 binlog 是否有空洞
SHOW BINARY LOGS;
-- 对比最近几次 checkpoint 的 binlog 文件名
```

→ 检查心跳：如果 `heartbeat.interval=0s`，低流量表无法感知断流
→ 修复：设 `heartbeat.interval=30s`；配置 PG 复制槽监控

**子分支 F2：多表间数据不同步**

→ CDC FLink 单任务多表时，一张表滞后不影响其他表
→ 如果多张表需要保持事务一致性（如订单+订单明细），需使用 Flink CDC 的事务对齐机制
→ 注意：Flink CDC 默认不保证跨表的事务边界一致性，binlog 是行级别的

**子分支 F3：Exactly-Once 被破坏**

→ 可能的原因：
- `debezium.snapshot.mode=never`（跳过快照，没有基线）
- `scan.incremental.snapshot.backfill.skip=true`（快照阶段变更被重放，at-least-once）【官网 §MySQL CDC → backfill.skip】
- 目标库不支持幂等写入（没有 UPSERT 语义）
- 修复：确保目标表有主键 + UPSERT，关闭 `backfill.skip`

─────────────────────────────────

## 三、龙蜥特殊约定

本技能不涉及 Anolis OS 系统级操作。若排查中需要安装依赖：

- 包管理用 `dnf`（Anolis 23 默认），不用 `yum`
- Java 环境：`dnf install java-11-openjdk`
- 服务管理用 `systemctl`
- 社区反馈：https://forum.openanolis.cn

---

## 四、易错点

1. **CDC 日志正常 ≠ 数据正确**：乱序覆盖时 CDC 是正常的，问题在目标库写入顺序
2. **时区只转一次不够**：源库、业务代码、数仓可能三个时区
3. **增量窗口卡太紧**：不加缓冲一定漏边界数据
4. **同时设 `scan.startup.mode` 和 `debezium.snapshot.mode`**：两者冲突，前者失效【官网 §Db2 CDC → 启动模式】
5. **非主键列做 chunk key**：官方明确警告可能数据不一致【官网 §MySQL CDC → chunk.key-column】
6. **`backfill.skip=true`**：仅 at-least-once，快照阶段变更可能被重放【官网 §MySQL CDC → backfill.skip】
7. **PG 复制槽不清理**：CDC 停掉后 WAL 会膨胀到磁盘满
8. **MongoDB 不设心跳**：慢变更表 resumeToken 过期，恢复失败
9. **MySQL 8.4 用旧版 CDC**：`SHOW MASTER STATUS` 已弃用，CDC 2.x 不兼容 8.4
10. **无视版本映射表**：Flink CDC 3.x JAR 不能在 Flink 1.13 上跑
11. **Schema Evolution 无脑开**：字段改名丢数据、类型缩窄写失败，必须配合监控
12. **Flink SQL 模式期待自动 DDL**：Flink SQL 模式不支持自动 DDL，必须手动改 DDL 重启

---

## 五、验证

修复后逐项确认：

| # | 检查项 | 方法 | 通过标准 |
|---|--------|------|----------|
| 1 | 任务运行 | Flink WebUI | RUNNING，无 FAILED |
| 2 | Checkpoint | Flink WebUI | 最近 ≥ 3 个成功 |
| 3 | 数据量 | `COUNT(*)` 对比两端 | 差值 < 0.1% |
| 4 | 数据值 | 任取 10 条逐字段对比 | 完全一致 |
| 5 | 乱序修复 | 手动模拟后到先写（先发新值后发旧值） | 目标库保留最新值 |
| 6 | 延迟 | Flink Metrics | 稳定在业务容忍范围内 |
| 7 | 稳定性 | 观察 30 分钟 | 无异常断开或报错 |
| 8 | 断流检测 | 检查心跳事件是否持续 | `heartbeat.interval` 内有数据流动 |

---

## 六、示例

### 示例 1：数据量对不上

**用户：** "今天业务库 10000 单，数仓只查到 7000 单。"

**Agent 执行：** 分支 A → A1 → 按小时对比 → 发现凌晨 2-5 点缺失 → 检查时区 → 源库 UTC-5，数仓 UTC+8 → 增量窗口前后各加 1h → 验证次日对账对齐

### 示例 2：CDC 数据乱序

**用户：** "订单已支付→已发货→已完成，查 Doris 还是已支付。CDC 和 Flink 日志正常。"

**Agent 执行：** 分支 B → B1 → 确认 CDC/Flink 日志正常 → 检查 Doris Sequence Column → 未启用 → 启用 ALTER TABLE ENABLE FEATURE "SEQUENCE_LOAD" → 验证乱序写入后保留最新值

### 示例 3：任务启动报错

**用户：** "Flink CDC 任务起不来。"

**Agent 执行：** 分支 C → C1 → 从错误表第一行开始问用户 → `SHOW VARIABLES LIKE 'log_bin'` → OFF → 开启 binlog → 确认 binlog_format=ROW → 任务启动成功

### 示例 4：恢复失败——binlog 被清理

**用户：** "CDC 任务停了一周，恢复时报错找不到 binlog。"

**Agent 执行：** 分支 C → C3 → 执行 `SHOW BINARY LOGS` → binlog 只保留了 3 天 → checkpoint 记录的文件已不存在 → 用户选 latest-offset → 接受丢 4 天数据 → 完成

### 示例 5：延迟越来越高

**用户：** "CDC 任务跑了一周，现在延迟达到 2 小时了。"

**Agent 执行：** 分支 D → D1 → Flink WebUI 看背压 → Sink HIGH → Doris 写入慢 → 增大并行度 + server-id 范围 → 增大 checkpoint 间隔 → 延迟逐步追上

---

## 参考文档

所有连接器官方文档索引：https://nightlies.apache.org/flink/flink-cdc-docs-master/zh/docs/connectors/flink-sources/overview/
