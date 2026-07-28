---
name: cdc-pipeline-debug
description: 诊断与修复 CDC 增量同步/实时数据管道中的数据一致性问题。当用户说"数据对不上""增量同步少了""CDC丢数据""数据值回退""精度丢失""时区偏差""任务启动失败""运行中断""恢复失败""OOM""延迟堆积""加字段未同步""改类型写入失败""主从切换异常""binlog""server-id""GTID""Sequence Column""数据类型映射""复制槽""WAL""LogMiner""checkpoint超时""schema evolution""heartbeat""增量快照""chunk key""debezium"时**立刻触发**。即使用户只说"实时数据不准""业务库和数仓对不上""flink任务起来就挂""数据恢复不了"而上下文是数据管道/ETL/实时数仓，也**应该触发**。基于 Apache Flink CDC 官方文档构建，每条诊断标注出处。不依赖用户具体架构，描述现象即可用。
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
  - 官方文档
suggested_sig: middleware
contributor_type: personal
---

# CDC 管道故障排查手册 / CDC Pipeline Symptom Diagnosis

基于 Apache Flink CDC 官方文档构建的症状驱动排查手册。

> 官方文档：https://nightlies.apache.org/flink/flink-cdc-docs-master/zh/docs/connectors/flink-sources/overview/

---

## 一、前置条件

用本技能排查前，先收集以下信息：

1. **CDC 工具与版本**：Flink CDC（精确版本号？）、DataX、Canal、Debezium——版本决定功能和已知 bug
2. **源库类型与版本**：MySQL 5.7/8.0/8.4+、Oracle 11/12/19/21、PostgreSQL 9.6-14、MongoDB 3.6-7.0、SQL Server 2012-2019 等
3. **目标库类型**：Doris / Paimon / ClickHouse / Kafka / Elasticsearch
4. **同步模式**：全量+增量（snapshot + CDC）/ 纯增量 / CDC 实时流
5. **问题特征**：数据量少/多、值不对、任务报错、延迟高、恢复失败

### Flink CDC 版本兼容性

先确认版本匹配，不匹配是很多"莫名其妙"报错的根因：

| CDC 版本 | 兼容 Flink 版本 |
|----------|----------------|
| 3.6.* | 1.20.*, 2.2.* |
| 3.5.* | 1.19.*, 1.20.* |
| 3.3.* | 1.18.*, 1.19.*, 1.20.*, 2.1.* |
| 3.0.* | 1.14.* ~ 1.18.* |
| 2.4.* | 1.13.* ~ 1.17.* |

【官网 §概述 → Supported Flink Versions】

---

## 二、症状排查入口

根据用户描述的现象跳转到对应症状章节。

| 现象 | 看哪节 |
|------|--------|
| 目标库记录比源库少 | → 症状 A |
| 目标库记录比源库多（重复/幽灵数据） | → 症状 B |
| 数据值不是最新值，回退了 | → 症状 C |
| 数值精度不对、小数错位 | → 症状 D |
| 时间差固定小时数 | → 症状 E |
| 任务一启动就报错 | → 症状 F |
| 运行中周期性断开 | → 症状 G |
| 从 checkpoint 恢复失败 | → 症状 H |
| 延迟越来越大（反压） | → 症状 I |
| OOM 崩溃 | → 症状 J |
| 源表加字段/改类型后同步异常 | → 症状 K |
| 数据库主从切换后异常 | → 症状 L |

---

### 症状 A：目标库记录数比源库少

> **反向排查：** 按小时对比两边数据量，确定缺失的时间区间。

#### 原因 A1：增量切分字段时区不统一，边界数据跨天错位

**定位依据：** 每天固定某个小时（通常是凌晨整点）的数据量差异最大。

**确认：** 运行以下 SQL 对比两边的按小时统计，检查差异集中时段。

**修复：** 增量窗口前后各加 1h 缓冲，宁可重复不可遗漏。下游去重层保障。
【官网 §MySQL CDC → server-time-zone】

---

#### 原因 A2：增量切分字段来自业务代码写入，有 NULL 或未来时间

**定位依据：** `modified_time` 非数据库自动维护，存在空洞。

**确认：** 检查切分字段的 NULL 比例和是否含未来时间。

**修复：** 改用 binlog 位点或自增 ID 范围切分。Flink CDC 基于 binlog 不受此影响——此问题主要影响 DataX 等工具。

---

#### 原因 A3：业务有物理 DELETE，增量未捕获

**定位依据：** 目标库存在源库已查不到的记录。

**确认：** `LEFT JOIN` 查主键差集。

**修复：** 推业务改逻辑删除；或用 binlog 监听 DELETE；或开启 `scan.read-changelog-as-append-only` 配合 `row_kind` 元数据字段做逻辑删除。【官网 §MySQL CDC → 连接器选项 → scan.read-changelog-as-append-only.enabled / 支持的元数据 → row_kind】

---

#### 原因 A4：快照分片键不是主键列，并发更新导致行遗漏

**官方警告：** "使用非主键列作为分片键可能会导致数据不一致。"【官网 §MySQL CDC → 连接器选项 → scan.incremental.snapshot.chunk.key-column】

**定位依据：** chunk key 配了非主键列。

**修复：** 改回主键第一列（默认）。无主键表需加主键或关闭增量快照。

---

### 症状 B：目标库记录数比源库多

#### 原因 B1：业务物理 DELETE 未捕获（同 A3）

见症状 A → 原因 A3。

---

#### 原因 B2：增量时间窗口缓冲过大，产生重复

**定位依据：** 查目标库主键是否有重复值。

**确认：** `SELECT id, COUNT(*) FROM target GROUP BY id HAVING COUNT(*) > 1`

**修复：** 在写入层做 UPSERT（幂等写入），以最新时间戳为准。

---

### 症状 C：数据最终值不是最新值

CDC 日志 ✅ → 引擎日志 ✅ → 目标库日志 ✅ → 但值是旧的。

#### 原因 C1：目标库"后写入覆盖"语义 + CDC 管道乱序

**根因：** Doris Unique Key / Paimon LSM-Tree 默认按写入版本号排序。CDC 管道中同一条记录的多次变更经并行处理后写入顺序被打乱——后发生的变更先入库，先发生的变更后入库，旧值覆盖了新值。

**定位依据：** 在源库查变更时间线 → 在目标库查最终值。发现最终值不是时间戳最大的那条。

**修复：** 启用 Sequence Column，用业务时间戳或自增版本号作排序依据。
```
Doris: ALTER TABLE orders ENABLE FEATURE "SEQUENCE_LOAD"
       WITH PROPERTIES ("function_column.sequence_type" = "DATETIME");
```
【Doris 官方文档 §Unique Key 并发更新控制】

---

#### 原因 C2：Sequence Column 粒度过粗

秒级时间戳在同秒多次变更时区分不了。

**修复：** 用毫秒字段或自增 BIGINT 版本号。

---

### 症状 D：数值精度不对

#### 原因 D1：DECIMAL 精度 > 38 被截断

**定位依据：** 源表 `DECIMAL(65, s)` 映射为 Flink `DECIMAL(38, s)`。

**修复：** 精度 > 38 的字段在 Flink DDL 中声明为 STRING。【官网 §MySQL CDC → 数据类型映射】

---

#### 原因 D2：BIGINT UNSIGNED 映射为 BIGINT 产生负数

**修复：** 映射为 `DECIMAL(20, 0)`。

---

### 症状 E：时间字段差固定小时数

#### 原因 E1：未设置 `server-time-zone`

MySQL TIMESTAMP 存储为 UTC，读取时按会话时区转换。如果 Flink DDL 未设 `server-time-zone`，则用 `ZoneId.systemDefault()`，可能与数据库实际时区不一致。

**确认：** `SHOW VARIABLES LIKE 'time_zone';` 对比 Flink DDL 中的时区设置。

**修复：** `'server-time-zone' = 'Asia/Shanghai'`（值应与数据库一致）。【官网 §MySQL CDC → 连接器选项 → server-time-zone】

---

### 症状 F：任务启动就报错

| 现象关键词 | 可能原因 | 确认 | 修复 | 出处 |
|-----------|---------|------|------|------|
| `log_bin=OFF` | binlog 未开 | `SHOW VARIABLES LIKE 'log_bin'` | 开启 binlog + `binlog_format=ROW` | 【MySQL CDC §配置服务器】 |
| `binlog_format` 非 ROW | binlog 格式不对 | `SHOW VARIABLES LIKE 'binlog_format'` | 设为 ROW | 【同上】 |
| `Access denied` / `REPLICATION` | CDC 用户权限不足 | `SHOW GRANTS` | 授予 `SELECT, REPLICATION SLAVE, REPLICATION CLIENT` | 【MySQL CDC §配置服务器】 |
| `slave id` / `conflict` | server-id 冲突 | `SHOW PROCESSLIST` 查看 | 用范围分配 `'server-id'='5401-5404'`，范围 ≥ 并行度 | 【MySQL CDC §注意事项 → Server id】 |
| `replication slot active` | PG 复制槽被占 | `SELECT slot_name, active FROM pg_replication_slots` | 换 slot.name | 【PG CDC §slot.name】 |
| `Archive Mode` 未开启 | Oracle 未开归档 | `archive log list` | 开启 ARCHIVELOG + supplemental logging | 【Oracle CDC §Setup】 |
| `wal_level` 非 logical | PG WAL 级别不对 | `SHOW wal_level` | `wal_level = logical` | 【PG CDC §Setup】 |
| `resumeToken` | MongoDB token 过期 | 检查心跳配置 | `heartbeat.interval.ms = 300000` | 【MongoDB CDC §连接器选项】 |

---

### 症状 G：任务运行中周期性断开

#### 原因 G1：MySQL `wait_timeout` / `interactive_timeout` 过短

大表快照阶段耗时长，连接空闲被关闭。【官网 §MySQL CDC → 注意事项 → 设置 MySQL 会话超时时间】

**确认：** `SHOW VARIABLES LIKE 'wait_timeout'`

**修复：** 在 my.cnf 中将两个值设为 86400（24 小时）。

---

#### 原因 G2：心跳未配置，慢变更表 binlog 被清理

**确认：** 检查 `heartbeat.interval`（默认 30s）。如果设为 `0s` 则已禁用。

**修复：** `'heartbeat.interval' = '30s'`，不要禁用。【官网 §MySQL CDC → 连接器选项 → heartbeat.interval】

---

### 症状 H：从 checkpoint/savepoint 恢复失败

#### 原因 H1：binlog 已被清理，恢复位点不存在

**确认：** `SHOW BINARY LOGS` 对比 checkpoint 记录的 binlog 文件。

**修复：**
- 从最新位点（丢数据）：`'scan.startup.mode' = 'latest-offset'`
- 重新 initial（完整但慢）：`'scan.startup.mode' = 'initial'`
- 指定可用位点：`'scan.startup.mode' = 'specific-offset'` + file/pos/gtid-set
【官网 §MySQL CDC → 连接器选项 → scan.startup.mode】

---

### 症状 I：延迟持续增大（反压）

#### 原因 I1：目标库写入能力跟不上

**确认：** Flink WebUI → Sink 算子背压状态 HIGH。

**修复：** 增加并行度 + 同步扩大 server-id 范围；或增大 checkpoint 间隔减少写入冲突。

---

#### 原因 I2：快照阶段 binlog 正常排队

**确认：** Flink Metrics 显示 `isSnapshotting = true` → 仍在全量阶段。

**说明：** 这是正常行为，快照完成后会追上。大表可以调大 `chunk.size`（默认 8096）。

---

### 症状 J：大表快照 OOM

#### 原因 J1：unbounded chunk 未优先分配

**修复：** `'scan.incremental.snapshot.unbounded-chunk-first.enabled' = 'true'`（默认已开启）。仍 OOM 则增大 TaskManager 内存。

【官网 §MySQL CDC → 连接器选项 → scan.incremental.snapshot.unbounded-chunk-first.enabled】

---

### 症状 K：源表结构变更后同步异常

- **加字段未同步**：Flink CDC SQL 模式不支持自动 DDL 同步，需手动改 DDL 后从 savepoint 重启。CDC YAML 模式可开启 `schema-change.enabled`，但注意字段改名丢数据。
- **改类型写入失败**：类型缩窄（如 VARCHAR(200)→VARCHAR(100)）或 DECIMAL 精度缩减会导致写入失败。
- **gh-ost/pt-osc 在线 DDL**：开启 `scan.parse.online.schema.changes.enabled`（实验性）。【官网 §MySQL CDC → 连接器选项】

---

### 症状 L：数据库主从切换后 CDC 异常

#### 原因 L1：MySQL GTID 未开启

**定位依据：** `SHOW VARIABLES LIKE 'gtid_mode'` → OFF。

**修复：** 集群配置 GTID：`gtid_mode=on` + `enforce_gtid_consistency=on`。监控从实例还需 `log-slave-updates=1`。配置 DNS/VIP 可自动切换。【官网 §MySQL CDC → MySQL 高可用性支持】

---

## 三、龙蜥特殊约定

本技能为 **通用型 CDC 排障技能**，不涉及 Anolis OS 系统级操作。如果排查中发现需要安装/更新 CDC 组件或依赖：

- 包管理用 `dnf` 而非 `yum`（Anolis 23 默认 dnf）
- 系统服务管理用 `systemctl`
- Java 环境安装：`dnf install java-11-openjdk`
- 社区支持与反馈：https://forum.openanolis.cn

如需适配 Anolis OS 特定环境，可参考龙蜥 SIG 相关文档。

---

## 四、易错点（Pitfalls）

1. **CDC 日志正常 ≠ 数据正确**：乱序覆盖时 CDC 是正常的，问题在目标库写入顺序
2. **时区只转一次不够**：源库时区、业务代码时区、数仓分区时区可能三个都不一样
3. **增量窗口卡太紧**：不加缓冲一定漏边界数据
4. **同时设置 `scan.startup.mode` 和 `debezium.snapshot.mode`**：两者会冲突，导致 `scan.startup.mode` 不生效【官网 §Db2/ SQL Server CDC → 启动模式】
5. **非主键列做 chunk key**：官方明确警告可能导致数据不一致
6. **skip backfill**：`scan.incremental.snapshot.backfill.skip=true` 仅保证 at-least-once，快照阶段变更可能被重放【官网 §MySQL CDC → 连接器选项】
7. **复制槽不清理（PG）**：CDC 停掉后槽不自动释放，WAL 会膨胀到磁盘满
8. **MongoDB 不设心跳**：慢变更表的 resumeToken 会过期，恢复失败
9. **body 里写触发条件**：Agent 识别是否使用技能只看 frontmatter 的 description，body 里写触发条件无效，白占 context

---

## 五、验证（Verification）

修复完成后按以下清单确认：

| 检查项 | 验证方法 | 通过标准 |
|--------|----------|----------|
| 任务运行 | Flink WebUI | Job 状态 RUNNING，无 FAILED |
| Checkpoint | Flink WebUI | 最近 >= 3 个 checkpoint 成功 |
| 数据量对账 | `SELECT COUNT(*)` 对比两端 | 差值 < 0.1% |
| 数据值正确 | 任取 10 条逐字段对比 | 完全一致 |
| 乱序修复 | 手动模拟乱序写入 | 目标库保留时间戳最大那条 |
| 延迟 | Flink Metrics | 端到端延迟在业务容忍范围内 |
| 稳定性 | 观察 30 分钟 | 无异常断开或报错 |

---

## 六、示例（Examples）

### 示例 1：增量数据对不上——用户说"数仓比业务库少了 3000 条"

**用户陈述：** "今天业务库开了 10000 单，数仓只查到 7000 单。"

**排查路径：**
1. 按小时对比 → 凌晨 2-5 点数仓缺失
2. 查时区 → 业务库 UTC-5，数仓 UTC+8，凌晨数据跨天错位
3. 修复：增量窗口前后加 1 小时缓冲
4. 验证：次日差值消失

---

### 示例 2：CDC 数据乱序——用户说"订单状态回退了"

**用户陈述：** "订单从已支付→已发货→已完成，但目标库还是已支付。CDC 和 Flink 日志都正常。"

**排查路径：**
1. 源库查变更时间线 → 三次变更完整
2. 目标库查最终值 → 是"已支付"（旧值）
3. 定位：CDC 正常，Flink 正常，问题在目标库写入顺序
4. 修复：启用 Sequence Column
5. 验证：乱序场景下目标库保留最新值

---

### 示例 3：任务启动报错——用户说"Flink CDC 任务起不来"

**用户陈述：** "MySQL CDC 任务提交就报错。"

**排查路径：**
1. `log_bin` 状态？ → OFF → 开启 `log_bin=ON` + `binlog_format=ROW`，重启 MySQL
2. CDC 用户权限？ → 缺 REPLICATION SLAVE → 补授权
3. `server-id` 冲突？ → 指定范围 `5401-5404`
4. 验证：任务启动成功

---

## 七、诊断命令速查

```sql
-- MySQL
SHOW VARIABLES LIKE 'log_bin';
SHOW VARIABLES LIKE 'binlog_format';
SHOW VARIABLES LIKE 'gtid_mode';
SHOW VARIABLES LIKE 'server_id';
SHOW BINARY LOGS;
SHOW VARIABLES LIKE 'binlog_expire_logs_seconds';
SHOW VARIABLES LIKE 'wait_timeout';
SHOW PROCESSLIST;

-- PostgreSQL
SELECT slot_name, active, wal_status FROM pg_replication_slots;
SHOW wal_level;

-- Oracle
archive log list;
SELECT supplemental_log_data_min FROM v$database;
```

---

## 八、参考文档

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
| 总览 | https://nightlies.apache.org/flink/flink-cdc-docs-master/zh/docs/connectors/flink-sources/overview/ |
