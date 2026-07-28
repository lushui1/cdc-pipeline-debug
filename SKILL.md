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

基于 Apache Flink CDC 官方文档构建。Agent 按以下步骤执行，不要跳过。

> 官方文档：https://nightlies.apache.org/flink/flink-cdc-docs-master/zh/docs/connectors/flink-sources/overview/

---

## 一、前置确认

动手前先收集这 5 项，不清楚就问用户：

```
1. CDC 工具及版本：Flink CDC（精确版本号？）/ DataX / Canal / Debezium
2. 源库类型及版本：MySQL / Oracle / PostgreSQL / MongoDB / SQL Server
3. 目标库类型：Doris / Paimon / ClickHouse / Kafka / ES
4. 同步模式：全量+增量 / 纯增量 / CDC 实时流
5. 问题一句话描述：_______
```

### 版本兼容性速查

Flink CDC 与 Flink 版本不匹配是"莫名其妙报错"的头号原因，先排除它：

| CDC 版本 | 兼容 Flink 版本 |
|----------|----------------|
| 3.6.* | 1.20.*, 2.2.* |
| 3.3.* | 1.18.*, 1.19.*, 1.20.*, 2.1.* |
| 3.0.* | 1.14.* ~ 1.18.* |
| 2.4.* | 1.13.* ~ 1.17.* |

【官网 §概述 → Supported Flink Versions】

---

## 二、执行流程

### 步骤 1：判断问题类型

让用户从以下选项中选择（或根据描述直接判断）：

```
A. 数据量不对（多了/少了）
B. 数据值不对（回退/精度/时区）
C. 任务异常（启动失败/运行断开/恢复失败）
D. 性能问题（延迟高/OOM）
E. 结构变更后异常（加字段/改类型/主从切换）
```

---

### 步骤 2：按分支执行

─────────────────────────────────
#### 分支 A：数据量不对
─────────────────────────────────

**子分支 A1：目标库比源库少**

→ 执行脚本 `scripts/diag_compare_count.sh` 按小时对比两端数据量

- **如果凌晨边界数据缺失** → 时区问题。增量窗口前后各加 1h 缓冲，下游去重。【官网 §MySQL CDC → server-time-zone】
- **如果只有主键差异（源库有目标库无）** → 检查 chunk key 是否非主键列。如果是，改回主键列。【官网 §MySQL CDC → scan.incremental.snapshot.chunk.key-column 警告】
- **如果是 DataX 类工具** → 检查增量切分字段（`modified_time`）是否有 NULL 或未来时间。如有，改用自增 ID 切分。

**子分支 A2：目标库比源库多**

- **检查物理删除**：执行 `LEFT JOIN` 找目标库有但源库无的主键
  - 如果存在 → 业务有物理 DELETE 但未捕获
  - 修复：推业务改逻辑删除；或开启 `scan.read-changelog-as-append-only` + `row_kind` 元数据做软删【官网 §MySQL CDC → scan.read-changelog-as-append-only】
- **检查主键重复**：`SELECT id, COUNT(*) FROM target GROUP BY id HAVING COUNT(*) > 1`
  - 如果存在 → 写入未做幂等，改 UPSERT

─────────────────────────────────
#### 分支 B：数据值不对
─────────────────────────────────

**子分支 B1：最终值不是最新值（CDC ✅→Flink ✅→目标库 ✅ 但值是旧的）**

→ **根因**：目标库"后写入覆盖"语义 + CDC 管道乱序。同一条记录毫秒级多次变更经并行处理后写入顺序被打乱。

→ 执行脚本 `scripts/diag_sequence_col.sh` 检查 Sequence Column 是否启用

- 未启用 → 启用 Sequence Column，用 `update_time` 或自增版本号
  ```sql
  ALTER TABLE orders ENABLE FEATURE "SEQUENCE_LOAD"
  WITH PROPERTIES ("function_column.sequence_type" = "DATETIME");
  ```
  【Doris 官方文档 §Unique Key 并发更新控制】

- 已启用但用秒级字段 → 同秒多次变更区分不了，改毫秒或 BIGINT 版本号

**子分支 B2：数值精度不对**

- DECIMAL(65, s) → DECIMAL(38, s) 截断 → Flink DDL 中声明为 STRING【官网 §MySQL CDC → 数据类型映射】
- BIGINT UNSIGNED → BIGINT 变负数 → 映射为 DECIMAL(20, 0)

**子分支 B3：时间字段差固定小时数**

→ 检查 Flink DDL 是否设了 `server-time-zone`。未设则用 JVM 默认时区，可能与数据库不一致。

- 修复：设 `'server-time-zone' = 'Asia/Shanghai'`（值应与数据库时区一致）【官网 §MySQL CDC → server-time-zone】

─────────────────────────────────
#### 分支 C：任务异常
─────────────────────────────────

**子分支 C1：启动就报错**

→ 按以下表格逐行排查，**一行一行试**：

| 报错特征 | 确认命令 | 修复 | 官网出处 |
|----------|----------|------|----------|
| `log_bin=OFF` / `binlog` | `SHOW VARIABLES LIKE 'log_bin'` | 开启 `log_bin=ON` + `binlog_format=ROW`，重启 MySQL | §配置服务器 |
| `binlog_format` 非 ROW | `SHOW VARIABLES LIKE 'binlog_format'` | 设为 ROW | §配置服务器 |
| `Access denied` / `REPLICATION` | `SHOW GRANTS FOR 'cdc_user'@'%'` | 授予 `SELECT, REPLICATION SLAVE, REPLICATION CLIENT` | §配置服务器 |
| `server-id` 冲突 / `slave id` | `SHOW PROCESSLIST` 看各连接 server-id | `'server-id'='5401-5404'`, 范围 ≥ 并行度 | §注意事项→Server id |
| `Archive Mode` 未开启 | `archive log list` | `ALTER DATABASE ARCHIVELOG` + supplemental logging | Oracle CDC §Setup |
| `wal_level` 非 logical | `SHOW wal_level` | `wal_level = logical`，重启 PG | PostgreSQL CDC §Setup |
| `replication slot active` | `SELECT slot_name, active FROM pg_replication_slots` | 换 `slot.name` | PostgreSQL CDC §slot.name |
| `ClassNotFoundException` | 查 CDC JAR 版本与 Flink 版本映射表 | 下载兼容版本的 JAR | §版本映射 |
| `resumeToken` 过期 | 查 MongoDB `heartbeat.interval.ms` | 设 `heartbeat.interval.ms = 300000` | MongoDB CDC §连接器选项 |

**子分支 C2：运行中周期性断开**

- **检查 MySQL 超时参数**：`SHOW VARIABLES LIKE 'wait_timeout'` → 设 `wait_timeout=86400`、`interactive_timeout=86400`【官网 §MySQL CDC → 注意事项→会话超时】
- **检查心跳配置**：流量低的表 binlog 位点不推进会被清理。确保 `heartbeat.interval=30s`，不要禁用心跳【官网 §MySQL CDC → heartbeat.interval】
- **网络不稳定**：跨机房场景检查延迟/丢包，配置 `connect.timeout=30s`、`connect.max-retries=3`

**子分支 C3：从 checkpoint/savepoint 恢复失败**

→ 执行脚本 `scripts/diag_binlog_position.sh` 检查 checkpoint 位点是否仍可用

- binlog 已被清理 → 三选一：
  - 从最新位点：`'scan.startup.mode' = 'latest-offset'`（可能丢数据）
  - 完整重跑：`'scan.startup.mode' = 'initial'`（慢但完整）
  - 指定位点：`'scan.startup.mode' = 'specific-offset'` + file/pos/gtid-set【官网 §MySQL CDC → scan.startup.mode】

─────────────────────────────────
#### 分支 D：性能问题
─────────────────────────────────

- **延迟高（反压）**：Flink WebUI 看 Sink 算子背压状态。HIGH → 增大并行度 + 同步扩大 server-id 范围；或增大 checkpoint 间隔
- **快照阶段延迟**：Flink Metrics 查 `isSnapshotting=true` → 正常，快照完成后 binlog 自动追上。大表调大 `chunk.size`
- **OOM**：开启 `scan.incremental.snapshot.unbounded-chunk-first.enabled=true`（默认已开）。Oracle LogMiner 改用 `debezium.log.mining.strategy=redo_log_catalog`【官网 §MySQL CDC → unbounded-chunk-first】

─────────────────────────────────
#### 分支 E：结构变更后异常
─────────────────────────────────

| 现象 | 原因 | 修复 |
|------|------|------|
| 源表加字段，目标库没有 | Flink SQL 模式不支持自动 DDL | 手动改 DDL，从 savepoint 重启 |
| 源表字段改名/类型缩窄，写入失败 | DDL 不兼容 | 避免自动同步字段改名（先删后加丢数据）；类型缩窄需评估 |
| gh-ost/pt-osc 在线 DDL 后异常 | 影子表交换导致 | 开启 `scan.parse.online.schema.changes.enabled=true`（实验性）【官网 §MySQL CDC → 连接器选项】 |
| 主从切换后数据不对 | GTID 未开启 | 集群配置 `gtid_mode=on` + `enforce_gtid_consistency=on` + 监控从库需 `log-slave-updates=1`。建议 DNS/VIP 自动切换【官网 §MySQL CDC → 高可用性支持】 |

---

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
9. **body 写触发条件**：Agent 只看 frontmatter description，body 写了无效

---

## 五、验证

修复后逐项确认：

| # | 检查项 | 方法 | 通过标准 |
|---|--------|------|----------|
| 1 | 任务运行 | Flink WebUI | RUNNING，无 FAILED |
| 2 | Checkpoint | Flink WebUI | 最近 ≥ 3 个成功 |
| 3 | 数据量 | `COUNT(*)` 对比两端 | 差值 < 0.1% |
| 4 | 数据值 | 任取 10 条逐字段对比 | 完全一致 |
| 5 | 乱序修复 | 手动模拟后到先写 | 目标库保留最新值 |
| 6 | 延迟 | Flink Metrics | 稳定在容忍范围内 |
| 7 | 稳定性 | 观察 30 分钟 | 无异常断开 |

---

## 六、示例

### 示例 1：数据量对不上

**用户：** "今天业务库 10000 单，数仓只查到 7000 单。"

**Agent 执行路径：** 分支 A → A1 → 按小时对比 → 发现凌晨缺失 → 检查时区 → 修复增量窗口 → 验证

### 示例 2：CDC 数据乱序

**用户：** "订单已支付→已发货→已完成，查目标库还是已支付。CDC 和 Flink 日志正常。"

**Agent 执行路径：** 分支 B → B1 → 检查 Sequence Column → 未启用 → 启用并验证

### 示例 3：任务启动报错

**用户：** "Flink CDC 任务起不来。"

**Agent 执行路径：** 分支 C → C1 → 按错误表逐行试 → 发现 `log_bin=OFF` → 开启 binlog → 验证

---

## 参考文档

所有连接器官方文档索引：https://nightlies.apache.org/flink/flink-cdc-docs-master/zh/docs/connectors/flink-sources/overview/
