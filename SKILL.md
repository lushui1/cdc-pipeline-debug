---
name: cdc-pipeline-debug
description: 诊断与修复 CDC 增量同步 / 实时数据管道中的数据一致性问题。当用户说「数据对不上」「丢数据」「对不齐」「同步慢」「延迟高」「起不来」「报错了」「挂了」「断了」「连不上」「超时」「重启」「恢复」「加字段」「删字段」「改类型」「DDL」「迁移」「扩容」「升级」「降级」「改密码」「主从切换」「缺数据」「没数据」「binlog」「怎么配」「第一次搭」或上下文涉及数据同步、ETL、实时数仓、数据库复制、Canal、Debezium、DataX、DTS、DMS 时**立刻触发**。即使用户只说「数据有问题」「同步停了」「帮我看看」「数据对吗」「对个账」而上下文是数据管道/ETL/实时数仓，也**应该触发**。覆盖 Flink CDC、Canal、Debezium、DataX、Kafka Connect、DTS、AWS DMS 等工具。基于 Apache Flink CDC 官方文档构建。不依赖用户具体架构，描述现象即可用。
version: 1.0.0
author: open-anolis
os_support:
  - Anolis OS 8
  - Anolis OS 23
  - 通用
tags:
  - 数据中间件
  - CDC
  - 故障排查
  - Flink CDC
  - 龙蜥官方
suggested_sig: middleware
contributor_type: personal
---

# CDC 管道故障排查手册

基于 Apache Flink CDC 官方文档构建。此技能供 Agent 按步骤排查 CDC 管道问题。官方文档：https://nightlies.apache.org/flink/flink-cdc-docs-master/zh/docs/connectors/flink-sources/overview/

---

## 一、前置确认

动手前先收集以下 5 项信息。不确定就问用户。

```
1. CDC 工具与版本（Flink CDC x.y / Canal / Debezium / DataX / DTS 等）
2. 源库类型（MySQL / Oracle / PG / MongoDB / SQL Server 等）
3. 目标库类型（Doris / Kafka / ES / ClickHouse / Hive 等）
4. 同步模式（全量+增量 / 纯增量 / CDC 实时流）
5. 一句话描述问题：_______
```

### 版本兼容性

Flink CDC 与 Flink 版本不匹配是启动报错头号原因。先确认：

| CDC 版本 | 兼容 Flink 版本 |
|----------|----------------|
| 3.6.* | 1.20.*, 2.2.* |
| 3.3.* | 1.18.* ~ 2.1.* |
| 3.0.* | 1.14.* ~ 1.18.* |
| 2.4.* | 1.13.* ~ 1.17.* |

【官网 §概述 → Supported Flink Versions】

---

## 二、执行步骤

按以下流程顺序执行，不要跳过。

### 步骤 1：判断问题类型

看用户描述，匹配一个类型：

- **A 数据量不对**：少了、多了、重复、对不上、缺数据
- **B 数据值不对**：是旧的、回退了、精度不对、时间差、乱码
- **C 任务异常**：起不来、报错、挂了、断了、恢复失败、超时
- **D 性能问题**：延迟高、追不上、卡、OOM、CPU高
- **E 结构变更**：加字段、改表、DDL、字段类型变了
- **F 运维操作**：迁移、扩容、升级、改密码、主从切换
- **G 配置搭建**：第一次、怎么配、选哪个、不会
- **H 其他工具**：Canal、Debezium、DataX、DTS、DMS

如果用户说得很模糊（如"数据有问题""帮我看看"），先问 3 个追问：
```
问 1：数据量有问题还是数据值有问题？还是任务跑不起来？
问 2：源库是什么、目标库是什么？
问 3：是第一次搭建还是以前正常、最近才出的问题？
```

---

### 步骤 2：按类型执行

#### 类型 A：数据量不对

**子步骤 A1：确认是多还是少**

让用户分别对源库和目标库执行 COUNT(*)。对比结果。

**子步骤 A2：如果是目标库比源库少**

A2a. 按小时对比数据量，查看差异集中在哪个时段。
- 如果每天凌晨固定小时缺失 → **时区问题**。增量窗口前后各加 1 小时缓冲，下游去重。【官网 §MySQL CDC → server-time-zone】
- 如果所有小时均匀偏少 → **增量切分字段不可靠**。检查 modified_time 是否有 NULL 或未来时间。改用自增 ID 范围切分。

A2b. 检查 chunk key是否非主键列。如果是，改回主键列。【官网 §MySQL CDC → scan.incremental.snapshot.chunk.key-column 警告】

**子步骤 A3：如果是目标库比源库多**

A3a. 执行 LEFT JOIN 找出"目标库有但源库无"的记录 → **物理 DELETE 未捕获**。
- 修复：推业务改逻辑删除；或开启 scan.read-changelog-as-append-only + row_kind 元数据做软删。【官网 §MySQL CDC → scan.read-changelog-as-append-only】

A3b. 检查主键是否重复：`SELECT id, COUNT(*) FROM target GROUP BY id HAVING COUNT(*) > 1`
- 如果重复 → 写入端未做幂等，改为 UPSERT 语义。

---

#### 类型 B：数据值不对

**子步骤 B1：确认是什么类型的数据不对**

- 时间字段差固定小时数 → **server-time-zone 未设置**。设 `'server-time-zone'='Asia/Shanghai'`（值与 MySQL 时区一致）。【官网 §MySQL CDC → server-time-zone】
- 小数精度不对 → **DECIMAL(65+) 在 Flink 中被截断为 DECIMAL(38,s)**。在 Flink DDL 中声明为 STRING。【官网 §MySQL CDC → 数据类型映射】
- 正数变成负数 → **BIGINT UNSIGNED 溢出**。映射为 DECIMAL(20,0)。
- 字符串乱码 → **字符集不匹配**。JDBC URL 加 `?useUnicode=true&characterEncoding=UTF-8`。
- NULL 变成默认值或默认值变成 NULL → 检查 DDL 约束和 Flink 转换逻辑。

**子步骤 B2：检查是否乱序覆盖（最隐蔽的问题）**

特征：CDC 日志正常、Flink 日志正常、目标库日志正常，但最终值是旧的。

2a. 在源库查该记录的变更时间线，在目标库查最终值。如果最终值不是时间戳最大的那条 → 乱序覆盖。
2b. 检查目标库是否启用 Sequence Column。未启用则启用，用 update_time 或自增版本号排序。
```sql
ALTER TABLE orders ENABLE FEATURE "SEQUENCE_LOAD"
WITH PROPERTIES ("function_column.sequence_type" = "DATETIME");
```

---

#### 类型 C：任务异常

**子步骤 C1：启动就报错**

询问用户看到的报错信息。按以下表格逐行匹配：

| 报错关键词 | 确认命令 | 修复方案 |
|-----------|----------|----------|
| log_bin / binlog | SHOW VARIABLES LIKE 'log_bin' | 开启 log_bin=ON + binlog_format=ROW，重启 MySQL |
| Access denied / REPLICATION | SHOW GRANTS | 授予 SELECT, REPLICATION SLAVE, REPLICATION CLIENT |
| server-id 冲突 / slave id | SHOW PROCESSLIST 查看各连接 | 设 server-id=5401-5404，范围 >= 并行度 |
| Archive Mode 未开启 | archive log list | ALTER DATABASE ARCHIVELOG + ADD SUPPLEMENTAL LOG DATA |
| wal_level 非 logical | SHOW wal_level | 设为 logical，重启 PG |
| replication slot active | SELECT slot_name, active FROM pg_replication_slots | 换 slot.name 或 KILL 旧进程 |
| ClassNotFoundException | 查 CDC JAR 版本 vs Flink 版本 | 下载匹配版本的 JAR |
| resumeToken 过期 | 检查 heartbeat.interval.ms | 设 heartbeat.interval.ms=300000 |
| Decimal precision 38 | 查表结构 | 映射为 STRING |
| 无主键 | SHOW CREATE TABLE | 加主键或关闭增量快照 |
| 连接失败 / timeout | ping / telnet | 检查连通性，加大超时配置 |

**子步骤 C2：运行中断开**

- 固定间隔断开 → 检查 MySQL wait_timeout，设 86400。【官网 §MySQL CDC → 注意事项】
- 低流量时断开 → 检查 heartbeat.interval（默认 30s），不要禁用。【官网 §MySQL CDC → heartbeat.interval】
- 随机断开 → 检查网络/SSL，配置 connect.timeout=30s、connect.max-retries=3。

**子步骤 C3：恢复失败**

执行 scripts/diag_binlog_position.sh 或手动检查：
```
SHOW BINARY LOGS;  -- 看 checkpoint 记录的文件是否还在
```
- 日志还在 → 从 checkpoint/savepoint 正常恢复。
- 日志被清理 → 三选一：latest-offset（丢数据）/ initial（重跑全量+增量）/ specific-offset（指定位点）。【官网 §MySQL CDC → scan.startup.mode】

---

#### 类型 D：性能问题

**子步骤 D1：延迟高**

Flink WebUI 查看背压：
- Source HIGH + Sink 正常 → 源库读取慢。增大并行度 + server-id 范围。
- Source 正常 + Sink HIGH → 目标库写入慢。增大 checkpoint 间隔；检查目标库 compaction。
- 全链路 HIGH → 资源不足。增大 TaskManager 内存。
- 全链路 LOW 但延迟高 → checkpoint 太频繁。增大 execution.checkpointing.interval。

**子步骤 D2：OOM**

- 快照阶段 OOM → 开启 scan.incremental.snapshot.unbounded-chunk-first.enabled=true（默认已开）。增大 TaskManager 内存。【官网 §MySQL CDC → 连接器选项】
- Oracle LogMiner OOM → debezium.log.mining.strategy=redo_log_catalog。【官网 §Oracle CDC → debezium.*】
- 高吞吐队列 OOM → 降低 debezium.max.queue.size。

---

#### 类型 E：结构变更

| 场景 | 原因 | 操作 |
|------|------|------|
| 源表加字段，目标库没有 | Flink SQL 模式不支持自动 DDL | 手动改 DDL，从 savepoint 重启。或切换到 CDC YAML 模式并开启 schema-change.enabled。 |
| 字段改名 | 映射为先删后加，数据丢失 | 不要自动同步改名，手动处理。 |
| 字段类型缩窄（VARCHAR 200→100） | 写入失败 | 先停 CDC，改目标表结构，再恢复。 |
| gh-ost/pt-osc 在线 DDL 后异常 | 影子表交换 | 开启 scan.parse.online.schema.changes.enabled=true（实验性）。【官网 §MySQL CDC → 连接器选项】 |
| MySQL 8.4 兼容性 | SHOW MASTER STATUS 已弃用 | 使用 CDC 3.0+（自动适配）。【官网 §MySQL CDC → MySQL 8.4+ 兼容性】 |

---

#### 类型 F：运维操作

**子步骤 F1：主从切换后异常**

检查 GTID：`SHOW VARIABLES LIKE 'gtid_mode'`
- OFF → 集群必须配置 gtid_mode=on + enforce_gtid_consistency=on。监控从库还需要 log-slave-updates=1。
- ON → 切换 hostname 后从 checkpoint 恢复。建议配置 DNS/VIP 实现自动切换。【官网 §MySQL CDC → MySQL 高可用性支持】

**子步骤 F2：迁移/升级后异常**

- CDC 版本升级后 savepoint 不兼容 → 只能全量重跑 initial。
- 源库版本升级后 CDC 异常 → 检查 binlog_format 是否被改回非 ROW 值。
- 改密码后报错 → 确认所有 CDC 配置文件的连接串都已更新。

---

#### 类型 G：配置搭建

给出 Flink CDC 最小配置示例：

```sql
CREATE TABLE source_table (
  id INT, name STRING,
  PRIMARY KEY(id) NOT ENFORCED
) WITH (
  'connector' = 'mysql-cdc',
  'hostname' = 'localhost',
  'port' = '3306',
  'username' = 'cdc_user',
  'password' = '***',
  'database-name' = 'mydb',
  'table-name' = 'source',
  'server-id' = '5401-5404',
  'server-time-zone' = 'Asia/Shanghai',
  'heartbeat.interval' = '30s'
);
```

参考 `assets/flink-cdc-yaml-template.yaml` 获取整库同步配置模板。
参考 `assets/ddl-templates.sql` 获取源表和目标表 DDL 示例。

---

#### 类型 H：其他工具

| 工具 | 常见问题 | 诊断路径 |
|------|----------|----------|
| Canal | 连接失败、权限不足、server-id 冲突 | 同类型 C → 子步骤 C1 表格 |
| Debezium | binlog 未开、wal_level 非 logical、复制槽冲突 | 同类型 C |
| DataX | 增量对不上、时区问题、字段切分 | 同类型 A |
| Kafka Connect | connector 状态异常、offset 丢失 | 检查 Connect REST API |
| DTS/DMS | 云产品迁移后数据不一致 | 按类型 A/B 排查数据问题 |

---

## 三、龙蜥特殊约定

本技能为通用型 CDC 排障技能。若排查中需要在龙蜥操作系统中安装依赖：

- 包管理用 `dnf`（Anolis 23 默认），不用 `yum`
- Java 环境：`dnf install java-11-openjdk`
- 系统服务管理：`systemctl`
- 镜像源优先使用 `mirrors.openanolis.cn`
- 内核相关排查注意 ANCK / RHCK 差异（`uname -r` 后缀判断）
- 社区反馈：https://forum.openanolis.cn

---

## 四、易错点

1. **CDC 日志正常不等于数据正确**：乱序覆盖时所有日志正常，问题在目标库写入顺序。
2. **时区只转一次不够**：源库时区、业务代码时区、数仓时区可能三个都不一样。
3. **增量窗口卡太紧**：不加缓冲一定漏边界数据。
4. **同时设置 scan.startup.mode 和 debezium.snapshot.mode**：两者冲突，前者失效。【官网 §Db2 CDC → 启动模式】
5. **非主键列做 chunk key**：官方明确警告可能数据不一致。【官网 §MySQL CDC → chunk.key-column】
6. **PG 复制槽不清理**：CDC 停掉后 WAL 会膨胀到磁盘满。
7. **MongoDB 不设心跳**：慢变更表 resumeToken 过期，恢复失败。
8. **MySQL 8.4 用旧版 CDC**：CDC 2.x 不兼容 MySQL 8.4。
9. **无视版本映射表**：Flink CDC 3.x JAR 不能在 Flink 1.13 上跑。
10. **Schema Evolution 无脑开**：字段改名丢数据、类型缩窄写失败。
11. **Flink SQL 模式期待自动 DDL**：Flink SQL 不支持自动 DDL。
12. **CDC 不是备份**：源库数据被删、目标库也会被删（开了 DELETE 同步的话）。
13. **Binlog 不是永久存储**：默认只保留几天，CDC 停了太久就恢复不了。

---

## 五、验证

修复后逐项确认：

| 检查项 | 方法 | 通过标准 |
|--------|------|----------|
| 任务运行 | Flink WebUI | RUNNING，无 FAILED |
| Checkpoint | Flink WebUI | 最近 3 个成功 |
| 数据量 | COUNT(*) 对比两端 | 差值 < 0.1% |
| 数据值 | 任取 10 条逐字段对比 | 完全一致 |
| 乱序修复 | 手动模拟后到先写 | 目标库保留最新值 |
| 延迟 | Flink Metrics | 稳定在容忍范围内 |
| 稳定性 | 自修复起观察 30 分钟 | 无异常断开或报错 |

---

## 六、示例

### 示例 1：数据量对不上

**用户说：** 今天业务库 10000 单，数仓只查到 7000 单。
**Agent 执行：** 类型 A → 子步骤 A2 → 按小时对比 → 凌晨缺失 → 检查时区 → 增量窗口加缓冲 → 验证。

### 示例 2：CDC 数据乱序

**用户说：** 订单状态从已支付到已完成，查 Doris 还是已支付。CDC 和 Flink 日志都正常。
**Agent 执行：** 类型 B → 子步骤 B2 → 检查 Sequence Column → 未启用 → 启用并验证。

### 示例 3：任务启动报错

**用户说：** Flink CDC 任务起不来，日志说 binlog。
**Agent 执行：** 类型 C → 子步骤 C1 → 匹配表格 → log_bin=OFF → 开启 binlog → 验证。

### 示例 4：延迟越来越高

**用户说：** CDC 延迟越来越高了，现在差 2 小时。
**Agent 执行：** 类型 D → 子步骤 D1 → Flink WebUI 看背压 → Sink HIGH → 增大并行度+checkpoint 间隔。

---

## 参考文档

- Flink CDC 官方文档：https://nightlies.apache.org/flink/flink-cdc-docs-master/zh/docs/connectors/flink-sources/overview/
- Debezium 文档：https://debezium.io/documentation/reference/stable/
