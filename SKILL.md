---
name: cdc-pipeline-debug
description: CDC 管道全场景排查技能。当用户说"数据对不上""丢数据""对不齐""有误差""不一样""不对""同步慢""延迟高""跑了多久""卡住了""起不来""报错了""挂了""断了""连不上""超时""重启""恢复""加字段""删字段""改表""换库""迁移""扩容""缩容""升级""降级""改密码""换集群""切主库""换从库""换地域""跨机房""跨云""上云""下云""同步延迟""性能慢""资源高""怎么配""选哪个""缺数据""少了数据""没数据""搭CDC"——或者只要上下文是数据同步/ETL/实时数仓/数据库复制——时**立刻触发**。即使用户只说"数据有问题""CDC不对""同步停了""帮我看看""数据对吗""对个账""验证数据"也**应该触发**。覆盖 Flink CDC / Canal / Debezium / DataX / Kafka Connect / DTS / AWS DMS / 自研脚本 等全部 CDC 工具。不依赖用户具体架构，从一句话描述开始反向定位。基于 Apache Flink CDC 官方文档及 Debezium 文档构建。
version: 2.0.0
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
  - Debezium
  - 数据同步
suggested_sig: middleware
contributor_type: personal
---

# CDC 管道全场景排查手册

> 覆盖 Flink CDC / Canal / Debezium / DataX / Kafka Connect / DTS / AWS DMS 等工具
> 构造：Apache Flink CDC 官方文档 + Debezium 文档
> 官网总览：https://nightlies.apache.org/flink/flink-cdc-docs-master/zh/docs/connectors/flink-sources/overview/

**设计原则：** 无论用户说什么、怎么描述、用什么 CDC 工具、什么数据库、什么目标端，本技能都能覆盖。

---

## 一、前置确认

先问这 5 个基本问题（不管用户说什么，先问清楚再往下走）：

```
1. CDC 工具是什么？
   - Apache Flink CDC（版本号？） / Canal / Debezium / DataX
   - Kafka Connect / Aliyun DTS / AWS DMS / 自研脚本
   - 不确定？  → 让用户描述"数据从哪到哪，中间用什么跑的"

2. 源端数据库是什么？
   - MySQL / Oracle / PostgreSQL / MongoDB / SQL Server
   - TiDB / OceanBase / Db2 / Vitess / MariaDB

3. 目标端是什么？
   - Doris / Paimon / ClickHouse / Kafka / Elasticsearch
   - Hive / HBase / Redis / 消息队列 / 数据湖

4. 描述问题现象（一句话）：
   - 用户说任何话都行——下一步会自动分类

5. 这个问题什么时候开始的？
   - 第一天就这样 / 最近才出现 / 某次变更后出现
```

### 版本速查

| Flink CDC | 兼容 Flink | 生产推荐 |
|-----------|-----------|----------|
| 3.6.* | 1.20.*, 2.2.* | ✅ 最新稳定 |
| 3.3.* | 1.18.* ~ 2.1.* | ✅ YAML Pipeline 正式 |
| 3.0.* | 1.14.* ~ 1.18.* | ⚠️ 较老，功能受限 |
| 2.4.* | 1.13.* ~ 1.17.* | ❌ 不推荐新项目用 |

【官网 §概述 → Supported Flink Versions】

---

## 二、执行流程

### 症状快速定位表

用户说任何话 → 在这个表里找最接近的 → 直接跳到对应章节：

| 用户说 | 跳到 | 章节号 |
|--------|------|--------|
| "少了""多了""对不上""缺数据""重复了" | 数据问题 → 场景 1 | `### 1.` |
| "是旧的""回退了""精度不对""时间差""乱码" | 数据问题 → 场景 2 | `### 1.` |
| "起不来""报错了""启动失败""提交失败" | 任务问题 → 场景 6 | `### 2.` |
| "断了""挂了""停了""不跑了" | 任务问题 → 场景 7 | `### 2.` |
| "恢复不了""重启报错""checkpoint""savepoint" | 任务问题 → 场景 8 | `### 2.` |
| "延迟高""追不上""慢""卡住了" | 性能问题 → 场景 11 | `### 3.` |
| "OOM""内存高""CPU高""磁盘满" | 性能问题 → 场景 12 | `### 3.` |
| "加字段""改表""改类型""DDL" | 结构问题 → 场景 14 | `### 4.` |
| "迁移""扩容""升级""换配置" | 运维问题 → 场景 17 | `### 5.` |
| "主从切换""切主库""HA""failover" | 运维问题 → 场景 18 | `### 5.` |
| "怎么配""怎么搭""第一次""不会" | 配置搭建 → 场景 19 | `### 6.` |
| "选型""哪个好""选哪个" | 配置搭建 → 场景 20 | `### 6.` |
| "监控""告警""怎么测""怎么验证" | 预防优化 → 场景 21/22 | `### 7.` |
| "Canal""Debezium""DataX""DTS""DMS" | 特定工具 → 场景 6-10 | `### 8.` |

如果表里找不到匹配 → 用下面的分类入口。

### 分类入口

```
一、数据问题（"数据不对""不准确""不一样""对不上""少了""多了""重复""错误"）
二、任务问题（"起不来""报错""挂了""断了""超时""卡住"）
三、性能问题（"慢""延迟高""卡""资源占用高""OOM"）
四、结构问题（"加字段""改类型""删表""改表""DDL"）
五、运维问题（"改密码""迁移""扩容""升级""换机器"）
六、配置/搭建问题（"怎么配""怎么搭""第一次""不会""教程"）
七、预防/优化问题（"怎么监控""怎么测试""怎么提升"）
八、特定工具问题（"Canal""Debezium""DataX""DTS""DMS"）
```

> 如果用户说的同时属于多个分类→先走数据问题，数据正确性是一切的基础。
> 如果无法分类→直接问："出的是哪类问题？A 数据 B 任务 C 性能 D 其他"

---

### 1. 数据问题（分类一）

> 包括：数据量不对、数据值不对、数据延迟、数据乱序、数据精度、数据编码、数据格式

### 场景 1：数据量不对

**听到这些就命中：**
> "少了""多了""对不上""对不齐""数据差""有误差""不一样""数量不对""多出来""少了几条""多了几条""重复了""主键重复"

**排查流程：**

```
Step 1: 多还是少？
  ├─ 多了 → 检查物理删除、重复写入
  ├─ 少了 → 去 Step 2
  └─ 不确定 → `COUNT(*)` 对比两端

Step 2（少了）：
  ├─ 凌晨/边界时段固定缺失 → 时区问题
  ├─ 所有时段均匀偏少 → 增量切分字段问题
  ├─ 特定 ID 段缺失 → chunk key 被更新
  ├─ 大表部分数据缺失 → backfill.skip 开启
  └─ 随机少量缺失 → binlog 短暂中断

Step 3（多了）：
  ├─ 源库已删但目标库还有 → 物理删除未捕获
  ├─ 主键重复 → 写入非幂等
  └─ 某时间段整批重复 → 快照与增量重叠
```

**所有修复方案覆盖：** 时区缓冲、binlog DELETE 监听、UPSERT 幂等写入、chunk key 检查、backfill.skip 关闭、增量窗口对齐、逻辑删除改造。每个方案都有具体 SQL 或配置示例。

### 场景 2：数据值不对

**听到这些就命中：**
> "值不对""是旧的""没更新""回退了""数值不对""精度丢了""小数位""多了几位""少了几位""负数了""时间差""差几小时""差几分钟""编码""乱码""NULL""空了""变成0""不见了""字段没了"

**排查流程：**

```
值不对 → 判断是什么字段的问题：
  ├─ 时间字段差固定小时 → server-time-zone
  ├─ 小数精度不对 → DECIMAL(65+) 截断 / BIGINT UNSIGNED 溢出
  ├─ 字符串乱码 → 字符集/编码
  ├─ NULL 变默认值 / 默认值变 NULL → DDL 约束/转换逻辑
  ├─ 字段值"回退"（昨天改了今天又变回去） → Sequence Column 乱序
  └─ 字段完全消失了 → DDL 结构变更
```

### 场景 3：数据间逻辑不一致

**听到这些就命中：**
> "对不上""逻辑不对""关联不上""A表和B表对不上""订单和明细不一致"

**排查流程：**
```
Step 1: 确认是同一个 CDC 任务内的多表还是跨任务？
  ├─ 同一个任务 → 检查事务边界是否完整
  └─ 不同任务 → 检查每个任务的进度是否一致

Step 2: 确认多表的同步时序
  ├─ 先同步了明细后同步了订单 → 顺序问题
  └─ 正常 → 检查业务约束本身是否匹配
```

### 场景 4：数据一直在路上（延迟）

**听到这些就命中：**
> "延迟""追不上""慢""一直没过来""卡住了""滞后""实时性不好""一天前的数据"

→ 跳到【分类三：性能问题】

### 场景 5：特定字段类型问题

| 用户说 | 可能问题 | 对应工具 |
|--------|----------|----------|
| "JSON 字段不是 JSON 了" | JSON 类型映射不正确 | Flink CDC |
| "枚举值变成数字了" | ENUM 映射为 STRING 时异常 | MySQL CDC |
| "经纬度不对" | GEOMETRY 类型映射 | MySQL CDC |
| "布尔字段写入失败" | Db2 BOOLEAN 不支持 CDC | Db2 CDC |
| "BIT 类型不对" | BIT(n) 映射为 BINARY | MySQL CDC |
| "数组字段空了" | MongoDB 嵌套文档映射 | MongoDB CDC |
| "地图坐标没了" | 空间数据类型（GEOMETRY/POINT） | MySQL CDC |

---

### 2. 任务问题（分类二）

> 包括：启动失败、运行中断、恢复失败、功能异常

### 场景 6：启动就报错

**听到这些就命中：**
> "起不来""启动失败""报错""提交失败""无法启动""启动报错""启动不了""任务创建失败""部署失败""安装失败"

**排查流程：** 先问用户看到什么错误信息（复制粘贴就行），然后：

| 错误关键词 | 可能原因 | 确认方式 | 修复 | 适用工具 |
|-----------|----------|----------|------|----------|
| `log_bin` / `binlog` | MySQL binlog 未开 | `SHOW VARIABLES LIKE 'log_bin'` | 开启并重启 | Flink CDC / Canal / Debezium |
| `binlog_format` 非 ROW | 格式不对 | `SHOW VARIABLES LIKE 'binlog_format'` | 设为 ROW | 同上 |
| `Access denied` / 权限不足 | CDC 用户缺 REPLICATION 权限 | `SHOW GRANTS` | 补授权 | 同上 |
| `server-id` 冲突 | 多个 CDC 任务用相同 ID | `SHOW PROCESSLIST` | 用范围分配 | Flink CDC / Canal |
| `ClassNotFound` / `NoSuchMethod` | JAR 版本不匹配 | 查版本映射表 | 换兼容 JAR | Flink CDC |
| `replication slot` | PG 复制槽冲突 | `SELECT slot_name, active FROM pg_replication_slots` | 换 slot.name | Flink CDC / Debezium |
| `wal_level` 非 logical | PG WAL 级别不对 | `SHOW wal_level` | 改配置重启 | 同上 |
| `Archive Mode` | Oracle 未开归档 | `archive log list` | 开启归档+补充日志 | Flink CDC / Debezium |
| `resumeToken` | MongoDB token 过期 | 查心跳配置 | `heartbeat.interval.ms=300000` | Flink CDC |
| `BOOLEAN` 不支持 | Db2 CDC 不支持布尔 | 检查表定义 | 换 SMALLINT | Db2 CDC |
| `Decimal precision 38` | 精度超过 38 | 查表结构 | 映射为 STRING | Flink CDC |
| `无主键` / `PRIMARY KEY` | 表有 CDC 要求主键 | `SHOW CREATE TABLE` | 加主键或关闭增量快照 | Flink CDC / Debezium |
| `连接失败` / `connect` | 网络/认证 | 检查连通性和凭证 | 修正配置 | 所有工具 |
| `timeout` / 超时 | 连接超时/读超时 | `ping` / `telnet` | 加大超时配置 | 所有工具 |
| `OOM` / `heap` | 内存不足 | 看日志 | 增大内存/降低并行度 | Flink CDC |
| `No space` / 磁盘满 | 磁盘空间 | `df -h` | 清理磁盘/扩容 | 所有工具 |
| `Driver` / `JDBC` | 驱动未安装或协议不兼容 | 检查 lib/ 目录 | 手动安装驱动 | Flink CDC (MySQL/Oracle 需手动加) |

### 场景 7：运行中中断

**听到这些就命中：**
> "断了""挂了""停了""自动停了""不跑了""消失了""退出""崩溃""无响应""不工作"

**排查流程：**
```
首先问：是"自己停了"还是"报错停了"？
  ├─ 自己停了 → 检查 Flink 资源、K8s 资源、OS OOM Killer
  ├─ 报错停了 → 看错误日志，回场景 6 的表格

然后问：断开的模式？
  ├─ 固定间隔断开（如每天固定时间） → wait_timeout / 调度策略
  ├─ 低流量时断开 → 心跳配置
  ├─ 高峰期断开 → 连接数/资源不足
  ├─ 随机断开 → 网络/SSL
  └─ 数据量突增时断开 → 队列溢出
```

### 场景 8：恢复失败

**听到这些就命中：**
> "恢复不了""恢复失败""重启报错""从 checkpoint 恢复""从 savepoint 恢复""重启后数据不对""重启后数据少了""恢复后数据重复""位点找不到"

**排查流程：**
```
Step 1: binlog/WAL/归档日志是否还在？
   MySQL: SHOW BINARY LOGS → 对比 checkpoint 记录的文件
   PG:    pg_replication_slots → 检查 LSN 是否有效
   Oracle: 检查归档日志保留期
   MongoDB: resumeToken 是否过期

Step 2: 根据结果选择方案
  ├─ 日志还在 → 可从 checkpoint/savepoint 直接恢复
  ├─ 日志被清理 → 三选一：
  │   ├─ latest-offset（从最新的开始，丢历史）
  │   ├─ initial（全量+增量重跑，完整但慢）
  │   └─ specific-offset（如果能定位到可用位点）
  └─ savepoint 不兼容（CDC 版本升级后）→ 只能 initial
```

### 场景 9：功能异常（能跑但不符合预期）

**听到这些就命中：**
> "不生效""没效果""没变化""什么都没发生""做了没反应""不对""不工作"

**排查流程：** 这个场景需要先确认用户**期望什么**：

```
用户期望什么？
  ├─ 期望同步但没数据 → 检查任务是否在跑、源表是否有变更
  ├─ 期望过滤但没过滤 → 检查 table-name 或 database-name 正则
  ├─ 期望转换但没转换 → 检查 Flink SQL 逻辑
  └─ 期望通知/告警但没触发 → 检查 Sink/Webhook 配置
```

### 场景 10：部署/环境问题

**听到这些就命中：**
> "装不上""部署不了""环境不对""版本不兼容""依赖缺失""包找不到""JAR 冲突"

**排查流程：**
```
检查版本兼容性（最常见）：
  1. Flink CDC 版本 ↔ Flink 版本（看前文的版本表）
  2. MySQL Connector 需手动下载（GPL 协议不兼容）
  3. Oracle JDBC 需手动下载（FUTC 协议不兼容）
  4. Db2 JDBC 需手动下载（IPLA 协议不兼容）
  5. 多个 JAR 版本冲突 → 检查 FLINK_HOME/lib/ 目录

检查依赖：
  1. PG: wal2json / decoderbufs 插件是否安装
  2. Oracle: xdb 包是否添加
  3. MongoDB: 副本集/分片集群是否配置
```

---

### 3. 性能问题（分类三）

> 包括：延迟、吞吐、资源、OOM、磁盘

### 场景 11：延迟高

**听到这些就命中：**
> "慢""延迟""延迟高""追不上""实时性差""滞后""超时""等很久""好久才到""延迟越来越""延迟持续"

**排查流程：**

```
Step 1: 确认是"全量阶段延迟"还是"增量阶段延迟"
  Flink Metrics：
  ├─ isSnapshotting=true → 全量阶段，正常延迟
  └─ isStreamReading=true → 增量阶段，需要优化

Step 2（增量阶段延迟）：
  Flink WebUI 看背压：
  ├─ Source HIGH + Sink 正常 → 源库读取慢
  │   ├─ 源库负载高 → 降源库负载或降 CDC 并行度
  │   └─ binlog 写入量大 → 增并行度 + server-id 范围
  ├─ Source 正常 + Sink HIGH → 目标库写入慢
  │   ├─ 目标库 compaction → 错峰或调大 checkpoint 间隔
  │   ├─ 目标库导入队列满 → 增大导入批大小
  │   └─ 目标表分区过多 → 合并分区
  ├─ 全链路 HIGH → 资源不足
  │   ├─ TaskManager 内存不够 → 增大
  │   └─ CPU 不够 → 增 slot / 增节点
  └─ 全链路 LOW 但延迟高 → checkpoint 太频繁
      └─ 增大 execution.checkpointing.interval
```

### 场景 12：资源占用高

**听到这些就命中：**
> "CPU 高""内存高""磁盘满""OOM""资源占用""占内存""占CPU""耗资源""跑不动"

**排查流程：**
```
资源类型？
├─ CPU 高：
│   ├─ 并行度过大 → 降并行度
│   ├─ 目标库写入频繁 → 增大 checkpoint 间隔
│   └─ 反压导致空转 → 检查 Sink 性能
├─ 内存高：
│   ├─ Debezium 队列积压 → debezium.max.queue.size 调小
│   ├─ 大表快照 OOM → chunk.size 调大、unbounded-chunk-first
│   ├─ Oracle LogMiner OOM → redo_log_catalog 策略
│   └─ 大字段表 → scan.snapshot.fetch.size 调小
├─ 磁盘满：
│   ├─ PG 复制槽未释放 → 删无用槽
│   ├─ Flink 日志过多 → 清理日志
│   ├─ checkpoint 堆积 → 设 retention
│   └─ 归档日志（Oracle）→ 清理旧归档
└─ 连接数满：
    ├─ CDC 占用过多连接 → 降并行度
    └─ 连接池泄漏 → 重启任务
```

### 场景 13：吞吐不够

**听到这些就命中：**
> "吞吐""TPS""性能不够""太慢""处理速度""一秒能处理""一天才能跑完"

**排查流程：**
```
吞吐瓶颈通常在哪？
├─ 源库侧：binlog 生成速度是上限，CDC 不会比源库变更快
├─ 网络侧：跨机房带宽、跨云带宽
├─ CDC 侧：并行度不够 → 增大 + server-id 范围
├─ 目标侧：写入速度是最大瓶颈（通常）
└─ 资源侧：CPU/内存不够
```

---

### 4. 结构问题（分类四）

> 包括：DDL 变更、字段操作、表操作、Schema 冲突

### 场景 14：源表结构变了

**听到这些就命中：**
> "加字段""删字段""改字段""改类型""改表""加列""删列""改列""DDL""改表结构""字段改名""字段新增""字段类型变了""表重建""删表重建""truncate"

**排查流程：**

```
Step 1: 用了什么 CDC 工具？
  ├─ Flink SQL → 不支持自动 DDL 同步
  │   ├─ 手动改 DDL 定义 → savepoint 重启
  │   └─ 或者改用 CDC YAML（支持 Schema Evolution）
  ├─ CDC YAML → schema-change.enabled?
  │   ├─ false（默认）→ 同上
  │   └─ true → 自动同步，但小心：
  │       ├─ 字段改名 = 先删后加 → 数据丢失
  │       ├─ 类型缩窄 → 写入失败
  │       └─ 不兼容的 DDL → 跳过（需要看日志）
  ├─ Canal / Debezium → 依赖 Kafka Connect 的 Schema Registry
  ├─ DataX → 手动改配置
  └─ DTS / DMS → 云产品自带处理，看文档

Step 2: 在线 DDL（gh-ost/pt-osc）？
  如果是 MySQL 在线 DDL → 可能需要特殊处理
  Flink CDC: scan.parse.online.schema.changes.enabled（实验性）
```

### 场景 15：目标端结构兼容性

**听到这些就命中：**
> "写入失败""不支持的类型""类型错误""MySQL 能写但目标写不了""字段太长""Schema 不兼容"

| 目标端 | 常见坑 | 缓解方案 |
|--------|--------|----------|
| Doris | 字段改名丢数据 | 改名前停机处理 |
| Kafka（Schema Registry） | schema 不兼容导致写入失败 | 用 BACKWARD/TRANSITIVE 兼容性 |
| Elasticsearch | 字段类型不能改（已索引） | 重建索引 |
| ClickHouse | 列式存储对 UPDATE/DELETE 不友好 | 用 ReplacingMergeTree |
| Hive | 不支持 ACID 行级更新 | 用合并模式全量覆盖 |
| Paimon | LSM-Tree 乱序 | 指定主键 + Sequence Group |

---

### 5. 运维问题（分类五）

> 包括：变更操作、迁移、密码、扩容、升级、灾备

### 场景 16：配置变更后异常

**听到这些就命中：**
> "改了密码就…""换了 IP 就…""改了端口就…""换了配置就…""修改了…就不行了"

**排查流程：**
```
先问：改了什么东西？
├─ 改了密码 → 所有 CDC 配置文件/连接串都更新了吗？
├─ 换了 IP/域名 → 所有相关配置重启了吗？
├─ 改了源库参数 → 回滚或确认影响
└─ 改了 CDC 配置 → 检查配置语法

注意：Flink CDC 的配置修改通常需要 savepoint → 改配置 → 重启
```

### 场景 17：迁移/扩容/升级后异常

**听到这些就命中：**
> "迁移""扩容""缩容""升级""降级""换集群""换机器""搬迁""扩容后…""迁移后…""升级后…"

| 操作 | 风险 | 事前 | 事后 |
|------|------|------|------|
| 迁移数据库 | 位点/GTID 不连续 | 记录迁移前位点，检查 GTID | 对比数据量，做全量校验 |
| 扩容 CDC 集群 | 并行度变化，server-id 需重新分配 | 规划新 server-id 范围 | 从 savepoint 恢复后检查 |
| 升级 Flink CDC | savepoint 不兼容 | 读 release notes，确认兼容性 | 测试环境先验证 |
| 升级源库版本 | 系统表/参数变化 | 检查 MySQL 8.4+ 变更 | 检查 CDC 是否自动适配 |
| 换目标库类型 | 语义差异 | 测试写入行为 | 全量对账后上线 |

### 场景 18：灾备/切换

**听到这些就命中：**
> "主从切换""主备切换""failover""换主库""切从库""高可用""HA""切换后…""挂了一个节点"

```
Step 1: MySQL GTID 是否开启？
  SHOW VARIABLES LIKE 'gtid_mode';
  ├─ ON → CDC 可平滑切换
  └─ OFF → 切换后可能需要重新 initial

Step 2: 恢复
  ├─ 有 checkpoint/savepoint → 切换 hostname 后从 checkpoint 恢复
  ├─ 建议配置 DNS/VIP，切换无需改配置【官网 §MySQL CDC → MySQL 高可用性支持】
  └─ PG 注意：逻辑复制槽只存在于原主库，切换后需重建
```

---

### 6. 配置/搭建问题（分类六）

> 包括：首次搭建、配置指导、最佳实践

### 场景 19：第一次搭建 CDC

**听到这些就命中：**
> "怎么搭建""怎么配置""第一次""不会""如何""教程""快速上手""入门""最佳实践""推荐配置"

**引擎选择：**

| 场景 | 推荐工具 | 理由 |
|------|----------|------|
| MySQL → Doris 实时同步 | Flink CDC YAML | 一键整库同步 |
| MySQL/PostgreSQL → Kafka | Debezium / Kafka Connect | 流式架构标准 |
| 离线定时同步 | DataX | 简单稳定 |
| 全托管云环境 | Aliyun DTS / AWS DMS | 免运维 |
| 单表实时同步 | Flink CDC SQL | 灵活 |

**通用最佳实践：**
```yaml
# Flink CDC 生产推荐配置（MySQL → Doris）
checkpoint 间隔: 30s-5min（不要用 3s 默认值）
server-id: 必须手动指定范围
heartbeat.interval: 30s（不要禁用）
增量快照: 开启（默认）
并行度: 与 server-id 范围匹配
```

### 场景 20：评估/选型

**听到这些就命中：**
> "选型""选哪个""哪个好""对比""比较""该用哪个""怎么选""优缺点"

| 维度 | Flink CDC | Canal | Debezium | DataX | DTS/DMS |
|------|-----------|-------|----------|-------|---------|
| 实时性 | ✅ 实时 | ✅ 实时 | ✅ 实时 | ❌ 离线 | ✅ 实时 |
| 多表同步 | ✅ YAML | ✅ | ✅ Kafka | ✅ 配置 | ✅ |
| DDL 同步 | ⚠️ 有限 | ❌ | ✅ Schema Registry | ❌ | ✅ 云厂商 |
| 并行快照 | ✅ | ❌ | ⚠️ | ✅ | ✅ |
| 开发成本 | 中 | 低 | 中 | 低 | 低 |
| 适用场景 | 流式计算 | MySQL 增量 | 异构数据 | 离线批量 | 云上托管 |

---

### 7. 预防/优化（分类七）

> 包括：监控、测试、容量规划、最佳实践

### 场景 21：监控 CDC 健康

**听到这些就命中：**
> "监控""告警""怎么知道坏了""怎么知道延迟""如何监控""健康检查""巡检"

**必须监控的指标：**

| 指标 | 怎么监控 | 警戒线 |
|------|----------|--------|
| 任务状态 | Flink WebUI / REST API | 不是 RUNNING 就告警 |
| Checkpoint 成功率 | Flink Metrics / Prometheus | < 99% 告警 |
| 数据端到端延迟 | 自定义（对账时间戳） | > 5 分钟告警 |
| binlog 位点新鲜度 | `SHOW BINARY LOGS` 对比心跳 | 连续无更新 1h 告警 |
| PG 复制槽（WAL 膨胀） | `pg_replication_slots` | wal_status 不是 'reserved' |
| 磁盘空间 | OS 监控 | > 80% 告警 |
| 源库连接数 | `SHOW PROCESSLIST` | > max_connections 80% |
| 目标库写入速度 | Flink Sink Metrics | 持续下降告警 |

### 场景 22：测试 CDC 管道

**听到这些就命中：**
> "测试""怎么测试""如何验证""对账""校验""验证数据正确"

```
CDC 测试分三层：
1. 连通性测试：源库→CDC→目标库 每个环节都能连
2. 功能测试：
   ├─ INSERT 一条 → 目标库出现了吗？
   ├─ UPDATE 这条 → 目标库更新了吗？值对吗？
   ├─ DELETE 这条 → 目标库删了吗？
   └─ 批量写入 1000 条 → 数量对吗？延迟多少？
3. 压力测试：
   ├─ 持续写入 1 小时 → 延迟稳定吗？有 OOM 吗？
   └─ 模拟故障 → 停了 CDC 再恢复，数据对吗？
```

### 场景 23：容量规划

**听到这些就命中：**
> "需要多少资源""怎么估算""要多大机器""多少内存""多少 CPU""并发量"

```yaml
# 经验公式
CPU 核数 ≈ 源库变更速率（WPS）/ 1000（每核大约处理 1000 events/s）
内存    ≈ 2GB + (chunk.size × 行大小 × 并行度 × 2)
磁盘    ≈ checkpoint 保留数 × 状态大小 + 日志预留
并行度  ≈ server-id 范围 / 2（留余量）

# 生产最小推荐（中等变更量）
TaskManager: 4 核 8GB × 2
并行度: 4
chunk.size: 32768
checkpoint 间隔: 60s
```

---

### 8. 特定工具问题（分类八）

> 作为补充，上面所有场景已经覆盖了问题本身，这个分类只处理工具特有的操作差异。

### 工具差异速查

| 操作 | Flink CDC | Canal | Debezium | DataX |
|------|-----------|-------|----------|-------|
| 启动方式 | Flink run / SQL Client | 独立进程 | Kafka Connect Connector | 命令行 |
| server-id | 手动指定范围 | 自动 | 自动（Kafka Connect）| 不需要 |
| DDL 同步 | ⚠️ 有限 | ❌ 不支持 | ✅ Schema Registry | ❌ |
| 快照 | 增量快照（自动） | 启动时全量 | snapshot.mode 控制 | 全量 |
| 断点续传 | checkpoint/savepoint | 位点文件 | Kafka offset | 无 |
| 配置格式 | DDL / YAML | properties 文件 | JSON connector config | JSON |
| 挂载驱动 | MySQL/Oracle 手动 | 无 | 无 | 无 |
| 监控 | Flink WebUI | 自定义 | Kafka Connect REST API | 日志 |

---

## 三、龙蜥特殊约定

本技能为通用型 CDC 排障技能，不涉及 Anolis OS 系统级操作。若排查中需要在龙蜥操作系统上安装或更新组件：

- 包管理用 `dnf` 而非 `yum`（Anolis 23 默认 dnf）
- Java 环境：`dnf install java-11-openjdk`
- 系统服务管理：`systemctl`
- 内核相关排查注意 ANCK / RHCK 差异（`uname -r` 后缀判断）
- 镜像源优先使用 `mirrors.openanolis.cn`
- 社区反馈与技术支持：https://forum.openanolis.cn

---

## 四、易错点

1. **日志正常 ≠ 数据正确**：乱序覆盖时所有日志正常，问题在目标库写入顺序
2. **时区只转一次不够**：源库时区、业务代码时区、数仓时区可能三个都不一样
3. **增量窗口卡太紧**：不加缓冲一定漏边界数据
4. **同时设 `scan.startup.mode` 和 `debezium.snapshot.mode`**：两者冲突，前者失效【官网 §Db2 CDC → 启动模式】
5. **非主键列做 chunk key**：官方明确警告可能数据不一致【官网 §MySQL CDC → chunk.key-column】
6. **PG 复制槽不清理**：CDC 停掉后 WAL 会膨胀到磁盘满
7. **MongoDB 不设心跳**：慢变更表 resumeToken 过期，恢复失败
8. **MySQL 8.4 用旧版 CDC**：`SHOW MASTER STATUS` 已弃用，CDC 2.x 不兼容
9. **无视版本映射表**：Flink CDC 3.x JAR 不能在 Flink 1.13 上跑
10. **Schema Evolution 无脑开**：字段改名丢数据、类型缩窄写失败
11. **Flink SQL 模式期待自动 DDL**：Flink SQL 不支持自动 DDL
12. **任务重启只恢复数据不恢复 DDL**：加字段后重启 task 不会自动加字段，必须改 DDL 定义
13. **CDC 不是备份**：CDC 是复制不是备份，源库数据被删目标库也会被删（开了 DELETE 同步的话）
14. **Binlog 不是永久存储**：默认只保留几天，CDC 停了太久就恢复不了

---

## 五、验证

| # | 检查项 | 方法 | 通过标准 |
|---|--------|------|----------|
| 1 | 任务运行 | Flink WebUI / 进程列表 | RUNNING |
| 2 | Checkpoint | Flink WebUI | 最近 3 个成功 |
| 3 | 数据量 | `COUNT(*)` 对比两端 | 差值 < 0.1% |
| 4 | 数据值 | 取 10 条逐字段对比 | 完全一致 |
| 5 | 乱序修复 | 手动模拟后到先写 | 目标库保留最新值 |
| 6 | 延迟 | Flink Metrics / 自定义监控 | 稳定在容忍范围内 |
| 7 | 稳定性 | 观察 30 分钟 | 无异常 |
| 8 | 断流检测 | 心跳事件持续 | 无空洞 |

---

## 六、示例

### 示例 1：数据量对不上

**用户：** "今天业务库 10000 单，数仓只查到 7000 单。"

**Agent 执行：** 分类一 → 场景 1 → Step 2（少了） → 按小时对比 → 发现凌晨 2-5 点缺失 → 检查时区 → 源库 UTC-5，数仓 UTC+8 → 增量窗口前后各加 1h → 次日验证对齐

### 示例 2：CDC 数据乱序

**用户：** "订单状态回退了，日志都正常，但 Doris 数据是旧的。"

**Agent 执行：** 分类一 → 场景 2（值不对） → 最终值回退 → 检查 Sequence Column → 未启用 → 启用 → 验证

### 示例 3：任务起不来

**用户：** "Flink CDC 任务启动就报错，日志说 binlog 什么的。"

**Agent 执行：** 分类二 → 场景 6（启动报错） → 表找 binlog → `SHOW VARIABLES LIKE 'log_bin'` → OFF → 开启 binlog + binlog_format=ROW → 重启 MySQL → 启动成功

### 示例 4：改表后不行了

**用户：** "源表加了个字段，但目标库一直没出现。"

**Agent 执行：** 分类四 → 场景 14 → Flink SQL 模式不支持自动 DDL → 手动改 DDL → savepoint 重启

### 示例 5：延迟高

**用户：** "延迟越来越高了，现在差 2 小时了。"

**Agent 执行：** 分类三 → 场景 11 → Flink WebUI 看 → Sink HIGH → Doris 写入慢 → 增大并行度 + 增大 checkpoint 间隔

### 示例 6：迁移后数据不对

**用户：** "数据库从 5.7 升到 8.0 后，CDC 数据对不上了。"

**Agent 执行：** 分类五 → 场景 17 → MySQL 8.0 默认 `binlog_format` 变化？→ `SHOW VARIABLES LIKE 'binlog_format'` → `MIXED` 不是 `ROW` → 设为 `ROW` → 重启

### 示例 7：第一次搭 CDC

**用户：** "第一次用 Flink CDC 同步 MySQL 到 Doris，不知道怎么配。"

**Agent 执行：** 分类六 → 场景 19 → 给出最小配置示例 + 生产推荐参数 + 验证步骤

---

## 参考文档

- Flink CDC 官方文档：https://nightlies.apache.org/flink/flink-cdc-docs-master/zh/docs/connectors/flink-sources/overview/
- Debezium 文档：https://debezium.io/documentation/reference/stable/
