# CDC Pipeline 生产环境排障案例集

> 本文件作为 cdc-pipeline-debug Skill 的参考素材，Agent 在排查具体问题时按需读取。

---

## 案例一：时区不一致导致增量数据跨天错位

### 背景

某业务系统使用增量同步将数据从业务库抽取到数仓，每日凌晨跑一次。业务方反馈数仓的数据量连续多日与业务库对不上。

### 同步链路

```
源库 → 增量同步工具 → 目标库 ODS（按天分区）
增量切分条件：modified_time >= 昨天00:00 AND modified_time < 今天00:00
```

### 根因

| 层面 | 时区 | 偏移 |
|------|------|------|
| 源数据库时区 | UTC-5 | 比 UTC 慢 5 小时 |
| 业务代码写入时区 | UTC+8 | 比 UTC 快 8 小时 |
| 数仓分区时区 | 按 UTC 时间分区 | 与源库不一致 |

源库 UTC-5 的凌晨数据，在数仓 UTC+8 的视角里属于"前一天"，被写入了错误的分区。

### 修复方案

**最终方案**：增量时间窗口前后各留缓冲

```sql
-- 原逻辑（紧贴边界）
WHERE modified_time >= 昨天00:00 AND modified_time < 今天00:00

-- 改进后（前后各留缓冲）
WHERE modified_time >= 昨天00:00 - 1h AND modified_time < 今天00:00 + 1h
```

### 效果

缓冲后次日对账基本对齐，少量重复记录由下游去重层处理。

### 教训

**时区不统一时，增量时间窗口必须留缓冲，宁可重复不能遗漏。**

---

## 案例二：业务物理 DELETE 未捕获，指标计算异常

### 背景

某业务指标（如及时率、完成率）连续多周异常，排查了计算逻辑和调度链路均未发现问题，最后定位到数据源层面。

### 问题描述

业务系统在录错数据时，做法是物理 DELETE 原记录再 INSERT 新记录。
但增量同步只识别 `modified_time` 变化的记录，DELETE 不触发 `modified_time` 更新，数仓完全不知道记录被删除。

### 数据流转

```
业务库                         目标库 ODS
记录A INSERT  →               记录A 存在
记录A DELETE  →               （记录A 仍存在——因为增量未捕获删除）
记录A INSERT（新值）→        旧值 + 新值同时存在
```

结果：基于这些数据计算的业务指标异常。

### 解决方案

| 方案 | 描述 | 适用场景 |
|------|------|----------|
| 逻辑删除 | 业务加 `is_deleted` 字段，删时 UPDATE 标记 | 业务系统可改造 |
| 全量对比 | 每日对比全量主键，找出被删记录 | 表量级小的兜底方案 |
| binlog 监听 | Canal/Debezium 监听 binlog，捕获 DELETE 事件 | 追求实时性且能部署 CDC |

### 教训

**增量同步默认只捕获 INSERT 和 UPDATE，不处理 DELETE。如果业务存在物理删除，必须额外处理。**

---

## 案例三：CDC 数据乱序——Sequence Column 缺失

### 背景

CDC 实时同步链路，源库→计算引擎→目标库链路中，三段日志均正常，但目标库中的最终值不是最新值。

### 排查路径

```
第1步：确认 CDC 是否捕获了所有变更 → 已捕获 ✓
第2步：确认计算引擎是否输出了全部记录 → 已输出 ✓
第3步：查目标库最终值 → 不是最新值 ✗
→ 问题定位在目标库写入层
```

### 根因

部分目标库（如 Doris Unique Key、Paimon 等）依赖"后写入覆盖"语义。CDC 管道中间一条记录的多次变更在毫秒级内到达，并行处理后写入顺序可能被打乱——后发生的变更先入库，先发生的变更后入库，后入库的旧值覆盖了先入库的新值。

这不是偶发的网络问题，是 CDC 管道的结构性特征。

### 修复方案

启用 Sequence Column，用业务时间戳作为排序依据。

```sql
-- 目标库示例：建表时启用
CREATE TABLE orders (
  order_id BIGINT,
  status VARCHAR(32),
  update_time DATETIME
) UNIQUE KEY(order_id)
PROPERTIES (
  "function_column.sequence_col" = "update_time"
);

-- 已有表启用
ALTER TABLE orders ENABLE FEATURE "SEQUENCE_LOAD"
WITH PROPERTIES ("function_column.sequence_type" = "DATETIME");
```

### 验证

不管记录到达顺序如何，目标库始终保留业务时间戳最大的那条。

### 教训

**CDC 四层排查法：源库层 → 网络层 → 计算引擎层 → 存储引擎层。一切日志正常但数据不对的问题，最难排查，也最容易绕弯路。**

---

## 案例四：多表 CDC 整库同步配置优化

### 背景

10 张源表需要 CDC 同步到目标库。最初采用逐表配置的方式，每张表一个独立任务，配置重复、维护困难。

### 原始方案痛点

| 问题 | 表现 |
|------|------|
| 配置重复 | 每张表一份连接信息，改密码改 N 处 |
| server-id 冲突 | 手动分配，容易重叠 |
| 运维成本 | N 个任务分别监控 |

### 优化方案

采用 YAML Pipeline 模式，一个文件定义整条链路：

```yaml
source:
  type: mysql
  hostname: 10.0.0.1
  port: 3306
  username: cdc_user
  password: "***"
  tables: db.table1, db.table2

sink:
  type: doris
  fenodes: 10.0.0.2:8030
  username: root
  password: ""

route:
  - source-table: db.table1
    sink-table: ods.ods_table1

pipeline:
  name: cdc_pipeline
  parallelism: 4
```

### 常见配置踩坑

| 坑 | 现象 | 预防/解决 |
|----|------|-----------|
| server-id 冲突 | 新旧任务 server-id 重叠 | 先停旧任务，或手动指定不同范围 |
| 正则误匹配 | 同步了临时表、备份表 | 用排除规则过滤 |
| Schema Evolution 副作用 | 字段改名丢数据 | 开启但要配合监控 |
| Checkpoint 超时 | 目标库写入慢导致 checkpoint 超时 | 调大 checkpoint 间隔 |

### 对比

| 维度 | 逐表配置（N 个任务） | YAML 管道（1 个任务） |
|------|-------------------|---------------------|
| 配置量 | N 份重复 | 1 份 |
| 改密码 | 改 N 个地方 | 改 1 个地方 |
| server-id | 手动分配 | 自动 |
| Schema Evolution | 不支持 | 支持 |

### 教训

**多表 CDC 优先用管道模式统一管理，改一处胜于改 N 处。**
