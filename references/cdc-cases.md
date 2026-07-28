# CDC 管道排障案例参考

> 本文件作为 cdc-pipeline-debug Skill 的参考素材，Agent 在排查时可按需读取。
> 所有案例基于 Apache Flink CDC 官方文档及 Debezium 文档构建。

---

## 案例 1：时区不一致导致增量数据跨天错位

**适用症状：** 数据量不对 → 目标库比源库少 → 凌晨边界缺失

**官方依据：** MySQL CDC 连接器选项 `server-time-zone` 控制 TIMESTAMP 转换行为。如果未设置，使用 `ZoneId.systemDefault()`，可能与数据库实际时区不一致。【官网 §MySQL CDC → 连接器选项 → server-time-zone】

**典型排查路径：**
1. 按小时对比两端数据量，发现每天凌晨固定时段目标库缺失
2. 检查源库时区：`SHOW VARIABLES LIKE 'time_zone'`
3. 对比 Flink DDL 中的 `server-time-zone` 设置
4. 增量窗口前后各加 1 小时缓冲

---

## 案例 2：CDC 乱序导致数据回退

**适用症状：** 数据值不对 → 最终值不是最新值

**官方依据：** MySQL CDC 使用增量快照算法，全量阶段多 chunk 并行读取，binlog 阶段单线程消费。并行处理后写入目标库的顺序可能被打乱。【官网 §MySQL CDC → 增量快照读取的工作原理】

**典型排查路径：**
1. 确认 CDC 日志、Flink 日志、目标库日志都正常
2. 在源库查变更时间线，在目标库查最终值
3. 检查目标库是否启用 Sequence Column
4. 启用 Sequence Column 并用业务时间戳排序

---

## 案例 3：server-id 冲突导致任务启动失败

**适用症状：** 任务异常 → 启动就报错

**官方依据：** "每个用于读取 binlog 的 MySQL 数据库客户端都应该有一个唯一的 id，称为 Server id。如果不同的作业共享相同的 Server id，则可能导致从错误的 binlog 位置读取数据。"【官网 §MySQL CDC → 注意事项 → 为每个 Reader 设置不同的 Server id】

**典型排查路径：**
1. 用 `SHOW PROCESSLIST` 查看各连接的 server-id
2. 确认当前 CDC 任务的 server-id 配置
3. 配置范围格式：`'server-id'='5401-5404'`，范围必须 ≥ 并行度

---

## 案例 4：PG 复制槽未释放导致 WAL 膨胀

**适用症状：** 运行中断 → 磁盘满 / 性能问题 → 资源占用高

**官方依据：** PostgreSQL CDC 连接器使用逻辑复制槽追踪 WAL 位点。复制槽名称只能包含小写字母、数字和下划线。CDC 任务停止后复制槽不会自动释放。【官网 §PostgreSQL CDC → 连接器选项 → slot.name】

**典型排查路径：**
1. 检查复制槽状态：`SELECT slot_name, active, wal_status FROM pg_replication_slots`
2. 确认有无不再使用的复制槽
3. 手动清理：`SELECT pg_drop_replication_slot('slot_name')`

---

## 案例 5：MongoDB resumeToken 过期导致恢复失败

**适用症状：** 恢复失败 → MongoDB

**官方依据：** "当从检查点或保存点恢复 Flink 作业时，心跳事件可以向前推送 resumeToken，以避免 resumeToken 过期。"【官网 §MongoDB CDC → 连接器选项 → heartbeat.interval.ms】

**典型排查路径：**
1. 检查 `heartbeat.interval.ms` 配置
2. 如果未设置或设为 0，设置 `heartbeat.interval.ms=300000`
3. 从最近的 savepoint 恢复

---

## 案例 6：非主键列做 chunk key 导致数据不一致

**适用症状：** 数据量不对 → 目标库比源库少 → 特定 ID 段缺失

**官方依据：** "使用非主键列作为分片键可能会导致数据不一致。"【官网 §MySQL CDC → 连接器选项 → scan.incremental.snapshot.chunk.key-column 警告】

**典型排查路径：**
1. 检查 Flink DDL 中 `scan.incremental.snapshot.chunk.key-column` 配置
2. 如果使用了非主键列，且该列在快照期间可能被更新
3. 改回主键第一列（默认值）

---

## 案例 7：MySQL 8.4 兼容性

**适用症状：** 任务启动报错 / 运行异常

**官方依据：** "MySQL 8.4 引入了一些影响 CDC 连接器的重大变更：SHOW MASTER STATUS 已被弃用，替换为 SHOW BINARY LOG STATUS；错误消息术语从 slave/master 变更为 replica/source。MySQL CDC 连接器会自动探测 MySQL 服务器版本。"【官网 §MySQL CDC → MySQL 8.4+ 兼容性】

**典型排查路径：**
1. 确认 MySQL 版本：`SELECT VERSION()`
2. 确认 Flink CDC 版本（3.0+ 自动适配）
3. 如果使用旧版 CDC，升级到 3.0+

---

## 参考文档

所有案例对应的完整官方文档：
- MySQL CDC：https://nightlies.apache.org/flink/flink-cdc-docs-master/zh/docs/connectors/flink-sources/mysql-cdc/
- PostgreSQL CDC：https://nightlies.apache.org/flink/flink-cdc-docs-master/zh/docs/connectors/flink-sources/postgres-cdc/
- MongoDB CDC：https://nightlies.apache.org/flink/flink-cdc-docs-master/zh/docs/connectors/flink-sources/mongodb-cdc/
- Oracle CDC：https://nightlies.apache.org/flink/flink-cdc-docs-master/zh/docs/connectors/flink-sources/oracle-cdc/
