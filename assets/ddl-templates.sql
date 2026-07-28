# CDC 源表 DDL 要求
# 不同数据库对 CDC 的 DDL 要求不同，建表时应注意

-- ========== MySQL ==========
-- 建表时需要主键（Flink CDC 增量快照需要）
-- 不支持 BOOLEAN？MySQL 用 TINYINT(1) 替代
-- DECIMAL 精度 > 38 时 Flink 不支持，应改为 STRING

CREATE TABLE orders (
  order_id    BIGINT          NOT NULL AUTO_INCREMENT,
  status      VARCHAR(32)     NOT NULL,
  amount      DECIMAL(10, 2)  NOT NULL,  -- 精度 ≤ 38
  created_at  DATETIME        NOT NULL,
  updated_at  DATETIME        NOT NULL,
  PRIMARY KEY (order_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;


-- ========== Flink DDL（MySQL CDC Source） ==========
-- 生产配置要点：
-- ① server-id 用范围，≥ 并行度
-- ② server-time-zone 必须与 MySQL 的 time_zone 一致
-- ③ heartbeat.interval 不要禁用
-- ④ scan.incremental.snapshot.enabled 保持默认 true

CREATE TABLE orders_cdc (
  order_id    BIGINT,
  status      STRING,
  amount      DECIMAL(10, 2),
  created_at  TIMESTAMP(3),
  updated_at  TIMESTAMP(3),
  -- 元数据列：记录变更来源
  db_name     STRING METADATA FROM 'database_name' VIRTUAL,
  tbl_name    STRING METADATA FROM 'table_name' VIRTUAL,
  op_ts       TIMESTAMP_LTZ(3) METADATA FROM 'op_ts' VIRTUAL,
  operation   STRING METADATA FROM 'row_kind' VIRTUAL,
  PRIMARY KEY (order_id) NOT ENFORCED
) WITH (
  'connector' = 'mysql-cdc',
  'hostname' = '<host>',
  'port' = '3306',
  'username' = '<cdc_user>',
  'password' = '<password>',
  'database-name' = '<db>',
  'table-name' = 'orders',
  'server-id' = '5401-5404',
  'server-time-zone' = 'Asia/Shanghai',
  'heartbeat.interval' = '30s',
  'scan.incremental.snapshot.chunk.size' = '8096'
);


-- ========== Doris 目标表 ==========
-- 使用 Unique Key 模型 + Sequence Column 保证 CDC 乱序下的数据正确性

CREATE TABLE ods.ods_orders (
  order_id    BIGINT,
  status      VARCHAR(32),
  amount      DECIMAL(10, 2),
  created_at  DATETIME,
  updated_at  DATETIME
) UNIQUE KEY(order_id)
DISTRIBUTED BY HASH(order_id) BUCKETS 8
PROPERTIES (
  'function_column.sequence_col' = 'updated_at',  -- 按更新时间排序
  'replication_num' = '1',
  'light_schema_change' = 'true'
);
