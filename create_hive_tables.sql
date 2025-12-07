-- Spark EventLog解析结果表结构
-- 数据库：meta
-- 执行方式：hive -f create_hive_tables.sql

-- 创建数据库（如果不存在）
CREATE DATABASE IF NOT EXISTS meta
COMMENT 'Spark EventLog解析元数据库';

USE meta;

-- =====================================================
-- 1. 应用表（spark_applications）
-- =====================================================
CREATE TABLE IF NOT EXISTS spark_applications (
    cluster_name STRING COMMENT '集群名称',
    app_id STRING COMMENT '应用ID',
    app_name STRING COMMENT '应用名称',
    start_time BIGINT COMMENT '启动时间（时间戳，毫秒）',
    end_time BIGINT COMMENT '结束时间（时间戳，毫秒）',
    duration_ms BIGINT COMMENT '运行时长（毫秒）',
    status STRING COMMENT '状态：RUNNING, FINISHED, FAILED, KILLED',
    app_user STRING COMMENT '用户',
    spark_version STRING COMMENT 'Spark版本',
    executor_count INT COMMENT 'Executor数量',
    total_cores INT COMMENT '总核心数',
    total_memory_mb BIGINT COMMENT '总内存（MB）',
    create_time TIMESTAMP COMMENT '记录创建时间'
) 
COMMENT 'Spark应用级别指标'
PARTITIONED BY (dt STRING COMMENT '分区日期，格式：YYYY-MM-DD')
STORED AS PARQUET
TBLPROPERTIES (
    'parquet.compress'='SNAPPY',
    'parquet.block.size'='268435456'
);

-- =====================================================
-- 2. Job表（spark_jobs）
-- =====================================================
CREATE TABLE IF NOT EXISTS spark_jobs (
    cluster_name STRING COMMENT '集群名称',
    app_id STRING COMMENT '应用ID',
    job_id INT COMMENT 'Job ID',
    submission_time BIGINT COMMENT '提交时间（时间戳，毫秒）',
    completion_time BIGINT COMMENT '完成时间（时间戳，毫秒）',
    duration_ms BIGINT COMMENT '运行时长（毫秒）',
    status STRING COMMENT '状态：SUCCEEDED, FAILED',
    stage_count INT COMMENT 'Stage数量',
    create_time TIMESTAMP COMMENT '记录创建时间'
) 
COMMENT 'Spark Job级别指标'
PARTITIONED BY (dt STRING COMMENT '分区日期，格式：YYYY-MM-DD')
STORED AS PARQUET
TBLPROPERTIES (
    'parquet.compress'='SNAPPY'
);

-- =====================================================
-- 3. Stage表（spark_stages）
-- =====================================================
CREATE TABLE IF NOT EXISTS spark_stages (
    cluster_name STRING COMMENT '集群名称',
    app_id STRING COMMENT '应用ID',
    job_id INT COMMENT 'Job ID',
    stage_id INT COMMENT 'Stage ID',
    stage_name STRING COMMENT 'Stage名称',
    submission_time BIGINT COMMENT '提交时间（时间戳，毫秒）',
    completion_time BIGINT COMMENT '完成时间（时间戳，毫秒）',
    duration_ms BIGINT COMMENT '运行时长（毫秒）',
    status STRING COMMENT '状态',
    input_bytes BIGINT COMMENT '输入数据量（字节）',
    input_records BIGINT COMMENT '输入记录数',
    output_bytes BIGINT COMMENT '输出数据量（字节）',
    output_records BIGINT COMMENT '输出记录数',
    shuffle_read_bytes BIGINT COMMENT 'Shuffle读取数据量（字节）',
    shuffle_read_records BIGINT COMMENT 'Shuffle读取记录数',
    shuffle_write_bytes BIGINT COMMENT 'Shuffle写入数据量（字节）',
    shuffle_write_records BIGINT COMMENT 'Shuffle写入记录数',
    num_tasks INT COMMENT 'Task数量',
    num_failed_tasks INT COMMENT '失败Task数量',
    -- 聚合统计指标（替代全量Task明细）
    task_duration_p50 BIGINT COMMENT 'Task耗时中位数(ms)',
    task_duration_p75 BIGINT COMMENT 'Task耗时P75(ms)',
    task_duration_p95 BIGINT COMMENT 'Task耗时P95(ms)',
    task_duration_max BIGINT COMMENT 'Task耗时最大值(ms)',
    skew_factor DOUBLE COMMENT '时间倾斜因子(Max/Median)',
    input_skew_factor DOUBLE COMMENT '数据倾斜因子(MaxInput/MedianInput)',
    peak_memory_max BIGINT COMMENT 'Stage内Executor最大内存峰值(Bytes)',
    create_time TIMESTAMP COMMENT '记录创建时间'
) 
COMMENT 'Spark Stage级别指标（包含聚合统计）'
PARTITIONED BY (dt STRING COMMENT '分区日期，格式：YYYY-MM-DD')
STORED AS PARQUET
TBLPROPERTIES (
    'parquet.compress'='SNAPPY'
);

-- =====================================================
-- 4. Executor表（spark_executors）
-- =====================================================
CREATE TABLE IF NOT EXISTS spark_executors (
    cluster_name STRING COMMENT '集群名称',
    app_id STRING COMMENT '应用ID',
    executor_id STRING COMMENT 'Executor ID',
    host STRING COMMENT '主机地址',
    add_time BIGINT COMMENT '添加时间（时间戳，毫秒）',
    remove_time BIGINT COMMENT '移除时间（时间戳，毫秒）',
    total_cores INT COMMENT '总核心数',
    max_memory_mb BIGINT COMMENT '最大内存（MB）',
    create_time TIMESTAMP COMMENT '记录创建时间'
) 
COMMENT 'Spark Executor信息'
PARTITIONED BY (dt STRING COMMENT '分区日期，格式：YYYY-MM-DD')
STORED AS PARQUET
TBLPROPERTIES (
    'parquet.compress'='SNAPPY'
);

-- =====================================================
-- 5. 诊断建议表（spark_diagnosis）
-- =====================================================
CREATE TABLE IF NOT EXISTS spark_diagnosis (
    cluster_name STRING COMMENT '集群名称',
    app_id STRING COMMENT '应用ID',
    rule_id STRING COMMENT '规则ID',
    rule_desc STRING COMMENT '规则描述',
    severity STRING COMMENT '严重等级：CRITICAL, WARNING, INFO',
    diagnosis_detail STRING COMMENT '诊断详情（JSON格式，包含相关StageID或数值）',
    suggestion STRING COMMENT '优化建议',
    create_time TIMESTAMP COMMENT '记录创建时间'
) 
COMMENT 'Spark作业诊断建议'
PARTITIONED BY (dt STRING COMMENT '分区日期，格式：YYYY-MM-DD')
STORED AS PARQUET
TBLPROPERTIES (
    'parquet.compress'='SNAPPY'
);

-- =====================================================
-- 6. SQL执行表（spark_sql_executions）
-- =====================================================
CREATE TABLE IF NOT EXISTS spark_sql_executions (
    cluster_name STRING COMMENT '集群名称',
    app_id STRING COMMENT '应用ID',
    execution_id INT COMMENT 'SQL执行ID',
    sql_text STRING COMMENT 'SQL语句文本',
    description STRING COMMENT 'SQL描述',
    physical_plan_description STRING COMMENT '物理执行计划描述',
    start_time BIGINT COMMENT '开始时间（时间戳，毫秒）',
    end_time BIGINT COMMENT '结束时间（时间戳，毫秒）',
    duration_ms BIGINT COMMENT '执行时长（毫秒）',
    job_ids STRING COMMENT '关联的Job IDs（JSON数组字符串）',
    status STRING COMMENT '状态：SUCCEEDED, FAILED',
    error_message STRING COMMENT '错误信息（如果失败）',
    create_time TIMESTAMP COMMENT '记录创建时间'
) 
COMMENT 'Spark SQL执行记录'
PARTITIONED BY (dt STRING COMMENT '分区日期，格式：YYYY-MM-DD')
STORED AS PARQUET
TBLPROPERTIES (
    'parquet.compress'='SNAPPY'
);

-- =====================================================
-- 7. Spark配置参数表（spark_configs）
-- =====================================================
CREATE TABLE IF NOT EXISTS spark_configs (
    cluster_name STRING COMMENT '集群名称',
    app_id STRING COMMENT '应用ID',
    config_key STRING COMMENT '配置键',
    config_value STRING COMMENT '配置值',
    config_category STRING COMMENT '配置类别：spark, system, java',
    create_time TIMESTAMP COMMENT '记录创建时间'
) 
COMMENT 'Spark应用配置参数'
PARTITIONED BY (dt STRING COMMENT '分区日期，格式：YYYY-MM-DD')
STORED AS PARQUET
TBLPROPERTIES (
    'parquet.compress'='SNAPPY'
);

-- =====================================================
-- 8. 解析状态表（spark_parser_status）
-- =====================================================
CREATE TABLE IF NOT EXISTS spark_parser_status (
    cluster_name STRING COMMENT '集群名称',
    file_path STRING COMMENT 'EventLog文件路径',
    process_date STRING COMMENT '处理日期',
    status STRING COMMENT '处理状态：SUCCESS, FAILED, PROCESSING',
    record_count INT COMMENT '解析记录数',
    process_time TIMESTAMP COMMENT '处理时间',
    duration_ms BIGINT COMMENT '处理耗时（毫秒）',
    error_msg STRING COMMENT '错误信息（如果失败）'
) 
COMMENT 'EventLog解析状态表（用于幂等性保证和断点续传）'
PARTITIONED BY (dt STRING COMMENT '分区日期，格式：YYYY-MM-DD')
STORED AS PARQUET
TBLPROPERTIES (
    'parquet.compress'='SNAPPY'
);

-- =====================================================
-- 9. Task表（可选，仅在需要时创建）
-- =====================================================
-- 注意：Task级别数据量巨大，建议默认不创建和解析
-- 如果需要详细的Task分析，可以取消下面的注释


CREATE TABLE IF NOT EXISTS spark_tasks (
    cluster_name STRING COMMENT '集群名称',
    app_id STRING COMMENT '应用ID',
    stage_id INT COMMENT 'Stage ID',
    task_id BIGINT COMMENT 'Task ID',
    executor_id STRING COMMENT 'Executor ID',
    host STRING COMMENT '主机地址',
    launch_time BIGINT COMMENT '启动时间（时间戳，毫秒）',
    finish_time BIGINT COMMENT '完成时间（时间戳，毫秒）',
    duration_ms BIGINT COMMENT '运行时长（毫秒）',
    status STRING COMMENT '状态',
    input_bytes BIGINT COMMENT '输入数据量（字节）',
    output_bytes BIGINT COMMENT '输出数据量（字节）',
    shuffle_read_bytes BIGINT COMMENT 'Shuffle读取数据量（字节）',
    shuffle_write_bytes BIGINT COMMENT 'Shuffle写入数据量（字节）',
    gc_time BIGINT COMMENT 'GC时间（毫秒）',
    create_time TIMESTAMP COMMENT '记录创建时间'
) 
COMMENT 'Spark Task级别详细指标（数据量大，谨慎使用）'
PARTITIONED BY (dt STRING COMMENT '分区日期，格式：YYYY-MM-DD')
STORED AS PARQUET
TBLPROPERTIES (
    'parquet.compress'='SNAPPY'
);


-- =====================================================
-- 创建常用视图
-- =====================================================

-- 每日应用数量统计视图
CREATE OR REPLACE VIEW spark_daily_app_stats AS
SELECT 
    dt,
    cluster_name,
    COUNT(*) as total_apps,
    SUM(CASE WHEN status = 'FINISHED' THEN 1 ELSE 0 END) as finished_apps,
    SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) as failed_apps,
    SUM(CASE WHEN status = 'KILLED' THEN 1 ELSE 0 END) as killed_apps,
    AVG(duration_ms) as avg_duration_ms,
    PERCENTILE(CAST(duration_ms AS BIGINT), 0.5) as p50_duration_ms,
    PERCENTILE(CAST(duration_ms AS BIGINT), 0.95) as p95_duration_ms,
    MAX(duration_ms) as max_duration_ms
FROM spark_applications
GROUP BY dt, cluster_name;

-- 数据倾斜严重的Stage视图
CREATE OR REPLACE VIEW spark_skewed_stages AS
SELECT 
    dt,
    cluster_name,
    app_id,
    stage_id,
    stage_name,
    skew_factor,
    input_skew_factor,
    task_duration_max,
    task_duration_p50,
    num_tasks
FROM spark_stages
WHERE skew_factor > 5  -- 倾斜因子大于5
ORDER BY dt DESC, skew_factor DESC;

-- 运行时间最长的应用视图
CREATE OR REPLACE VIEW spark_long_running_apps AS
SELECT 
    dt,
    cluster_name,
    app_id,
    app_name,
    app_user,
    duration_ms,
    duration_ms / 1000 / 60 as duration_minutes,
    status,
    executor_count,
    total_cores
FROM spark_applications
WHERE duration_ms > 3600000  -- 超过1小时
ORDER BY dt DESC, duration_ms DESC;

-- =====================================================
-- 输出建表结果
-- =====================================================
SHOW TABLES IN meta;

-- 验证表结构
DESCRIBE FORMATTED spark_applications;

-- 打印创建完成信息
SELECT '====================================' as msg
UNION ALL SELECT 'Hive表创建完成！' as msg
UNION ALL SELECT '====================================' as msg
UNION ALL SELECT '创建的表：' as msg
UNION ALL SELECT '  1. spark_applications   - 应用指标' as msg
UNION ALL SELECT '  2. spark_jobs            - Job指标' as msg
UNION ALL SELECT '  3. spark_stages          - Stage指标' as msg
UNION ALL SELECT '  4. spark_executors       - Executor信息' as msg
UNION ALL SELECT '  5. spark_diagnosis       - 诊断建议' as msg
UNION ALL SELECT '  6. spark_sql_executions  - SQL执行记录' as msg
UNION ALL SELECT '  7. spark_configs         - Spark配置参数' as msg
UNION ALL SELECT '  8. spark_parser_status   - 解析状态' as msg
UNION ALL SELECT '====================================' as msg;

