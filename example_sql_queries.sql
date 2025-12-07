-- Spark SQL执行和配置查询示例
-- 用途：演示如何查询SQL语句和Spark配置参数

-- ============================
-- 1. SQL执行查询
-- ============================

-- 1.1 查看所有SQL执行记录
SELECT 
    app_id,
    execution_id,
    sql_text,
    status,
    duration_ms / 1000.0 as duration_sec,
    start_time,
    end_time
FROM meta.spark_sql_executions
WHERE dt = '2025-12-05' 
  AND cluster_name = 'cluster1'
ORDER BY start_time DESC
LIMIT 20;

-- 1.2 查找失败的SQL执行
SELECT 
    app_id,
    execution_id,
    sql_text,
    error_message,
    duration_ms / 1000.0 as duration_sec
FROM meta.spark_sql_executions
WHERE dt = '2025-12-05' 
  AND cluster_name = 'cluster1'
  AND status = 'FAILED'
ORDER BY start_time DESC;

-- 1.3 查找耗时最长的SQL（Top 20）
SELECT 
    app_id,
    execution_id,
    LEFT(sql_text, 100) as sql_preview,  -- 只显示前100个字符
    duration_ms / 1000.0 / 60 as duration_minutes,
    status
FROM meta.spark_sql_executions
WHERE dt = '2025-12-05' 
  AND cluster_name = 'cluster1'
ORDER BY duration_ms DESC
LIMIT 20;

-- 1.4 按应用统计SQL执行情况
SELECT 
    app_id,
    COUNT(*) as total_sql_executions,
    SUM(CASE WHEN status = 'SUCCEEDED' THEN 1 ELSE 0 END) as succeeded,
    SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) as failed,
    AVG(duration_ms) / 1000.0 as avg_duration_sec,
    MAX(duration_ms) / 1000.0 as max_duration_sec
FROM meta.spark_sql_executions
WHERE dt = '2025-12-05' 
  AND cluster_name = 'cluster1'
GROUP BY app_id
ORDER BY total_sql_executions DESC;

-- 1.5 查找包含特定关键词的SQL
SELECT 
    app_id,
    execution_id,
    sql_text,
    duration_ms / 1000.0 as duration_sec
FROM meta.spark_sql_executions
WHERE dt = '2025-12-05' 
  AND cluster_name = 'cluster1'
  AND sql_text LIKE '%INSERT%'  -- 查找包含INSERT的SQL
ORDER BY start_time DESC;

-- ============================
-- 2. Spark配置参数查询
-- ============================

-- 2.1 查看某个应用的所有Spark配置
SELECT 
    config_key,
    config_value,
    config_category
FROM meta.spark_configs
WHERE dt = '2025-12-05' 
  AND cluster_name = 'cluster1'
  AND app_id = 'application_1234567890_0001'
ORDER BY config_category, config_key;

-- 2.2 查找使用特定配置的应用
SELECT DISTINCT
    app_id,
    config_value
FROM meta.spark_configs
WHERE dt = '2025-12-05' 
  AND cluster_name = 'cluster1'
  AND config_key = 'spark.sql.shuffle.partitions'
ORDER BY app_id;

-- 2.3 统计配置使用情况
SELECT 
    config_key,
    COUNT(DISTINCT app_id) as app_count,
    COUNT(*) as total_configs
FROM meta.spark_configs
WHERE dt = '2025-12-05' 
  AND cluster_name = 'cluster1'
  AND config_category = 'spark'
GROUP BY config_key
ORDER BY app_count DESC
LIMIT 50;

-- 2.4 查找内存配置异常的应用
SELECT DISTINCT
    app_id,
    MAX(CASE WHEN config_key = 'spark.executor.memory' THEN config_value END) as executor_memory,
    MAX(CASE WHEN config_key = 'spark.driver.memory' THEN config_value END) as driver_memory
FROM meta.spark_configs
WHERE dt = '2025-12-05' 
  AND cluster_name = 'cluster1'
  AND config_key IN ('spark.executor.memory', 'spark.driver.memory')
GROUP BY app_id
HAVING executor_memory IS NOT NULL OR driver_memory IS NOT NULL
ORDER BY app_id;

-- 2.5 查找使用动态分区的应用
SELECT DISTINCT
    app_id,
    config_value as partition_mode
FROM meta.spark_configs
WHERE dt = '2025-12-05' 
  AND cluster_name = 'cluster1'
  AND config_key = 'spark.sql.sources.partitionOverwriteMode'
ORDER BY app_id;

-- ============================
-- 3. SQL执行与Job关联查询
-- ============================

-- 3.1 查看SQL执行及其关联的Job
SELECT 
    s.app_id,
    s.execution_id,
    LEFT(s.sql_text, 80) as sql_preview,
    s.job_ids,
    s.duration_ms / 1000.0 as sql_duration_sec,
    j.job_id,
    j.duration_ms / 1000.0 as job_duration_sec,
    j.status as job_status
FROM meta.spark_sql_executions s
LEFT JOIN meta.spark_jobs j 
    ON s.app_id = j.app_id 
    AND s.dt = j.dt 
    AND s.cluster_name = j.cluster_name
WHERE s.dt = '2025-12-05' 
  AND s.cluster_name = 'cluster1'
ORDER BY s.start_time DESC
LIMIT 50;

-- ============================
-- 4. 综合查询：应用 + SQL + 配置
-- ============================

-- 4.1 查看应用的完整信息（包括SQL和配置）
SELECT 
    a.app_id,
    a.app_name,
    a.user,
    a.status as app_status,
    a.duration_ms / 1000.0 / 60 as app_duration_minutes,
    COUNT(DISTINCT s.execution_id) as sql_count,
    COUNT(DISTINCT c.config_key) as config_count
FROM meta.spark_applications a
LEFT JOIN meta.spark_sql_executions s 
    ON a.app_id = s.app_id 
    AND a.dt = s.dt 
    AND a.cluster_name = s.cluster_name
LEFT JOIN meta.spark_configs c 
    ON a.app_id = c.app_id 
    AND a.dt = c.dt 
    AND a.cluster_name = c.cluster_name
WHERE a.dt = '2025-12-05' 
  AND a.cluster_name = 'cluster1'
GROUP BY a.app_id, a.app_name, a.user, a.status, a.duration_ms
ORDER BY a.start_time DESC
LIMIT 20;

-- 4.2 查找包含大量SQL执行的应用
SELECT 
    a.app_id,
    a.app_name,
    COUNT(DISTINCT s.execution_id) as sql_execution_count,
    SUM(CASE WHEN s.status = 'FAILED' THEN 1 ELSE 0 END) as failed_sql_count,
    a.duration_ms / 1000.0 / 60 as app_duration_minutes
FROM meta.spark_applications a
JOIN meta.spark_sql_executions s 
    ON a.app_id = s.app_id 
    AND a.dt = s.dt 
    AND a.cluster_name = s.cluster_name
WHERE a.dt = '2025-12-05' 
  AND a.cluster_name = 'cluster1'
GROUP BY a.app_id, a.app_name, a.duration_ms
HAVING sql_execution_count > 10
ORDER BY sql_execution_count DESC;

