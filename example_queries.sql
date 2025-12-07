-- Spark EventLog 解析结果查询示例
-- 用途：演示如何查询和分析解析后的数据

-- ============================
-- 1. 基础查询
-- ============================

-- 1.1 查看每日应用数量
SELECT dt, cluster_name, COUNT(*) as app_count
FROM meta.spark_applications
WHERE dt >= DATE_SUB(CURRENT_DATE, 7)
GROUP BY dt, cluster_name
ORDER BY dt DESC, cluster_name;

-- 1.2 查看某天的数据量统计
SELECT 
    COUNT(DISTINCT app_id) as unique_apps,
    COUNT(*) as total_apps,
    SUM(CASE WHEN status = 'FINISHED' THEN 1 ELSE 0 END) as finished,
    SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) as failed,
    SUM(CASE WHEN status = 'KILLED' THEN 1 ELSE 0 END) as killed
FROM meta.spark_applications
WHERE dt = '2025-12-05' AND cluster_name = 'cluster1';

-- 1.3 查看应用详情（最新10个）
SELECT 
    app_id,
    app_name,
    user,
    status,
    ROUND(duration_ms / 1000.0, 2) as duration_sec,
    executor_count,
    total_cores
FROM meta.spark_applications
WHERE dt = '2025-12-05' AND cluster_name = 'cluster1'
ORDER BY start_time DESC
LIMIT 10;

-- ============================
-- 2. 性能分析
-- ============================

-- 2.1 查找耗时最长的应用（Top 20）
SELECT 
    app_id,
    app_name,
    user,
    ROUND(duration_ms / 1000.0 / 60, 2) as duration_min,
    status
FROM meta.spark_applications
WHERE dt = '2025-12-05' AND cluster_name = 'cluster1'
ORDER BY duration_ms DESC
LIMIT 20;

-- 2.2 查找失败的应用
SELECT 
    app_id,
    app_name,
    user,
    ROUND(duration_ms / 1000.0, 2) as duration_sec,
    spark_version
FROM meta.spark_applications
WHERE dt = '2025-12-05' 
  AND cluster_name = 'cluster1'
  AND status = 'FAILED'
ORDER BY start_time DESC;

-- 2.3 Stage性能分析 - 查找数据倾斜严重的Stage
SELECT 
    a.app_id,
    a.app_name,
    s.stage_id,
    s.stage_name,
    s.skew_factor,
    s.task_duration_p50,
    s.task_duration_p95,
    s.task_duration_max,
    ROUND(s.duration_ms / 1000.0, 2) as stage_duration_sec
FROM meta.spark_stages s
JOIN meta.spark_applications a 
    ON s.app_id = a.app_id AND s.dt = a.dt AND s.cluster_name = a.cluster_name
WHERE s.dt = '2025-12-05'
  AND s.cluster_name = 'cluster1'
  AND s.skew_factor > 5  -- 最慢Task是中位数的5倍以上
ORDER BY s.skew_factor DESC
LIMIT 50;

-- 2.4 Shuffle数据量分析
SELECT 
    app_id,
    stage_id,
    stage_name,
    ROUND(shuffle_read_bytes / 1024.0 / 1024 / 1024, 2) as shuffle_read_gb,
    ROUND(shuffle_write_bytes / 1024.0 / 1024 / 1024, 2) as shuffle_write_gb,
    ROUND((shuffle_read_bytes + shuffle_write_bytes) / 1024.0 / 1024 / 1024, 2) as total_shuffle_gb
FROM meta.spark_stages
WHERE dt = '2025-12-05' 
  AND cluster_name = 'cluster1'
  AND (shuffle_read_bytes > 0 OR shuffle_write_bytes > 0)
ORDER BY (shuffle_read_bytes + shuffle_write_bytes) DESC
LIMIT 20;

-- ============================
-- 3. 资源使用分析
-- ============================

-- 3.1 按用户统计资源使用
SELECT 
    user,
    COUNT(DISTINCT app_id) as app_count,
    AVG(executor_count) as avg_executors,
    AVG(total_cores) as avg_cores,
    SUM(duration_ms) / 1000.0 / 3600 as total_runtime_hours
FROM meta.spark_applications
WHERE dt = '2025-12-05' AND cluster_name = 'cluster1'
GROUP BY user
ORDER BY total_runtime_hours DESC;

-- 3.2 Executor使用统计
SELECT 
    app_id,
    COUNT(DISTINCT executor_id) as executor_count,
    AVG(total_cores) as avg_cores_per_executor,
    AVG(max_memory_mb) as avg_memory_mb_per_executor
FROM meta.spark_executors
WHERE dt = '2025-12-05' AND cluster_name = 'cluster1'
GROUP BY app_id
ORDER BY executor_count DESC
LIMIT 20;

-- ============================
-- 4. 趋势分析
-- ============================

-- 4.1 每日应用数量趋势（最近30天）
SELECT 
    dt,
    COUNT(*) as total_apps,
    SUM(CASE WHEN status = 'FINISHED' THEN 1 ELSE 0 END) as finished,
    SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) as failed,
    ROUND(SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as failure_rate
FROM meta.spark_applications
WHERE dt >= DATE_SUB(CURRENT_DATE, 30)
  AND cluster_name = 'cluster1'
GROUP BY dt
ORDER BY dt DESC;

-- 4.2 每日资源使用趋势
SELECT 
    dt,
    SUM(duration_ms) / 1000.0 / 3600 as total_runtime_hours,
    AVG(executor_count) as avg_executors,
    AVG(total_cores) as avg_cores
FROM meta.spark_applications
WHERE dt >= DATE_SUB(CURRENT_DATE, 7)
  AND cluster_name = 'cluster1'
GROUP BY dt
ORDER BY dt DESC;

-- ============================
-- 5. 数据质量检查
-- ============================

-- 5.1 检查重复记录
SELECT cluster_name, app_id, dt, COUNT(*) as cnt
FROM meta.spark_applications
WHERE dt = '2025-12-05'
GROUP BY cluster_name, app_id, dt
HAVING COUNT(*) > 1;

-- 5.2 检查数据完整性
SELECT 
    COUNT(*) as total,
    SUM(CASE WHEN app_id IS NULL THEN 1 ELSE 0 END) as null_app_id,
    SUM(CASE WHEN app_name IS NULL THEN 1 ELSE 0 END) as null_app_name,
    SUM(CASE WHEN start_time IS NULL THEN 1 ELSE 0 END) as null_start_time,
    SUM(CASE WHEN end_time IS NULL THEN 1 ELSE 0 END) as null_end_time,
    SUM(CASE WHEN duration_ms < 0 THEN 1 ELSE 0 END) as negative_duration
FROM meta.spark_applications
WHERE dt = '2025-12-05' AND cluster_name = 'cluster1';

-- 5.3 检查时间合理性
SELECT 
    app_id,
    start_time,
    end_time,
    duration_ms
FROM meta.spark_applications
WHERE dt = '2025-12-05' 
  AND cluster_name = 'cluster1'
  AND (start_time > end_time OR duration_ms < 0)
LIMIT 10;

-- ============================
-- 6. 高级分析
-- ============================

-- 6.1 应用运行时段分布（按小时）
SELECT 
    HOUR(FROM_UNIXTIME(start_time / 1000)) as hour,
    COUNT(*) as app_count
FROM meta.spark_applications
WHERE dt = '2025-12-05' AND cluster_name = 'cluster1'
GROUP BY HOUR(FROM_UNIXTIME(start_time / 1000))
ORDER BY hour;

-- 6.2 Job级别统计
SELECT 
    a.app_id,
    a.app_name,
    COUNT(DISTINCT j.job_id) as job_count,
    AVG(j.duration_ms) / 1000.0 as avg_job_duration_sec,
    SUM(CASE WHEN j.status = 'FAILED' THEN 1 ELSE 0 END) as failed_jobs
FROM meta.spark_applications a
LEFT JOIN meta.spark_jobs j 
    ON a.app_id = j.app_id AND a.dt = j.dt AND a.cluster_name = j.cluster_name
WHERE a.dt = '2025-12-05' AND a.cluster_name = 'cluster1'
GROUP BY a.app_id, a.app_name
HAVING failed_jobs > 0
ORDER BY failed_jobs DESC
LIMIT 20;

-- 6.3 Stage级别汇总
SELECT 
    app_id,
    COUNT(DISTINCT stage_id) as stage_count,
    SUM(num_tasks) as total_tasks,
    SUM(num_failed_tasks) as total_failed_tasks,
    ROUND(AVG(skew_factor), 2) as avg_skew_factor,
    MAX(skew_factor) as max_skew_factor
FROM meta.spark_stages
WHERE dt = '2025-12-05' AND cluster_name = 'cluster1'
GROUP BY app_id
HAVING max_skew_factor > 10  -- 存在严重倾斜
ORDER BY max_skew_factor DESC
LIMIT 20;

-- ============================
-- 7. 导出报表
-- ============================

-- 7.1 每日汇总报表
SELECT 
    dt as 日期,
    cluster_name as 集群,
    COUNT(DISTINCT app_id) as 应用数,
    ROUND(SUM(duration_ms) / 1000.0 / 3600, 2) as 总运行时长_小时,
    ROUND(AVG(executor_count), 2) as 平均Executor数,
    SUM(CASE WHEN status = 'FINISHED' THEN 1 ELSE 0 END) as 成功数,
    SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) as 失败数,
    ROUND(SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as 失败率
FROM meta.spark_applications
WHERE dt >= DATE_SUB(CURRENT_DATE, 7)
GROUP BY dt, cluster_name
ORDER BY dt DESC, cluster_name;

