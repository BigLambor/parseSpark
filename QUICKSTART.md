# 快速上手指南

本文档帮助您在10分钟内完成第一次Spark EventLog解析。

## 1. 环境检查

确保您的环境满足以下要求：

```bash
# 检查Spark版本
spark-submit --version  # 需要 >= 3.0.0

# 检查Python版本
python --version  # 需要 >= 3.7

# 检查Hadoop客户端
hadoop version  # 需要 >= 3.0.0

# 检查HDFS连接
hadoop fs -ls /  # 确保能访问HDFS

# 检查Hive
hive --version  # 需要 >= 3.0.0
```

## 2. 克隆项目

```bash
cd /opt/apps
git clone <your-repo-url> spark-eventlog-parser
cd spark-eventlog-parser
```

## 3. 配置文件

复制配置模板并修改：

```bash
cp config.yaml.example config.yaml
vim config.yaml
```

**最小化配置（仅需修改这3项）：**

```yaml
hdfs:
  clusters:
    cluster1:
      event_log_dir: "/spark-logs"  # 修改为你的EventLog目录

hive:
  database: "meta"  # 修改为你的Hive库名

parser:
  parse_parallelism: 2000  # 根据集群规模调整
```

## 4. 创建Hive表

```bash
# 方式1：直接执行SQL文件
hive -f create_hive_tables.sql

# 方式2：通过Beeline
beeline -u "jdbc:hive2://hive-server:10000" -f create_hive_tables.sql
```

验证表是否创建成功：

```bash
hive -e "SHOW TABLES IN meta LIKE 'spark_*'"
```

应该看到：
- spark_applications
- spark_jobs
- spark_stages
- spark_executors
- spark_diagnosis (可选)

## 5. 小规模测试

先用少量数据测试（1000个文件）：

```bash
# 设置资源配置
export NUM_EXECUTORS=10
export EXECUTOR_MEMORY=4g
export PARALLELISM=100

# 提交任务（解析昨天）
./submit_parser.sh cluster1
```

**预期输出：**

```
==========================================
Spark EventLog解析任务提交 (PySpark)
==========================================
集群名称: cluster1
目标日期: 2024-01-14
...
任务提交成功！
==========================================
```

## 6. 查看执行状态

### 方式1：YARN UI

访问：`http://your-yarn-rm:8088`

找到名为 `SparkEventLogParser-cluster1-2024-01-14` 的任务。

### 方式2：Spark History Server

访问：`http://your-spark-history:18080`

### 方式3：查看日志

```bash
# 获取Application ID
yarn application -list -appStates RUNNING | grep SparkEventLogParser

# 查看日志
yarn logs -applicationId <application_id>
```

## 7. 验证结果

任务完成后，查询Hive表：

```sql
-- 查看应用数量
SELECT COUNT(*) as app_count
FROM meta.spark_applications
WHERE dt = '2024-01-14' AND cluster_name = 'cluster1';

-- 查看状态分布
SELECT status, COUNT(*) as cnt
FROM meta.spark_applications
WHERE dt = '2024-01-14' AND cluster_name = 'cluster1'
GROUP BY status;

-- 查看解析的第一个应用
SELECT app_id, app_name, user, duration_ms, status
FROM meta.spark_applications
WHERE dt = '2024-01-14' AND cluster_name = 'cluster1'
LIMIT 1;
```

**期望结果：**

- app_count > 0 （如果有任务运行）
- 看到 FINISHED / FAILED / KILLED 等状态
- 数据字段完整（无大量NULL）

## 8. 全量运行

小规模测试成功后，使用生产配置：

```bash
# 恢复默认配置
unset NUM_EXECUTORS
unset EXECUTOR_MEMORY
unset PARALLELISM

# 提交全量任务
./submit_parser.sh cluster1
```

**预计耗时：**

- 1万文件：5-10分钟
- 10万文件：15-30分钟
- 30万文件：30-60分钟

## 9. 设置定时调度

### 方式1：Crontab（简单）

```bash
# 编辑crontab
crontab -e

# 添加定时任务（每天凌晨2点执行）
0 2 * * * cd /opt/apps/spark-eventlog-parser && ./submit_parser.sh cluster1 >> /var/log/spark_parser.log 2>&1
```

### 方式2：Airflow（推荐）

参考 `README.md` 中的Airflow配置。

## 10. 监控和告警

### 查看解析统计

```sql
-- 每日解析数量趋势
SELECT dt, COUNT(*) as app_count
FROM meta.spark_applications
WHERE dt >= DATE_SUB(CURRENT_DATE, 7)
  AND cluster_name = 'cluster1'
GROUP BY dt
ORDER BY dt DESC;

-- 失败率统计
SELECT 
    dt,
    SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) as failed_count,
    COUNT(*) as total_count,
    ROUND(SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as failure_rate
FROM meta.spark_applications
WHERE dt >= DATE_SUB(CURRENT_DATE, 7)
  AND cluster_name = 'cluster1'
GROUP BY dt
ORDER BY dt DESC;
```

## 常见问题

### Q1: 找不到EventLog目录

**错误信息：** `FileNotFoundError: EventLog目录不存在`

**解决方案：**
```bash
# 检查EventLog目录配置
grep event_log_dir config.yaml

# 列出HDFS上的EventLog目录
hadoop fs -ls /spark-logs

# 如果路径不同，修改config.yaml中的event_log_dir
```

### Q2: 未找到任何文件

**错误信息：** `未找到任何文件，退出`

**解决方案：**
```bash
# 检查目标日期是否有文件
hadoop fs -ls /spark-logs/2024-01-14/

# 如果没有日期子目录，检查文件修改时间
hadoop fs -ls /spark-logs/ | grep 2024-01-14
```

### Q3: Hive表不存在

**错误信息：** `Table or view 'spark_applications' not found`

**解决方案：**
```bash
# 确认表是否存在
hive -e "SHOW TABLES IN meta LIKE 'spark_*'"

# 如果不存在，执行建表SQL
hive -f create_hive_tables.sql

# 检查是否成功
hive -e "DESC meta.spark_applications"
```

### Q4: 内存不足

**错误信息：** `ExecutorLostFailure` 或 `OutOfMemoryError`

**解决方案：**
```bash
# 增加executor内存
EXECUTOR_MEMORY=16g ./submit_parser.sh cluster1

# 或者减少executor数量
NUM_EXECUTORS=100 ./submit_parser.sh cluster1
```

## 下一步

- 阅读 [完整文档](README.md) 了解更多功能
- 查看 [设计文档](Spark作业解析方案设计.md) 了解架构细节
- 参考 [优化总结](方案优化总结.md) 进行性能调优
- 配置监控告警（Prometheus + Grafana）

## 获取帮助

遇到问题？

1. 查看 [故障排查](README.md#故障排查) 章节
2. 搜索项目Issues
3. 联系维护团队

---

**恭喜！** 您已完成第一次Spark EventLog解析 🎉

