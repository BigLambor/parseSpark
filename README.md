# Spark EventLog 解析器 (PySpark版本)

基于PySpark的大规模EventLog解析方案，支持每天30万+任务的解析，将解析结果存储到Hive数据仓库。

## ⚡ 快速开始

### 1. 环境要求

- Spark 3.x (PySpark)
- Hadoop 3.x (HDFS)
- Hive 3.x
- Python 3.7+
- PyYAML 5.4+

### 2. 配置文件

复制配置文件模板并修改：

```bash
cp config.yaml.example config.yaml
vim config.yaml
```

关键配置项：

```yaml
hdfs:
  clusters:
    cluster1:
      event_log_dir: "/spark-logs"  # 修改为实际路径
      use_date_subdir: true          # 是否按日期子目录组织

hive:
  database: "meta"                   # Hive库名

parser:
  parse_parallelism: 2000            # 并行度
  skip_inprogress: true              # 跳过运行中的任务
```

### 3. 安装Python依赖

```bash
# 安装依赖
pip install -r requirements.txt

# 或者安装为Python包
pip install -e .
```

### 4. 创建Hive表

```bash
# 执行建表SQL
hive -f create_hive_tables.sql
```

主要表：
- `meta.spark_applications` - 应用级别指标
- `meta.spark_stages` - Stage级别指标
- `meta.spark_executors` - Executor信息
- `meta.spark_diagnosis` - 诊断建议
- `meta.spark_parser_status` - 解析状态

### 5. 提交任务

#### 方式1：使用提交脚本（推荐）

```bash
# 解析昨天的日志
./submit_parser.sh cluster1

# 解析指定日期
./submit_parser.sh cluster1 2024-01-15

# 自定义资源配置
NUM_EXECUTORS=300 PARALLELISM=3000 ./submit_parser.sh cluster1 2024-01-15
```

#### 方式2：直接提交（开发测试）

```bash
# 先打包Python模块
cd /path/to/parseSpark
zip -r parser.zip parser/
zip -r models.zip models/
zip -r utils.zip utils/

# 提交任务
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --driver-memory 8g \
  --executor-memory 12g \
  --executor-cores 4 \
  --num-executors 200 \
  --conf spark.app.cluster_name=cluster1 \
  --conf spark.app.target_date=2024-01-15 \
  --conf spark.app.config_path=./config.yaml \
  --py-files parser.zip,models.zip,utils.zip \
  parse_spark_logs.py
```

#### 方式3：Airflow调度（生产环境）

参考 `airflow/spark_parser_dag.py` 配置DAG：

```python
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

parse_task = SparkSubmitOperator(
    task_id='parse_cluster1',
    application='/path/to/parse_spark_logs.py',
    py_files='/path/to/parser.zip,/path/to/models.zip,/path/to/utils.zip',
    conf={
        'spark.app.cluster_name': 'cluster1',
        'spark.app.target_date': '{{ ds }}',
    },
    ...
)
```

## 📊 架构设计

### 核心架构

```
┌─────────────────────────────────────┐
│  调度系统（Crontab/Airflow）         │
└──────────────┬──────────────────────┘
               │ spark-submit
               ▼
┌─────────────────────────────────────┐
│  Spark任务（All-in-One）             │
│  1. 分布式扫描HDFS                   │
│  2. 并行解析EventLog                 │
│  3. 数据质量校验                     │
│  4. 写入Hive（幂等）                 │
│  5. 监控指标上报                     │
└─────────────────────────────────────┘
```

### 关键特性

✅ **纯Spark方案** - 无Python主控，避免单点瓶颈  
✅ **分布式扫描** - Spark并行扫描HDFS，支持百万级文件  
✅ **幂等性保证** - INSERT OVERWRITE，支持任务重跑  
✅ **数据质量校验** - 解析后立即验证，避免脏数据  
✅ **容错机制** - 单文件失败不影响全局  
✅ **监控告警** - Prometheus + Grafana实时监控  

## 🔧 配置说明

### 资源配置（根据文件数量调整）

| 文件数量 | Executor数量 | 并行度 | 预计耗时 |
|---------|-------------|--------|---------|
| 1,000 | 10 | 100 | 2-5分钟 |
| 10,000 | 50 | 500 | 5-10分钟 |
| 100,000 | 150 | 1500 | 15-30分钟 |
| 300,000+ | 200-500 | 2000-5000 | 20-60分钟 |

### 并行度计算公式

```
并行度 = executor数量 × executor核心数 × (2-3)
```

示例：
- 200 executors × 4 cores × 2.5 = 2000 partitions

## 📈 监控和告警

### Prometheus指标

```
# 解析文件数
spark_eventlog_parse_total{cluster="cluster1", status="success"}

# 解析耗时
spark_eventlog_parse_duration_seconds{cluster="cluster1"}

# 应用数量
spark_applications_count{cluster="cluster1", date="2024-01-15"}
```

### Grafana Dashboard

访问：`http://grafana:3000/d/spark-parser`

主要面板：
- 解析成功率趋势
- 解析耗时分布
- 每日应用数量
- 失败文件列表

### 告警规则

- **解析失败率过高**：失败率 > 5%
- **任务超时**：超过4小时未完成
- **数据量异常**：相比昨天变化超过30%

## 🔍 数据查询示例

### 查询每日应用数量

```sql
SELECT dt, cluster_name, COUNT(*) as app_count
FROM meta.spark_applications
WHERE dt >= '2024-01-01'
GROUP BY dt, cluster_name
ORDER BY dt DESC;
```

### 查询失败任务

```sql
SELECT app_id, app_name, user, duration_ms
FROM meta.spark_applications
WHERE dt = '2024-01-15' 
  AND cluster_name = 'cluster1'
  AND status = 'FAILED'
ORDER BY duration_ms DESC
LIMIT 100;
```

### 查询数据倾斜严重的Stage

```sql
SELECT app_id, stage_id, stage_name, 
       skew_factor, task_duration_p95, task_duration_max
FROM meta.spark_stages
WHERE dt = '2024-01-15'
  AND skew_factor > 5  -- 最慢Task是中位数的5倍以上
ORDER BY skew_factor DESC
LIMIT 50;
```

### 查询诊断建议

```sql
SELECT app_id, rule_desc, severity, diagnosis_detail, suggestion
FROM meta.spark_diagnosis
WHERE dt = '2024-01-15'
  AND severity IN ('CRITICAL', 'WARNING')
ORDER BY 
  CASE severity 
    WHEN 'CRITICAL' THEN 1
    WHEN 'WARNING' THEN 2
    ELSE 3
  END;
```

## 🐛 故障排查

### 问题1：任务执行超时

**排查步骤：**

1. 查看Spark UI，检查是否有卡住的Task
2. 检查文件数量是否超出预期
3. 检查并行度配置是否合理
4. 检查HDFS是否有性能问题

**解决方案：**
```bash
# 增加executor数量和并行度
--num-executors 400 \
--conf spark.app.parse_parallelism=4000
```

### 问题2：数据量异常

**排查步骤：**

```sql
-- 1. 检查解析文件数
SELECT COUNT(DISTINCT file_path) as file_count
FROM meta.spark_parser_status
WHERE dt = '2024-01-15' AND status = 'SUCCESS';

-- 2. 检查应用数量
SELECT COUNT(*) as app_count
FROM meta.spark_applications
WHERE dt = '2024-01-15';

-- 3. 对比前一天
SELECT dt, COUNT(*) as app_count
FROM meta.spark_applications
WHERE dt IN ('2024-01-14', '2024-01-15')
GROUP BY dt;
```

**解决方案：**
- 检查EventLog目录配置是否正确
- 检查文件过滤规则是否太严格
- 检查HDFS权限

### 问题3：内存溢出（OOM）

**排查步骤：**

1. 查看executor日志，定位OOM位置
2. 检查是否有超大文件（>1GB）
3. 检查是否使用了流式解析

**解决方案：**
```bash
# 增加executor内存
--executor-memory 16g \
--conf spark.executor.memoryOverhead=3g

# 启用Kryo序列化
--conf spark.serializer=org.apache.spark.serializer.KryoSerializer
```

### 问题4：重复数据

**排查步骤：**

```sql
-- 检查重复记录
SELECT cluster_name, app_id, dt, COUNT(*) as cnt
FROM meta.spark_applications
WHERE dt = '2024-01-15'
GROUP BY cluster_name, app_id, dt
HAVING COUNT(*) > 1;
```

**解决方案：**
```sql
-- 手动去重
INSERT OVERWRITE TABLE meta.spark_applications PARTITION(dt='2024-01-15')
SELECT * FROM (
  SELECT *, ROW_NUMBER() OVER(
    PARTITION BY cluster_name, app_id, dt 
    ORDER BY create_time DESC
  ) as rn
  FROM meta.spark_applications WHERE dt='2024-01-15'
) t WHERE rn = 1;
```

## 📚 文档

- [详细设计文档](./Spark作业解析方案设计.md) - 完整的技术方案设计
- [优化总结](./方案优化总结.md) - 优化建议和最佳实践
- [配置文件示例](./config.yaml.example) - 配置文件模板

## 🤝 贡献

欢迎提交Issue和Pull Request！

## 📄 License

MIT License

## 🔗 相关项目

- [Spark官方文档](https://spark.apache.org/docs/latest/)
- [Spark History Server](https://spark.apache.org/docs/latest/monitoring.html)
- [Dr.Elephant](https://github.com/linkedin/dr-elephant) - LinkedIn开源的Spark性能分析工具
- [Sparklint](https://github.com/groupon/sparklint) - Groupon开源的Spark性能分析工具

---

**维护者**: Data Platform Team  
**最后更新**: 2024-01-15

