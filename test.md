# Spark EventLog 解析器 - 测试指南

> 本文档面向测试人员，指导如何配置环境、运行测试并验证功能。

---

## 📋 目录

1. [环境准备](#1-环境准备)
2. [项目部署](#2-项目部署)
3. [配置说明](#3-配置说明)
4. [测试类型](#4-测试类型)
5. [测试用例](#5-测试用例)
6. [结果验证](#6-结果验证)
7. [注意事项](#7-注意事项)
8. [常见问题](#8-常见问题)

---

## 1. 环境准备

### 1.1 前置依赖

| 组件 | 版本要求 | 验证命令 |
|------|----------|----------|
| Python | 3.7+ | `python3 --version` |
| Spark | 3.x | `spark-submit --version` |
| Hadoop | 3.x | `hadoop version` |
| Hive | 3.x | `hive --version` |
| YARN | 3.x | `yarn version` |

### 1.2 环境变量检查

```bash
# 确认以下环境变量已正确设置
echo $SPARK_HOME     # Spark安装目录
echo $HADOOP_HOME    # Hadoop安装目录
echo $HIVE_HOME      # Hive安装目录
echo $JAVA_HOME      # JDK安装目录
```

### 1.3 服务检查

```bash
# 检查YARN ResourceManager状态
yarn node -list

# 检查HDFS可用性
hdfs dfsadmin -report

# 检查Hive Metastore
hive -e "SHOW DATABASES"
```

### 1.4 网络权限

确保测试机器可以访问：
- HDFS NameNode 端口（默认 8020/9000）
- YARN ResourceManager 端口（默认 8088）
- Hive Metastore 端口（默认 9083）
- Spark History Server 端口（默认 18080）

---

## 2. 项目部署

### 2.1 获取代码

```bash
# 切换到部署目录
cd /path/to/deploy

# 拷贝或解压项目
cp -r /source/parseSpark ./
cd parseSpark
```

### 2.2 安装Python依赖

```bash
# 安装依赖
pip install -r requirements.txt

# 验证安装
python3 -c "import yaml; print('PyYAML 安装成功')"
```

### 2.3 创建配置文件

```bash
# 复制配置模板
cp config.yaml.example config.yaml

# 编辑配置文件（详见第3节）
vim config.yaml
```

### 2.4 创建Hive表

```bash
# 执行建表SQL
hive -f create_hive_tables.sql

# 验证表创建成功
hive -e "USE meta; SHOW TABLES;"
```

**预期输出：**
```
spark_applications
spark_jobs
spark_stages
spark_executors
spark_diagnosis
spark_sql_executions
spark_configs
spark_parser_status
```

---

## 3. 配置说明

### 3.1 关键配置项

编辑 `config.yaml` 文件，修改以下关键配置：

```yaml
# HDFS配置 - 根据实际集群修改
hdfs:
  default_cluster: "cluster_sanqier"  # 默认集群名
  clusters:
    cluster_sanqier:
      event_log_dir: "hdfs://beh006/var/log/hadoop-spark"  # EventLog目录
      use_date_subdir: false    # 是否按日期子目录
      date_dir_format: "yyyy-MM-dd"

# Hive配置
hive:
  database: "meta"              # Hive数据库名
  metastore_uri: "thrift://hive-metastore:9083"  # Metastore地址

# 解析配置
parser:
  scan_mode: "date_subdir"      # 扫描模式
  parse_parallelism: 2000       # 并行度
  skip_inprogress: true         # 跳过运行中的任务
```

### 3.2 配置验证

```bash
# 检查配置文件语法
python3 -c "import yaml; yaml.safe_load(open('config.yaml')); print('配置文件格式正确')"

# 检查EventLog目录是否可访问
hadoop fs -ls hdfs://beh006/var/log/hadoop-spark/ | head -5
```

### 3.3 资源配置参考

| 场景 | Executor数量 | 内存 | 并行度 | 预计耗时 |
|------|-------------|------|--------|---------|
| 小规模测试 (<1000文件) | 10 | 4g | 100 | 2-5分钟 |
| 中规模测试 (1万文件) | 50 | 8g | 500 | 5-10分钟 |
| 大规模测试 (10万文件) | 150 | 12g | 1500 | 15-30分钟 |

---

## 4. 测试类型

### 4.1 单元测试

#### 运行方式
```bash
# 方式1：使用测试脚本
./run_tests.sh

# 方式2：直接运行pytest
export PYTHONPATH=$PYTHONPATH:$(pwd)
python -m pytest tests/ -v --tb=short
```

#### 预期结果
```
tests/test_parser.py::TestApplicationState::test_app_metrics_conversion PASSED
tests/test_parser.py::TestApplicationState::test_job_metrics_conversion PASSED
tests/test_parser.py::TestApplicationState::test_job_status_detection PASSED
tests/test_parser.py::TestApplicationState::test_job_failure_marks_application_failed PASSED
tests/test_parser.py::TestApplicationState::test_skip_task_collection_when_disabled PASSED
tests/test_parser.py::TestMetricsCalculator::test_percentile PASSED
tests/test_parser.py::TestMetricsCalculator::test_stage_aggregates PASSED
tests/test_parser.py::TestMetricsCalculator::test_duration_calculation PASSED
======================== 8 passed in X.XXs ========================
```

### 4.2 集成测试

#### 方式1：使用提交脚本（推荐）

```bash
# 解析昨天的日志
./submit_parser.sh cluster_sanqier

# 解析指定日期
./submit_parser.sh cluster_sanqier 2025-11-26
```

#### 方式2：小规模测试（调整资源）

```bash
# 小规模测试，减少资源消耗
NUM_EXECUTORS=10 PARALLELISM=100 ./submit_parser.sh cluster_sanqier 2025-11-26
```

#### 方式3：手动spark-submit

```bash
# 打包Python模块
zip -r parser.zip parser/ -x "*.pyc" -x "*__pycache__*"
zip -r models.zip models/ -x "*.pyc" -x "*__pycache__*"
zip -r utils.zip utils/ -x "*.pyc" -x "*__pycache__*"

# 提交任务
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --driver-memory 4g \
  --executor-memory 4g \
  --executor-cores 2 \
  --num-executors 10 \
  --conf spark.app.cluster_name=cluster_sanqier \
  --conf spark.app.target_date=2025-11-26 \
  --conf spark.app.config_path=./config.yaml \
  --py-files parser.zip,models.zip,utils.zip \
  parse_spark_logs.py
```

---

## 5. 测试用例

### 5.1 功能测试用例

| 用例ID | 用例描述 | 前置条件 | 测试步骤 | 预期结果 |
|--------|----------|----------|----------|----------|
| TC001 | 解析单日EventLog | HDFS有EventLog文件 | 执行 `./submit_parser.sh cluster_sanqier 2025-11-26` | 任务成功，Hive有数据 |
| TC002 | 解析空目录 | EventLog目录无文件 | 执行解析任务 | 程序正常退出，提示"未找到文件" |
| TC003 | 跳过inprogress文件 | 有.inprogress文件 | 执行解析任务 | .inprogress文件被跳过 |
| TC004 | 并行解析多文件 | 有100+文件 | 执行解析任务 | 多Executor并行处理 |
| TC005 | 单文件解析失败 | 有损坏的EventLog | 执行解析任务 | 跳过坏文件，继续处理 |
| TC006 | 幂等性测试 | 已解析过数据 | 重复执行相同任务 | 数据覆盖，无重复 |

### 5.2 性能测试用例

| 用例ID | 用例描述 | 测试数据量 | 资源配置 | 验收标准 |
|--------|----------|------------|----------|----------|
| PT001 | 小规模性能 | 1000文件 | 10 executors | <5分钟 |
| PT002 | 中规模性能 | 10000文件 | 50 executors | <15分钟 |
| PT003 | 大规模性能 | 100000文件 | 150 executors | <30分钟 |

### 5.3 异常测试用例

| 用例ID | 用例描述 | 测试步骤 | 预期结果 |
|--------|----------|----------|----------|
| ET001 | 配置文件缺失 | 删除config.yaml后运行 | 程序报错退出，提示配置缺失 |
| ET002 | HDFS连接失败 | 配置错误的HDFS地址 | 程序报错，提示连接失败 |
| ET003 | Hive库不存在 | 配置不存在的数据库名 | 程序报错，提示数据库不存在 |
| ET004 | 权限不足 | 使用无权限用户 | 程序报错，提示权限不足 |

---

## 6. 结果验证

### 6.1 任务状态检查

```bash
# 查看YARN应用状态
yarn application -list -appStates ALL | grep SparkEventLogParser

# 查看Spark History Server（如果启用）
# 访问: http://your-spark-history-server:18080
```

### 6.2 Hive数据验证

#### 检查应用表数据
```sql
-- 查看记录数
SELECT COUNT(*) AS app_count 
FROM meta.spark_applications 
WHERE dt = '2025-11-26' AND cluster_name = 'cluster_sanqier';

-- 查看数据样本
SELECT app_id, app_name, user, status, duration_ms
FROM meta.spark_applications
WHERE dt = '2025-11-26' AND cluster_name = 'cluster_sanqier'
LIMIT 10;
```

#### 检查各表数据量
```sql
-- 各表数据统计
SELECT 'spark_applications' AS table_name, COUNT(*) AS cnt FROM meta.spark_applications WHERE dt = '2025-11-26'
UNION ALL
SELECT 'spark_jobs', COUNT(*) FROM meta.spark_jobs WHERE dt = '2025-11-26'
UNION ALL
SELECT 'spark_stages', COUNT(*) FROM meta.spark_stages WHERE dt = '2025-11-26'
UNION ALL
SELECT 'spark_executors', COUNT(*) FROM meta.spark_executors WHERE dt = '2025-11-26'
UNION ALL
SELECT 'spark_parser_status', COUNT(*) FROM meta.spark_parser_status WHERE dt = '2025-11-26';
```

#### 检查解析状态
```sql
-- 解析成功率
SELECT 
    status,
    COUNT(*) AS cnt,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS pct
FROM meta.spark_parser_status
WHERE dt = '2025-11-26'
GROUP BY status;
```

### 6.3 数据质量验证

```sql
-- 1. 检查必填字段是否为空
SELECT COUNT(*) AS null_count
FROM meta.spark_applications
WHERE dt = '2025-11-26' 
  AND (app_id IS NULL OR cluster_name IS NULL);
-- 预期: 0

-- 2. 检查时间逻辑是否正确
SELECT COUNT(*) AS invalid_time_count
FROM meta.spark_applications
WHERE dt = '2025-11-26' 
  AND start_time > end_time;
-- 预期: 0

-- 3. 检查重复数据
SELECT cluster_name, app_id, COUNT(*) AS cnt
FROM meta.spark_applications
WHERE dt = '2025-11-26'
GROUP BY cluster_name, app_id
HAVING COUNT(*) > 1;
-- 预期: 无结果

-- 4. 检查应用状态分布
SELECT status, COUNT(*) AS cnt
FROM meta.spark_applications
WHERE dt = '2025-11-26'
GROUP BY status;
-- 预期状态: FINISHED, FAILED, KILLED, RUNNING
```

### 6.4 诊断建议验证

```sql
-- 查看诊断建议
SELECT app_id, rule_desc, severity, suggestion
FROM meta.spark_diagnosis
WHERE dt = '2025-11-26'
ORDER BY 
  CASE severity 
    WHEN 'CRITICAL' THEN 1
    WHEN 'WARNING' THEN 2
    ELSE 3
  END
LIMIT 20;
```

---

## 7. 注意事项

### 7.1 测试前准备

1. **确认EventLog存在**
   ```bash
   # 检查目标日期是否有EventLog文件
   hadoop fs -ls hdfs://beh006/var/log/hadoop-spark/ | grep "2025-11-26" | head -5
   ```

2. **避免与生产任务冲突**
   - 测试时选择低峰期
   - 小规模测试先用少量Executor

3. **备份配置文件**
   ```bash
   cp config.yaml config.yaml.bak
   ```

### 7.2 测试期间

1. **实时监控任务**
   - 通过YARN UI查看任务状态: `http://your-yarn-rm:8088`
   - 查看Driver/Executor日志

2. **资源监控**
   - 监控集群CPU/内存使用率
   - 确保不影响其他任务

### 7.3 测试后清理

```sql
-- 如需清理测试数据
ALTER TABLE meta.spark_applications DROP IF EXISTS PARTITION (dt='2025-11-26');
ALTER TABLE meta.spark_jobs DROP IF EXISTS PARTITION (dt='2025-11-26');
ALTER TABLE meta.spark_stages DROP IF EXISTS PARTITION (dt='2025-11-26');
ALTER TABLE meta.spark_executors DROP IF EXISTS PARTITION (dt='2025-11-26');
ALTER TABLE meta.spark_diagnosis DROP IF EXISTS PARTITION (dt='2025-11-26');
ALTER TABLE meta.spark_parser_status DROP IF EXISTS PARTITION (dt='2025-11-26');
ALTER TABLE meta.spark_sql_executions DROP IF EXISTS PARTITION (dt='2025-11-26');
ALTER TABLE meta.spark_configs DROP IF EXISTS PARTITION (dt='2025-11-26');
```

### 7.4 重跑数据，譬如前期逻辑问题，待bug修正后，需要重跑数据

- 执行pre_rerun.sh 脚本，删除表里对应日期/集群的分区
- 若只想补处理失败文件而不全量重跑，可以只删除 spark_parser_status 中 status='FAILED' 的记录或对应分区。


### 7.5 安全注意事项

- ⚠️ 不要在生产环境直接测试DELETE/DROP操作
- ⚠️ 测试完成后恢复配置文件
- ⚠️ 大规模测试前先小规模验证

---

## 8. 常见问题

### Q1: 任务提交失败 "配置文件不存在"

**原因：** config.yaml 文件未创建

**解决方案：**
```bash
cp config.yaml.example config.yaml
# 编辑配置文件
vim config.yaml
```

### Q2: 连接HDFS失败

**原因：** HDFS地址配置错误或网络不通

**解决方案：**
```bash
# 1. 检查HDFS地址
hdfs dfs -ls hdfs://beh006/

# 2. 检查配置文件中的event_log_dir
grep event_log_dir config.yaml
```

### Q3: Hive表不存在

**原因：** 未执行建表SQL

**解决方案：**
```bash
hive -f create_hive_tables.sql
```

### Q4: 解析数据为0条

**原因：** 
1. EventLog目录路径错误
2. 目标日期无EventLog文件
3. 文件过滤规则太严格

**排查步骤：**
```bash
# 1. 检查配置的EventLog目录
hadoop fs -ls hdfs://beh006/var/log/hadoop-spark/

# 2. 检查文件名是否符合规则（以application_开头）
hadoop fs -ls hdfs://beh006/var/log/hadoop-spark/ | grep "application_"

# 3. 检查文件修改时间是否在目标日期
hadoop fs -ls hdfs://beh006/var/log/hadoop-spark/ | head -20
```

### Q5: 任务执行超时

**原因：** 文件数量多但资源配置不足

**解决方案：**
```bash
# 增加Executor数量和并行度
NUM_EXECUTORS=200 PARALLELISM=2000 ./submit_parser.sh cluster_sanqier 2025-11-26
```

### Q6: 内存溢出 (OOM)

**原因：** 单个EventLog文件过大或内存配置不足

**解决方案：**
```bash
# 增加Executor内存
EXECUTOR_MEMORY=16g EXECUTOR_MEMORY_OVERHEAD=3g ./submit_parser.sh cluster_sanqier 2025-11-26
```

### Q7: 数据有重复

**原因：** 任务被重复执行但写入模式配置为append

**解决方案：**
1. 确认配置文件中 `write_mode: "overwrite"`
2. 手动清理重复分区后重跑

---

## 📞 问题反馈

如遇到本文档未涵盖的问题，请收集以下信息后反馈：

1. 执行的命令
2. 完整的错误日志
3. config.yaml 配置（脱敏后）
4. 环境信息（Spark/Hadoop版本等）
5. YARN Application ID（如有）

---

**文档版本：** v1.0  
**最后更新：** 2025-12-07

