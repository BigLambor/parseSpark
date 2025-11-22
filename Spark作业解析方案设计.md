# Spark作业解析方案设计（基于日志解析）

## 一、数据来源

### 1.1 Spark EventLog文件位置
- **存储位置**：确认在HDFS上，路径通过配置文件指定
- **部署要求**：部署机器需要安装Hadoop客户端和Spark客户端，Hadoop客户端会自动读取配置文件（core-site.xml等）获取NameNode等信息
- **EventLog目录**：通过配置文件指定，也可以通过命令从Spark配置中获取（`spark.eventLog.dir`）
- **文件命名规则**：通常为 `application_<timestamp>_<appId>.inprogress` 或 `application_<timestamp>_<appId>`
- **文件格式**：JSON格式，每行一个事件（JSON Lines格式）

### 1.2 配置文件设计

#### 1.2.1 配置文件格式（config.yaml）
```yaml
# HDFS配置
# 注意：部署机器需要安装Hadoop客户端和Spark客户端
# Hadoop客户端会自动读取core-site.xml等配置文件获取NameNode等信息
hdfs:
  default_cluster: "cluster1"
  clusters:
    cluster1:
      # EventLog目录路径（可通过命令获取：查看spark-defaults.conf中的spark.eventLog.dir）
      event_log_dir: "/spark-logs"
    
    cluster2:
      event_log_dir: "/spark-logs"

# Hive配置
hive:
  database: "meta"
  metastore_uri: "thrift://hive-metastore:9083"
  write_mode: "overwrite"
  dynamic_partition: true

# 解析配置
parser:
  default_date: null
  scan_mode: "mtime"
  parse_tasks: false
  batch_size: 10000
  skip_inprogress: true
```

#### 1.2.2 EventLog目录获取方式

由于部署机器已安装Hadoop客户端和Spark客户端，可以通过以下方式获取EventLog目录：

1. **从Spark配置文件获取**：
   ```bash
   # 查看spark-defaults.conf中的配置
   grep spark.eventLog.dir $SPARK_HOME/conf/spark-defaults.conf
   
   # 或者使用spark-submit查看配置
   spark-submit --conf spark.eventLog.dir 2>&1 | grep eventLog
   ```

2. **从Spark History Server配置获取**：
   ```bash
   # 查看spark-env.sh或相关配置文件
   grep -r "spark.eventLog.dir" $SPARK_HOME/conf/
   ```

3. **手动配置**：如果无法通过命令获取，可以在配置文件中手动指定

### 1.2 日志文件结构
Spark EventLog采用JSON Lines格式，每行代表一个Spark事件，包含以下主要事件类型：
- `SparkListenerApplicationStart` - 应用启动事件
- `SparkListenerApplicationEnd` - 应用结束事件
- `SparkListenerJobStart` - Job启动事件
- `SparkListenerJobEnd` - Job结束事件
- `SparkListenerStageSubmitted` - Stage提交事件
- `SparkListenerStageCompleted` - Stage完成事件
- `SparkListenerTaskStart` - Task启动事件
- `SparkListenerTaskEnd` - Task结束事件
- `SparkListenerExecutorAdded` - Executor添加事件
- `SparkListenerExecutorRemoved` - Executor移除事件
- `SparkListenerBlockManagerAdded` - BlockManager添加事件
- `SparkListenerEnvironmentUpdate` - 环境更新事件

## 二、可获取的信息清单

### 2.1 应用级别信息
- **应用ID** (`appId`)
- **应用名称** (`appName`)
- **启动时间** (`startTime`)
- **结束时间** (`endTime`)
- **运行时长** (计算得出)
- **用户** (`user`)
- **Spark版本** (`sparkVersion`)
- **应用状态** (`state`: RUNNING, FINISHED, FAILED, KILLED)

### 2.2 Job级别信息
- **Job ID** (`jobId`)
- **Job提交时间** (`submissionTime`)
- **Job完成时间** (`completionTime`)
- **Job状态** (`status`: SUCCEEDED, FAILED)
- **关联的Stage IDs** (`stageIds`)
- **Job结果** (`result`)

### 2.3 Stage级别信息
- **Stage ID** (`stageId`)
- **Stage名称** (`stageName`)
- **Stage提交时间** (`submissionTime`)
- **Stage完成时间** (`completionTime`)
- **Stage状态** (`status`)
- **输入数据量** (`inputBytes`, `inputRecords`)
- **输出数据量** (`outputBytes`, `outputRecords`)
- **Shuffle读取数据量** (`shuffleReadBytes`, `shuffleReadRecords`)
- **Shuffle写入数据量** (`shuffleWriteBytes`, `shuffleWriteRecords`)
- **Stage类型** (`stageType`: ResultStage, ShuffleMapStage)
- **关联的Job ID** (`jobId`)
- **Task数量** (`numTasks`)
- **失败Task数量** (`numFailedTasks`)

### 2.4 Task级别信息
- **Task ID** (`taskId`)
- **Task类型** (`taskType`)
- **Task启动时间** (`launchTime`)
- **Task完成时间** (`finishTime`)
- **Task执行时长** (计算得出)
- **Task状态** (`status`)
- **Executor ID** (`executorId`)
- **Host** (`host`)
- **输入数据量** (`inputBytes`, `inputRecords`)
- **输出数据量** (`outputBytes`, `outputRecords`)
- **Shuffle读取数据量** (`shuffleReadBytes`, `shuffleReadRecords`)
- **Shuffle写入数据量** (`shuffleWriteBytes`, `shuffleWriteRecords`)
- **GC时间** (`gcTime`)
- **结果序列化时间** (`resultSerializationTime`)
- **获取结果时间** (`gettingResultTime`)

### 2.5 资源使用信息
- **Executor信息**：
  - Executor ID
  - 添加时间 (`time`)
  - 移除时间 (`time`)
  - 主机地址 (`executorHost`)
  - 总核心数 (`totalCores`)
  - 最大内存 (`maxMemory`)
- **资源分配**：
  - 分配的Executor数量
  - 总核心数
  - 总内存
- **资源利用率**：
  - CPU使用率（通过Task执行时间计算）
  - 内存使用情况（从BlockManager事件获取）

### 2.6 环境配置信息
- **Spark配置参数** (`sparkProperties`)
- **系统属性** (`systemProperties`)
- **类路径** (`classpathEntries`)
- **JVM信息** (`javaVersion`, `javaHome`)

## 三、解析方式

### 3.1 解析策略
采用**流式解析**方式处理EventLog文件：
1. **逐行读取**：由于是JSON Lines格式，逐行读取并解析
2. **事件分类**：根据事件类型（`Event`字段）进行分类处理
3. **状态构建**：维护应用、Job、Stage、Task的状态树
4. **关联关系**：通过ID关联不同级别的事件

### 3.2 解析流程
```
1. 读取命令行参数（集群名、解析日期、配置文件路径）
2. 加载配置文件，获取指定集群的HDFS配置
3. 初始化HDFS客户端连接
4. 扫描EventLog目录，获取待解析的文件列表（根据日期过滤）
5. 按应用ID分组，确定每个应用的日志文件
6. 对每个日志文件：
   a. 逐行读取JSON事件
   b. 解析事件类型和内容
   c. 更新应用状态树
   d. 提取关键指标
7. 生成结构化数据（应用、Job、Stage、Task层级），添加cluster_name字段
8. 计算衍生指标（时长、数据量汇总等）
9. 批量写入Hive表（带cluster_name标识和日期分区）
10. 记录解析状态，避免重复处理
```

### 3.3 程序入口设计（纯Spark方案）

```bash
# Spark任务提交示例
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --name "SparkEventLogParser-cluster1-2024-01-15" \
  --driver-memory 8g \
  --executor-memory 12g \
  --executor-cores 4 \
  --num-executors 200 \
  --conf spark.sql.shuffle.partitions=2000 \
  --conf spark.sql.sources.partitionOverwriteMode=dynamic \
  --conf spark.app.cluster_name=cluster1 \
  --conf spark.app.target_date=2024-01-15 \
  --conf spark.app.config_path=hdfs:///configs/config.yaml \
  --class com.company.spark.EventLogParserApp \
  spark-eventlog-parser-assembly-1.0.jar

# 或使用PySpark
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --name "SparkEventLogParser-cluster1-2024-01-15" \
  --driver-memory 8g \
  --executor-memory 12g \
  --executor-cores 4 \
  --num-executors 200 \
  --conf spark.sql.shuffle.partitions=2000 \
  --conf spark.app.cluster_name=cluster1 \
  --conf spark.app.target_date=2024-01-15 \
  parse_spark_logs.py

# 参数说明（通过spark.conf传递）：
# spark.app.cluster_name: 集群名称（必填）
# spark.app.target_date: 解析日期YYYY-MM-DD（可选，默认昨天）
# spark.app.config_path: 配置文件路径（可选，默认hdfs:///configs/config.yaml）
# spark.app.skip_inprogress: 是否跳过.inprogress文件（默认true）
# spark.app.parse_tasks: 是否解析Task级别（默认false）
```

### 3.3 关键技术点
- **JSON解析**：使用高效的JSON解析库（如Jackson、Gson）
- **状态管理**：使用Map/缓存维护应用状态
- **增量解析**：支持从上次解析位置继续（T+1场景）
- **容错处理**：处理不完整或损坏的日志文件
- **并发处理**：支持多文件并行解析

### 3.4 数据关联关系
```
Application (1) -> (N) Job
Job (1) -> (N) Stage
Stage (1) -> (N) Task
Application (1) -> (N) Executor
```

## 四、T+1实现方式

### 4.1 文件发现策略
- **时间分区**：EventLog通常按日期组织（如 `2024/01/15/`）
- **文件过滤**：根据文件修改时间或文件名中的时间戳过滤
- **增量扫描**：只处理前一天（T-1）的日志文件
- **文件状态标记**：记录已解析的文件，避免重复处理

### 4.2 T+1调度方案
1. **调度方式**：使用crontab定时调度
2. **调度时间**：每天凌晨（如02:00）执行
3. **处理范围**：处理前一天的日志文件（T-1），默认昨天，支持参数传入
4. **参数设计**：
   - `--cluster`：集群名称（必填）
   - `--date`：解析日期，格式YYYY-MM-DD（可选，默认昨天）
   - `--config`：配置文件路径（可选，默认./config.yaml）
5. **文件识别**：
   - 方式1：根据文件修改时间（`mtime`）判断
   - 方式2：根据文件路径中的日期目录
   - 方式3：根据文件名中的时间戳
6. **状态管理**：
   - 维护解析状态表，记录已处理的文件
   - 支持断点续传，处理中断后继续

### 4.3 调度方案

#### 方案1：Crontab调度（简单场景）
```bash
# 每天凌晨2点执行，解析cluster1集群昨天的日志
# 使用轻量wrapper脚本计算昨天日期
0 2 * * * /opt/spark/bin/submit_parser.sh cluster1 >> /var/log/spark_parser.log 2>&1

# wrapper脚本示例：submit_parser.sh
#!/bin/bash
CLUSTER_NAME=$1
TARGET_DATE=${2:-$(date -d "yesterday" +%Y-%m-%d)}

/opt/spark/bin/spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --name "SparkEventLogParser-${CLUSTER_NAME}-${TARGET_DATE}" \
  --driver-memory 8g \
  --executor-memory 12g \
  --executor-cores 4 \
  --num-executors 200 \
  --conf spark.sql.shuffle.partitions=2000 \
  --conf spark.app.cluster_name=${CLUSTER_NAME} \
  --conf spark.app.target_date=${TARGET_DATE} \
  --class com.company.spark.EventLogParserApp \
  /opt/spark/jars/spark-eventlog-parser-assembly-1.0.jar

# 手动执行，解析指定日期的日志
/opt/spark/bin/submit_parser.sh cluster1 2024-01-15
```

#### 方案2：Airflow调度（推荐生产环境）
```python
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=15),
}

dag = DAG(
    'spark_eventlog_parser',
    default_args=default_args,
    description='解析Spark EventLog并写入Hive',
    schedule_interval='0 2 * * *',  # 每天凌晨2点
    catchup=False,
    max_active_runs=1,
)

parse_cluster1 = SparkSubmitOperator(
    task_id='parse_cluster1',
    application='/opt/spark/jars/spark-eventlog-parser-assembly-1.0.jar',
    name='SparkEventLogParser-cluster1-{{ ds }}',
    java_class='com.company.spark.EventLogParserApp',
    conf={
        'spark.sql.shuffle.partitions': '2000',
        'spark.sql.sources.partitionOverwriteMode': 'dynamic',
        'spark.app.cluster_name': 'cluster1',
        'spark.app.target_date': '{{ ds }}',  # Airflow自动传入执行日期
    },
    driver_memory='8g',
    executor_memory='12g',
    executor_cores=4,
    num_executors=200,
    verbose=True,
    dag=dag,
)

# 数据质量检查任务
quality_check = BashOperator(
    task_id='quality_check',
    bash_command='''
        hive -e "
        SELECT 
            COUNT(*) as app_count,
            COUNT(DISTINCT app_id) as unique_apps
        FROM meta.spark_applications 
        WHERE dt='{{ ds }}' AND cluster_name='cluster1'
        " > /tmp/quality_check_{{ ds }}.txt
        
        # 检查记录数是否在合理范围
        count=$(head -n 2 /tmp/quality_check_{{ ds }}.txt | tail -n 1 | awk '{print $1}')
        if [ "$count" -lt 1000 ]; then
            echo "ERROR: Too few records: $count"
            exit 1
        fi
    ''',
    dag=dag,
)

parse_cluster1 >> quality_check
```

### 4.4 数据分区策略
- **按日期分区**：数据按执行日期（`execution_date`）分区存储
- **分区格式**：`dt=2024-01-15`
- **多集群支持**：通过`cluster_name`字段区分不同集群的数据
- **便于查询**：支持按集群、日期范围快速查询

## 五、存储方案

### 5.1 存储选型
**采用Hive数据仓库存储**

- **Hive库名**：`meta`
- **存储格式**：Parquet（列式存储，压缩比高，查询性能好）
- **分区方式**：按日期分区（`dt=YYYY-MM-DD`）
- **优势**：
  1. **大数据量支持**：适合存储30万+任务的解析结果
  2. **列式存储**：Parquet格式查询性能好，压缩比高
  3. **分区查询**：按日期分区，查询时只扫描相关分区，性能高
  4. **成本低**：存储在HDFS上，成本低
  5. **扩展性好**：可以存储历史数据，支持长期分析
  6. **SQL支持**：支持标准SQL查询，便于数据分析

### 5.2 Hive配置
- **Hive Metastore**：需要配置Hive Metastore连接信息
- **HDFS路径**：表数据存储在HDFS上，路径由Hive管理
- **表格式**：使用外部表（EXTERNAL TABLE），便于数据管理

### 5.3 数据模型设计（Hive表结构）

所有表存储在Hive库 `meta` 中，使用日期分区（`dt`字段）。

#### 5.2.1 应用表（meta.spark_applications）

**表结构：**
```sql
CREATE EXTERNAL TABLE IF NOT EXISTS meta.spark_applications (
    cluster_name STRING COMMENT '集群名称',
    app_id STRING COMMENT '应用ID',
    app_name STRING COMMENT '应用名称',
    start_time BIGINT COMMENT '启动时间（时间戳，毫秒）',
    end_time BIGINT COMMENT '结束时间（时间戳，毫秒）',
    duration_ms BIGINT COMMENT '运行时长（毫秒）',
    status STRING COMMENT '状态：RUNNING, FINISHED, FAILED, KILLED',
    user STRING COMMENT '用户',
    spark_version STRING COMMENT 'Spark版本',
    executor_count INT COMMENT 'Executor数量',
    total_cores INT COMMENT '总核心数',
    total_memory_mb BIGINT COMMENT '总内存（MB）',
    create_time TIMESTAMP COMMENT '记录创建时间'
) 
PARTITIONED BY (dt STRING COMMENT '分区日期，格式：YYYY-MM-DD')
STORED AS PARQUET
LOCATION '/warehouse/meta/spark_applications'
TBLPROPERTIES ('parquet.compress'='SNAPPY');
```

**字段说明：**
- `cluster_name` + `app_id` + `dt` 构成唯一标识
- `dt` 为分区字段，格式：`YYYY-MM-DD`（如：`2024-01-15`）
- 使用Parquet格式，Snappy压缩

#### 5.2.2 Job表（meta.spark_jobs）

**表结构：**
```sql
CREATE EXTERNAL TABLE IF NOT EXISTS meta.spark_jobs (
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
PARTITIONED BY (dt STRING COMMENT '分区日期，格式：YYYY-MM-DD')
STORED AS PARQUET
LOCATION '/warehouse/meta/spark_jobs'
TBLPROPERTIES ('parquet.compress'='SNAPPY');
```

#### 5.2.3 Stage表（meta.spark_stages）

**表结构：**
```sql
CREATE EXTERNAL TABLE IF NOT EXISTS meta.spark_stages (
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
PARTITIONED BY (dt STRING COMMENT '分区日期，格式：YYYY-MM-DD')
STORED AS PARQUET
LOCATION '/warehouse/meta/spark_stages'
TBLPROPERTIES ('parquet.compress'='SNAPPY');
```

#### 5.2.4 Task表（meta.spark_tasks，可选，建议聚合）

**注意**：Task级别数据量巨大（30万任务可能产生数千万至亿级Task记录）。
**最佳实践**：
1. **默认关闭**：通常不解析全量Task数据。
2. **聚合替代**：通过Stage表中的`p50`, `p95`, `skew_factor`等统计指标满足99%的性能分析需求。
3. **按需采样**：仅解析`status != 'SUCCESS'`的失败Task，或仅针对开启了详细诊断的任务解析。

**表结构（仅在需要时创建）：**
```sql
CREATE EXTERNAL TABLE IF NOT EXISTS meta.spark_tasks (
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
    create_time TIMESTAMP COMMENT '记录创建时间'
) 
PARTITIONED BY (dt STRING COMMENT '分区日期，格式：YYYY-MM-DD')
STORED AS PARQUET
LOCATION '/warehouse/meta/spark_tasks'
TBLPROPERTIES ('parquet.compress'='SNAPPY');
```

**注意**：Task级别数据量巨大（30万任务可能产生数千万Task记录），建议默认不解析，或只解析失败Task。

#### 5.2.5 Executor表（meta.spark_executors）

**表结构：**
```sql
CREATE EXTERNAL TABLE IF NOT EXISTS meta.spark_executors (
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
PARTITIONED BY (dt STRING COMMENT '分区日期，格式：YYYY-MM-DD')
STORED AS PARQUET
LOCATION '/warehouse/meta/spark_executors'
TBLPROPERTIES ('parquet.compress'='SNAPPY');
```

#### 5.2.6 诊断建议表（meta.spark_diagnosis，新增）

用于直接存储基于规则分析出的诊断结果，便于前端展示或发送报告。

**表结构：**
```sql
CREATE EXTERNAL TABLE IF NOT EXISTS meta.spark_diagnosis (
    cluster_name STRING COMMENT '集群名称',
    app_id STRING COMMENT '应用ID',
    rule_id STRING COMMENT '规则ID',
    rule_desc STRING COMMENT '规则描述',
    severity STRING COMMENT '严重等级：CRITICAL, WARNING, INFO',
    diagnosis_detail STRING COMMENT '诊断详情（JSON格式，包含相关StageID或数值）',
    suggestion STRING COMMENT '优化建议',
    create_time TIMESTAMP COMMENT '记录创建时间'
) 
PARTITIONED BY (dt STRING COMMENT '分区日期，格式：YYYY-MM-DD')
STORED AS PARQUET
LOCATION '/warehouse/meta/spark_diagnosis'
TBLPROPERTIES ('parquet.compress'='SNAPPY');
```

### 5.3 分区管理

**分区格式：**
- 分区字段：`dt`（STRING类型）
- 分区值格式：`YYYY-MM-DD`（如：`2024-01-15`）
- 分区路径：`/warehouse/meta/{table_name}/dt=2024-01-15/`

**分区操作：**
```sql
-- 添加分区（如果不存在）
ALTER TABLE meta.spark_applications ADD IF NOT EXISTS PARTITION (dt='2024-01-15');

-- 删除分区（清理历史数据）
ALTER TABLE meta.spark_applications DROP IF EXISTS PARTITION (dt='2023-01-01');

-- 查看分区
SHOW PARTITIONS meta.spark_applications;
```

**分区策略：**
- 每天自动创建新分区
- 支持按日期范围查询，只扫描相关分区
- 历史数据可以按需归档或删除

### 5.4 Spark写入Hive示例代码

**Scala示例：**
```scala
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

// 创建SparkSession，启用Hive支持
val spark = SparkSession.builder()
  .appName("SparkEventLogParser")
  .enableHiveSupport()
  .getOrCreate()

// 设置动态分区
spark.sql("set spark.sql.sources.partitionOverwriteMode=dynamic")
spark.sql("set hive.exec.dynamic.partition=true")
spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")

// 解析EventLog后得到DataFrame
val applicationsDF = spark.createDataFrame(applicationsData)

// 添加日期分区字段
val applicationsWithPartition = applicationsDF
  .withColumn("dt", lit("2024-01-15"))  // 从参数或配置获取日期

// 写入Hive表（覆盖模式）
applicationsWithPartition
  .coalesce(200)  // 控制输出文件数量，避免小文件
  .write
  .mode("overwrite")
  .format("parquet")
  .option("compression", "snappy")
  .insertInto("meta.spark_applications")

// 或者使用saveAsTable（会自动创建表，如果不存在）
// applicationsWithPartition
//   .coalesce(200)
//   .write
//   .mode("overwrite")
//   .format("parquet")
//   .option("compression", "snappy")
//   .partitionBy("dt")
//   .saveAsTable("meta.spark_applications")
```

**Python示例：**
```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

# 创建SparkSession，启用Hive支持
spark = SparkSession.builder \
    .appName("SparkEventLogParser") \
    .enableHiveSupport() \
    .getOrCreate()

# 设置动态分区
spark.sql("set spark.sql.sources.partitionOverwriteMode=dynamic")
spark.sql("set hive.exec.dynamic.partition=true")
spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")

# 解析EventLog后得到DataFrame
applications_df = spark.createDataFrame(applications_data)

# 添加日期分区字段
applications_with_partition = applications_df \
    .withColumn("dt", lit("2024-01-15"))  # 从参数或配置获取日期

# 写入Hive表（覆盖模式）
applications_with_partition \
    .coalesce(200) \  # 控制输出文件数量，避免小文件
    .write \
    .mode("overwrite") \
    .format("parquet") \
    .option("compression", "snappy") \
    .insertInto("meta.spark_applications")
```

**关键配置参数：**
- `spark.sql.sources.partitionOverwriteMode=dynamic`：动态分区覆盖模式
- `hive.exec.dynamic.partition=true`：启用动态分区
- `hive.exec.dynamic.partition.mode=nonstrict`：非严格模式，允许所有分区都是动态的
- `coalesce(200)`：控制输出文件数量，根据数据量调整
- `compression=snappy`：使用Snappy压缩

## 六、技术架构

### 6.1 组件架构
```
┌─────────────────┐
│  调度系统       │ (Airflow/XXL-Job/Cron)
│  (T+1调度)      │
└────────┬────────┘
         │
┌────────▼────────┐
│  日志扫描模块   │ (发现T-1的EventLog文件)
└────────┬────────┘
         │
┌────────▼────────┐
│  日志解析模块   │ (解析JSON Lines，提取事件)
└────────┬────────┘
         │
┌────────▼────────┐
│  数据转换模块   │ (构建层级关系，计算指标)
└────────┬────────┘
         │
┌────────▼────────┐
│  数据存储模块   │ (写入Hive表)
└─────────────────┘
```

### 6.2 技术栈建议

#### 6.2.1 解析程序形式选择

**采用方案：Spark任务**

**场景分析：**
- **文件数量**：每天30万+个Spark任务，对应30万+个EventLog文件
- **文件位置**：EventLog存储在HDFS上
- **处理特点**：需要大规模并行处理多个文件，每个文件独立解析
- **存储目标**：解析结果写入Hive表（meta库），使用日期分区

**方案选择理由：**
1. **超大规模场景**：30万+文件/天，必须使用Spark的分布式计算能力
2. **强大的并行能力**：可以充分利用Spark集群资源，并行处理大量文件
3. **自动容错**：Spark自动处理节点故障，任务自动重试
4. **资源管理**：通过YARN/K8s等资源管理器统一管理资源
5. **扩展性好**：文件数量增长时，可以通过增加资源快速扩展
6. **HDFS原生支持**：Spark对HDFS有很好的支持，读取效率高
7. **Hive集成**：Spark内置Hive支持，可以直接写入Hive表

**技术栈：**
- **解析引擎**：Spark 3.x (Scala/Python)
- **HDFS访问**：Spark原生支持
- **JSON解析**：Spark SQL的`from_json`函数或自定义解析器
- **数据写入**：Spark直接写入Hive表（使用DataFrame API）
- **Hive集成**：Spark内置Hive支持，可以直接写入Hive表
- **调度**：通过`spark-submit`提交，由crontab调用Python脚本提交

**架构设计：**

##### 超大规模架构设计（30万+文件/天）

**核心要求：**
1. **使用Spark任务**：充分利用Spark集群的分布式计算能力
2. **高并行度**：需要设置非常高的并行度（建议1000-5000）
3. **资源充足**：需要分配足够的Spark资源（executor数量、cores、memory）
4. **分批处理**：可以考虑将30万文件分成多个批次处理
5. **增量处理**：支持断点续传，处理失败后可以继续
6. **幂等性保证**：支持重复执行，结果一致

**架构设计（纯Spark方案）：**

```
┌─────────────────────────────────────┐
│  调度系统（Crontab/Airflow/XXL-Job） │
│  每天凌晨2点执行                     │
└──────────────┬──────────────────────┘
               │
               │ spark-submit
               ▼
┌─────────────────────────────────────────────────────┐
│  Spark任务（All-in-One，无需Python主控）             │
│                                                      │
│  ┌────────────────────────────────────────┐        │
│  │ 1. Driver: 分布式扫描HDFS目录           │        │
│  │    - 使用Spark并行ListStatus            │        │
│  │    - 按日期过滤文件（T-1）               │        │
│  │    - 跳过.inprogress文件                │        │
│  └────────────────┬───────────────────────┘        │
│                   │                                 │
│  ┌────────────────▼───────────────────────┐        │
│  │ 2. 文件路径并行化为RDD                   │        │
│  │    - repartition(2000-5000)             │        │
│  │    - 智能分区（按文件大小均衡负载）       │        │
│  └────────────────┬───────────────────────┘        │
│                   │                                 │
│  ┌────────────────▼───────────────────────┐        │
│  │ 3. Executor: 并行解析EventLog            │        │
│  │    - 流式解析JSON Lines                  │        │
│  │    - 提取Application/Job/Stage指标       │        │
│  │    - 计算聚合统计（p50/p95/倾斜因子）     │        │
│  │    - 容错处理（单文件失败不影响全局）     │        │
│  └────────────────┬───────────────────────┘        │
│                   │                                 │
│  ┌────────────────▼───────────────────────┐        │
│  │ 4. 数据转换与质量校验                    │        │
│  │    - 转换为DataFrame                     │        │
│  │    - 数据去重（按app_id+dt）             │        │
│  │    - 质量校验（记录数、字段完整性）       │        │
│  └────────────────┬───────────────────────┘        │
│                   │                                 │
│  ┌────────────────▼───────────────────────┐        │
│  │ 5. 批量写入Hive（动态分区）              │        │
│  │    - coalesce控制小文件                  │        │
│  │    - INSERT OVERWRITE保证幂等性          │        │
│  │    - 写入诊断表（spark_diagnosis）       │        │
│  └────────────────┬───────────────────────┘        │
│                   │                                 │
│  ┌────────────────▼───────────────────────┐        │
│  │ 6. 监控指标上报                          │        │
│  │    - 处理文件数、失败数、耗时             │        │
│  │    - 推送到监控系统（Prometheus/自定义）  │        │
│  └────────────────────────────────────────┘        │
└─────────────────────────────────────────────────────┘
```

**关键技术点（核心优化）：**

1. **文件列表生成**：
   - Python脚本扫描HDFS，生成文件列表（HDFS路径），写入临时文件 `file_list.txt`。
   - 避免Driver端直接调用 `hdfs ls` 或 Spark `wholeTextFiles` 读取大量小文件。

2. **Spark读取策略（RDD并行读取 + Executor解析）**：
   - **反模式**：`spark.read.json(file_list)`。这种方式会导致Driver串行读取所有文件元数据，极易OOM且速度慢。
   - **最佳实践**：将文件路径列表并行化为RDD，在Executor端读取并解析文件内容。

   ```scala
   import org.apache.hadoop.fs.{FileSystem, Path}
   import org.apache.hadoop.conf.Configuration
   import org.apache.spark.sql.Row
   import scala.collection.mutable.ArrayBuffer
   
   // 定义数据模型
   case class AppMetrics(
     cluster_name: String, app_id: String, app_name: String,
     start_time: Long, end_time: Long, duration_ms: Long,
     status: String, user: String, spark_version: String,
     executor_count: Int, total_cores: Int, total_memory_mb: Long,
     dt: String
   )
   
   case class StageMetrics(
     cluster_name: String, app_id: String, job_id: Int, stage_id: Int,
     stage_name: String, submission_time: Long, completion_time: Long,
     duration_ms: Long, status: String, input_bytes: Long,
     shuffle_read_bytes: Long, shuffle_write_bytes: Long,
     num_tasks: Int, task_duration_p50: Long, task_duration_p95: Long,
     skew_factor: Double, dt: String
   )
   
   // 1. 将文件路径并行化为RDD
   val clusterName = spark.conf.get("spark.app.cluster_name", "cluster1")
   val targetDate = spark.conf.get("spark.app.target_date", "2024-01-15")
   
   val filePathsRDD = spark.sparkContext.parallelize(filePaths, numSlices = 2000)
   
   // 2. 智能分区：按文件大小均衡负载（可选优化）
   val filePathsWithSize = filePathsRDD.map { path =>
     val fs = FileSystem.get(new Configuration())
     val size = fs.getFileStatus(new Path(path)).getLen
     (path, size)
   }
   
   // 按大小排序后重新分区，避免数据倾斜
   val balancedRDD = filePathsWithSize
     .sortBy(_._2, ascending = false)  // 大文件优先
     .repartition(2000)
     .map(_._1)
   
   // 3. 并行解析（核心逻辑）
   val parseResults = balancedRDD.mapPartitions { paths =>
     // 在Executor端初始化（每个partition一次）
     val fs = FileSystem.get(new Configuration())
     val successFiles = ArrayBuffer[String]()
     val failedFiles = ArrayBuffer[(String, String)]()
     val apps = ArrayBuffer[AppMetrics]()
     val stages = ArrayBuffer[StageMetrics]()
     
     paths.foreach { pathStr =>
       try {
         val path = new Path(pathStr)
         val stream = fs.open(path)
         
         // 流式解析JSON Lines（避免OOM）
         val parser = new EventLogParser(clusterName, targetDate)
         val result = parser.parseStream(stream)
         
         stream.close()
         
         // 收集结果
         apps ++= result.applications
         stages ++= result.stages
         successFiles += pathStr
         
       } catch {
         case e: Exception => 
           // 容错：单文件失败不影响其他文件
           failedFiles += ((pathStr, e.getMessage))
           // 记录到日志系统
           println(s"ERROR: Failed to parse $pathStr: ${e.getMessage}")
       }
     }
     
     // 返回解析结果和统计信息
     Iterator(ParsePartitionResult(apps, stages, successFiles.size, failedFiles))
   }
   
   // 4. 收集统计信息
   val stats = parseResults.map(r => (r.successCount, r.failedFiles.size)).reduce {
     case ((s1, f1), (s2, f2)) => (s1 + s2, f1 + f2)
   }
   println(s"Parsed ${stats._1} files successfully, ${stats._2} failed")
   
   // 5. 转换为DataFrame并写入Hive
   val appsDF = parseResults.flatMap(_.applications).toDF()
   val stagesDF = parseResults.flatMap(_.stages).toDF()
   
   // 写入前去重（保证幂等性）
   appsDF.dropDuplicates("cluster_name", "app_id", "dt")
     .coalesce(40)  // 控制小文件数量
     .write
     .mode("overwrite")
     .format("parquet")
     .option("compression", "snappy")
     .insertInto("meta.spark_applications")
   ```

3. **资源分配建议**：
   - **Executor数量**：100-500个（根据集群规模）
   - **每个Executor核心数**：4-8 cores
   - **每个Executor内存**：8-16GB
   - **总并行度**：建议设置为Executor数量 × 每个Executor核心数 × 2-3
   - **Driver内存**：至少4GB，建议8GB

4. **分批处理策略**（可选）：
   - 将30万文件分成多个批次（如每批5万个）
   - 每个批次提交一个Spark任务
   - 可以并行运行多个批次（如果资源充足）
   - 或者串行运行，避免资源竞争

5. **性能优化**：
   - **广播变量**：将配置文件、解析规则等作为广播变量
   - **数据本地性**：尽量让任务在数据所在节点执行
   - **Hive写入**：使用DataFrame API直接写入Hive表，使用动态分区
   - **文件数量控制**：使用`coalesce`或`repartition`控制输出文件数量，避免小文件

6. **容错机制**：
   - **检查点**：记录已处理的文件，支持断点续传
   - **失败重试**：单个文件解析失败不影响其他文件
   - **数据去重**：Hive表使用`INSERT OVERWRITE`覆盖写入，或使用`INSERT INTO`追加写入（需要应用层去重）
   - **分区修复**：写入后执行`MSCK REPAIR TABLE`修复分区（如果需要）

7. **监控告警**：
   - 监控Spark任务执行时间
   - 监控处理文件数量
   - 监控Hive写入速度
   - 监控输出文件数量和大小
   - 设置超时告警（如超过4小时未完成）

**预估资源需求（30万文件）：**
- **假设**：每个文件平均10MB，解析时间平均1秒
- **总数据量**：30万 × 10MB = 3TB
- **总处理时间**（单线程）：30万秒 ≈ 83小时
- **所需并行度**：如果要在2小时内完成，需要约2000个并行任务
- **资源需求**：约250-500个executor（每个4-8 cores）

**实施建议：**
1. **先小规模测试**：先用1000个文件测试，验证方案可行性
2. **逐步扩大规模**：逐步增加到1万、10万、30万
3. **性能调优**：根据实际执行情况调整并行度、资源分配等参数
4. **分批处理**：如果单次处理30万文件压力太大，可以分成多个批次

**分批处理详细方案：**

对于30万+文件的场景，强烈建议采用**分批处理**策略：

1. **批次划分**：
   - 将30万文件分成多个批次，每批5-10万个文件
   - 每个批次提交一个独立的Spark任务
   - 批次之间可以串行或并行执行（根据资源情况）

2. **批次调度**：
   ```bash
   # 方案1：串行执行（资源有限时）
   # 批次1：处理0-10万文件
   # 批次2：处理10-20万文件（等待批次1完成）
   # 批次3：处理20-30万文件（等待批次2完成）
   
   # 方案2：并行执行（资源充足时）
   # 同时运行多个批次，但需要控制并发数，避免资源竞争
   ```

3. **文件列表生成**：
   ```python
   # Python脚本生成批次文件列表
   files = get_file_list_from_hdfs(date)
   batch_size = 100000
   for i in range(0, len(files), batch_size):
       batch_files = files[i:i+batch_size]
       batch_file_path = f"hdfs://.../batch_{i//batch_size}.txt"
       write_file_list(batch_files, batch_file_path)
   ```

4. **批次状态管理**：
   - 维护批次处理状态表
   - 记录每个批次的处理状态（pending/running/completed/failed）
   - 支持失败批次重试

**关键优化点：**

1. **Hive写入优化**：
   - 使用动态分区写入：`spark.sql("set spark.sql.sources.partitionOverwriteMode=dynamic")`
   - 使用`INSERT OVERWRITE`覆盖写入，避免重复数据
   - 控制输出文件数量：使用`coalesce`或`repartition`，避免产生大量小文件
   - 批量写入：设置合理的`spark.sql.shuffle.partitions`，控制shuffle分区数
   - 写入前创建分区：如果分区不存在，先执行`ALTER TABLE ADD PARTITION`
   - 使用临时表：先写入临时表，最后一次性合并到主表（可选，避免小文件）
   - 压缩优化：使用Snappy压缩，平衡压缩比和查询性能

2. **内存优化**：
   - 流式处理，避免一次性加载整个文件到内存
   - 及时释放不需要的数据
   - 合理设置Spark内存参数（executor memory, driver memory）

3. **网络优化**：
   - 尽量让Spark任务在HDFS数据节点执行（数据本地性）
   - Hive写入直接写入HDFS，无需额外网络传输
   - 批量写入减少小文件，提高HDFS写入效率

4. **容错优化**：
   - 单个文件解析失败不影响其他文件
   - 记录失败文件列表，支持单独重试
   - 使用Spark的checkpoint机制（如果需要）

**预估执行时间（30万文件）：**

假设条件：
- 每个文件平均解析时间：1-2秒
- 并行度：2000（500 executors × 4 cores）
- Hive写入时间：批量写入，耗时0.2秒

计算：
- 单文件处理时间：1.5秒（解析）+ 0.2秒（写入）= 1.7秒
- 并行处理时间：300000 / 2000 × 1.7秒 ≈ 255秒 ≈ 4.3分钟
- 加上Spark启动、调度等开销：预计10-20分钟完成

**如果单次处理压力大，分批处理：**
- 每批10万文件，分3批
- 每批预计5-10分钟
- 串行执行：总计15-30分钟
- 并行执行（3批同时）：总计5-10分钟

**资源需求估算：**

对于30万文件，建议资源分配：
- **Executor数量**：200-500个
- **每个Executor**：4-8 cores, 8-16GB memory
- **Driver**：4-8 cores, 8-16GB memory
- **总CPU核心**：800-4000 cores
- **总内存**：1.6TB-8TB

**注意事项：**
1. **资源竞争**：确保Spark集群有足够资源，避免与其他任务竞争
2. **HDFS压力**：30万文件的读取和写入会对HDFS造成压力，需要评估HDFS承载能力
3. **HDFS带宽**：大量文件读取会占用HDFS带宽，需要评估网络带宽
4. **任务超时**：设置合理的任务超时时间，避免任务长时间运行
5. **监控告警**：密切监控任务执行情况，设置告警机制

**技术栈详细建议：**
- **解析引擎**：Spark 3.x
- **开发语言**：Scala（性能好）或 Python（开发快）
- **HDFS访问**：Spark原生支持
- **JSON解析**：Spark SQL `from_json` 或自定义解析器
- **数据写入**：Spark直接写入Hive表（使用DataFrame API）
- **调度**：通过`spark-submit`提交，由crontab调用Python脚本提交

## 七、性能优化建议

### 7.1 解析性能优化

#### 7.1.1 核心性能瓶颈解决
- **避免Driver瓶颈**：严禁使用 `spark.read.json(30w_files)`。这会使Driver端成为元数据请求的单点瓶颈。必须采用 **RDD[Path] -> Executor Read** 模式。
- **Executor并发读取**：利用Spark的分布式能力，让数千个Core同时发起HDFS读请求。
- **HDFS Listing优化**：如果HDFS目录未按日期归档（所有日志在一个目录下），`ls` 操作会非常慢。
  - *长期方案*：配置Spark `spark.eventLog.dir` 使用日期子目录。
  - *短期方案*：使用Java HDFS API的多线程Listing，或者直接分析FSImage（仅限超大规模）。

#### 7.1.2 资源与内存
- **文件分区**：确保RDD分区数足够大（如2000-5000），使每个Task处理的文件总大小控制在合理范围（如200MB以内）。
- **内存管理**：流式解析（Streaming Parse），不要将整个EventLog文件加载到内存字符串中，而是使用Jackson/Gson的Stream API逐行解析。
- **对象复用**：在Parser中复用对象，减少GC压力。

#### 7.1.3 写入优化
- **小文件控制**：Hive写入前必须执行 `coalesce(N)` 或 `repartition(N)`。
  - N的计算建议：`TotalSize / 256MB`。
  - 例如：30万文件解析后数据量可能只有10GB，只需 `coalesce(40)` 即可，而不是生成2000个小文件。

### 7.2 存储优化
- **分区存储**：按日期分区（dt字段），提高查询效率，只扫描相关分区
- **文件格式**：使用Parquet格式，列式存储，压缩比高，查询性能好
- **压缩算法**：使用Snappy压缩，平衡压缩比和查询性能
- **小文件优化**：控制输出文件数量，避免产生大量小文件影响查询性能
- **数据归档**：历史数据定期归档到冷存储，或删除旧分区
- **数据聚合**：Task级别数据可考虑只保留汇总，不保留明细
- **表统计信息**：定期执行`ANALYZE TABLE`更新表统计信息，优化查询计划

## 八、监控与告警

### 8.1 解析监控
- **解析成功率**：监控解析失败的文件数量
- **解析延迟**：监控T+1任务的执行时长
- **数据质量**：监控数据完整性（应用数、Job数等）
- **存储空间**：监控存储使用情况

### 8.2 告警规则
- 解析失败率超过阈值
- T+1任务执行超时
- 数据量异常波动
- 存储空间不足

## 九、实施步骤

### 阶段一：数据源确认
1. 确认Spark EventLog的存储位置和访问方式
2. 确认日志文件格式和命名规则
3. 采样分析日志文件结构

### 阶段二：解析器开发
1. 开发JSON Lines解析器
2. 实现事件分类和状态构建
3. 实现指标提取和计算逻辑

### 阶段三：存储设计
1. 设计数据模型和表结构
2. 实现数据写入逻辑
3. 建立索引和分区

### 阶段四：T+1调度
1. 实现文件发现和过滤逻辑
2. 集成调度系统
3. 实现状态管理和断点续传

### 阶段五：测试与优化
1. 小规模测试验证
2. 性能优化
3. 生产环境部署

## 十、程序架构设计

### 10.1 目录结构（纯Spark方案）

#### Scala项目结构（推荐）
```
spark-eventlog-parser/
├── build.sbt                           # SBT构建文件
├── project/
│   ├── build.properties
│   └── plugins.sbt
├── src/main/scala/
│   └── com/company/spark/
│       ├── EventLogParserApp.scala     # 主程序入口
│       ├── config/
│       │   ├── ParserConfig.scala      # 配置类
│       │   └── ConfigLoader.scala      # 配置加载
│       ├── parser/
│       │   ├── EventLogParser.scala    # 事件解析核心
│       │   ├── EventTypes.scala        # 事件类型定义
│       │   └── MetricsCalculator.scala # 指标计算
│       ├── model/
│       │   ├── AppMetrics.scala        # 应用指标模型
│       │   ├── StageMetrics.scala      # Stage指标模型
│       │   └── DiagnosisResult.scala   # 诊断结果模型
│       ├── writer/
│       │   └── HiveWriter.scala        # Hive写入逻辑
│       └── utils/
│           ├── HDFSUtils.scala         # HDFS工具
│           └── DateUtils.scala         # 日期工具
├── src/main/resources/
│   ├── config-default.yaml             # 默认配置
│   └── log4j.properties                # 日志配置
├── src/test/scala/
│   └── com/company/spark/
│       ├── EventLogParserTest.scala
│       └── MetricsCalculatorTest.scala
└── README.md
```

#### PySpark项目结构（备选）
```
spark-eventlog-parser/
├── setup.py                            # Python打包配置
├── requirements.txt                    # Python依赖
├── parse_spark_logs.py                 # 主程序入口（PySpark）
├── parser/
│   ├── __init__.py
│   ├── config_loader.py                # 配置加载
│   ├── file_scanner.py                 # 文件扫描（Spark并行）
│   ├── event_parser.py                 # 事件解析
│   ├── metrics_calculator.py           # 指标计算
│   └── hive_writer.py                  # Hive写入
├── models/
│   ├── __init__.py
│   ├── app_metrics.py                  # 应用指标模型
│   └── stage_metrics.py                # Stage指标模型
├── utils/
│   ├── __init__.py
│   ├── hdfs_utils.py                   # HDFS工具
│   └── date_utils.py                   # 日期工具
├── tests/
│   ├── test_parser.py
│   └── test_metrics.py
├── config/
│   └── config.yaml.example             # 配置示例
└── scripts/
    ├── submit_parser.sh                # 提交脚本
    └── deploy.sh                       # 部署脚本
```

### 10.2 核心模块设计（纯Spark方案）

#### 10.2.1 主程序入口（EventLogParserApp）
**职责：**
- 初始化SparkSession
- 读取配置参数（从spark.conf或配置文件）
- 协调文件扫描、解析、写入流程
- 收集执行统计信息和监控指标

**关键代码框架（Scala）：**
```scala
object EventLogParserApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("SparkEventLogParser")
      .enableHiveSupport()
      .getOrCreate()
    
    try {
      // 1. 加载配置
      val config = ConfigLoader.load(spark)
      
      // 2. 扫描文件
      val filePaths = FileScanner.scan(spark, config)
      println(s"Found ${filePaths.length} files to process")
      
      // 3. 并行解析
      val metrics = EventLogParser.parse(spark, filePaths, config)
      
      // 4. 写入Hive
      HiveWriter.write(spark, metrics, config)
      
      // 5. 上报监控指标
      MetricsReporter.report(spark, metrics.stats)
      
    } finally {
      spark.stop()
    }
  }
}
```

#### 10.2.2 配置加载模块（ConfigLoader）
**职责：**
- 读取YAML配置文件（从HDFS或本地）
- 解析Spark Conf参数（cluster_name, target_date等）
- 验证配置完整性
- 提供配置访问接口

**关键配置项：**
```scala
case class ParserConfig(
  clusterName: String,
  targetDate: String,
  eventLogDir: String,
  hiveDatabase: String,
  skipInprogress: Boolean,
  parseTasks: Boolean,
  numPartitions: Int,
  batchSize: Int
)
```

#### 10.2.3 文件扫描模块（FileScanner）
**职责：**
- 使用Spark并行扫描HDFS目录
- 按日期过滤文件（支持mtime或文件名匹配）
- 跳过.inprogress文件
- 智能分区（按文件大小均衡负载）

**关键方法：**
```scala
object FileScanner {
  def scan(spark: SparkSession, config: ParserConfig): Array[String] = {
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val logDir = new Path(config.eventLogDir)
    
    // 方案1：如果有日期子目录
    val targetDir = new Path(logDir, config.targetDate)
    if (fs.exists(targetDir)) {
      return scanDirectory(fs, targetDir, config)
    }
    
    // 方案2：全目录扫描并过滤
    scanAndFilter(spark, fs, logDir, config)
  }
  
  private def scanAndFilter(spark: SparkSession, fs: FileSystem, 
                            dir: Path, config: ParserConfig): Array[String] = {
    // 使用Spark并行过滤大量文件
    val allFiles = fs.listStatus(dir)
    spark.sparkContext.parallelize(allFiles, numSlices = 100)
      .filter(f => matchesDate(f, config.targetDate) && 
                   !f.getPath.getName.endsWith(".inprogress"))
      .map(_.getPath.toString)
      .collect()
  }
}
```

#### 10.2.4 事件解析模块（EventLogParser）
**职责：**
- 流式解析JSON Lines格式的EventLog
- 识别不同类型的事件（ApplicationStart, JobEnd, StageCompleted等）
- 维护应用状态树（Application -> Job -> Stage）
- 计算聚合指标（p50, p95, 倾斜因子等）
- 容错处理（单文件失败不影响全局）

**核心解析逻辑：**
```scala
class EventLogParser(clusterName: String, targetDate: String) {
  def parseStream(stream: FSDataInputStream): ParseResult = {
    val reader = new BufferedReader(new InputStreamReader(stream))
    val appState = new ApplicationState()
    
    try {
      var line = reader.readLine()
      while (line != null) {
        try {
          val json = JSON.parseObject(line)
          val eventType = json.getString("Event")
          
          eventType match {
            case "SparkListenerApplicationStart" => 
              handleApplicationStart(json, appState)
            case "SparkListenerJobStart" => 
              handleJobStart(json, appState)
            case "SparkListenerStageCompleted" => 
              handleStageCompleted(json, appState)
            case "SparkListenerApplicationEnd" => 
              handleApplicationEnd(json, appState)
            case _ => // 忽略其他事件
          }
        } catch {
          case e: Exception => 
            // 单行解析失败不中断整个文件
            logError(s"Failed to parse line: $line", e)
        }
        line = reader.readLine()
      }
      
      // 计算最终指标
      ParseResult(
        applications = appState.toAppMetrics(clusterName, targetDate),
        stages = appState.toStageMetrics(clusterName, targetDate)
      )
      
    } finally {
      reader.close()
    }
  }
}
```

#### 10.2.5 指标计算模块（MetricsCalculator）
**职责：**
- 计算Stage级别的聚合统计（p50, p75, p95, max）
- 计算倾斜因子（skew_factor = max/median）
- 计算资源利用率
- 生成诊断建议

**关键计算逻辑：**
```scala
object MetricsCalculator {
  def calculateStageMetrics(tasks: Seq[TaskMetrics]): StageAggMetrics = {
    val durations = tasks.map(_.duration).sorted
    val inputBytes = tasks.map(_.inputBytes).sorted
    
    StageAggMetrics(
      taskDurationP50 = percentile(durations, 0.5),
      taskDurationP75 = percentile(durations, 0.75),
      taskDurationP95 = percentile(durations, 0.95),
      taskDurationMax = durations.lastOption.getOrElse(0L),
      skewFactor = durations.lastOption.getOrElse(0L).toDouble / 
                   percentile(durations, 0.5).max(1).toDouble,
      inputSkewFactor = inputBytes.lastOption.getOrElse(0L).toDouble / 
                        percentile(inputBytes, 0.5).max(1).toDouble
    )
  }
  
  private def percentile(sorted: Seq[Long], p: Double): Long = {
    if (sorted.isEmpty) return 0L
    val index = (sorted.size * p).toInt.min(sorted.size - 1)
    sorted(index)
  }
}
```

#### 10.2.6 Hive写入模块（HiveWriter）
**职责：**
- 使用Spark DataFrame API写入Hive表
- 支持动态分区写入
- 控制输出文件数量，避免小文件
- 数据去重（保证幂等性）
- 数据质量校验

**关键实现：**
```scala
object HiveWriter {
  def write(spark: SparkSession, metrics: ParseResult, 
            config: ParserConfig): Unit = {
    import spark.implicits._
    
    // 设置动态分区
    spark.sql("SET spark.sql.sources.partitionOverwriteMode=dynamic")
    spark.sql("SET hive.exec.dynamic.partition=true")
    spark.sql("SET hive.exec.dynamic.partition.mode=nonstrict")
    
    // 写入应用表
    val appsDF = metrics.applications.toDF()
      .dropDuplicates("cluster_name", "app_id", "dt")
    
    val numFiles = Math.max(appsDF.count() / 100000, 10).toInt
    
    appsDF.coalesce(numFiles)
      .write
      .mode("overwrite")
      .format("parquet")
      .option("compression", "snappy")
      .insertInto(s"${config.hiveDatabase}.spark_applications")
    
    println(s"Written ${appsDF.count()} applications")
    
    // 写入Stage表（类似逻辑）
    writeStages(spark, metrics.stages, config)
    
    // 写入诊断表
    writeDiagnosis(spark, metrics.diagnosis, config)
  }
}
```

#### 10.2.7 监控上报模块（MetricsReporter）
**职责：**
- 收集解析统计信息
- 推送到监控系统（Prometheus/自定义）
- 记录执行日志

```scala
object MetricsReporter {
  def report(spark: SparkSession, stats: ParseStats): Unit = {
    // 打印统计信息
    println(s"""
      |=== Parse Statistics ===
      |Total Files: ${stats.totalFiles}
      |Success: ${stats.successFiles}
      |Failed: ${stats.failedFiles}
      |Duration: ${stats.durationMs}ms
      |Applications: ${stats.appCount}
      |Stages: ${stats.stageCount}
      |========================
    """.stripMargin)
    
    // 推送到Prometheus（可选）
    pushToPrometheus(stats)
    
    // 记录到Hive审计表（可选）
    logToHive(spark, stats)
  }
}
```

## 十一、关键优化实践

### 11.1 幂等性保证（Idempotency）

**问题：**任务失败重试或手动重跑时，可能导致数据重复。

**解决方案：**

#### 方案1：使用INSERT OVERWRITE按分区覆盖（推荐）
```scala
// 按日期分区覆盖写入，天然保证幂等性
appsDF.write
  .mode("overwrite")  // 覆盖模式
  .format("parquet")
  .insertInto("meta.spark_applications")  // 自动覆盖dt分区
```

#### 方案2：写入前数据去重
```scala
// 读取已存在的数据
val existingDF = spark.table("meta.spark_applications")
  .filter($"dt" === targetDate && $"cluster_name" === clusterName)

// 合并新旧数据并去重
val mergedDF = newDF.union(existingDF)
  .dropDuplicates("cluster_name", "app_id", "dt")
  .orderBy($"start_time".desc)  // 保留最新数据
```

#### 方案3：使用状态表记录已处理文件
```sql
-- 创建处理状态表
CREATE TABLE IF NOT EXISTS meta.spark_parser_status (
    cluster_name STRING,
    file_path STRING,
    process_date STRING,
    status STRING,  -- SUCCESS, FAILED, PROCESSING
    record_count INT,
    process_time TIMESTAMP,
    error_msg STRING
) PARTITIONED BY (dt STRING);

-- 解析前检查
SELECT file_path FROM meta.spark_parser_status
WHERE dt = '2024-01-15' 
  AND cluster_name = 'cluster1'
  AND status = 'SUCCESS'
```

```scala
// 过滤已处理文件
val processedFiles = spark.table("meta.spark_parser_status")
  .filter($"dt" === targetDate && $"status" === "SUCCESS")
  .select("file_path")
  .collect()
  .map(_.getString(0))
  .toSet

val filesToProcess = allFiles.filterNot(processedFiles.contains)
```

### 11.2 数据质量校验

**在解析后立即校验数据质量，避免脏数据进入仓库。**

```scala
object DataQualityChecker {
  case class QualityCheckResult(
    passed: Boolean,
    totalRecords: Long,
    issues: Seq[String]
  )
  
  def check(appsDF: DataFrame, config: ParserConfig): QualityCheckResult = {
    val issues = ArrayBuffer[String]()
    val count = appsDF.count()
    
    // 1. 记录数检查
    if (count == 0) {
      issues += "ERROR: No records found"
    } else if (count < config.minExpectedRecords) {
      issues += s"WARNING: Only $count records (expected > ${config.minExpectedRecords})"
    }
    
    // 2. 必填字段检查
    val nullAppIds = appsDF.filter($"app_id".isNull).count()
    if (nullAppIds > 0) {
      issues += s"ERROR: $nullAppIds records with null app_id"
    }
    
    // 3. 时间合理性检查
    val invalidTimes = appsDF
      .filter($"start_time" > $"end_time" || $"duration_ms" < 0)
      .count()
    if (invalidTimes > 0) {
      issues += s"WARNING: $invalidTimes records with invalid timestamps"
    }
    
    // 4. 数据分布检查
    val statusDist = appsDF.groupBy("status").count().collect()
    val failedRatio = statusDist.find(_.getString(0) == "FAILED")
      .map(_.getLong(1).toDouble / count)
      .getOrElse(0.0)
    
    if (failedRatio > 0.5) {
      issues += s"WARNING: High failure rate: ${failedRatio * 100}%"
    }
    
    // 5. 重复数据检查
    val duplicates = appsDF.groupBy("cluster_name", "app_id", "dt")
      .count()
      .filter($"count" > 1)
      .count()
    
    if (duplicates > 0) {
      issues += s"ERROR: $duplicates duplicate records found"
    }
    
    QualityCheckResult(
      passed = !issues.exists(_.startsWith("ERROR")),
      totalRecords = count,
      issues = issues.toSeq
    )
  }
}

// 使用示例
val qualityResult = DataQualityChecker.check(appsDF, config)
if (!qualityResult.passed) {
  throw new RuntimeException(
    s"Data quality check failed:\n${qualityResult.issues.mkString("\n")}"
  )
}
```

### 11.3 增强监控和告警

#### 11.3.1 Spark任务内部监控
```scala
object MonitoringMetrics {
  // 使用Spark Accumulator收集统计信息
  val totalFilesAcc = spark.sparkContext.longAccumulator("totalFiles")
  val successFilesAcc = spark.sparkContext.longAccumulator("successFiles")
  val failedFilesAcc = spark.sparkContext.longAccumulator("failedFiles")
  val totalBytesAcc = spark.sparkContext.longAccumulator("totalBytes")
  
  // 在解析过程中更新
  def recordSuccess(fileSize: Long): Unit = {
    successFilesAcc.add(1)
    totalBytesAcc.add(fileSize)
  }
  
  def recordFailure(): Unit = {
    failedFilesAcc.add(1)
  }
  
  // 任务结束时上报
  def report(): Map[String, Long] = {
    Map(
      "total_files" -> totalFilesAcc.value,
      "success_files" -> successFilesAcc.value,
      "failed_files" -> failedFilesAcc.value,
      "total_bytes" -> totalBytesAcc.value
    )
  }
}
```

#### 11.3.2 推送到Prometheus
```scala
import io.prometheus.client.{Counter, Gauge, Histogram}
import io.prometheus.client.exporter.PushGateway

object PrometheusReporter {
  val parseCounter = Counter.build()
    .name("spark_eventlog_parse_total")
    .labelNames("cluster", "status")
    .help("Total parsed files")
    .register()
  
  val parseDuration = Histogram.build()
    .name("spark_eventlog_parse_duration_seconds")
    .labelNames("cluster")
    .help("Parse duration")
    .register()
  
  val appCountGauge = Gauge.build()
    .name("spark_applications_count")
    .labelNames("cluster", "date")
    .help("Number of applications")
    .register()
  
  def pushMetrics(cluster: String, date: String, stats: ParseStats): Unit = {
    parseCounter.labels(cluster, "success").inc(stats.successFiles)
    parseCounter.labels(cluster, "failed").inc(stats.failedFiles)
    parseDuration.labels(cluster).observe(stats.durationMs / 1000.0)
    appCountGauge.labels(cluster, date).set(stats.appCount)
    
    val gateway = new PushGateway("prometheus-pushgateway:9091")
    gateway.push(CollectorRegistry.defaultRegistry, "spark_eventlog_parser")
  }
}
```

#### 11.3.3 告警规则示例（Prometheus）
```yaml
# prometheus_alerts.yml
groups:
  - name: spark_eventlog_parser
    interval: 5m
    rules:
      # 解析失败率告警
      - alert: HighParseFailureRate
        expr: |
          rate(spark_eventlog_parse_total{status="failed"}[1h]) / 
          rate(spark_eventlog_parse_total[1h]) > 0.05
        for: 10m
        labels:
          severity: warning
        annotations:
          summary: "Spark EventLog解析失败率过高"
          description: "集群 {{ $labels.cluster }} 解析失败率超过5%"
      
      # 任务超时告警
      - alert: ParseTaskTimeout
        expr: |
          time() - spark_eventlog_parse_last_success_timestamp > 14400
        for: 1h
        labels:
          severity: critical
        annotations:
          summary: "Spark EventLog解析任务超时"
          description: "集群 {{ $labels.cluster }} 超过4小时未完成解析"
      
      # 数据量异常告警
      - alert: AbnormalApplicationCount
        expr: |
          abs(spark_applications_count - spark_applications_count offset 1d) / 
          spark_applications_count offset 1d > 0.3
        for: 30m
        labels:
          severity: warning
        annotations:
          summary: "应用数量异常波动"
          description: "集群 {{ $labels.cluster }} 应用数量相比昨天变化超过30%"
```

### 11.4 错误处理和恢复机制

#### 11.4.1 分级错误处理
```scala
sealed trait ParseError
case class FileNotFoundError(path: String) extends ParseError
case class MalformedJSONError(path: String, line: Int) extends ParseError
case class CorruptedFileError(path: String, reason: String) extends ParseError
case class ResourceExhaustedError(message: String) extends ParseError

object ErrorHandler {
  // 可恢复错误：记录日志，继续处理
  def handleRecoverableError(error: ParseError): Unit = error match {
    case MalformedJSONError(path, line) =>
      logger.warn(s"Skipping malformed JSON at $path:$line")
      metricsRecorder.recordSkippedLine(path)
    
    case CorruptedFileError(path, reason) =>
      logger.error(s"Corrupted file $path: $reason")
      quarantineFile(path)  // 移到隔离目录，后续人工处理
  }
  
  // 不可恢复错误：立即失败
  def handleFatalError(error: ParseError): Unit = error match {
    case ResourceExhaustedError(msg) =>
      throw new RuntimeException(s"Resource exhausted: $msg")
    
    case FileNotFoundError(path) if isRequired(path) =>
      throw new RuntimeException(s"Required file not found: $path")
  }
}
```

#### 11.4.2 断点续传机制
```scala
object CheckpointManager {
  // 保存检查点
  def saveCheckpoint(processedFiles: Set[String], targetDate: String): Unit = {
    val checkpointPath = s"hdfs:///tmp/parser_checkpoint_${targetDate}.json"
    val json = JSON.toJSONString(processedFiles.toArray)
    
    val fs = FileSystem.get(new Configuration())
    val output = fs.create(new Path(checkpointPath), true)
    output.write(json.getBytes("UTF-8"))
    output.close()
  }
  
  // 加载检查点
  def loadCheckpoint(targetDate: String): Set[String] = {
    val checkpointPath = s"hdfs:///tmp/parser_checkpoint_${targetDate}.json"
    val fs = FileSystem.get(new Configuration())
    val path = new Path(checkpointPath)
    
    if (!fs.exists(path)) return Set.empty
    
    val input = fs.open(path)
    val json = IOUtils.toString(input, "UTF-8")
    input.close()
    
    JSON.parseArray(json, classOf[String]).asScala.toSet
  }
  
  // 使用示例
  def parseWithCheckpoint(allFiles: Array[String], targetDate: String): Unit = {
    val processedFiles = loadCheckpoint(targetDate)
    val remainingFiles = allFiles.filterNot(processedFiles.contains)
    
    println(s"Checkpoint loaded: ${processedFiles.size} files already processed")
    println(s"Remaining files: ${remainingFiles.length}")
    
    // 每处理100个文件保存一次检查点
    remainingFiles.grouped(100).foreach { batch =>
      val batchResults = processBatch(batch)
      val newProcessed = processedFiles ++ batchResults.successFiles
      saveCheckpoint(newProcessed, targetDate)
    }
  }
}
```

### 11.5 性能优化补充

#### 11.5.1 动态调整并行度
```scala
object DynamicPartitioner {
  def calculateOptimalPartitions(files: Array[(String, Long)]): Int = {
    val totalSize = files.map(_._2).sum
    val avgSize = totalSize / files.length
    
    // 目标：每个partition处理200MB数据
    val targetSizePerPartition = 200 * 1024 * 1024L
    val optimalPartitions = (totalSize / targetSizePerPartition).toInt
    
    // 限制范围：1000-5000
    Math.max(1000, Math.min(5000, optimalPartitions))
  }
  
  // 智能分区：大文件单独partition，小文件合并
  def smartPartition(files: Array[(String, Long)], 
                     numPartitions: Int): RDD[(String, Long)] = {
    val (largeFiles, smallFiles) = files.partition(_._2 > 50 * 1024 * 1024)
    
    // 大文件：每个独立partition
    val largeRDD = spark.sparkContext.parallelize(largeFiles, largeFiles.length)
    
    // 小文件：合并到少量partition
    val smallRDD = spark.sparkContext.parallelize(
      smallFiles, 
      Math.max(100, smallFiles.length / 100)
    )
    
    largeRDD.union(smallRDD)
  }
}
```

#### 11.5.2 内存优化
```scala
// 使用Kryo序列化减少内存占用
spark.conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
spark.conf.set("spark.kryo.registrationRequired", "false")

// 调整executor内存配置
spark.conf.set("spark.executor.memory", "12g")
spark.conf.set("spark.executor.memoryOverhead", "2g")
spark.conf.set("spark.memory.fraction", "0.6")
spark.conf.set("spark.memory.storageFraction", "0.3")

// 及时释放不需要的RDD
parsedRDD.unpersist()
```

#### 11.5.3 HDFS读取优化
```scala
// 增加HDFS读取缓冲区
spark.sparkContext.hadoopConfiguration.setInt("io.file.buffer.size", 65536)

// 启用HDFS短路读取（Short-circuit reads）
spark.sparkContext.hadoopConfiguration.setBoolean(
  "dfs.client.read.shortcircuit", true
)

// 设置合理的超时时间
spark.sparkContext.hadoopConfiguration.setInt(
  "dfs.client.socket-timeout", 180000
)
```

## 十二、实施路线图

### 阶段一：准备与设计（Week 1-2）

#### 1. 环境准备
- [ ] 确认Spark EventLog存储位置和结构
- [ ] 申请Spark集群资源（200-500 executors）
- [ ] 创建Hive表结构（applications, stages, executors等）
- [ ] 准备开发环境（Scala/PySpark）

#### 2. 技术预研
- [ ] 采样分析EventLog文件格式（不同Spark版本）
- [ ] 测试HDFS文件列表性能（ls 30万文件的耗时）
- [ ] 评估单文件解析性能（平均耗时、内存占用）
- [ ] 确定最优并行度和资源配置

#### 3. 架构设计评审
- [ ] 确认纯Spark方案（去掉Python主控）
- [ ] 设计数据模型和表结构
- [ ] 制定幂等性和容错策略
- [ ] 制定监控和告警方案

### 阶段二：核心开发（Week 3-5）

#### 1. 基础框架开发
```scala
// 主要模块：
// - ConfigLoader: 配置加载
// - FileScanner: 文件扫描（Spark并行）
// - EventLogParser: 事件解析
// - MetricsCalculator: 指标计算
// - HiveWriter: Hive写入
```

**开发优先级：**
1. **P0（必须）**：基础解析框架、Application/Stage表、Hive写入
2. **P1（重要）**：幂等性保证、数据质量校验、错误处理
3. **P2（优化）**：监控上报、断点续传、诊断建议

#### 2. 单元测试
- [ ] EventLog解析逻辑测试（mock JSON数据）
- [ ] 指标计算逻辑测试（p50/p95/倾斜因子）
- [ ] 数据质量校验测试
- [ ] 幂等性测试（重复执行结果一致）

### 阶段三：小规模验证（Week 6）

#### 1. 测试环境部署
```bash
# 小规模测试：1000个文件
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --executor-memory 4g \
  --executor-cores 2 \
  --num-executors 10 \
  --conf spark.app.cluster_name=test_cluster \
  --conf spark.app.target_date=2024-01-15 \
  --conf spark.app.file_limit=1000 \
  spark-eventlog-parser-assembly-1.0.jar
```

#### 2. 验证清单
- [ ] 解析正确性：随机抽样对比Spark History Server数据
- [ ] 幂等性：重复执行3次，验证数据一致
- [ ] 容错性：注入文件错误，验证不中断
- [ ] 性能：1000文件的解析耗时 < 5分钟
- [ ] 数据质量：校验规则全部通过
- [ ] 监控指标：成功推送到Prometheus

### 阶段四：灰度上线（Week 7-8）

#### 1. 逐步扩大规模
```
Day 1: 1,000文件   → 验证稳定性
Day 2: 10,000文件  → 验证性能
Day 3: 50,000文件  → 验证资源配置
Day 4: 100,000文件 → 验证HDFS压力
Day 5: 300,000文件 → 全量验证
```

#### 2. 性能调优
- [ ] 根据实际执行情况调整并行度
- [ ] 优化executor内存配置
- [ ] 调整输出文件数量
- [ ] 优化HDFS读取参数

#### 3. 监控告警配置
- [ ] 配置Prometheus告警规则
- [ ] 配置钉钉/邮件通知
- [ ] 配置任务超时告警
- [ ] 配置数据质量告警

### 阶段五：生产上线（Week 9）

#### 1. 生产环境部署
```bash
# Airflow DAG配置
# 每天凌晨2点自动执行
# 支持断点续传和自动重试
```

#### 2. 文档完善
- [ ] 操作手册（如何手动触发、如何查看日志）
- [ ] 故障排查手册（常见问题和解决方案）
- [ ] 监控大盘（Grafana Dashboard）
- [ ] 告警处理流程

#### 3. 交接与培训
- [ ] 向运维团队交接
- [ ] 培训故障排查和日常运维
- [ ] 建立On-Call机制

### 阶段六：持续优化（Week 10+）

#### 1. 性能持续优化
- [ ] 分析慢查询，优化Hive表结构
- [ ] 评估是否需要增加索引或物化视图
- [ ] 优化小文件问题（定期合并）

#### 2. 功能增强
- [ ] 开发诊断建议功能（基于规则引擎）
- [ ] 支持实时EventLog解析（流式处理）
- [ ] 开发可视化Dashboard

## 十三、最佳实践总结

### 架构设计
✅ **采用纯Spark方案**，避免Python主控的单点瓶颈  
✅ **使用RDD并行读取**，在Executor端解析文件  
✅ **分布式扫描HDFS**，避免Driver端ls瓶颈  
✅ **智能分区策略**，按文件大小均衡负载  

### 数据可靠性
✅ **INSERT OVERWRITE保证幂等性**，支持任务重跑  
✅ **分级错误处理**，单文件失败不影响全局  
✅ **数据质量校验**，解析后立即验证  
✅ **状态表记录**，支持断点续传  

### 性能优化
✅ **流式解析JSON**，避免OOM  
✅ **动态调整并行度**，根据数据量自适应  
✅ **coalesce控制小文件**，避免HDFS小文件问题  
✅ **Kryo序列化**，减少内存占用  

### 监控运维
✅ **Prometheus + Grafana**，实时监控解析状态  
✅ **多维度告警**，失败率、超时、数据量异常  
✅ **完善日志**，记录每个阶段的执行情况  
✅ **Airflow调度**，支持重试和依赖管理  

### 数据建模
✅ **按日期分区**，提高查询性能  
✅ **Parquet + Snappy**，平衡压缩比和查询速度  
✅ **Stage聚合统计**，避免存储海量Task数据  
✅ **多集群隔离**，通过cluster_name区分  

## 十四、常见问题和解决方案

### Q1: 任务执行超过4小时，如何优化？
**原因分析：**
- 文件数量太多（>50万）
- 并行度不足
- 单个文件过大
- HDFS读取慢

**解决方案：**
1. 增加executor数量和并行度
2. 分批处理（每批10万文件）
3. 检查HDFS性能，优化网络
4. 启用HDFS短路读取

### Q2: 出现重复数据怎么办？
**原因分析：**
- 使用了INSERT INTO而非INSERT OVERWRITE
- 同时运行了多个相同日期的任务
- 分区覆盖模式配置错误

**解决方案：**
```sql
-- 手动去重
INSERT OVERWRITE TABLE meta.spark_applications PARTITION(dt='2024-01-15')
SELECT * FROM (
  SELECT *, ROW_NUMBER() OVER(PARTITION BY cluster_name, app_id, dt ORDER BY create_time DESC) as rn
  FROM meta.spark_applications WHERE dt='2024-01-15'
) t WHERE rn = 1;
```

### Q3: 数据量异常，只解析到几百条记录？
**原因分析：**
- 文件过滤规则太严格
- 日期参数错误
- HDFS权限问题
- EventLog目录配置错误

**解决方案：**
1. 检查日志，确认扫描到的文件数量
2. 手动ls目录验证文件存在
3. 检查HDFS权限
4. 验证配置文件中的event_log_dir路径

### Q4: 内存溢出（OOM）怎么办？
**原因分析：**
- 单个文件过大（>1GB）
- 没有使用流式解析
- executor内存不足
- 对象没有及时释放

**解决方案：**
1. 增加executor内存（12g → 16g）
2. 确保使用流式解析，不要一次性加载整个文件
3. 及时unpersist不需要的RDD
4. 使用Kryo序列化

### Q5: 如何验证解析结果的正确性？
**验证方法：**
```sql
-- 1. 对比总量
SELECT dt, cluster_name, COUNT(*) as app_count
FROM meta.spark_applications
WHERE dt >= '2024-01-01'
GROUP BY dt, cluster_name
ORDER BY dt DESC;

-- 2. 抽样对比（与Spark History Server对比）
SELECT app_id, app_name, start_time, end_time, duration_ms, status
FROM meta.spark_applications
WHERE dt = '2024-01-15' AND cluster_name = 'cluster1'
LIMIT 10;

-- 3. 数据分布检查
SELECT status, COUNT(*) as cnt
FROM meta.spark_applications
WHERE dt = '2024-01-15'
GROUP BY status;
```

## 十五、参考资料

### Spark EventLog官方文档
- [Spark Monitoring and Instrumentation](https://spark.apache.org/docs/latest/monitoring.html)
- [Spark Event Logging](https://spark.apache.org/docs/latest/configuration.html#spark-events)

### 技术博客
- [大规模Spark日志解析实践](https://example.com)
- [30万任务级EventLog解析性能优化](https://example.com)

### 开源项目
- [spark-log-parser](https://github.com/example/spark-log-parser)
- [sparklint](https://github.com/groupon/sparklint)

### 监控工具
- [Spark History Server](https://spark.apache.org/docs/latest/monitoring.html#viewing-after-the-fact)
- [Dr.Elephant](https://github.com/linkedin/dr-elephant)（LinkedIn开源）

## 十六、注意事项

1. **日志文件完整性**：确保解析时日志文件已完整写入（通过文件状态判断，跳过.inprogress文件）
2. **时区处理**：统一使用UTC时间或业务时区，确保时间字段一致性
3. **数据去重**：使用INSERT OVERWRITE或显式去重，保证幂等性
4. **异常处理**：处理损坏、不完整的日志文件，记录错误日志但不中断整体流程
5. **版本兼容**：Spark 3.x不同小版本的事件格式可能有差异，需要兼容处理
6. **存储成本**：Task级别数据量巨大，需评估存储成本，建议默认不解析Task级别
7. **查询性能**：根据查询需求设计合适的分区策略，按日期分区提高查询效率
8. **多集群隔离**：确保不同集群的数据通过cluster_name字段完全隔离，避免数据混淆
9. **配置文件安全**：配置文件包含敏感信息（Hive Metastore配置等），注意权限控制
10. **资源规划**：提前评估Spark集群资源需求，避免影响其他任务
11. **监控完善**：建立完善的监控体系，及时发现和处理问题
12. **数据质量**：每次解析后进行数据质量校验，确保数据准确性
13. **幂等性测试**：充分测试任务重跑场景，确保不会产生重复数据
14. **渐进式上线**：先小规模测试（1000文件），逐步扩大到全量（30万）
15. **版本管理**：做好代码版本管理和配置管理，便于回滚
16. **应急预案**：制定故障应急预案，明确升级路径

