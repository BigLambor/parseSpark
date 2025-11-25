# SQL语句和会话参数获取功能说明

## 功能概述

本次更新实现了从Spark EventLog中提取**SQL语句**和**会话参数设置**的功能，扩展了原有的解析能力。

## ✅ 实现的功能

### 1. SQL语句提取

- **支持的事件类型**：
  - `SparkListenerSQLExecutionStart` - SQL执行开始
  - `SparkListenerSQLExecutionEnd` - SQL执行结束

- **提取的信息**：
  - SQL语句文本（`sql_text`）
  - SQL描述（`description`）
  - 物理执行计划描述（`physical_plan_description`）
  - 执行开始/结束时间
  - 执行状态（SUCCEEDED/FAILED）
  - 错误信息（如果失败）
  - 关联的Job IDs

### 2. 会话参数提取

- **支持的事件类型**：
  - `SparkListenerEnvironmentUpdate` - 环境更新事件

- **提取的参数类别**：
  - **Spark配置参数** (`spark.*`) - 所有spark.conf设置的参数
  - **系统属性** (`system.*`) - JVM系统属性
  - **Java属性** - Java版本、Java Home等

## 📊 新增Hive表

### 1. `spark_sql_executions` 表

存储SQL执行记录：

```sql
CREATE EXTERNAL TABLE spark_sql_executions (
    cluster_name STRING,
    app_id STRING,
    execution_id INT,
    sql_text STRING,              -- SQL语句文本
    description STRING,
    physical_plan_description STRING,
    start_time BIGINT,
    end_time BIGINT,
    duration_ms BIGINT,
    job_ids STRING,              -- JSON数组字符串
    status STRING,
    error_message STRING,
    create_time TIMESTAMP
) PARTITIONED BY (dt STRING);
```

### 2. `spark_configs` 表

存储Spark配置参数：

```sql
CREATE EXTERNAL TABLE spark_configs (
    cluster_name STRING,
    app_id STRING,
    config_key STRING,           -- 配置键
    config_value STRING,         -- 配置值
    config_category STRING,      -- spark/system/java
    create_time TIMESTAMP
) PARTITIONED BY (dt STRING);
```

## 🔧 代码变更

### 新增文件

1. **`models/sql_metrics.py`** - SQL和配置数据模型
   - `SQLMetrics` - SQL执行指标模型
   - `SparkConfigMetrics` - Spark配置指标模型

2. **`example_sql_queries.sql`** - SQL查询示例

### 修改文件

1. **`parser/event_parser.py`**
   - 扩展`ApplicationState`类，添加SQL和配置存储
   - 添加`to_sql_metrics()`和`to_config_metrics()`方法
   - 添加`SparkListenerSQLExecutionStart/End`事件处理
   - 添加`SparkListenerEnvironmentUpdate`事件处理

2. **`create_hive_tables.sql`**
   - 添加`spark_sql_executions`表定义
   - 添加`spark_configs`表定义

3. **`parser/config_loader.py`**
   - 添加新表名配置：`sql_executions`、`spark_configs`

4. **`parser/hive_writer.py`**
   - 添加`write_sql_executions()`方法
   - 添加`write_spark_configs()`方法
   - 更新`write_all()`方法，包含新数据写入

5. **`parse_spark_logs.py`**
   - 更新解析结果，包含SQL和配置数据
   - 更新统计信息，包含SQL和配置计数

## 📝 使用方法

### 1. 创建新表

执行更新后的建表SQL：

```bash
hive -f create_hive_tables.sql
```

### 2. 运行解析程序

解析程序会自动提取SQL和配置信息，无需额外配置：

```bash
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --conf spark.app.cluster_name=cluster1 \
  --conf spark.app.target_date=2024-01-15 \
  parse_spark_logs.py
```

### 3. 查询SQL执行记录

```sql
-- 查看所有SQL执行
SELECT * FROM meta.spark_sql_executions
WHERE dt = '2024-01-15' AND cluster_name = 'cluster1'
ORDER BY start_time DESC;

-- 查找失败的SQL
SELECT sql_text, error_message 
FROM meta.spark_sql_executions
WHERE dt = '2024-01-15' 
  AND status = 'FAILED';
```

### 4. 查询配置参数

```sql
-- 查看应用的Spark配置
SELECT config_key, config_value
FROM meta.spark_configs
WHERE dt = '2024-01-15' 
  AND app_id = 'application_1234567890_0001'
  AND config_category = 'spark'
ORDER BY config_key;
```

更多查询示例请参考 `example_sql_queries.sql`。

## ⚠️ 注意事项

### 1. 兼容性

- SQL事件处理兼容不同Spark版本的字段名
- 如果EventLog中没有SQL事件，相关表将为空（不影响其他数据）

### 2. 数据量

- SQL执行记录：每个Spark SQL应用可能有多条SQL执行记录
- 配置参数：每个应用可能有数百条配置记录（spark配置较多）

### 3. SQL文本长度

- SQL文本可能很长，查询时建议使用`LEFT(sql_text, 100)`截断显示
- 物理执行计划描述可能非常长，建议按需查询

### 4. 字段兼容性

不同Spark版本的EventLog字段名可能不同，代码已做兼容处理：
- `executionId` / `Execution ID` / `execution_id`
- `sqlText` / `SQL Text` / `sql`
- `physicalPlanDescription` / `Physical Plan Description`

## 🔍 验证方法

### 1. 检查数据是否写入

```sql
-- 检查SQL执行记录数
SELECT COUNT(*) FROM meta.spark_sql_executions WHERE dt = '2024-01-15';

-- 检查配置记录数
SELECT COUNT(*) FROM meta.spark_configs WHERE dt = '2024-01-15';
```

### 2. 抽样验证

```sql
-- 随机查看几条SQL记录
SELECT app_id, execution_id, LEFT(sql_text, 50) as sql_preview
FROM meta.spark_sql_executions
WHERE dt = '2024-01-15'
LIMIT 10;
```

## 📈 性能影响

- **解析性能**：SQL和配置事件处理开销很小，对整体解析性能影响可忽略
- **存储空间**：SQL文本和配置参数会增加存储空间，但通常不会超过原有数据的10%
- **查询性能**：新增表按日期分区，查询性能良好

## 🎯 应用场景

1. **SQL性能分析**：找出执行最慢的SQL语句
2. **SQL错误排查**：查看失败的SQL及其错误信息
3. **配置审计**：查看应用使用的Spark配置参数
4. **配置优化**：分析不同配置对性能的影响
5. **SQL审计**：记录所有执行的SQL语句

## 📚 相关文档

- `example_sql_queries.sql` - SQL查询示例
- `create_hive_tables.sql` - Hive表结构定义
- `README.md` - 项目总体说明

