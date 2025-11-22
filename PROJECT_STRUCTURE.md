# 项目结构说明

```
parseSpark/
├── README.md                           # 项目主文档
├── QUICKSTART.md                       # 快速上手指南
├── CHANGELOG.md                        # 更新日志
├── PROJECT_STRUCTURE.md                # 本文件
├── Spark作业解析方案设计.md             # 详细设计文档
├── 方案优化总结.md                      # 优化建议
│
├── config.yaml.example                 # 配置文件示例
├── requirements.txt                    # Python依赖
├── setup.py                            # Python包配置
├── .gitignore                          # Git忽略文件
│
├── parse_spark_logs.py                 # 主程序入口 ★
├── submit_parser.sh                    # 任务提交脚本 ★
├── run_tests.sh                        # 测试运行脚本
│
├── create_hive_tables.sql              # Hive建表SQL ★
├── example_queries.sql                 # 查询示例SQL
│
├── parser/                             # 解析模块
│   ├── __init__.py
│   ├── config_loader.py                # 配置加载
│   ├── file_scanner.py                 # 文件扫描（Spark并行）
│   ├── event_parser.py                 # 事件解析核心 ★★
│   ├── metrics_calculator.py           # 指标计算
│   └── hive_writer.py                  # Hive写入
│
├── models/                             # 数据模型
│   ├── __init__.py
│   ├── app_metrics.py                  # 应用指标模型
│   └── stage_metrics.py                # Stage/Job指标模型
│
├── utils/                              # 工具模块
│   ├── __init__.py
│   ├── date_utils.py                   # 日期工具
│   └── hdfs_utils.py                   # HDFS工具
│
└── tests/                              # 单元测试
    ├── __init__.py
    ├── test_parser.py                  # 解析器测试
    └── test_utils.py                   # 工具模块测试
```

## 核心文件说明

### 1. 主程序文件

#### `parse_spark_logs.py` ★★★
**作用：** 程序主入口，协调整个解析流程

**主要功能：**
- 初始化SparkSession
- 加载配置
- 调用文件扫描
- 并行解析EventLog
- 写入Hive
- 收集统计信息

**执行方式：**
```bash
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --conf spark.app.cluster_name=cluster1 \
  --conf spark.app.target_date=2024-01-15 \
  --py-files parser.zip,models.zip,utils.zip \
  parse_spark_logs.py
```

#### `submit_parser.sh` ★★
**作用：** 任务提交包装脚本，简化提交流程

**主要功能：**
- 参数解析和验证
- Python模块打包
- 配置文件上传到HDFS
- 调用spark-submit
- 返回执行结果

**使用方式：**
```bash
./submit_parser.sh cluster1 2024-01-15
```

### 2. 配置文件

#### `config.yaml.example` ★★
**作用：** 配置文件模板

**关键配置：**
- HDFS EventLog目录
- Hive数据库配置
- 解析并行度
- 资源分配
- 监控配置

**使用步骤：**
```bash
cp config.yaml.example config.yaml
vim config.yaml  # 修改配置
```

### 3. 核心解析模块

#### `parser/event_parser.py` ★★★
**作用：** EventLog解析核心逻辑

**关键类：**
- `ApplicationState` - 应用状态管理
- `EventLogParser` - 事件解析器

**支持的事件类型：**
- SparkListenerApplicationStart
- SparkListenerApplicationEnd
- SparkListenerJobStart/End
- SparkListenerStageSubmitted/Completed
- SparkListenerTaskEnd
- SparkListenerExecutorAdded/Removed

#### `parser/file_scanner.py` ★★
**作用：** 分布式扫描HDFS文件

**主要功能：**
- 并行扫描HDFS目录
- 按日期过滤文件
- 智能分区（按文件大小均衡负载）

#### `parser/hive_writer.py` ★★
**作用：** 写入Hive表

**主要功能：**
- 动态分区写入
- 数据去重（幂等性保证）
- 控制输出文件数量
- 支持多表写入

### 4. 数据模型

#### `models/app_metrics.py`
定义应用级别指标数据结构：
- AppMetrics - 应用指标
- ExecutorMetrics - Executor指标

#### `models/stage_metrics.py`
定义Job/Stage级别指标数据结构：
- JobMetrics - Job指标
- StageMetrics - Stage指标（包含Task聚合统计）

### 5. 工具模块

#### `utils/date_utils.py`
日期处理工具，支持：
- 日期解析和格式化
- 时间戳转换
- 日期比较

#### `utils/hdfs_utils.py`
HDFS操作工具，支持：
- 文件大小获取
- 文件分组（大文件/小文件）
- 应用ID提取

### 6. 数据库表

#### `create_hive_tables.sql` ★★
**作用：** 创建Hive表

**主要表：**
1. `spark_applications` - 应用级别指标
2. `spark_jobs` - Job级别指标
3. `spark_stages` - Stage级别指标（含Task聚合）
4. `spark_executors` - Executor信息
5. `spark_diagnosis` - 诊断建议（可选）

**分区策略：** 按日期分区（dt字段）

### 7. 测试文件

#### `tests/test_parser.py`
解析器单元测试，覆盖：
- ApplicationState转换
- Job/Stage指标计算
- Task聚合统计

#### `tests/test_utils.py`
工具模块单元测试，覆盖：
- 日期工具
- HDFS工具

## 数据流程

```
1. 启动
   ├─ spark-submit 提交任务
   └─ 初始化SparkSession（启用Hive支持）

2. 配置加载
   ├─ 读取config.yaml
   ├─ 解析Spark Conf参数
   └─ 验证配置完整性

3. 文件扫描
   ├─ 连接HDFS
   ├─ 扫描EventLog目录
   ├─ 按日期过滤
   └─ 返回文件路径列表

4. 并行解析
   ├─ 将文件路径并行化为RDD
   ├─ 在Executor端读取文件
   ├─ 流式解析JSON Lines
   ├─ 提取事件并构建应用状态
   └─ 返回解析结果

5. 数据汇总
   ├─ 收集所有解析结果
   ├─ 合并应用/Job/Stage/Executor数据
   └─ 计算聚合统计

6. 数据写入
   ├─ 转换为DataFrame
   ├─ 数据去重
   ├─ 写入Hive表（动态分区）
   └─ 验证写入成功

7. 统计上报
   ├─ 打印解析统计
   ├─ 推送监控指标
   └─ 返回执行结果
```

## 关键设计原则

### 1. 纯Spark方案
- ✅ 无Python主控，避免单点瓶颈
- ✅ Driver端只负责协调，不处理数据
- ✅ Executor端并行读取和解析

### 2. 容错机制
- ✅ 单文件解析失败不影响全局
- ✅ 记录失败文件列表
- ✅ 支持断点续传

### 3. 幂等性保证
- ✅ INSERT OVERWRITE按分区覆盖
- ✅ 数据去重
- ✅ 支持任务重跑

### 4. 性能优化
- ✅ 流式解析，避免OOM
- ✅ 智能分区，均衡负载
- ✅ 控制小文件数量
- ✅ Kryo序列化

### 5. 可维护性
- ✅ 模块化设计
- ✅ 清晰的目录结构
- ✅ 完善的文档
- ✅ 单元测试覆盖

## 扩展性

### 支持新的事件类型
在 `parser/event_parser.py` 中添加新的事件处理函数：

```python
elif event_type == 'SparkListenerNewEvent':
    handle_new_event(event, app_state)
```

### 支持新的指标
1. 在 `models/` 中定义新的数据模型
2. 在 `parser/event_parser.py` 中提取指标
3. 在 `parser/hive_writer.py` 中添加写入逻辑
4. 在 `create_hive_tables.sql` 中创建新表

### 支持新的数据源
实现新的Scanner类，继承FileScanner的接口。

## 性能基准

| 文件数量 | Executor数量 | 并行度 | 预计耗时 | 实测耗时 |
|---------|-------------|--------|---------|---------|
| 1,000   | 10          | 100    | 2-5分钟  | -       |
| 10,000  | 50          | 500    | 5-10分钟 | -       |
| 100,000 | 150         | 1500   | 15-30分钟| -       |
| 300,000 | 200-500     | 2000-5000 | 20-60分钟| -    |

## 常见问题

### 如何修改并行度？
编辑 `submit_parser.sh`：
```bash
PARALLELISM=3000
```

### 如何增加资源？
编辑 `submit_parser.sh`：
```bash
NUM_EXECUTORS=400
EXECUTOR_MEMORY=16g
```

### 如何调试？
使用client模式本地运行：
```bash
spark-submit \
  --master local[*] \
  --deploy-mode client \
  --conf spark.app.cluster_name=test \
  --conf spark.app.target_date=2024-01-15 \
  parse_spark_logs.py
```

## 更多信息

- 详细设计：见 `Spark作业解析方案设计.md`
- 快速上手：见 `QUICKSTART.md`
- 查询示例：见 `example_queries.sql`
- 更新日志：见 `CHANGELOG.md`

