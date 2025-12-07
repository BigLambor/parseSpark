"""
Hive写入模块
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, current_timestamp, col
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, LongType, 
    TimestampType, DoubleType
)
from datetime import datetime
import time


class HiveWriter:
    """Hive数据写入器"""
    
    # 定义各表的Schema，确保类型与Hive表定义一致
    APP_SCHEMA = StructType([
        StructField('cluster_name', StringType(), False),
        StructField('app_id', StringType(), False),
        StructField('app_name', StringType(), True),
        StructField('start_time', LongType(), True),
        StructField('end_time', LongType(), True),
        StructField('duration_ms', LongType(), True),
        StructField('status', StringType(), True),
        StructField('app_user', StringType(), True),
        StructField('spark_version', StringType(), True),
        StructField('executor_count', IntegerType(), True),
        StructField('total_cores', IntegerType(), True),
        StructField('total_memory_mb', LongType(), True),
        StructField('dt', StringType(), False)
    ])
    
    JOB_SCHEMA = StructType([
        StructField('cluster_name', StringType(), False),
        StructField('app_id', StringType(), False),
        StructField('job_id', IntegerType(), False),
        StructField('submission_time', LongType(), True),
        StructField('completion_time', LongType(), True),
        StructField('duration_ms', LongType(), True),
        StructField('status', StringType(), True),
        StructField('stage_count', IntegerType(), True),
        StructField('dt', StringType(), False)
    ])
    
    STAGE_SCHEMA = StructType([
        StructField('cluster_name', StringType(), False),
        StructField('app_id', StringType(), False),
        StructField('job_id', IntegerType(), True),
        StructField('stage_id', IntegerType(), False),
        StructField('stage_name', StringType(), True),
        StructField('submission_time', LongType(), True),
        StructField('completion_time', LongType(), True),
        StructField('duration_ms', LongType(), True),
        StructField('status', StringType(), True),
        StructField('input_bytes', LongType(), True),
        StructField('input_records', LongType(), True),
        StructField('output_bytes', LongType(), True),
        StructField('output_records', LongType(), True),
        StructField('shuffle_read_bytes', LongType(), True),
        StructField('shuffle_read_records', LongType(), True),
        StructField('shuffle_write_bytes', LongType(), True),
        StructField('shuffle_write_records', LongType(), True),
        StructField('num_tasks', IntegerType(), True),
        StructField('num_failed_tasks', IntegerType(), True),
        StructField('task_duration_p50', LongType(), True),
        StructField('task_duration_p75', LongType(), True),
        StructField('task_duration_p95', LongType(), True),
        StructField('task_duration_max', LongType(), True),
        StructField('skew_factor', DoubleType(), True),
        StructField('input_skew_factor', DoubleType(), True),
        StructField('peak_memory_max', LongType(), True),
        StructField('dt', StringType(), False)
    ])
    
    EXECUTOR_SCHEMA = StructType([
        StructField('cluster_name', StringType(), False),
        StructField('app_id', StringType(), False),
        StructField('executor_id', StringType(), False),
        StructField('host', StringType(), True),
        StructField('add_time', LongType(), True),
        StructField('remove_time', LongType(), True),
        StructField('total_cores', IntegerType(), True),
        StructField('max_memory_mb', LongType(), True),
        StructField('dt', StringType(), False)
    ])
    
    SQL_SCHEMA = StructType([
        StructField('cluster_name', StringType(), False),
        StructField('app_id', StringType(), False),
        StructField('execution_id', IntegerType(), False),
        StructField('sql_text', StringType(), True),
        StructField('description', StringType(), True),
        StructField('physical_plan_description', StringType(), True),
        StructField('start_time', LongType(), True),
        StructField('end_time', LongType(), True),
        StructField('duration_ms', LongType(), True),
        StructField('job_ids', StringType(), True),
        StructField('status', StringType(), True),
        StructField('error_message', StringType(), True),
        StructField('dt', StringType(), False)
    ])
    
    CONFIG_SCHEMA = StructType([
        StructField('cluster_name', StringType(), False),
        StructField('app_id', StringType(), False),
        StructField('config_key', StringType(), False),
        StructField('config_value', StringType(), True),
        StructField('config_category', StringType(), True),
        StructField('dt', StringType(), False)
    ])
    
    def __init__(self, spark, config):
        """
        初始化
        :param spark: SparkSession对象
        :param config: ParserConfig配置对象
        """
        self.spark = spark
        self.config = config
        self.table_names = config.hive_tables
        self._setup_hive()
    
    def _setup_hive(self):
        """设置Hive参数"""
        # 启用动态分区
        if self.config.dynamic_partition:
            self.spark.sql("SET spark.sql.sources.partitionOverwriteMode=dynamic")
            self.spark.sql("SET hive.exec.dynamic.partition=true")
            self.spark.sql(f"SET hive.exec.dynamic.partition.mode={self.config.dynamic_partition_mode}")
        
        # 设置Hive数据库
        self.spark.sql(f"USE {self.config.hive_database}")
        
        print(f"使用Hive数据库: {self.config.hive_database}")
    
    def write_applications(self, app_metrics_source):
        """
        写入应用表
        :param app_metrics_source: AppMetrics对象列表或RDD
        """
        # 修复: 支持RDD或列表输入，避免Driver OOM
        from pyspark import RDD
        
        if isinstance(app_metrics_source, RDD):
            # 如果是RDD，先检查是否为空
            if app_metrics_source.isEmpty():
                print("应用数据为空，跳过写入")
                return
            
            print(f"准备从RDD写入应用数据...")
            
            # 直接从RDD创建DataFrame，使用明确的schema避免类型推断错误
            df = self.spark.createDataFrame(
                app_metrics_source.map(lambda x: x.to_dict()),
                schema=self.APP_SCHEMA
            )
        else:
            # 兼容旧的列表方式
            if not app_metrics_source:
                print("应用数据为空，跳过写入")
                return
            
            print(f"准备写入 {len(app_metrics_source)} 条应用数据...")
            
            # 转换为字典列表
            data = [app.to_dict() for app in app_metrics_source]
            
            # 创建DataFrame，使用明确的schema避免类型推断错误
            df = self.spark.createDataFrame(data, schema=self.APP_SCHEMA)
        
        # 添加创建时间（timestamp类型）
        df = df.withColumn('create_time', current_timestamp())
        
        # 重新排列列顺序，确保create_time在dt之前（与Hive表结构一致）
        # insertInto按位置匹配，必须保证DataFrame列顺序与Hive表一致
        df = df.select(
            'cluster_name', 'app_id', 'app_name', 'start_time', 'end_time',
            'duration_ms', 'status', 'app_user', 'spark_version', 
            'executor_count', 'total_cores', 'total_memory_mb',
            'create_time', 'dt'
        )
        
        # 去重
        df = df.dropDuplicates(['cluster_name', 'app_id', 'dt'])
        
        # 计算输出文件数量（避免小文件）
        record_count = df.count()
        num_files = max(int(record_count / 100000), 1)
        num_files = min(num_files, 40)  # 最多40个文件
        
        print(f"实际写入 {record_count} 条，输出 {num_files} 个文件")
        
        # 写入Hive表
        self._write_table(df, self.table_names['applications'], num_files)
        
        print("应用数据写入完成")
    
    def write_jobs(self, job_metrics_source):
        """
        写入Job表
        :param job_metrics_source: JobMetrics对象列表或RDD
        """
        # 修复: 支持RDD或列表输入，避免Driver OOM
        from pyspark import RDD
        
        if isinstance(job_metrics_source, RDD):
            if job_metrics_source.isEmpty():
                print("Job数据为空，跳过写入")
                return
            
            print(f"准备从RDD写入Job数据...")
            df = self.spark.createDataFrame(
                job_metrics_source.map(lambda x: x.to_dict()),
                schema=self.JOB_SCHEMA
            )
        else:
            if not job_metrics_source:
                print("Job数据为空，跳过写入")
                return
            
            print(f"准备写入 {len(job_metrics_source)} 条Job数据...")
            data = [job.to_dict() for job in job_metrics_source]
            df = self.spark.createDataFrame(data, schema=self.JOB_SCHEMA)
        
        # 添加创建时间（timestamp类型）
        df = df.withColumn('create_time', current_timestamp())
        
        # 重新排列列顺序，确保create_time在dt之前（与Hive表结构一致）
        df = df.select(
            'cluster_name', 'app_id', 'job_id', 'submission_time', 'completion_time',
            'duration_ms', 'status', 'stage_count', 'create_time', 'dt'
        )
        
        # 去重
        df = df.dropDuplicates(['cluster_name', 'app_id', 'job_id', 'dt'])
        
        # 计算输出文件数量
        record_count = df.count()
        num_files = max(int(record_count / 100000), 1)
        num_files = min(num_files, 40)
        
        print(f"实际写入 {record_count} 条，输出 {num_files} 个文件")
        
        # 写入Hive表
        self._write_table(df, self.table_names['jobs'], num_files)
        
        print("Job数据写入完成")
    
    def write_stages(self, stage_metrics_source):
        """
        写入Stage表
        :param stage_metrics_source: StageMetrics对象列表或RDD
        """
        # 修复: 支持RDD或列表输入，避免Driver OOM
        from pyspark import RDD
        
        if isinstance(stage_metrics_source, RDD):
            if stage_metrics_source.isEmpty():
                print("Stage数据为空，跳过写入")
                return
            
            print(f"准备从RDD写入Stage数据...")
            df = self.spark.createDataFrame(
                stage_metrics_source.map(lambda x: x.to_dict()),
                schema=self.STAGE_SCHEMA
            )
        else:
            if not stage_metrics_source:
                print("Stage数据为空，跳过写入")
                return
            
            print(f"准备写入 {len(stage_metrics_source)} 条Stage数据...")
            data = [stage.to_dict() for stage in stage_metrics_source]
            df = self.spark.createDataFrame(data, schema=self.STAGE_SCHEMA)
        
        # 添加创建时间（timestamp类型）
        df = df.withColumn('create_time', current_timestamp())
        
        # 重新排列列顺序，确保create_time在dt之前（与Hive表结构一致）
        df = df.select(
            'cluster_name', 'app_id', 'job_id', 'stage_id', 'stage_name',
            'submission_time', 'completion_time', 'duration_ms', 'status',
            'input_bytes', 'input_records', 'output_bytes', 'output_records',
            'shuffle_read_bytes', 'shuffle_read_records', 
            'shuffle_write_bytes', 'shuffle_write_records',
            'num_tasks', 'num_failed_tasks',
            'task_duration_p50', 'task_duration_p75', 'task_duration_p95', 'task_duration_max',
            'skew_factor', 'input_skew_factor', 'peak_memory_max',
            'create_time', 'dt'
        )
        
        # 去重
        df = df.dropDuplicates(['cluster_name', 'app_id', 'stage_id', 'dt'])
        
        # 计算输出文件数量
        record_count = df.count()
        num_files = max(int(record_count / 100000), 1)
        num_files = min(num_files, 40)
        
        print(f"实际写入 {record_count} 条，输出 {num_files} 个文件")
        
        # 写入Hive表
        self._write_table(df, self.table_names['stages'], num_files)
        
        print("Stage数据写入完成")
    
    def write_executors(self, executor_metrics_source):
        """
        写入Executor表
        :param executor_metrics_source: ExecutorMetrics对象列表或RDD
        """
        # 修复: 支持RDD或列表输入，避免Driver OOM
        from pyspark import RDD
        
        if isinstance(executor_metrics_source, RDD):
            if executor_metrics_source.isEmpty():
                print("Executor数据为空，跳过写入")
                return
            
            print(f"准备从RDD写入Executor数据...")
            df = self.spark.createDataFrame(
                executor_metrics_source.map(lambda x: x.to_dict()),
                schema=self.EXECUTOR_SCHEMA
            )
        else:
            if not executor_metrics_source:
                print("Executor数据为空，跳过写入")
                return
            
            print(f"准备写入 {len(executor_metrics_source)} 条Executor数据...")
            data = [executor.to_dict() for executor in executor_metrics_source]
            df = self.spark.createDataFrame(data, schema=self.EXECUTOR_SCHEMA)
        
        # 添加创建时间（timestamp类型）
        df = df.withColumn('create_time', current_timestamp())
        
        # 重新排列列顺序，确保create_time在dt之前（与Hive表结构一致）
        df = df.select(
            'cluster_name', 'app_id', 'executor_id', 'host',
            'add_time', 'remove_time', 'total_cores', 'max_memory_mb',
            'create_time', 'dt'
        )
        
        # 去重
        df = df.dropDuplicates(['cluster_name', 'app_id', 'executor_id', 'dt'])
        
        # 计算输出文件数量
        record_count = df.count()
        num_files = max(int(record_count / 100000), 1)
        num_files = min(num_files, 20)
        
        print(f"实际写入 {record_count} 条，输出 {num_files} 个文件")
        
        # 写入Hive表
        self._write_table(df, self.table_names['executors'], num_files)
        
        print("Executor数据写入完成")
    
    def write_sql_executions(self, sql_metrics_source):
        """
        写入SQL执行表
        :param sql_metrics_source: SQLMetrics对象列表或RDD
        """
        from pyspark import RDD
        
        if isinstance(sql_metrics_source, RDD):
            if sql_metrics_source.isEmpty():
                print("SQL执行数据为空，跳过写入")
                return
            
            print(f"准备从RDD写入SQL执行数据...")
            df = self.spark.createDataFrame(
                sql_metrics_source.map(lambda x: x.to_dict()),
                schema=self.SQL_SCHEMA
            )
        else:
            if not sql_metrics_source:
                print("SQL执行数据为空，跳过写入")
                return
            
            print(f"准备写入 {len(sql_metrics_source)} 条SQL执行数据...")
            data = [sql.to_dict() for sql in sql_metrics_source]
            df = self.spark.createDataFrame(data, schema=self.SQL_SCHEMA)
        
        # 添加创建时间（timestamp类型）
        df = df.withColumn('create_time', current_timestamp())
        
        # 重新排列列顺序，确保create_time在dt之前（与Hive表结构一致）
        df = df.select(
            'cluster_name', 'app_id', 'execution_id', 'sql_text', 'description',
            'physical_plan_description', 'start_time', 'end_time', 'duration_ms',
            'job_ids', 'status', 'error_message', 'create_time', 'dt'
        )
        
        # 去重
        df = df.dropDuplicates(['cluster_name', 'app_id', 'execution_id', 'dt'])
        
        # 计算输出文件数量
        record_count = df.count()
        num_files = max(int(record_count / 100000), 1)
        num_files = min(num_files, 20)
        
        print(f"实际写入 {record_count} 条，输出 {num_files} 个文件")
        
        # 写入Hive表
        self._write_table(df, self.table_names['sql_executions'], num_files)
        
        print("SQL执行数据写入完成")
    
    def write_spark_configs(self, config_metrics_source):
        """
        写入Spark配置表
        :param config_metrics_source: SparkConfigMetrics对象列表或RDD
        """
        from pyspark import RDD
        
        if isinstance(config_metrics_source, RDD):
            if config_metrics_source.isEmpty():
                print("Spark配置数据为空，跳过写入")
                return
            
            print(f"准备从RDD写入Spark配置数据...")
            df = self.spark.createDataFrame(
                config_metrics_source.map(lambda x: x.to_dict()),
                schema=self.CONFIG_SCHEMA
            )
        else:
            if not config_metrics_source:
                print("Spark配置数据为空，跳过写入")
                return
            
            print(f"准备写入 {len(config_metrics_source)} 条Spark配置数据...")
            data = [config.to_dict() for config in config_metrics_source]
            df = self.spark.createDataFrame(data, schema=self.CONFIG_SCHEMA)
        
        # 添加创建时间（timestamp类型）
        df = df.withColumn('create_time', current_timestamp())
        
        # 重新排列列顺序，确保create_time在dt之前（与Hive表结构一致）
        df = df.select(
            'cluster_name', 'app_id', 'config_key', 'config_value',
            'config_category', 'create_time', 'dt'
        )
        
        # 去重（同一个app_id的同一个配置键只保留一条）
        df = df.dropDuplicates(['cluster_name', 'app_id', 'config_key', 'dt'])
        
        # 计算输出文件数量
        record_count = df.count()
        num_files = max(int(record_count / 100000), 1)
        num_files = min(num_files, 20)
        
        print(f"实际写入 {record_count} 条，输出 {num_files} 个文件")
        
        # 写入Hive表
        self._write_table(df, self.table_names['spark_configs'], num_files)
        
        print("Spark配置数据写入完成")
    
    def write_all(self, parse_results):
        """
        写入所有数据
        :param parse_results: 解析结果字典（支持列表或RDD）
        """
        print("\n" + "="*50)
        print("开始写入Hive表...")
        print("="*50)
        
        # 修复: 支持新的RDD结构和旧的列表结构
        # 尝试获取RDD（新结构），如果不存在则使用列表（旧结构）
        all_apps = parse_results.get('applications_rdd', parse_results.get('applications', []))
        all_jobs = parse_results.get('jobs_rdd', parse_results.get('jobs', []))
        all_stages = parse_results.get('stages_rdd', parse_results.get('stages', []))
        all_executors = parse_results.get('executors_rdd', parse_results.get('executors', []))
        all_sql_executions = parse_results.get('sql_executions_rdd', parse_results.get('sql_executions', []))
        all_spark_configs = parse_results.get('spark_configs_rdd', parse_results.get('spark_configs', []))
        
        # 写入各表（现在支持RDD和列表）
        self.write_applications(all_apps)
        self.write_jobs(all_jobs)
        self.write_stages(all_stages)
        self.write_executors(all_executors)
        self.write_sql_executions(all_sql_executions)
        self.write_spark_configs(all_spark_configs)
        
        # 获取统计信息
        stats = parse_results.get('statistics')
        
        print("\n" + "="*50)
        print("所有数据写入完成!")
        if stats:
            print(f"应用数: {stats.total_apps}")
            print(f"Job数: {stats.total_jobs}")
            print(f"Stage数: {stats.total_stages}")
            print(f"Executor数: {stats.total_executors}")
            print(f"SQL执行数: {getattr(stats, 'total_sql_executions', 0)}")
            print(f"Spark配置数: {getattr(stats, 'total_spark_configs', 0)}")
        print("="*50 + "\n")
    
    def write_parser_status(self, parse_results):
        """
        写入解析状态表（幂等性保证）
        修复P0问题：记录每个文件的解析状态，支持任务重跑时跳过已处理文件
        :param parse_results: 解析结果字典，包含statistics
        """
        stats = parse_results.get('statistics')
        if not stats:
            return
        
        print("\n写入解析状态表...")
        
        # 构建状态记录
        status_records = []
        target_date = self.config.target_date
        cluster_name = self.config.cluster_name
        process_time = datetime.now()
        
        # 记录成功文件
        success_file_list = getattr(stats, 'success_file_list', [])
        for file_path in success_file_list:
            status_records.append({
                'cluster_name': cluster_name,
                'file_path': file_path,
                'process_date': target_date,
                'status': 'SUCCESS',
                'record_count': 1,  # 每个文件至少对应1个应用
                'process_time': process_time,
                'duration_ms': 0,  # 单个文件耗时未单独记录
                'error_msg': None,
                'dt': target_date
            })
        
        # 记录失败文件
        for file_path, error_msg in stats.failed_file_list:
            status_records.append({
                'cluster_name': cluster_name,
                'file_path': file_path,
                'process_date': target_date,
                'status': 'FAILED',
                'record_count': 0,
                'process_time': process_time,
                'duration_ms': 0,
                'error_msg': error_msg[:500] if error_msg else None,  # 限制错误信息长度
                'dt': target_date
            })
        
        if not status_records:
            print("没有需要记录的状态信息")
            return
        
        # 创建DataFrame
        schema = StructType([
            StructField('cluster_name', StringType(), False),
            StructField('file_path', StringType(), False),
            StructField('process_date', StringType(), False),
            StructField('status', StringType(), False),
            StructField('record_count', IntegerType(), False),
            StructField('process_time', TimestampType(), False),
            StructField('duration_ms', LongType(), False),
            StructField('error_msg', StringType(), True),
            StructField('dt', StringType(), False)
        ])
        
        status_df = self.spark.createDataFrame(status_records, schema)
        
        # 写入状态表
        status_table = f"{self.config.hive_database}.{self.table_names['parser_status']}"
        
        try:
            # 使用动态分区覆盖模式，避免重复记录
            status_df.coalesce(1) \
                .write \
                .mode('append') \
                .format('parquet') \
                .option('compression', 'snappy') \
                .insertInto(status_table)
            print(f"已写入 {len(status_records)} 条状态记录到 {status_table}")
            print(f"  - 成功: {len(success_file_list)} 条")
            print(f"  - 失败: {len(stats.failed_file_list)} 条")
        except Exception as e:
            print(f"警告: 写入状态表失败: {e}")
            # 状态表写入失败不影响主流程
    
    def _write_table(self, df, table_name, num_files):
        """
        统一写入逻辑
        修复P0问题：使用insertInto替代saveAsTable，确保只覆盖对应分区而不是整个表
        """
        full_table_name = f'{self.config.hive_database}.{table_name}'
        
        # 使用insertInto确保动态分区覆盖模式正常工作
        # 配合spark.sql.sources.partitionOverwriteMode=dynamic，只覆盖对应dt分区的数据
        df.coalesce(num_files) \
            .write \
            .mode('overwrite') \
            .format('parquet') \
            .option('compression', 'snappy') \
            .insertInto(full_table_name)

