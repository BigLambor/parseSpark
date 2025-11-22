"""
Hive写入模块
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, current_timestamp
from datetime import datetime


class HiveWriter:
    """Hive数据写入器"""
    
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
            
            # 直接从RDD创建DataFrame
            df = self.spark.createDataFrame(
                app_metrics_source.map(lambda x: x.to_dict())
            )
        else:
            # 兼容旧的列表方式
            if not app_metrics_source:
                print("应用数据为空，跳过写入")
                return
            
            print(f"准备写入 {len(app_metrics_source)} 条应用数据...")
            
            # 转换为字典列表
            data = [app.to_dict() for app in app_metrics_source]
            
            # 创建DataFrame
            df = self.spark.createDataFrame(data)
        
        # 添加创建时间
        df = df.withColumn('create_time', current_timestamp())
        
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
                job_metrics_source.map(lambda x: x.to_dict())
            )
        else:
            if not job_metrics_source:
                print("Job数据为空，跳过写入")
                return
            
            print(f"准备写入 {len(job_metrics_source)} 条Job数据...")
            data = [job.to_dict() for job in job_metrics_source]
            df = self.spark.createDataFrame(data)
        
        # 添加创建时间
        df = df.withColumn('create_time', current_timestamp())
        
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
                stage_metrics_source.map(lambda x: x.to_dict())
            )
        else:
            if not stage_metrics_source:
                print("Stage数据为空，跳过写入")
                return
            
            print(f"准备写入 {len(stage_metrics_source)} 条Stage数据...")
            data = [stage.to_dict() for stage in stage_metrics_source]
            df = self.spark.createDataFrame(data)
        
        # 添加创建时间
        df = df.withColumn('create_time', current_timestamp())
        
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
                executor_metrics_source.map(lambda x: x.to_dict())
            )
        else:
            if not executor_metrics_source:
                print("Executor数据为空，跳过写入")
                return
            
            print(f"准备写入 {len(executor_metrics_source)} 条Executor数据...")
            data = [executor.to_dict() for executor in executor_metrics_source]
            df = self.spark.createDataFrame(data)
        
        # 添加创建时间
        df = df.withColumn('create_time', current_timestamp())
        
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
        
        # 写入各表（现在支持RDD和列表）
        self.write_applications(all_apps)
        self.write_jobs(all_jobs)
        self.write_stages(all_stages)
        self.write_executors(all_executors)
        
        # 获取统计信息
        stats = parse_results.get('statistics')
        
        print("\n" + "="*50)
        print("所有数据写入完成!")
        if stats:
            print(f"应用数: {stats.total_apps}")
            print(f"Job数: {stats.total_jobs}")
            print(f"Stage数: {stats.total_stages}")
            print(f"Executor数: {stats.total_executors}")
        print("="*50 + "\n")
    
    def _write_table(self, df, table_name, num_files):
        """统一写入逻辑"""
        full_table_name = f'{self.config.hive_database}.{table_name}'
        df.coalesce(num_files) \
            .write \
            .mode(self.config.write_mode) \
            .format('parquet') \
            .option('compression', 'snappy') \
            .saveAsTable(full_table_name)

