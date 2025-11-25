#!/usr/bin/env python3
"""
Spark EventLog 解析主程序

使用方式:
    spark-submit \
      --master yarn \
      --deploy-mode cluster \
      --conf spark.app.cluster_name=cluster1 \
      --conf spark.app.target_date=2024-01-15 \
      parse_spark_logs.py
"""

import sys
import time
from pyspark.sql import SparkSession
from parser.config_loader import ConfigLoader
from parser.file_scanner import FileScanner
from parser.event_parser import EventLogParser
from parser.hive_writer import HiveWriter


class ParseStatistics:
    """解析统计信息"""
    
    def __init__(self):
        self.start_time = time.time()
        self.total_files = 0
        self.success_files = 0
        self.failed_files = 0
        self.total_apps = 0
        self.total_jobs = 0
        self.total_stages = 0
        self.total_executors = 0
        self.total_sql_executions = 0
        self.total_spark_configs = 0
        self.failed_file_list = []
        self.success_file_list = []  # 修复P0问题：添加成功文件列表
    
    def record_success(self):
        """记录成功"""
        self.success_files += 1
    
    def record_failure(self, file_path, error):
        """记录失败"""
        self.failed_files += 1
        self.failed_file_list.append((file_path, str(error)))
    
    def get_duration(self):
        """获取执行时长（秒）"""
        return time.time() - self.start_time
    
    def print_summary(self):
        """打印统计摘要"""
        duration = self.get_duration()
        
        print("\n" + "="*60)
        print("解析统计摘要")
        print("="*60)
        print(f"总文件数: {self.total_files}")
        print(f"成功解析: {self.success_files}")
        print(f"解析失败: {self.failed_files}")
        print(f"成功率: {self.success_files / max(self.total_files, 1) * 100:.2f}%")
        print(f"-" * 60)
        print(f"应用数: {self.total_apps}")
        print(f"Job数: {self.total_jobs}")
        print(f"Stage数: {self.total_stages}")
        print(f"Executor数: {self.total_executors}")
        print(f"SQL执行数: {self.total_sql_executions}")
        print(f"Spark配置数: {self.total_spark_configs}")
        print(f"-" * 60)
        print(f"执行时长: {duration:.2f} 秒")
        print(f"平均速度: {self.success_files / max(duration, 1):.2f} 文件/秒")
        print("="*60 + "\n")
        
        # 打印失败文件列表（如果有）
        if self.failed_file_list:
            print("失败文件列表:")
            for file_path, error in self.failed_file_list[:10]:  # 最多显示10个
                print(f"  - {file_path}")
                print(f"    错误: {error}")
            if len(self.failed_file_list) > 10:
                print(f"  ... 还有 {len(self.failed_file_list) - 10} 个失败文件")
            print()


def parse_eventlogs(spark, config):
    """
    解析EventLog文件
    :param spark: SparkSession
    :param config: ParserConfig配置
    :return: 解析结果
    """
    print("\n" + "="*60)
    print("Spark EventLog 解析程序")
    print("="*60)
    print(config)
    
    # 统计信息
    stats = ParseStatistics()
    
    # 1. 扫描文件
    print("步骤 1: 扫描EventLog文件...")
    file_paths = FileScanner.scan(spark, config)
    stats.total_files = len(file_paths)
    
    if stats.total_files == 0:
        print("未找到任何文件，退出")
        return None
    
    print(f"找到 {stats.total_files} 个文件待解析\n")
    
    # 2. 并行解析
    print("步骤 2: 并行解析EventLog...")
    
    # 使用Spark并行化处理文件
    sc = spark.sparkContext
    
    # 计算合适的分区数 - 避免文件少时产生过多空分区
    configured_partitions = max(1, config.num_partitions)
    num_partitions = min(configured_partitions, len(file_paths))
    
    print(f"使用 {num_partitions} 个并行任务处理文件")
    
    # 创建RDD
    files_rdd = sc.parallelize(file_paths, num_partitions)
    
    # 解析函数
    def parse_file_wrapper(file_path):
        """包装解析函数，添加错误处理"""
        try:
            result = EventLogParser.parse_file(
                file_path, 
                config.cluster_name,
                config.target_date,
                config.parse_tasks
            )
            return ('success', file_path, result)
        except Exception as e:
            return ('failed', file_path, str(e))
    
    # 并行解析
    parse_results_rdd = files_rdd.map(parse_file_wrapper)
    
    # 修复: 避免Driver OOM - 分离成功和失败的结果，只collect统计信息
    print("\n步骤 3: 分离并汇总解析结果...")
    
    # 缓存RDD避免重复计算
    parse_results_rdd.persist()
    
    # 分离成功和失败的结果
    success_rdd = parse_results_rdd.filter(lambda x: x[0] == 'success')
    failed_rdd = parse_results_rdd.filter(lambda x: x[0] == 'failed')
    
    # 只collect失败信息（数量少）和统计信息
    failed_list = failed_rdd.map(lambda x: (x[1], x[2])).collect()
    success_count = success_rdd.count()
    failed_count = failed_rdd.count()
    
    # 修复P0问题：收集成功文件列表（仅路径，用于状态记录）
    # 注意：只收集文件路径，不收集解析结果，避免Driver OOM
    success_file_list = success_rdd.map(lambda x: x[1]).collect()
    
    # 记录统计
    stats.success_files = success_count
    stats.failed_files = failed_count
    stats.failed_file_list = failed_list
    stats.success_file_list = success_file_list  # 添加成功文件列表
    
    # 修复: 直接从RDD提取数据并展平，避免在Driver端汇总大数据
    # 提取各类指标并转换为对象列表（仍在RDD中）
    apps_rdd = success_rdd.map(lambda x: x[2]['app'])
    jobs_rdd = success_rdd.flatMap(lambda x: x[2]['jobs'])
    stages_rdd = success_rdd.flatMap(lambda x: x[2]['stages'])
    executors_rdd = success_rdd.flatMap(lambda x: x[2]['executors'])
    sql_executions_rdd = success_rdd.flatMap(lambda x: x[2].get('sql_executions', []))
    spark_configs_rdd = success_rdd.flatMap(lambda x: x[2].get('spark_configs', []))
    
    # 计算统计信息（只collect数量，不collect数据）
    stats.total_apps = apps_rdd.count()
    stats.total_jobs = jobs_rdd.count()
    stats.total_stages = stages_rdd.count()
    stats.total_executors = executors_rdd.count()
    stats.total_sql_executions = sql_executions_rdd.count()
    stats.total_spark_configs = spark_configs_rdd.count()
    
    # 打印统计
    stats.print_summary()
    
    # 修复: 返回RDD而不是列表，让后续处理直接使用RDD转DataFrame
    return {
        'applications_rdd': apps_rdd,
        'jobs_rdd': jobs_rdd,
        'stages_rdd': stages_rdd,
        'executors_rdd': executors_rdd,
        'sql_executions_rdd': sql_executions_rdd,
        'spark_configs_rdd': spark_configs_rdd,
        'statistics': stats,
        'parse_results_rdd': parse_results_rdd  # 保留原始RDD以便需要时使用
    }


def main():
    """主函数"""
    # 创建SparkSession
    spark = SparkSession.builder \
        .appName("SparkEventLogParser") \
        .enableHiveSupport() \
        .getOrCreate()
    
    try:
        # 加载配置
        config = ConfigLoader.load(spark)
        
        # 解析EventLog
        parse_results = parse_eventlogs(spark, config)
        
        if parse_results is None:
            print("解析失败，退出")
            sys.exit(1)
        
        # 写入Hive
        writer = HiveWriter(spark, config)
        writer.write_all(parse_results)
        
        # 修复P0问题：写入解析状态表（幂等性保证）
        writer.write_parser_status(parse_results)
        
        # 获取统计信息
        stats = parse_results['statistics']
        
        # 判断是否成功
        if stats.failed_files > stats.total_files * 0.1:
            print(f"警告: 失败率过高 ({stats.failed_files}/{stats.total_files})")
            sys.exit(1)
        
        print("程序执行成功!")
        sys.exit(0)
    
    except Exception as e:
        print(f"程序执行失败: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    
    finally:
        spark.stop()


if __name__ == '__main__':
    main()

