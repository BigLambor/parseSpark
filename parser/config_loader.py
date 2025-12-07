"""
配置加载模块
"""

import yaml
import os
from typing import Dict, Any

from parser.config_filter import normalize_filter


class ParserConfig:
    """解析器配置类"""
    
    def __init__(self, config_dict: Dict[str, Any], spark_conf: Dict[str, str]):
        """
        初始化配置
        :param config_dict: YAML配置字典
        :param spark_conf: Spark配置字典
        """
        self.config = config_dict
        self.spark_conf = spark_conf
        
        # 从Spark配置或默认值获取参数
        self.cluster_name = spark_conf.get('spark.app.cluster_name', 
                                           config_dict.get('hdfs', {}).get('default_cluster', 'cluster1'))
        self.target_date = spark_conf.get('spark.app.target_date', None)
        self.skip_inprogress = spark_conf.get('spark.app.skip_inprogress', 'true').lower() == 'true'
        self.parse_tasks = spark_conf.get('spark.app.parse_tasks', 'false').lower() == 'true'
        
        # HDFS配置
        hdfs_config = config_dict.get('hdfs', {})
        clusters = hdfs_config.get('clusters', {})
        cluster_config = clusters.get(self.cluster_name, {})
        
        self.event_log_dir = cluster_config.get('event_log_dir', '/spark-logs')
        self.use_date_subdir = cluster_config.get('use_date_subdir', False)
        self.date_dir_format = cluster_config.get('date_dir_format', 'yyyy-MM-dd')
        
        # Hive配置
        hive_config = config_dict.get('hive', {})
        self.hive_database = hive_config.get('database', 'meta')
        self.hive_metastore_uri = hive_config.get('metastore_uri')
        self.write_mode = hive_config.get('write_mode', 'overwrite')
        self.dynamic_partition = hive_config.get('dynamic_partition', True)
        self.dynamic_partition_mode = hive_config.get('dynamic_partition_mode', 'nonstrict')
        tables_default = {
            'applications': 'spark_applications',
            'jobs': 'spark_jobs',
            'stages': 'spark_stages',
            'executors': 'spark_executors',
            'diagnosis': 'spark_diagnosis',
            'sql_executions': 'spark_sql_executions',
            'spark_configs': 'spark_configs',
            'parser_status': 'spark_parser_status'
        }
        self.hive_tables = {**tables_default, **hive_config.get('tables', {})}
        
        # 解析配置
        parser_config = config_dict.get('parser', {})
        self.scan_mode = parser_config.get('scan_mode', 'mtime')
        self.batch_size = parser_config.get('batch_size', 10000)
        self.num_partitions = int(spark_conf.get('spark.sql.shuffle.partitions', '2000'))

        # Spark 配置过滤
        self.spark_config_filter = normalize_filter(config_dict.get('spark_config_filter'))
        
    def validate(self):
        """验证配置完整性"""
        errors = []
        
        if not self.cluster_name:
            errors.append("缺少集群名称（cluster_name）")
        
        if not self.target_date:
            errors.append("缺少目标日期（target_date）")
        
        if not self.event_log_dir:
            errors.append("缺少EventLog目录配置（event_log_dir）")
        
        if not self.hive_database:
            errors.append("缺少Hive数据库配置（hive_database）")
        
        if errors:
            raise ValueError(f"配置验证失败:\n" + "\n".join(errors))
        
        return True
    
    def __str__(self):
        """打印配置信息"""
        return f"""
=== 解析器配置 ===
集群名称: {self.cluster_name}
目标日期: {self.target_date}
EventLog目录: {self.event_log_dir}
Hive数据库: {self.hive_database}
跳过.inprogress: {self.skip_inprogress}
解析Task级别: {self.parse_tasks}
并行度: {self.num_partitions}
================
"""


class ConfigLoader:
    """配置加载器"""
    
    @staticmethod
    def load(spark_session, config_path=None):
        """
        加载配置
        :param spark_session: SparkSession对象
        :param config_path: 配置文件路径（可选）
        :return: ParserConfig对象
        """
        # 读取Spark配置
        spark_conf = {}
        for key in ['spark.app.cluster_name', 'spark.app.target_date', 
                    'spark.app.config_path', 'spark.app.skip_inprogress',
                    'spark.app.parse_tasks', 'spark.sql.shuffle.partitions']:
            try:
                value = spark_session.conf.get(key)
                spark_conf[key] = value
            except Exception:
                pass
        
        # 确定配置文件路径
        if not config_path:
            config_path = spark_conf.get('spark.app.config_path', './config.yaml')
        
        # 读取YAML配置
        config_dict = ConfigLoader._load_yaml(config_path)
        
        # 创建配置对象
        config = ParserConfig(config_dict, spark_conf)
        
        # 如果没有指定target_date，使用昨天
        if not config.target_date:
            from utils.date_utils import DateUtils
            config.target_date = DateUtils.get_yesterday()
        
        # 验证配置
        config.validate()
        
        return config
    
    @staticmethod
    def _load_yaml(file_path):
        """加载YAML配置文件"""
        # 支持HDFS路径
        if file_path.startswith('hdfs://'):
            # 使用Spark读取HDFS文件
            from pyspark.sql import SparkSession
            spark = SparkSession.builder.getOrCreate()
            
            # 使用Hadoop FileSystem API读取
            sc = spark.sparkContext
            fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(
                sc._jsc.hadoopConfiguration()
            )
            path = sc._jvm.org.apache.hadoop.fs.Path(file_path)
            input_stream = fs.open(path)
            
            try:
                # 逐行读取，兼容Java 8环境
                reader = sc._jvm.java.io.BufferedReader(
                    sc._jvm.java.io.InputStreamReader(input_stream, "UTF-8")
                )
                lines = []
                line = reader.readLine()
                while line is not None:
                    lines.append(line)
                    line = reader.readLine()
                reader.close()
            finally:
                try:
                    input_stream.close()
                except Exception:
                    pass
            
            import io
            yaml_content = "\n".join(lines)
            return yaml.safe_load(io.StringIO(yaml_content))
        else:
            # 本地文件
            if not os.path.exists(file_path):
                raise FileNotFoundError(f"配置文件不存在: {file_path}")
            
            with open(file_path, 'r', encoding='utf-8') as f:
                return yaml.safe_load(f)

