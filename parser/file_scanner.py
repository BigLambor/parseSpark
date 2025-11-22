"""
文件扫描模块 - 使用Spark并行扫描HDFS目录
"""

import os
from datetime import datetime


class FileScanner:
    """EventLog文件扫描器"""
    
    @staticmethod
    def scan(spark, config):
        """
        扫描HDFS目录，获取待解析的文件列表
        :param spark: SparkSession对象
        :param config: ParserConfig配置对象
        :return: 文件路径列表
        """
        print(f"开始扫描HDFS目录: {config.event_log_dir}")
        
        # 获取Hadoop FileSystem
        sc = spark.sparkContext
        fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(
            sc._jsc.hadoopConfiguration()
        )
        
        log_dir_path = sc._jvm.org.apache.hadoop.fs.Path(config.event_log_dir)
        
        # 检查目录是否存在
        if not fs.exists(log_dir_path):
            raise FileNotFoundError(f"EventLog目录不存在: {config.event_log_dir}")
        
        # 方案1: 如果有日期子目录 (例如: /spark-logs/2024-01-15/)
        date_subdir = sc._jvm.org.apache.hadoop.fs.Path(
            config.event_log_dir, 
            config.target_date
        )
        
        if fs.exists(date_subdir):
            print(f"发现日期子目录: {date_subdir}")
            files = FileScanner._scan_directory(fs, date_subdir, config)
        else:
            # 方案2: 全目录扫描并按日期过滤
            print(f"未发现日期子目录，扫描全目录并按日期过滤...")
            files = FileScanner._scan_and_filter(spark, fs, log_dir_path, config)
        
        print(f"扫描完成，找到 {len(files)} 个文件")
        return files
    
    @staticmethod
    def _scan_directory(fs, dir_path, config):
        """扫描指定目录"""
        files = []
        
        try:
            file_statuses = fs.listStatus(dir_path)
            
            for status in file_statuses:
                path = status.getPath().toString()
                
                # 跳过目录
                if status.isDirectory():
                    continue
                
                # 跳过.inprogress文件
                if config.skip_inprogress and path.endswith('.inprogress'):
                    continue
                
                files.append(path)
        
        except Exception as e:
            print(f"扫描目录失败: {dir_path}, 错误: {e}")
        
        return files
    
    @staticmethod
    def _scan_and_filter(spark, fs, dir_path, config):
        """
        扫描全目录并按文件修改时间过滤
        使用 Spark 3.x binaryFile 数据源进行优化扫描
        """
        from pyspark.sql.functions import col, to_date, lit
        
        print(f"使用 Spark binaryFile 数据源扫描目录: {config.event_log_dir}")
        
        try:
            # 使用 binaryFile 读取元数据 (不读取 content)
            # recursiveFileLookup=true: 递归查找并禁用分区推断
            df = spark.read.format("binaryFile") \
                .option("pathGlobFilter", "*") \
                .option("recursiveFileLookup", "true") \
                .load(config.event_log_dir) \
                .select("path", "modificationTime")
            
            # 构建过滤条件
            # 1. 过滤 .inprogress 文件
            if config.skip_inprogress:
                df = df.filter(~col("path").endswith(".inprogress"))
            
            # 2. 按修改时间过滤
            # modificationTime 是 TimestampType
            df = df.filter(to_date(col("modificationTime")) == lit(config.target_date))
            
            # 获取符合条件的文件路径列表
            # 注意：这里只 collect 最终符合条件的路径，大大减少 Driver 内存压力
            print(f"开始执行 Spark SQL 过滤...")
            filtered_files = df.select("path").rdd.flatMap(lambda x: x).collect()
            
            return filtered_files
            
        except Exception as e:
            print(f"Spark binaryFile 扫描失败: {e}")
            print("降级使用 Driver 端单线程扫描...")
            # 如果 binaryFile 失败（例如 Spark 版本过低），回退到手动扫描
            # 但仅扫描第一层以避免 OOM
            return FileScanner._scan_directory(fs, dir_path, config)
    
    @staticmethod
    def _list_all_files(fs, dir_path, max_depth=3, current_depth=0):
        """
        递归列出所有文件
        :param fs: Hadoop FileSystem
        :param dir_path: 目录路径
        :param max_depth: 最大递归深度
        :param current_depth: 当前深度
        :return: 文件路径列表
        """
        files = []
        
        if current_depth > max_depth:
            return files
        
        try:
            file_statuses = fs.listStatus(dir_path)
            
            for status in file_statuses:
                if status.isDirectory():
                    # 递归扫描子目录
                    sub_files = FileScanner._list_all_files(
                        fs, status.getPath(), max_depth, current_depth + 1
                    )
                    files.extend(sub_files)
                else:
                    # 添加文件
                    files.append(status.getPath().toString())
        
        except Exception as e:
            print(f"列出目录失败: {dir_path}, 错误: {e}")
        
        return files
    
    @staticmethod
    def prepare_file_paths_with_size(spark, file_paths):
        """
        准备文件路径和大小信息（用于智能分区）
        :param spark: SparkSession
        :param file_paths: 文件路径列表
        :return: [(path, size), ...] 列表
        """
        sc = spark.sparkContext
        
        def get_size(path_str):
            try:
                fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(
                    sc._jsc.hadoopConfiguration()
                )
                path = sc._jvm.org.apache.hadoop.fs.Path(path_str)
                size = fs.getFileStatus(path).getLen()
                return (path_str, int(size))
            except Exception:
                return (path_str, 0)
        
        # 并行获取文件大小
        paths_rdd = sc.parallelize(file_paths, min(len(file_paths), 500))
        paths_with_size = paths_rdd.map(get_size).collect()
        
        return paths_with_size

