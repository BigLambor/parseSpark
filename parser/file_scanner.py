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
        修复P0问题：添加幂等性保证，过滤已处理的文件
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
        
        files = []
        
        # 方案1: 如果配置了日期子目录 (例如: /spark-logs/2024-01-15/)
        if config.use_date_subdir:
            date_subdir_path = FileScanner._build_date_subdir_path(sc, config)
            if fs.exists(date_subdir_path):
                print(f"发现日期子目录: {date_subdir_path}")
                files = FileScanner._scan_directory(fs, date_subdir_path, config)
        
        # 方案2: 全目录扫描并按日期过滤
        if not files:
            print(f"按修改时间过滤目录: {config.event_log_dir}")
            files = FileScanner._scan_and_filter(fs, log_dir_path, config)
        
        # 修复P0问题：过滤已处理的文件（幂等性保证）
        files = FileScanner.filter_processed_files(spark, files, config)
        
        print(f"扫描完成，找到 {len(files)} 个待处理文件")
        return files
    
    @staticmethod
    def filter_processed_files(spark, file_paths, config):
        """
        过滤已处理的文件（幂等性保证）
        :param spark: SparkSession对象
        :param file_paths: 文件路径列表
        :param config: ParserConfig配置对象
        :return: 过滤后的文件路径列表
        """
        if not file_paths:
            return file_paths
        
        try:
            status_table = f"{config.hive_database}.{config.hive_tables['parser_status']}"
            
            # 检查表是否存在
            try:
                spark.sql(f"SELECT 1 FROM {status_table} LIMIT 1").collect()
            except Exception:
                # 表不存在，返回所有文件
                print(f"状态表 {status_table} 不存在，跳过幂等性检查")
                return file_paths
            
            # 查询已成功处理的文件
            processed_df = spark.sql(f"""
                SELECT DISTINCT file_path 
                FROM {status_table}
                WHERE dt = '{config.target_date}' 
                  AND cluster_name = '{config.cluster_name}'
                  AND status = 'SUCCESS'
            """)
            
            processed_files = {row.file_path for row in processed_df.collect()}
            
            if processed_files:
                print(f"发现 {len(processed_files)} 个已处理文件，将跳过")
            
            filtered_files = [f for f in file_paths if f not in processed_files]
            
            if len(filtered_files) < len(file_paths):
                print(f"过滤后剩余 {len(filtered_files)} 个文件待处理")
            
            return filtered_files
            
        except Exception as e:
            # 查询失败，记录警告但继续处理所有文件
            print(f"警告: 查询已处理文件失败，将处理所有文件: {e}")
            return file_paths
    
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
    def _scan_and_filter(fs, dir_path, config):
        """
        扫描全目录并按文件修改时间过滤
        仅使用Hadoop FileSystem元数据，避免读取文件内容
        """
        from collections import deque
        target_date = datetime.strptime(config.target_date, '%Y-%m-%d').date()
        matched_files = []
        
        queue = deque([dir_path])
        while queue:
            current = queue.popleft()
            try:
                iterator = fs.listStatusIterator(current)
            except Exception as e:
                print(f"列出目录失败: {current}, 错误: {e}")
                continue
            
            while iterator.hasNext():
                status = iterator.next()
                if status.isDirectory():
                    queue.append(status.getPath())
                    continue
                
                path = status.getPath()
                
                # 跳过.inprogress文件
                if config.skip_inprogress and path.getName().endswith('.inprogress'):
                    continue
                
                mod_time = status.getModificationTime() / 1000.0
                file_date = datetime.fromtimestamp(mod_time).date()
                if file_date == target_date:
                    matched_files.append(path.toString())
        
        return matched_files
    
    @staticmethod
    def _build_date_subdir_path(sc, config):
        """根据配置格式构造日期子目录路径"""
        relative = FileScanner._format_target_date(config.target_date, config.date_dir_format)
        return sc._jvm.org.apache.hadoop.fs.Path(config.event_log_dir, relative)
    
    @staticmethod
    def _format_target_date(target_date, pattern):
        """将目标日期按配置格式化"""
        dt = datetime.strptime(target_date, '%Y-%m-%d')
        python_pattern = pattern
        replacements = {
            'yyyy': '%Y',
            'MM': '%m',
            'dd': '%d'
        }
        for key, value in replacements.items():
            python_pattern = python_pattern.replace(key, value)
        return dt.strftime(python_pattern)
    
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

