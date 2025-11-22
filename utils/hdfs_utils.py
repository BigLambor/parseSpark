"""
HDFS工具类
"""

import os
from typing import List


class HDFSUtils:
    """HDFS操作工具"""
    
    @staticmethod
    def get_file_size(fs, path):
        """获取文件大小"""
        try:
            status = fs.getFileStatus(path)
            return status.getLen()
        except Exception:
            return 0
    
    @staticmethod
    def is_inprogress_file(file_path):
        """判断是否是未完成的日志文件"""
        return file_path.endswith('.inprogress')
    
    @staticmethod
    def extract_app_id_from_path(file_path):
        """从文件路径提取应用ID"""
        # EventLog文件名通常为: application_<timestamp>_<appId>
        filename = os.path.basename(file_path)
        
        # 移除.inprogress后缀
        if filename.endswith('.inprogress'):
            filename = filename[:-len('.inprogress')]
        
        # 提取app_id (通常是文件名本身或最后一部分)
        if filename.startswith('application_'):
            return filename
        
        return filename
    
    @staticmethod
    def filter_files_by_date(file_paths, target_date, date_getter):
        """按日期过滤文件列表"""
        filtered = []
        for path in file_paths:
            try:
                file_date = date_getter(path)
                if file_date == target_date:
                    filtered.append(path)
            except Exception:
                # 无法获取日期，跳过
                continue
        return filtered
    
    @staticmethod
    def group_files_by_size(file_paths_with_size, large_file_threshold=50*1024*1024):
        """将文件按大小分组（大文件/小文件）"""
        large_files = []
        small_files = []
        
        for path, size in file_paths_with_size:
            if size > large_file_threshold:
                large_files.append((path, size))
            else:
                small_files.append((path, size))
        
        return large_files, small_files

