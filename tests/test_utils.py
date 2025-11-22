"""
工具模块单元测试
"""

import unittest
from datetime import datetime, timedelta
from utils.date_utils import DateUtils
from utils.hdfs_utils import HDFSUtils


class TestDateUtils(unittest.TestCase):
    """日期工具测试"""
    
    def test_get_yesterday(self):
        """测试获取昨天日期"""
        yesterday = DateUtils.get_yesterday()
        expected = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        self.assertEqual(yesterday, expected)
    
    def test_parse_date(self):
        """测试日期解析"""
        dt = DateUtils.parse_date('2024-01-15')
        self.assertEqual(dt.year, 2024)
        self.assertEqual(dt.month, 1)
        self.assertEqual(dt.day, 15)
        
        # 测试错误格式
        with self.assertRaises(ValueError):
            DateUtils.parse_date('2024/01/15')
    
    def test_timestamp_conversion(self):
        """测试时间戳转换"""
        timestamp_ms = 1705276800000  # 2024-01-15 00:00:00
        dt = DateUtils.timestamp_to_datetime(timestamp_ms)
        
        self.assertEqual(dt.year, 2024)
        self.assertEqual(dt.month, 1)
        self.assertEqual(dt.day, 15)
    
    def test_get_date_from_timestamp(self):
        """测试从时间戳提取日期"""
        timestamp_ms = 1705276800000  # 2024-01-15
        date_str = DateUtils.get_date_from_timestamp(timestamp_ms)
        self.assertEqual(date_str, '2024-01-15')


class TestHDFSUtils(unittest.TestCase):
    """HDFS工具测试"""
    
    def test_is_inprogress_file(self):
        """测试识别未完成文件"""
        self.assertTrue(HDFSUtils.is_inprogress_file('app_123.inprogress'))
        self.assertFalse(HDFSUtils.is_inprogress_file('app_123'))
    
    def test_extract_app_id_from_path(self):
        """测试提取应用ID"""
        path1 = '/spark-logs/application_1234567890_0001'
        app_id1 = HDFSUtils.extract_app_id_from_path(path1)
        self.assertEqual(app_id1, 'application_1234567890_0001')
        
        path2 = '/spark-logs/application_1234567890_0002.inprogress'
        app_id2 = HDFSUtils.extract_app_id_from_path(path2)
        self.assertEqual(app_id2, 'application_1234567890_0002')
    
    def test_group_files_by_size(self):
        """测试按大小分组"""
        files = [
            ('/path/large1', 100 * 1024 * 1024),  # 100MB
            ('/path/small1', 10 * 1024 * 1024),   # 10MB
            ('/path/large2', 200 * 1024 * 1024),  # 200MB
            ('/path/small2', 5 * 1024 * 1024),    # 5MB
        ]
        
        large, small = HDFSUtils.group_files_by_size(files)
        
        self.assertEqual(len(large), 2)
        self.assertEqual(len(small), 2)


if __name__ == '__main__':
    unittest.main()

