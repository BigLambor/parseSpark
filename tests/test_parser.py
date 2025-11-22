"""
解析器单元测试
"""

import unittest
import json
from parser.event_parser import ApplicationState, EventLogParser
from parser.metrics_calculator import MetricsCalculator


class TestApplicationState(unittest.TestCase):
    """应用状态管理器测试"""
    
    def setUp(self):
        """初始化测试数据"""
        self.app_state = ApplicationState('test_cluster', '2024-01-15')
    
    def test_app_metrics_conversion(self):
        """测试应用指标转换"""
        self.app_state.app_id = 'application_1234567890_0001'
        self.app_state.app_name = 'TestApp'
        self.app_state.start_time = 1000000
        self.app_state.end_time = 2000000
        self.app_state.user = 'test_user'
        self.app_state.status = 'FINISHED'
        
        metrics = self.app_state.to_app_metrics()
        
        self.assertEqual(metrics.cluster_name, 'test_cluster')
        self.assertEqual(metrics.app_id, 'application_1234567890_0001')
        self.assertEqual(metrics.app_name, 'TestApp')
        self.assertEqual(metrics.duration_ms, 1000000)
        self.assertEqual(metrics.status, 'FINISHED')
        self.assertEqual(metrics.dt, '2024-01-15')
    
    def test_job_metrics_conversion(self):
        """测试Job指标转换"""
        self.app_state.app_id = 'application_1234567890_0001'
        self.app_state.jobs[0] = {
            'submission_time': 1000000,
            'completion_time': 1500000,
            'status': 'SUCCEEDED',
            'stage_ids': [0, 1]
        }
        
        job_metrics_list = self.app_state.to_job_metrics()
        
        self.assertEqual(len(job_metrics_list), 1)
        job = job_metrics_list[0]
        self.assertEqual(job.job_id, 0)
        self.assertEqual(job.duration_ms, 500000)
        self.assertEqual(job.status, 'SUCCEEDED')
        self.assertEqual(job.stage_count, 2)


class TestMetricsCalculator(unittest.TestCase):
    """指标计算器测试"""
    
    def test_percentile(self):
        """测试百分位数计算"""
        values = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
        
        p50 = MetricsCalculator.calculate_percentile(values, 0.5)
        p95 = MetricsCalculator.calculate_percentile(values, 0.95)
        
        self.assertEqual(p50, 5)
        self.assertEqual(p95, 9)
    
    def test_stage_aggregates(self):
        """测试Stage聚合统计"""
        tasks = [
            {'duration_ms': 1000, 'input_bytes': 100, 'peak_memory': 1000000},
            {'duration_ms': 2000, 'input_bytes': 200, 'peak_memory': 2000000},
            {'duration_ms': 3000, 'input_bytes': 300, 'peak_memory': 3000000},
            {'duration_ms': 10000, 'input_bytes': 1000, 'peak_memory': 5000000},  # 倾斜任务
        ]
        
        agg = MetricsCalculator.calculate_stage_aggregates(tasks)
        
        self.assertEqual(agg['task_duration_max'], 10000)
        self.assertGreater(agg['skew_factor'], 1.0)  # 存在倾斜
        self.assertGreater(agg['input_skew_factor'], 1.0)
        self.assertEqual(agg['peak_memory_max'], 5000000)
    
    def test_duration_calculation(self):
        """测试时长计算"""
        duration = MetricsCalculator.calculate_duration(1000000, 2000000)
        self.assertEqual(duration, 1000000)
        
        # 测试空值处理
        duration_none = MetricsCalculator.calculate_duration(1000000, None)
        self.assertEqual(duration_none, 0)


if __name__ == '__main__':
    unittest.main()

