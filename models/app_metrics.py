"""
应用级别指标数据模型
"""

from dataclasses import dataclass
from typing import Optional


@dataclass
class AppMetrics:
    """应用指标模型"""
    cluster_name: str
    app_id: str
    app_name: str
    start_time: int
    end_time: Optional[int]
    duration_ms: int
    status: str
    app_user: str
    spark_version: str
    executor_count: int
    total_cores: int
    total_memory_mb: int
    dt: str
    
    def to_dict(self):
        """转换为字典，确保类型正确"""
        return {
            'cluster_name': str(self.cluster_name) if self.cluster_name else '',
            'app_id': str(self.app_id) if self.app_id else '',
            'app_name': str(self.app_name) if self.app_name else '',
            'start_time': int(self.start_time) if self.start_time is not None else None,
            'end_time': int(self.end_time) if self.end_time is not None else None,
            'duration_ms': int(self.duration_ms) if self.duration_ms is not None else 0,
            'status': str(self.status) if self.status else 'UNKNOWN',
            'app_user': str(self.app_user) if self.app_user else '',
            'spark_version': str(self.spark_version) if self.spark_version else '',
            'executor_count': int(self.executor_count) if self.executor_count is not None else 0,
            'total_cores': int(self.total_cores) if self.total_cores is not None else 0,
            'total_memory_mb': int(self.total_memory_mb) if self.total_memory_mb is not None else 0,
            'dt': str(self.dt) if self.dt else ''
        }


@dataclass
class ExecutorMetrics:
    """Executor指标模型"""
    cluster_name: str
    app_id: str
    executor_id: str
    host: str
    add_time: int
    remove_time: Optional[int]
    total_cores: int
    max_memory_mb: int
    dt: str
    
    def to_dict(self):
        """转换为字典，确保类型正确"""
        return {
            'cluster_name': str(self.cluster_name) if self.cluster_name else '',
            'app_id': str(self.app_id) if self.app_id else '',
            'executor_id': str(self.executor_id) if self.executor_id else '',
            'host': str(self.host) if self.host else '',
            'add_time': int(self.add_time) if self.add_time is not None else None,
            'remove_time': int(self.remove_time) if self.remove_time is not None else None,
            'total_cores': int(self.total_cores) if self.total_cores is not None else 0,
            'max_memory_mb': int(self.max_memory_mb) if self.max_memory_mb is not None else 0,
            'dt': str(self.dt) if self.dt else ''
        }

