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
    user: str
    spark_version: str
    executor_count: int
    total_cores: int
    total_memory_mb: int
    dt: str
    
    def to_dict(self):
        """转换为字典"""
        return {
            'cluster_name': self.cluster_name,
            'app_id': self.app_id,
            'app_name': self.app_name,
            'start_time': self.start_time,
            'end_time': self.end_time,
            'duration_ms': self.duration_ms,
            'status': self.status,
            'user': self.user,
            'spark_version': self.spark_version,
            'executor_count': self.executor_count,
            'total_cores': self.total_cores,
            'total_memory_mb': self.total_memory_mb,
            'dt': self.dt
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
        """转换为字典"""
        return {
            'cluster_name': self.cluster_name,
            'app_id': self.app_id,
            'executor_id': self.executor_id,
            'host': self.host,
            'add_time': self.add_time,
            'remove_time': self.remove_time,
            'total_cores': self.total_cores,
            'max_memory_mb': self.max_memory_mb,
            'dt': self.dt
        }

