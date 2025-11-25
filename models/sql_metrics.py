"""
SQL和配置指标数据模型
"""

from dataclasses import dataclass
from typing import Optional, Dict


@dataclass
class SQLMetrics:
    """SQL执行指标模型"""
    cluster_name: str
    app_id: str
    execution_id: int
    sql_text: str
    description: Optional[str]
    physical_plan_description: Optional[str]
    start_time: int
    end_time: Optional[int]
    duration_ms: int
    job_ids: str  # JSON数组字符串，存储关联的Job IDs
    status: str  # SUCCEEDED, FAILED
    error_message: Optional[str]
    dt: str
    
    def to_dict(self):
        """转换为字典"""
        return {
            'cluster_name': self.cluster_name,
            'app_id': self.app_id,
            'execution_id': self.execution_id,
            'sql_text': self.sql_text,
            'description': self.description,
            'physical_plan_description': self.physical_plan_description,
            'start_time': self.start_time,
            'end_time': self.end_time,
            'duration_ms': self.duration_ms,
            'job_ids': self.job_ids,
            'status': self.status,
            'error_message': self.error_message,
            'dt': self.dt
        }


@dataclass
class SparkConfigMetrics:
    """Spark配置参数模型"""
    cluster_name: str
    app_id: str
    config_key: str
    config_value: str
    config_category: str  # spark, system, java, classpath等
    dt: str
    
    def to_dict(self):
        """转换为字典"""
        return {
            'cluster_name': self.cluster_name,
            'app_id': self.app_id,
            'config_key': self.config_key,
            'config_value': self.config_value,
            'config_category': self.config_category,
            'dt': self.dt
        }

