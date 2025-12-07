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
        """转换为字典，确保类型正确"""
        return {
            'cluster_name': str(self.cluster_name) if self.cluster_name else '',
            'app_id': str(self.app_id) if self.app_id else '',
            'execution_id': int(self.execution_id) if self.execution_id is not None else 0,
            'sql_text': str(self.sql_text) if self.sql_text else '',
            'description': str(self.description) if self.description else None,
            'physical_plan_description': str(self.physical_plan_description) if self.physical_plan_description else None,
            'start_time': int(self.start_time) if self.start_time is not None else None,
            'end_time': int(self.end_time) if self.end_time is not None else None,
            'duration_ms': int(self.duration_ms) if self.duration_ms is not None else 0,
            'job_ids': str(self.job_ids) if self.job_ids else '[]',
            'status': str(self.status) if self.status else 'UNKNOWN',
            'error_message': str(self.error_message) if self.error_message else None,
            'dt': str(self.dt) if self.dt else ''
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
        """转换为字典，确保类型正确"""
        return {
            'cluster_name': str(self.cluster_name) if self.cluster_name else '',
            'app_id': str(self.app_id) if self.app_id else '',
            'config_key': str(self.config_key) if self.config_key else '',
            'config_value': str(self.config_value) if self.config_value else '',
            'config_category': str(self.config_category) if self.config_category else '',
            'dt': str(self.dt) if self.dt else ''
        }

