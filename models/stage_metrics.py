"""
Job和Stage级别指标数据模型
"""

from dataclasses import dataclass
from typing import Optional


@dataclass
class JobMetrics:
    """Job指标模型"""
    cluster_name: str
    app_id: str
    job_id: int
    submission_time: int
    completion_time: Optional[int]
    duration_ms: int
    status: str
    stage_count: int
    dt: str
    
    def to_dict(self):
        """转换为字典"""
        return {
            'cluster_name': self.cluster_name,
            'app_id': self.app_id,
            'job_id': self.job_id,
            'submission_time': self.submission_time,
            'completion_time': self.completion_time,
            'duration_ms': self.duration_ms,
            'status': self.status,
            'stage_count': self.stage_count,
            'dt': self.dt
        }


@dataclass
class StageMetrics:
    """Stage指标模型"""
    cluster_name: str
    app_id: str
    job_id: int
    stage_id: int
    stage_name: str
    submission_time: int
    completion_time: Optional[int]
    duration_ms: int
    status: str
    input_bytes: int
    input_records: int
    output_bytes: int
    output_records: int
    shuffle_read_bytes: int
    shuffle_read_records: int
    shuffle_write_bytes: int
    shuffle_write_records: int
    num_tasks: int
    num_failed_tasks: int
    task_duration_p50: int
    task_duration_p75: int
    task_duration_p95: int
    task_duration_max: int
    skew_factor: float
    input_skew_factor: float
    peak_memory_max: int
    dt: str
    
    def to_dict(self):
        """转换为字典"""
        return {
            'cluster_name': self.cluster_name,
            'app_id': self.app_id,
            'job_id': self.job_id,
            'stage_id': self.stage_id,
            'stage_name': self.stage_name,
            'submission_time': self.submission_time,
            'completion_time': self.completion_time,
            'duration_ms': self.duration_ms,
            'status': self.status,
            'input_bytes': self.input_bytes,
            'input_records': self.input_records,
            'output_bytes': self.output_bytes,
            'output_records': self.output_records,
            'shuffle_read_bytes': self.shuffle_read_bytes,
            'shuffle_read_records': self.shuffle_read_records,
            'shuffle_write_bytes': self.shuffle_write_bytes,
            'shuffle_write_records': self.shuffle_write_records,
            'num_tasks': self.num_tasks,
            'num_failed_tasks': self.num_failed_tasks,
            'task_duration_p50': self.task_duration_p50,
            'task_duration_p75': self.task_duration_p75,
            'task_duration_p95': self.task_duration_p95,
            'task_duration_max': self.task_duration_max,
            'skew_factor': self.skew_factor,
            'input_skew_factor': self.input_skew_factor,
            'peak_memory_max': self.peak_memory_max,
            'dt': self.dt
        }

