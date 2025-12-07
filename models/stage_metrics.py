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
        """转换为字典，确保类型正确"""
        return {
            'cluster_name': str(self.cluster_name) if self.cluster_name else '',
            'app_id': str(self.app_id) if self.app_id else '',
            'job_id': int(self.job_id) if self.job_id is not None else 0,
            'submission_time': int(self.submission_time) if self.submission_time is not None else None,
            'completion_time': int(self.completion_time) if self.completion_time is not None else None,
            'duration_ms': int(self.duration_ms) if self.duration_ms is not None else 0,
            'status': str(self.status) if self.status else 'UNKNOWN',
            'stage_count': int(self.stage_count) if self.stage_count is not None else 0,
            'dt': str(self.dt) if self.dt else ''
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
        """转换为字典，确保类型正确"""
        return {
            'cluster_name': str(self.cluster_name) if self.cluster_name else '',
            'app_id': str(self.app_id) if self.app_id else '',
            'job_id': int(self.job_id) if self.job_id is not None else 0,
            'stage_id': int(self.stage_id) if self.stage_id is not None else 0,
            'stage_name': str(self.stage_name) if self.stage_name else '',
            'submission_time': int(self.submission_time) if self.submission_time is not None else None,
            'completion_time': int(self.completion_time) if self.completion_time is not None else None,
            'duration_ms': int(self.duration_ms) if self.duration_ms is not None else 0,
            'status': str(self.status) if self.status else 'UNKNOWN',
            'input_bytes': int(self.input_bytes) if self.input_bytes is not None else 0,
            'input_records': int(self.input_records) if self.input_records is not None else 0,
            'output_bytes': int(self.output_bytes) if self.output_bytes is not None else 0,
            'output_records': int(self.output_records) if self.output_records is not None else 0,
            'shuffle_read_bytes': int(self.shuffle_read_bytes) if self.shuffle_read_bytes is not None else 0,
            'shuffle_read_records': int(self.shuffle_read_records) if self.shuffle_read_records is not None else 0,
            'shuffle_write_bytes': int(self.shuffle_write_bytes) if self.shuffle_write_bytes is not None else 0,
            'shuffle_write_records': int(self.shuffle_write_records) if self.shuffle_write_records is not None else 0,
            'num_tasks': int(self.num_tasks) if self.num_tasks is not None else 0,
            'num_failed_tasks': int(self.num_failed_tasks) if self.num_failed_tasks is not None else 0,
            'task_duration_p50': int(self.task_duration_p50) if self.task_duration_p50 is not None else 0,
            'task_duration_p75': int(self.task_duration_p75) if self.task_duration_p75 is not None else 0,
            'task_duration_p95': int(self.task_duration_p95) if self.task_duration_p95 is not None else 0,
            'task_duration_max': int(self.task_duration_max) if self.task_duration_max is not None else 0,
            'skew_factor': float(self.skew_factor) if self.skew_factor is not None else 0.0,
            'input_skew_factor': float(self.input_skew_factor) if self.input_skew_factor is not None else 0.0,
            'peak_memory_max': int(self.peak_memory_max) if self.peak_memory_max is not None else 0,
            'dt': str(self.dt) if self.dt else ''
        }

