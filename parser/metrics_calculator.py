"""
指标计算模块
"""

import statistics


class MetricsCalculator:
    """指标计算器"""
    
    @staticmethod
    def calculate_percentile(values, percentile):
        """
        计算百分位数
        :param values: 数值列表
        :param percentile: 百分位（0.0-1.0）
        :return: 百分位值
        """
        if not values:
            return 0
        
        sorted_values = sorted(values)
        if len(sorted_values) == 1:
            return sorted_values[0]
        
        index = int((len(sorted_values) - 1) * percentile)
        index = max(0, min(index, len(sorted_values) - 1))
        
        return sorted_values[index]
    
    @staticmethod
    def calculate_stage_aggregates(tasks):
        """
        计算Stage级别的聚合统计
        优化：只排序一次
        :param tasks: Task列表
        :return: 聚合统计字典
        """
        if not tasks:
            return {
                'task_duration_p50': 0,
                'task_duration_p75': 0,
                'task_duration_p95': 0,
                'task_duration_max': 0,
                'skew_factor': 0.0,
                'input_skew_factor': 0.0,
                'peak_memory_max': 0
            }
        
        # 1. 提取并排序时长 (一次排序)
        durations = sorted([t.get('duration_ms', 0) for t in tasks if t.get('duration_ms', 0) > 0])
        count = len(durations)
        
        if count > 0:
            p50 = durations[min(int(count * 0.50), count - 1)]
            p75 = durations[min(int(count * 0.75), count - 1)]
            p95 = durations[min(int(count * 0.95), count - 1)]
            p_max = durations[-1]
        else:
            p50 = p75 = p95 = p_max = 0
            
        # 2. 计算倾斜因子
        skew_factor = (p_max / max(p50, 1)) if p50 > 0 else 0.0
        
        # 3. 计算Input倾斜 (Input bytes通常不需要全排序，只看Max和P50即可，这里简单处理)
        input_bytes = [t.get('input_bytes', 0) for t in tasks if t.get('input_bytes', 0) > 0]
        if input_bytes:
            # 如果Input数据量大，可以考虑采样或只找Max，这里保持排序逻辑但优化调用
            sorted_input = sorted(input_bytes)
            input_len = len(sorted_input)
            input_p50 = sorted_input[min(int(input_len * 0.5), input_len - 1)]
            input_max = sorted_input[-1]
            input_skew_factor = (input_max / max(input_p50, 1)) if input_p50 > 0 else 0.0
        else:
            input_skew_factor = 0.0
            
        # 4. 内存峰值
        peak_memories = [t.get('peak_memory', 0) for t in tasks]
        peak_memory_max = max(peak_memories) if peak_memories else 0
        
        return {
            'task_duration_p50': int(p50),
            'task_duration_p75': int(p75),
            'task_duration_p95': int(p95),
            'task_duration_max': int(p_max),
            'skew_factor': round(skew_factor, 2),
            'input_skew_factor': round(input_skew_factor, 2),
            'peak_memory_max': int(peak_memory_max)
        }
    
    @staticmethod
    def calculate_duration(start_time, end_time):
        """计算时长（毫秒）"""
        if end_time is None or start_time is None:
            return 0
        return max(0, end_time - start_time)
    
    @staticmethod
    def safe_divide(numerator, denominator):
        """安全除法，避免除零"""
        if denominator == 0:
            return 0.0
        return numerator / denominator

