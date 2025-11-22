"""
日期工具类
"""

from datetime import datetime, timedelta


class DateUtils:
    """日期处理工具"""
    
    @staticmethod
    def get_yesterday(date_format='%Y-%m-%d'):
        """获取昨天日期"""
        yesterday = datetime.now() - timedelta(days=1)
        return yesterday.strftime(date_format)
    
    @staticmethod
    def parse_date(date_str):
        """解析日期字符串"""
        try:
            return datetime.strptime(date_str, '%Y-%m-%d')
        except ValueError as e:
            raise ValueError(f"日期格式错误，应为 YYYY-MM-DD: {date_str}") from e
    
    @staticmethod
    def format_date(dt, date_format='%Y-%m-%d'):
        """格式化日期"""
        return dt.strftime(date_format)
    
    @staticmethod
    def timestamp_to_datetime(timestamp_ms):
        """毫秒时间戳转datetime"""
        return datetime.fromtimestamp(timestamp_ms / 1000.0)
    
    @staticmethod
    def is_same_day(timestamp_ms, target_date_str):
        """判断时间戳是否在目标日期"""
        dt = DateUtils.timestamp_to_datetime(timestamp_ms)
        target_dt = DateUtils.parse_date(target_date_str)
        return dt.date() == target_dt.date()
    
    @staticmethod
    def get_date_from_timestamp(timestamp_ms, date_format='%Y-%m-%d'):
        """从时间戳提取日期"""
        dt = DateUtils.timestamp_to_datetime(timestamp_ms)
        return dt.strftime(date_format)

