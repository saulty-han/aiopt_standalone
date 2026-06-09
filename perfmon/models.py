"""
性能对比数据模型

定义性能监控模块使用的数据结构
"""
from dataclasses import dataclass
from datetime import datetime


@dataclass
class PerfMetrics:
    """单个时间段的性能指标"""
    execution_count: int
    query_time_avg: float
    query_time_min: float
    query_time_max: float
    rows_examined_avg: float
    rows_examined_min: float
    rows_examined_max: float
    
    @classmethod
    def zero(cls) -> 'PerfMetrics':
        """创建零值指标（用于缺失语句）"""
        return cls(
            execution_count=0,
            query_time_avg=0.0,
            query_time_min=0.0,
            query_time_max=0.0,
            rows_examined_avg=0.0,
            rows_examined_min=0.0,
            rows_examined_max=0.0
        )


@dataclass
class PeriodMetrics:
    """一个周期的完整指标（包含元数据）"""
    period_index: int
    period_start_time: datetime
    period_end_time: datetime
    task_id: str | None
    operation: str | None
    metrics: PerfMetrics | None
    is_finalized: bool
    best_validation_log_id: int | None = None
