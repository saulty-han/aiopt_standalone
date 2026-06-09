"""
训练时间估计模块

基于工作负载特征预估训练时间，用于决定申请 RO/Slave 还是克隆实例

算法设计基于当前代码中的超时和预热逻辑:
- 每个 SQL 执行时: warmup_runs=1 + 实际执行 1 次 = 2 次执行
- 动态超时: min(default_elapsed * 1.5, default_elapsed + 10.0)
- 实际观察: 大部分候选计划因 PlanID/Digest 去重被跳过，有效评估计划数约 3-5 个

估算公式 (保守估计):
  单个 SQL 训练时间 ≈ default_elapsed * (1 + warmup) * (1 + avg_unique_plans)
  总训练时间 = Σ 每个 SQL 的训练时间 * overhead_factor
"""

from data_models import WorkloadRowAggregated


# 估算参数 (基于实际观察调整)
WARMUP_RUNS = 1                  # 预热次数
AVG_UNIQUE_PLANS = 5             # 平均有效候选计划数 (去重后，基于实测约 3-5 个)
OVERHEAD_FACTOR = 1.5            # 额外开销系数 (explain、digest 计算、网络延迟等)
LIGHT_WORKLOAD_THRESHOLD_MINUTES = 20.0  # 轻负载阈值（分钟）


def estimate_training_time(workload_rows: list[WorkloadRowAggregated]) -> float:
    """
    估算训练时间
    
    :param workload_rows: 预处理后的工作负载列表
    :return: 预估训练时间（秒）
    """
    if not workload_rows:
        return 0.0
    
    total_time = 0.0
    
    for row in workload_rows:
        # 默认计划执行时间 (单位: 秒)
        default_elapsed = row.elapsed_time_avg
        
        # 单个 SQL 训练时间估算:
        # - 默认计划评估: (1 + warmup) 次执行
        # - 候选计划评估: avg_unique_plans * (1 + warmup) 次执行
        # - 候选计划执行时间近似默认计划 (大部分不会超时)
        
        runs_per_plan = 1 + WARMUP_RUNS  # = 2
        total_plans = 1 + AVG_UNIQUE_PLANS  # 默认 + 候选
        
        sql_time = default_elapsed * runs_per_plan * total_plans
        total_time += sql_time
    
    return total_time * OVERHEAD_FACTOR


def estimate_training_time_minutes(workload_rows: list[WorkloadRowAggregated]) -> float:
    """估算训练时间（分钟）"""
    return estimate_training_time(workload_rows) / 60.0


def is_light_workload(workload_rows: list[WorkloadRowAggregated]) -> bool:
    """
    判断是否为轻负载
    
    :return: True = 轻负载（< 20 分钟），使用 RO/Slave
             False = 重负载（>= 20 分钟），需要克隆实例
    """
    estimated_minutes = estimate_training_time_minutes(workload_rows)
    return estimated_minutes < LIGHT_WORKLOAD_THRESHOLD_MINUTES

