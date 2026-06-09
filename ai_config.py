# ========== TrainingParameters with lazy GlobalConfig loading ==========

# Lazy import to avoid circular dependency
_GlobalConfig = None

def _get_global_config():
    global _GlobalConfig
    if _GlobalConfig is None:
        from config.config import GlobalConfig
        _GlobalConfig = GlobalConfig
    return _GlobalConfig


class classproperty:
    """Descriptor for class-level properties with lazy loading."""
    def __init__(self, func):
        self.func = func
    
    def __get__(self, obj, objtype=None):
        _get_global_config()
        return self.func(objtype)


class TrainingParameters:
    """
    Training parameters configuration.
    
    NOTE: This class proxies to GlobalConfig for backward compatibility.
    Values are now loaded from TOML configuration file.
    """
    
    @classproperty
    def max_allowed_sql_length(cls) -> int:
        return _GlobalConfig.max_allowed_sql_length
    
    @classproperty
    def index_hints_enumeration_limit(cls) -> int:
        return _GlobalConfig.index_hints_enumeration_limit
    
    @classproperty
    def with_ignore_index_hints(cls) -> bool:
        return _GlobalConfig.with_ignore_index_hints
    
    @classproperty
    def default_plan_timeout_seconds(cls) -> float:
        return _GlobalConfig.default_plan_timeout_seconds

    @classproperty
    def better_plan_ratio(cls) -> float:
        return _GlobalConfig.better_plan_ratio
    
    @classproperty
    def feedback_timeout_rate(cls) -> float:
        return _GlobalConfig.feedback_timeout_rate


class MCTSConfig:
    """
    MCTS configuration.
    
    NOTE: This class proxies to GlobalConfig for backward compatibility.
    Values are now loaded from TOML configuration file.
    """
    
    @classproperty
    def custom_cfg(cls) -> str:
        return _GlobalConfig.mcts_custom_cfg
    
    @classproperty
    def output_dir(cls) -> str:
        return _GlobalConfig.mcts_output_dir
    
    @classproperty
    def llm_api_url_key(cls) -> list:
        """LLM API 资源池配置：三元组列表 [[url, key, model], ...]"""
        return _GlobalConfig.mcts_llm_api_url_key
    
    @classproperty
    def iterations(cls) -> int:
        """MCTS 搜索迭代次数（rollout 轮数）"""
        return _GlobalConfig.mcts_iterations

    @classproperty
    def explain_timeout_seconds(cls) -> float:
        """EXPLAIN（仅取 plan digest，非 EXPLAIN ANALYZE）的墙钟超时（秒），默认 30。"""
        return _GlobalConfig.mcts_explain_timeout_seconds

    @classproperty
    def stop_mcts_search_plan_time_threshold_seconds(cls) -> float:
        """MCTS 计划时间提前结束阈值（秒），默认 0.1；<=0 关闭。"""
        return _GlobalConfig.mcts_stop_mcts_search_plan_time_threshold_seconds

    @classproperty
    def stop_mcts_search_estimated_tokens_budget(cls) -> int:
        """MCTS 预估 tokens 预算（整数）；0 表示不限制。"""
        return _GlobalConfig.mcts_stop_mcts_search_estimated_tokens_budget