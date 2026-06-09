# coding:utf-8
"""
Configuration access layer.

Provides GetConf function and GlobalConfig class for accessing configuration.
Underlying implementation uses TomlConfig for TOML-based configuration.
"""
import os
from .toml_config import TomlConfig
from data_models import InstanceConfig


def generate_meta_server_config() -> InstanceConfig:
    """
    Generate the meta server configuration from TOML config.
    
    Moved from interfaces/_task.py during refactoring.
    """
    meta_server_ip = TomlConfig.get_instance().get("meta_server", "ip")
    meta_server_port = TomlConfig.get_instance().get("meta_server", "port")
    meta_server_user = TomlConfig.get_instance().get("meta_server", "user")
    meta_server_pass = TomlConfig.get_instance().get("meta_server", "password")
    return InstanceConfig.model_validate({
        "instance_id": "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
        "ip": meta_server_ip,
        "port": meta_server_port,
        "user": meta_server_user,
        "password": meta_server_pass,
        "read_only": False,
        "with_ai_marker": False,
        "allow_reconnect": True,
        "is_meta_server": True
    })

def GetConf(section: str, key: str) -> str:
    """
    Get a configuration value as string.
    
    Used for backward compatibility where string values are expected
    (e.g., URL construction, string concatenation).
    
    Args:
        section: The configuration section (e.g., "meta_server", "scheduler")
        key: The key within the section
        
    Returns:
        The configuration value as a string
    """
    return str(TomlConfig.get_instance().get(section, key))



class GlobalConfig:
    """
    Global configuration singleton.
    
    Provides centralized access to all configuration values including:
    - Original config values (meta_server, scheduler, etc.)
    - Training parameters (migrated from TrainingParameters class)
    """

    # ========== Logger Config ==========

    @classmethod
    @property
    def log_dir(cls) -> str:
        """Log directory."""
        return TomlConfig.get_instance().get("logger", "log_dir") or "logs"

    @classmethod
    @property
    def log_level(cls) -> str:
        """Log level."""
        return TomlConfig.get_instance().get("logger", "log_level") or "DEBUG"
    
    @classmethod
    @property
    def log_max_bytes(cls) -> int:
        """Maximum bytes per log file."""
        return int(TomlConfig.get_instance().get("logger", "max_bytes") or 10485760)

    @classmethod
    @property
    def log_backup_count(cls) -> int:
        """Number of backup log files to keep."""
        return int(TomlConfig.get_instance().get("logger", "backup_count") or 10)
    
    # ========== Original Config Properties ==========
    
    @classmethod
    @property
    def ai_metadata_database(cls) -> str:
        return GetConf("meta_server", "database")
    
    # ========== Training Parameters (migrated from ai_config.py) ==========
    
    @classmethod
    @property
    def max_allowed_sql_length(cls) -> int:
        return TomlConfig.get_instance().get("training", "max_allowed_sql_length")
    
    @classmethod
    @property
    def index_hints_enumeration_limit(cls) -> int:
        return TomlConfig.get_instance().get("training", "index_hints_enumeration_limit")
    
    @classmethod
    @property
    def with_ignore_index_hints(cls) -> bool:
        return TomlConfig.get_instance().get("training", "with_ignore_index_hints")
    
    @classmethod
    @property
    def default_plan_timeout_seconds(cls) -> float:
        """默认执行计划评估超时（秒）。超时则跳过该 SQL 模板，生成 DEFAULT 决策。"""
        return TomlConfig.get_instance().get("training", "default_plan_timeout_seconds")

    @classmethod
    @property
    def better_plan_ratio(cls) -> float:
        return TomlConfig.get_instance().get("training", "better_plan_ratio")
    
    
    @classmethod
    @property
    def feedback_timeout_rate(cls) -> float:
        return TomlConfig.get_instance().get("training", "feedback_timeout_rate")

    @classmethod
    @property
    def optimizer_type(cls) -> str:
        """
        Optimizer type: 'small_model' or 'llm' etc.
        """
        return TomlConfig.get_instance().get("optimizer", "optimizer_type")

    @classmethod
    @property
    def parallel_workers(cls) -> int:
        """
        Number of parallel workers for SQL template optimization.

        - 1: Sequential execution (default, backward compatible)
        - 2-16: Parallel execution with process pool
        """
        value = TomlConfig.get_instance().get("training", "parallel_workers")
        if value is None:
            return 1
        return max(1, min(16, int(value)))

    @classmethod
    @property
    def sql_template_limit_per_task(cls) -> int:
        """单次任务最大训练 SQL 模板数。0 表示不限制。"""
        return max(0, int(TomlConfig.get_instance().get("training", "sql_template_limit_per_task")))

    # ========== Workload Config ==========

    @classmethod
    @property
    def workload_window_days(cls) -> int:
        """Time window in days for slowlog/AWR workload loading."""
        return TomlConfig.get_instance().get("workload", "window_days")

    @classmethod
    @property
    def workload_min_query_time(cls) -> float:
        """Minimum query time in seconds for workload filtering."""
        return TomlConfig.get_instance().get("workload", "min_query_time")

    @classmethod
    @property
    def allow_mock_sources(cls) -> bool:
        """Whether to allow mock data sources (AWR_TABLE, SLOW_LOG_TABLE). Should be false in production."""
        try:
            return TomlConfig.get_instance().get("mock", "allow_mock_sources")
        except (KeyError, TypeError):
            return False  # Default to false if [mock] section doesn't exist

    @classmethod
    @property
    def test_ck_cluster_id_override(cls) -> int | None:
        """
        Override ClickHouse cluster ID for test region.
        
        If set, this value will be used instead of the cluster_id from instance_info
        when querying ClickHouse in test region. Useful for testing with a specific cluster.
        
        :return: The override cluster ID, or None if not set
        """
        try:
            value = TomlConfig.get_instance().get("mock", "test_ck_cluster_id_override")
            return int(value) if value is not None else None
        except (KeyError, TypeError):
            return None  # Default to None if not configured

    @staticmethod
    def get_effective_ck_cluster_id(region: str, cluster_id: int) -> int:
        """
        Get effective ClickHouse cluster ID, with test region override support.
        
        :param region: The region string
        :param cluster_id: The original cluster ID
        :return: The effective cluster ID to use for ClickHouse queries
        """
        if region == 'test':
            ck_cluster_id_override = GlobalConfig.test_ck_cluster_id_override
            if ck_cluster_id_override is not None:
                return ck_cluster_id_override
        return cluster_id

    @classmethod
    @property
    def perfmon_cluster_id(cls) -> int:
        """
        Cluster ID for performance monitoring.
        
        Used to identify the cluster in performance monitoring.
        """
        return int(TomlConfig.get_instance().get("perfmon", "cluster_id"))

    @classmethod
    @property
    def force_awr_perfmon(cls) -> bool:
        """
        Whether to force using AWR for performance monitoring regardless of workload source.
        Useful when slow log is used for training but AWR is available and preferred for monitoring.
        """
        return TomlConfig.get_instance().get("perfmon", "force_awr_perfmon")
    
    @classmethod
    @property
    def mcts_custom_cfg(cls) -> str:
        """Path to MCTS YAML configuration file."""
        cfg_path = TomlConfig.get_instance().get("mcts", "custom_cfg")
        
        # Convert relative path to absolute path based on project root
        if not os.path.isabs(cfg_path):
            # Get project root (same logic as TomlConfig)
            current_dir = os.path.dirname(os.path.abspath(__file__))
            project_root = os.path.dirname(current_dir)
            cfg_path = os.path.join(project_root, cfg_path)
        
        return os.path.normpath(cfg_path)
    
    @classmethod
    @property
    def mcts_llm_api_url_key(cls) -> list:
        """LLM API 资源池配置：三元组列表 [[url, key, model], ...]"""
        return TomlConfig.get_instance().get("mcts", "llm_api_url_key")
    
    @classmethod
    @property
    def mcts_iterations(cls) -> int:
        """MCTS 搜索迭代次数（rollout 轮数）"""
        return int(TomlConfig.get_instance().get("mcts", "iterations"))

    @classmethod
    @property
    def mcts_explain_timeout_seconds(cls) -> float:
        """EXPLAIN（仅取 plan digest，非 EXPLAIN ANALYZE）的墙钟超时（秒）。"""
        return float(TomlConfig.get_instance().get("mcts", "explain_timeout_seconds"))

    @classmethod
    @property
    def mcts_stop_mcts_search_plan_time_threshold_seconds(cls) -> float:
        """MCTS 提前结束：计划时间阈值（秒）；<=0 表示关闭。"""
        return float(TomlConfig.get_instance().get("mcts", "stop_mcts_search_plan_time_threshold_seconds"))

    @classmethod
    @property
    def mcts_stop_mcts_search_estimated_tokens_budget(cls) -> int:
        """MCTS 提前结束：预估 tokens 预算（整数）；0 表示不限制。"""
        return int(TomlConfig.get_instance().get("mcts", "stop_mcts_search_estimated_tokens_budget"))
    
    @classmethod
    @property
    def mcts_output_dir(cls) -> str:
        """Directory to save MCTS output JSON files."""
        output_dir = TomlConfig.get_instance().get("mcts", "output_dir")
        
        # Empty string means disabled
        if not output_dir:
            return ""
        
        # Convert relative path to absolute path based on project root
        if not os.path.isabs(output_dir):
            # Get project root (same logic as TomlConfig)
            current_dir = os.path.dirname(os.path.abspath(__file__))
            project_root = os.path.dirname(current_dir)
            output_dir = os.path.join(project_root, output_dir)
        
        return os.path.normpath(output_dir)
    
    # ========== Config Management ==========
    
    @classmethod
    def reload(cls, config_path: str = None) -> None:
        """
        Reload configuration from file.
        
        Args:
            config_path: Optional path to config file. If None, reloads from current path.
        """
        TomlConfig.reload(config_path)
