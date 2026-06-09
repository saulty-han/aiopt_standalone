"""
Interfaces 模块

管控接口封装，支持 Mock 模式（通过配置控制）
"""
from . import _mock
from ._perfmon import get_perfmon_instances
from ._instance import query_master_node_info
from ai_logger import aiopt_logger


if _mock.is_mocking_enabled():
    aiopt_logger.debug(f"[Interface] query_master_node_info: Mock mode enabled")
    query_master_node_info = _mock.mock_query_master_node_info


__all__ = ["get_perfmon_instances", "query_master_node_info"]