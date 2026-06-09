"""
Mock 接口实现模块

当配置 [mock] interface_mocking_enabled = true 时，提供模拟的管控接口实现。
生产环境应设置为 false 以使用真实接口。
"""

from data_models import InstanceInfo, NodeConfig
from config.config import TomlConfig


def is_mocking_enabled() -> bool:
    """检查是否启用 Mock 模式"""
    try:
        return TomlConfig.get_instance().get("mock", "interface_mocking_enabled")
    except:
        return False


# =============================================================================
# Mock 实现
# =============================================================================

def mock_query_master_node_info(instance_info: InstanceInfo, **kwargs) -> NodeConfig:
    """
    Mock 实现 - 获取实例的最新主节点连接信息
    
    生产环境需要对接管控 API。Mock 模式下抛出 NotImplementedError，
    需要测试代码 patch 此函数或提供测试配置。
    
    :param instance_info: 实例信息
    :param **kwargs: 可选参数，用于 Mock 测试
    :return: 主节点连接配置
    """
    if "mocked_master_node" in kwargs:
        return kwargs["mocked_master_node"]
    else:
        raise NotImplementedError(
            f"Mock query_master_node_info: 请在测试代码中 patch 此函数，"
            f"或提供测试配置。instance_id={instance_info.instance_id}"
        )
