"""
实例管理接口

提供从管控获取实例节点信息的接口。
"""

import requests
import json
from data_models import InstanceInfo, NodeConfig
import time

OPERATOR = "aiopt_standalone"
OSS_QUERY_SET_INFO_API_URL = "http://127.0.0.1:8080/cdb2/fun_logic/cgi-bin/public_api_20/ncdb_query_set_info.cgi"

def _query_master_node_from_api(instance_info: InstanceInfo) -> NodeConfig:
    """
    从管控 API 获取主节点信息（核心实现）

    :param instance_info: 实例信息
    :return: 主节点连接配置
    :raises ValueError: 当请求参数无效或找不到主节点时抛出
    :raises requests.RequestException: 当 HTTP 请求失败时抛出
    """
    request_data = {
        "operator": OPERATOR,
        "cluster_id": instance_info.cluster_id,
        "set_id_list": [instance_info.instance_id],
        "cur_page": 0,
        "per_page": 100
    }

    response = requests.post(
        OSS_QUERY_SET_INFO_API_URL,
        data={'data': json.dumps(request_data)},
        headers={'Content-Type': 'application/x-www-form-urlencoded'}
    )

    response.raise_for_status()
    result = response.json()

    if result.get('errno', 0) != 0:
        error_msg = result.get('error', 'Unknown error')
        raise ValueError(f"管控接口ncdb_query_set_info返回异常: errno={result.get('errno')}, error={error_msg}")

    set_list = result.get('set_list', [])
    if not set_list:
        raise ValueError(f"管控未找到实例信息: instance_id={instance_info.instance_id}, cluster_id={instance_info.cluster_id}")

    node_list = set_list[0].get('node_list', [])
    if not node_list:
        raise ValueError(f"管控实例没有节点信息: instance_id={instance_info.instance_id}")

    master_node = None
    for node in node_list:
        if node.get('role') == 'master':
            master_node = node
            break

    if not master_node:
        raise ValueError(f"管控未找到主节点: instance_id={instance_info.instance_id}")

    node_config = NodeConfig(
        node_ip=master_node.get('node_ip', master_node.get('ip', '')),
        node_port=master_node.get('node_port', master_node.get('port', 0)),
        node_uuid=master_node.get('node_uuid', ''),
        username='tencentroot',
        password=''
    )

    return node_config


def query_master_node_info(instance_info: InstanceInfo, **kwargs) -> NodeConfig:
    """
    实现 - 调用管控 API 获取主节点信息（带重试机制）

    :param instance_info: 实例信息
    :return: 主节点连接配置
    :param **kwargs: 可选参数，包含:
        - max_retries: 最大重试次数（默认为10）
        - retry_interval: 重试间隔秒数（默认为3）
    :raises ValueError: 当请求参数无效或找不到主节点时抛出
    :raises requests.RequestException: 当 HTTP 请求失败时抛出
    """
    max_retries = kwargs.get('max_retries', 10)
    retry_interval = kwargs.get('retry_interval', 3)

    last_error = None
    for attempt in range(max_retries):
        try:
            return _query_master_node_from_api(instance_info)
        except (ValueError, requests.RequestException) as e:
            last_error = e
            if attempt < max_retries - 1:
                time.sleep(retry_interval)

    raise last_error
