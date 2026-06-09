"""
性能监控管控接口

提供从管控获取性能监控节点列表的接口
"""
import requests
import json
from data_models import PerfMonInstanceInfo
from config.config import GlobalConfig

OPERATOR = "aiopt_standalone"
OSS_QUERY_AI_OPTIMIZE_INST_API_URL = "http://127.0.0.1:8080/cdb2/fun_logic/cgi-bin/public_api_20/ncdb_batch_query_inst_ai_optimizer_config.cgi"

def get_perfmon_instances() -> list[PerfMonInstanceInfo]:
    """
    从管控接口获取需要执行性能监控的节点列表

    从 [perfmon] section 读取 cluster_id，作为请求参数传递给管控接口，
    同时返回的实例信息中 cluster_id 字段也使用该配置值。

    :return: 性能监控节点列表
    :raises ValueError: 当请求参数无效或数据解析失败时抛出
    :raises requests.RequestException: 当 HTTP 请求失败时抛出
    """
    cluster_id = GlobalConfig.perfmon_cluster_id
    
    request_data = {
        "operator": OPERATOR,
        "cluster_id": cluster_id
    }

    response = requests.post(
        OSS_QUERY_AI_OPTIMIZE_INST_API_URL,
        data={'data': json.dumps(request_data)},
        headers={'Content-Type': 'application/x-www-form-urlencoded'}
    )

    response.raise_for_status()
    result = response.json()

    if result.get('errno', 0) != 0:
        error_msg = result.get('error', 'Unknown error')
        raise ValueError(f"管控接口返回错误: errno={result.get('errno')}, error={error_msg}")

    return [
        PerfMonInstanceInfo(
            cluster_id=cluster_id,
            product_type=item['product_type'].lower(),
            instance_id=item['instance_id'],
            node_uuid=item['node_uuid'],
            region=item['region'].lower()
        )
        for item in result.get('data', [])
    ]
