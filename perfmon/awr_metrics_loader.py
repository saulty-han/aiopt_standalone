"""
AWR 性能指标加载器

从 ClickHouse AWR 数据源（snap 快照表）加载性能指标
使用 awr_sql_agg_by_digest_plan_id_snap__ 表，跨多个快照和 plan_id 进行加权聚合
"""
import time
from datetime import datetime
from clickhouse_driver import Client

from ai_logger import perf_logger
from config.config import GetConf, GlobalConfig
from data_models import ProductType
from perfmon.models import PerfMetrics


def load_awr_metrics(
    db: str,
    digest: str,
    start_time: datetime,
    end_time: datetime,
    region: str,
    product_type: ProductType,
    node_uuid: str,
    cluster_id: int
) -> PerfMetrics | None:
    """
    从 AWR ClickHouse 加载指定时间段的性能指标
    
    使用 awr_sql_agg_by_digest_plan_id_snap__ 表（快照+PlanID 表）
    跨多个快照和 plan_id 进行加权聚合：
    - 平均值使用 count_star 作为权重
    - 最大值取所有快照/plan_id 的最大
    - 最小值取所有快照/plan_id 的最小
    
    :param db: 数据库名
    :param digest: SQL 模板的 digest (sql_hash)
    :param start_time: 开始时间
    :param end_time: 结束时间
    :param region: 区域（用于选择 ClickHouse 节点）
    :param product_type: 产品类型（CDB 或 NCDB）
    :param node_uuid: 节点 UUID（CDB: inst_id, NCDB: node_uuid）
    :param cluster_id: AWR cluster ID
    :return: PerfMetrics 对象，如果无数据则返回 None
    """
    if product_type == ProductType.CDB:
        return _load_metrics_cdb(db, digest, start_time, end_time, region, node_uuid, cluster_id)
    else:
        return _load_metrics_ncdb(db, digest, start_time, end_time, region, node_uuid, cluster_id)


def _load_metrics_cdb(
    db: str,
    digest: str,
    start_time: datetime,
    end_time: datetime,
    region: str,
    inst_id: str,
    cluster_id: int,
    inst_type: str = "master"
) -> PerfMetrics | None:
    """
    CDB 实例的 AWR 指标加载
    
    CDB 使用 (inst_id, inst_type) 过滤，与 workload/awr_loader.py 保持一致
    node_uuid 映射到 ClickHouse 的 inst_id 字段
    """
    effective_cluster_id = GlobalConfig.get_effective_ck_cluster_id(region, cluster_id)
    table_name = f"cdblog.awr_sql_agg_by_digest_plan_id_snap__{effective_cluster_id}"
    
    # 跨快照和 plan_id 加权聚合查询
    sql = f"""
        SELECT
            sum(count_star) AS execution_count,
            sum(count_star * elapsed_time_avg) / sum(count_star) / 1000 AS query_time_avg,
            min(elapsed_time_min) / 1000 AS query_time_min,
            max(elapsed_time_max) / 1000 AS query_time_max,
            sum(count_star * rows_scanned_avg) / sum(count_star) AS rows_examined_avg,
            min(rows_scanned_min) AS rows_examined_min,
            max(rows_scanned_max) AS rows_examined_max
        FROM {table_name}
        WHERE inst_id = %(inst_id)s
          AND inst_type = %(inst_type)s
          AND timestamp >= %(start_time)s
          AND timestamp <= %(end_time)s
          AND db = %(db)s
          AND sql_hash = %(digest)s
        GROUP BY db, sql_hash
    """
    
    params = {
        "inst_id": inst_id,
        "inst_type": inst_type,
        "start_time": start_time.strftime('%Y-%m-%d %H:%M:%S'),
        "end_time": end_time.strftime('%Y-%m-%d %H:%M:%S'),
        "db": db,
        "digest": digest
    }
    
    return _execute_metrics_query(sql, params, region)


def _load_metrics_ncdb(
    db: str,
    digest: str,
    start_time: datetime,
    end_time: datetime,
    region: str,
    node_uuid: str,
    cluster_id: int
) -> PerfMetrics | None:
    """
    NCDB 实例的 AWR 指标加载
    
    NCDB 使用 node_uuid 过滤
    """
    effective_cluster_id = GlobalConfig.get_effective_ck_cluster_id(region, cluster_id)
    table_name = f"ncdblog.awr_sql_agg_by_digest_plan_id_snap__{effective_cluster_id}"
    
    # 跨快照和 plan_id 加权聚合查询
    sql = f"""
        SELECT
            sum(count_star) AS execution_count,
            sum(count_star * elapsed_time_avg) / sum(count_star) / 1000 AS query_time_avg,
            min(elapsed_time_min) / 1000 AS query_time_min,
            max(elapsed_time_max) / 1000 AS query_time_max,
            sum(count_star * rows_scanned_avg) / sum(count_star) AS rows_examined_avg,
            min(rows_scanned_min) AS rows_examined_min,
            max(rows_scanned_max) AS rows_examined_max
        FROM {table_name}
        WHERE node_uuid = %(node_uuid)s
          AND timestamp >= %(start_time)s
          AND timestamp <= %(end_time)s
          AND db = %(db)s
          AND sql_hash = %(digest)s
        GROUP BY db, sql_hash
    """
    
    params = {
        "node_uuid": node_uuid,
        "start_time": start_time.strftime('%Y-%m-%d %H:%M:%S'),
        "end_time": end_time.strftime('%Y-%m-%d %H:%M:%S'),
        "db": db,
        "digest": digest
    }
    
    return _execute_metrics_query(sql, params, region)


def _execute_metrics_query(
    sql: str,
    params: dict,
    region: str,
    max_retries: int = 5
) -> PerfMetrics | None:
    """
    执行 AWR 查询并返回 PerfMetrics
    
    包含重试机制（5次重试，递增等待时间）
    """
    client = _get_clickhouse_client(region)
    
    perf_logger.debug(f"[AWR-Metrics] Query: {sql}")
    perf_logger.debug(f"[AWR-Metrics] Params: {params}")
    
    # 重试机制
    for attempt in range(max_retries):
        try:
            result = client.execute(sql, params)
            break
        except Exception as e:
            if attempt == max_retries - 1:
                perf_logger.error(f"[AWR-Metrics] Query failed after {max_retries} retries: {e}")
                raise
            wait_time = (attempt + 1) * 2  # 2, 4, 6, 8 秒
            perf_logger.warning(f"[AWR-Metrics] Query failed (attempt {attempt + 1}): {e}, retrying in {wait_time}s")
            time.sleep(wait_time)
    
    if not result or len(result) == 0:
        perf_logger.debug(f"[AWR-Metrics] No data found for db={params.get('db')}, digest={params.get('digest')}")
        return None
    
    row = result[0]
    return PerfMetrics(
        execution_count=int(row[0]) if row[0] else 0,
        query_time_avg=float(row[1]) if row[1] else 0.0,
        query_time_min=float(row[2]) if row[2] else 0.0,
        query_time_max=float(row[3]) if row[3] else 0.0,
        rows_examined_avg=float(row[4]) if row[4] else 0.0,
        rows_examined_min=float(row[5]) if row[5] else 0.0,
        rows_examined_max=float(row[6]) if row[6] else 0.0
    )


def _get_clickhouse_client(region: str) -> Client:
    """获取 ClickHouse 客户端"""
    if not region:
        raise ValueError("region is required for AWR metrics loading")
    
    host = GetConf("awr_clickhouse", f"{region}_clickhouse_host")
    user = GetConf("awr_clickhouse", "user")
    password = GetConf("awr_clickhouse", "password")
    
    perf_logger.debug(f"[AWR-Metrics] Connecting to ClickHouse: {host}")
    return Client(host, user=user, password=password)

