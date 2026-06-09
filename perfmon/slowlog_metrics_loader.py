"""
Slow Log 性能指标加载器

从 ClickHouse Slow Log 数据源加载性能指标，支持 CDB 和 NCDB 两种产品类型。

与 awr_metrics_loader.py 的主要区别：
- 使用 md5 而非 digest (sql_hash) 作为查询键
- 查询 cdblog.slowlog / ncdblog.slowlog 表
- 没有预聚合的 snap 表，直接查询原始日志
"""
import time
from datetime import datetime
from clickhouse_driver import Client

from ai_logger import perf_logger
from config.config import GetConf
from data_models import ProductType
from perfmon.models import PerfMetrics


def load_slowlog_metrics(
    db: str,
    md5: str,
    start_time: datetime,
    end_time: datetime,
    region: str,
    product_type: ProductType,
    node_uuid: str
) -> PerfMetrics | None:
    """
    从 Slow Log ClickHouse 加载指定时间段的性能指标
    
    使用 md5 字段作为查询键（与 AWR 使用 digest/sql_hash 不同）
    
    :param db: 数据库名
    :param md5: SQL 模板的 MD5 哈希
    :param start_time: 开始时间
    :param end_time: 结束时间
    :param region: 区域（用于选择 ClickHouse 节点）
    :param product_type: 产品类型（CDB 或 NCDB）
    :param node_uuid: 节点 UUID（CDB: inst_id, NCDB: node_uuid）
    :return: PerfMetrics 对象，如果无数据则返回 None
    """
    if product_type == ProductType.CDB:
        return _load_metrics_cdb(db, md5, start_time, end_time, region, node_uuid)
    else:
        return _load_metrics_ncdb(db, md5, start_time, end_time, region, node_uuid)


def _load_metrics_cdb(
    db: str,
    md5: str,
    start_time: datetime,
    end_time: datetime,
    region: str,
    inst_id: str,
    inst_type: str = "master"
) -> PerfMetrics | None:
    """
    CDB 实例的 Slow Log 指标加载
    
    CDB 使用 (instid, insttype) 过滤
    node_uuid 映射到 ClickHouse 的 instid 字段
    """
    table_name = "cdblog.slowlog"
    
    # 聚合查询 - 按 md5 聚合
    sql = f"""
        SELECT
            COUNT(*) AS execution_count,
            AVG(query_time) AS query_time_avg,
            MIN(query_time) AS query_time_min,
            MAX(query_time) AS query_time_max,
            AVG(rows_examined) AS rows_examined_avg,
            MIN(rows_examined) AS rows_examined_min,
            MAX(rows_examined) AS rows_examined_max
        FROM {table_name}
        WHERE instid = %(inst_id)s
          AND insttype = %(inst_type)s
          AND timestamp >= %(start_time)s
          AND timestamp <= %(end_time)s
          AND database = %(db)s
          AND md5 = %(md5)s
        GROUP BY database, md5
    """
    
    params = {
        "inst_id": inst_id,
        "inst_type": inst_type,
        "start_time": start_time.strftime('%Y-%m-%d %H:%M:%S'),
        "end_time": end_time.strftime('%Y-%m-%d %H:%M:%S'),
        "db": db,
        "md5": md5
    }
    
    return _execute_metrics_query(sql, params, region)


def _load_metrics_ncdb(
    db: str,
    md5: str,
    start_time: datetime,
    end_time: datetime,
    region: str,
    node_uuid: str
) -> PerfMetrics | None:
    """
    NCDB 实例的 Slow Log 指标加载
    
    NCDB 使用 node_uuid 过滤
    """
    table_name = "ncdblog.slowlog"
    
    # 聚合查询 - 按 md5 聚合
    sql = f"""
        SELECT
            COUNT(*) AS execution_count,
            AVG(query_time) AS query_time_avg,
            MIN(query_time) AS query_time_min,
            MAX(query_time) AS query_time_max,
            AVG(rows_examined) AS rows_examined_avg,
            MIN(rows_examined) AS rows_examined_min,
            MAX(rows_examined) AS rows_examined_max
        FROM {table_name}
        WHERE node_uuid = %(node_uuid)s
          AND timestamp >= %(start_time)s
          AND timestamp <= %(end_time)s
          AND database = %(db)s
          AND md5 = %(md5)s
        GROUP BY database, md5
    """
    
    params = {
        "node_uuid": node_uuid,
        "start_time": start_time.strftime('%Y-%m-%d %H:%M:%S'),
        "end_time": end_time.strftime('%Y-%m-%d %H:%M:%S'),
        "db": db,
        "md5": md5
    }
    
    return _execute_metrics_query(sql, params, region)


def _get_clickhouse_client(region: str) -> Client:
    """获取 Slow Log ClickHouse 客户端"""
    if not region:
        raise ValueError("region is required for Slowlog metrics loading")
    
    host = GetConf("slowlog_clickhouse", f"{region}_clickhouse_host")
    user = GetConf("slowlog_clickhouse", "user")
    password = GetConf("slowlog_clickhouse", "password")
    
    return Client(host, user=user, password=password)


def _execute_metrics_query(
    sql: str,
    params: dict,
    region: str,
    max_retries: int = 5
) -> PerfMetrics | None:
    """执行查询并返回 PerfMetrics"""
    client = _get_clickhouse_client(region)
    
    perf_logger.debug(f"[Slowlog-Metrics] Query: {sql}")
    perf_logger.debug(f"[Slowlog-Metrics] Params: {params}")
    
    result = None
    
    # 重试机制
    for attempt in range(max_retries):
        try:
            result = client.execute(sql, params)
            break
        except Exception as e:
            if attempt == max_retries - 1:
                perf_logger.error(f"[Slowlog-Metrics] Query failed after {max_retries} retries: {e}")
                raise
            wait_time = (attempt + 1) * 2  # 2, 4, 6, 8 秒
            perf_logger.warning(f"[Slowlog-Metrics] Query failed (attempt {attempt + 1}): {e}, retrying in {wait_time}s")
            time.sleep(wait_time)
    
    if not result or len(result) == 0:
        perf_logger.debug(f"[Slowlog-Metrics] No data found for db={params.get('db')}, md5={params.get('md5')}")
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
