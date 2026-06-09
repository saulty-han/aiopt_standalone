"""
Slow Log Workload Loader

从 ClickHouse Slow Log 数据源加载 workload，支持 CDB 和 NCDB 两种产品类型。

CDB/NCDB 差异：
- CDB: 使用 cdblog.slowlog 表，通过 (instid, insttype) 过滤
- NCDB: 使用 ncdblog.slowlog 表，通过 node_uuid 过滤
"""
from ai_logger import aiopt_logger
import time
from typing import List
from sqlalchemy import text
from data_models import InstanceConfig
from ai_config import TrainingParameters
from ai_exception import WorkloadLoadError
from connection_manager import global_connection_manager
from data_models import ProductType, SlowlogRowAggregated, WorkloadRow, WorkloadRowAggregated
from clickhouse_driver import Client
from config.config import GetConf
from workload.clickhouse_utils import execute_with_retry


def load_workload_from_slowlog(
    instance_id: str,
    region: str,
    product_type: ProductType,
    node_uuid: str,
    inst_type: str = "master",
    *,
    min_query_time: float,
    window_days: int
) -> List[WorkloadRowAggregated]:
    """
    从 Slow Log ClickHouse 加载 workload。

    根据 product_type 路由到 CDB 或 NCDB 实现。

    :param instance_id: 实例 ID (用于结果行)
    :param region: 区域 (用于选择 ClickHouse 节点)
    :param product_type: 产品类型 (CDB 或 NCDB)
    :param node_uuid: 节点 UUID (CDB: inst_id, NCDB: node_uuid)
    :param inst_type: 实例类型 (CDB 专用, master/ro)
    :param min_query_time: 最小查询时间过滤 (秒)
    :param window_days: 时间窗口天数
    :return: WorkloadRowAggregated 列表
    """
    if product_type == ProductType.CDB:
        slowlog_rows = _load_slowlog_cdb(
            instance_id, region, node_uuid, inst_type,
            min_query_time=min_query_time, window_days=window_days
        )
    else:
        slowlog_rows = _load_slowlog_ncdb(
            instance_id, region, node_uuid,
            min_query_time=min_query_time, window_days=window_days
        )
    
    # 转换为 WorkloadRowAggregated (不计算 digest，由下游处理)
    return _convert_slowlog_to_workload(slowlog_rows)


def _load_slowlog_cdb(
    instance_id: str,
    region: str,
    inst_id: str,
    inst_type: str,
    *,
    min_query_time: float,
    window_days: int
) -> List[SlowlogRowAggregated]:
    """
    CDB 实例的 Slow Log 加载

    CDB 使用 (instid, insttype) 过滤
    node_uuid 映射到 ClickHouse 的 instid 字段
    """
    table_name = "cdblog.slowlog"

    sql = f"""
        SELECT
            database,
            argMin(sql_raw_text, query_time) AS sql_text,
            argMin(sql_raw_text, query_time) AS sql_text_min,
            argMax(sql_raw_text, query_time) AS sql_text_max,
            COUNT(*) AS count_star,
            AVG(query_time) AS elapsed_time_avg,
            MIN(query_time) AS elapsed_time_min,
            MAX(query_time) AS elapsed_time_max,
            MAX(start_time) AS last_start_time,
            md5
        FROM {table_name}
        PREWHERE instid = %(inst_id)s AND insttype = %(inst_type)s
        WHERE user_name != 'tencentroot'
          AND timestamp >= toDate(now()) - {window_days}
          AND query_time >= %(min_query_time)s
          AND lower(sql_raw_text) LIKE '%%select%%'
          AND length(sql_raw_text) < %(max_allowed_sql_length)s
        GROUP BY database, md5
    """

    params = {
        "max_allowed_sql_length": TrainingParameters.max_allowed_sql_length,
        "min_query_time": min_query_time,
        "inst_id": inst_id,
        "inst_type": inst_type
    }

    aiopt_logger.info(f"[Slowlog-CDB] Loading workload, instid={inst_id}, window={window_days}d")
    return _execute_slowlog_query(instance_id, sql, params, region)


def _load_slowlog_ncdb(
    instance_id: str,
    region: str,
    node_uuid: str,
    *,
    min_query_time: float,
    window_days: int
) -> List[SlowlogRowAggregated]:
    """
    NCDB 实例的 Slow Log 加载

    NCDB 使用 node_uuid 过滤
    """
    table_name = "ncdblog.slowlog"

    sql = f"""
        SELECT
            database,
            argMin(sql_raw_text, query_time) AS sql_text,
            argMin(sql_raw_text, query_time) AS sql_text_min,
            argMax(sql_raw_text, query_time) AS sql_text_max,
            COUNT(*) AS count_star,
            AVG(query_time) AS elapsed_time_avg,
            MIN(query_time) AS elapsed_time_min,
            MAX(query_time) AS elapsed_time_max,
            MAX(start_time) AS last_start_time,
            md5
        FROM {table_name}
        PREWHERE node_uuid = %(node_uuid)s
        WHERE user_name != 'tencentroot'
          AND timestamp >= toDate(toDateTime(now())) - {window_days}
          AND query_time >= %(min_query_time)s
          AND lower(sql_raw_text) LIKE '%%select%%'
          AND length(sql_raw_text) < %(max_allowed_sql_length)s
        GROUP BY database, md5
    """

    params = {
        "max_allowed_sql_length": TrainingParameters.max_allowed_sql_length,
        "min_query_time": min_query_time,
        "node_uuid": node_uuid
    }

    aiopt_logger.info(f"[Slowlog-NCDB] Loading workload, node_uuid={node_uuid}, window={window_days}d")
    return _execute_slowlog_query(instance_id, sql, params, region)


def _is_oom_error(exc: Exception) -> bool:
    """判断 ClickHouse 异常是否为 MEMORY_LIMIT_EXCEEDED (Code: 241)"""
    return "Code: 241." in str(exc)


_OOM_FALLBACK_SETTINGS = "SETTINGS max_threads = 4, max_block_size = 2048"


def _execute_slowlog_query(
    instance_id: str,
    sql: str,
    params: dict,
    region: str
) -> List[SlowlogRowAggregated]:
    """
    执行 Slow Log 查询并返回结果

    首次以全并发执行（仅 PREWHERE，不限 threads）。
    若触发 MEMORY_LIMIT_EXCEEDED (Code: 241)，追加
    SETTINGS max_threads=4, max_block_size=2048 降级重试一次。
    """
    aiopt_logger.debug(sql)
    aiopt_logger.debug(params)

    if not region:
        raise ValueError("region is required for Slowlog loading")

    host = GetConf("slowlog_clickhouse", f"{region}_clickhouse_host")
    user = GetConf("slowlog_clickhouse", "user")
    password = GetConf("slowlog_clickhouse", "password")
    aiopt_logger.debug("Querying Slowlog ClickHouse: %s", host)

    client = Client(host, user=user, password=password)

    try:
        result = execute_with_retry(client, sql, params, context="[Slowlog]")
    except Exception as e:
        if not _is_oom_error(e):
            aiopt_logger.error("Query Slowlog ClickHouse failed for instance %s: %s", instance_id, e)
            raise WorkloadLoadError(f"Query Slowlog ClickHouse failed for instance {instance_id}") from e
        # OOM: 降级重试
        aiopt_logger.warning(
            "[Slowlog] MEMORY_LIMIT_EXCEEDED for instance %s, retrying with %s",
            instance_id, _OOM_FALLBACK_SETTINGS
        )
        fallback_sql = sql.rstrip().rstrip(";") + "\n        " + _OOM_FALLBACK_SETTINGS
        try:
            result = execute_with_retry(client, fallback_sql, params, context="[Slowlog-OOM-Retry]")
        except Exception as e2:
            aiopt_logger.error("Query Slowlog ClickHouse (OOM retry) failed for instance %s: %s", instance_id, e2)
            raise WorkloadLoadError(f"Query Slowlog ClickHouse failed for instance {instance_id}") from e2

    result_rows = []
    for row in result:
        result_rows.append(SlowlogRowAggregated(
            instance_id=instance_id,
            db=row[0],
            sql_text=row[1],
            sql_text_min=row[2],
            sql_text_max=row[3],
            count_star=row[4],
            elapsed_time_avg=row[5],
            elapsed_time_min=row[6],
            elapsed_time_max=row[7],
            last_start_time=row[8],
            md5=row[9]
        ))

    aiopt_logger.info(f"[Slowlog] Loaded {len(result_rows)} workload rows")
    return result_rows


def diagnose_slowlog_data_availability(
    region: str,
    product_type: ProductType,
    node_uuid: str,
    inst_type: str = "master",
    *,
    window_days: int
) -> dict:
    """
    诊断 Slow Log 数据可用性，返回不同条件下的行数统计。

    仅在 load_workload 返回空时调用，用于区分 CK 无数据和查询过滤导致的空结果。

    :return: {"total_rows": int, "window_rows": int}
    """
    if product_type == ProductType.CDB:
        table_name = "cdblog.slowlog"
        instance_filter = "instid = %(inst_id)s AND insttype = %(inst_type)s"
        params = {"inst_id": node_uuid, "inst_type": inst_type}
    else:
        table_name = "ncdblog.slowlog"
        instance_filter = "node_uuid = %(node_uuid)s"
        params = {"node_uuid": node_uuid}

    host = GetConf("slowlog_clickhouse", f"{region}_clickhouse_host")
    user = GetConf("slowlog_clickhouse", "user")
    password = GetConf("slowlog_clickhouse", "password")
    client = Client(host, user=user, password=password)

    # Level 1: 仅限定实例，无其他约束
    sql_total = f"SELECT count(*) FROM {table_name} WHERE {instance_filter}"
    rows = execute_with_retry(client, sql_total, params, context="[Slowlog-Diag]")
    total_rows = rows[0][0] if rows else 0

    # Level 2: 实例 + 时间窗口
    sql_window = f"SELECT count(*) FROM {table_name} WHERE {instance_filter} AND timestamp >= toDate(now()) - {window_days}"
    rows = execute_with_retry(client, sql_window, params, context="[Slowlog-Diag]")
    window_rows = rows[0][0] if rows else 0

    return {"total_rows": total_rows, "window_rows": window_rows}


def _convert_slowlog_to_workload(
    slowlog_rows: List[SlowlogRowAggregated]
) -> List[WorkloadRowAggregated]:
    """
    将 SlowlogRowAggregated 转换为 WorkloadRowAggregated
    
    注意: 
    - digest 字段留空，由 workload_preprocessor 使用 MySQL 的 statement_digest() 计算
    - md5 字段保留，用于后续性能优化
    - plan_id 为 None (Slow Log 没有 Plan ID 信息)
    """
    result = []
    for row in slowlog_rows:
        result.append(WorkloadRowAggregated(
            instance_id=row.instance_id,
            db=row.db,
            digest="",  # 待下游计算
            sql_text=row.sql_text,
            sql_text_min=row.sql_text_min,
            sql_text_max=row.sql_text_max,
            count_star=row.count_star,
            elapsed_time_avg=row.elapsed_time_avg,
            elapsed_time_min=row.elapsed_time_min,
            elapsed_time_max=row.elapsed_time_max,
            last_start_time=row.last_start_time,
            plan_id=None,  # Slow Log 没有 Plan ID
            md5=row.md5
        ))
    return result



def load_workload_from_slow_log_table(
    instance_config: InstanceConfig,
    *,
    min_query_time: float,
    window_days: int
) -> List[WorkloadRow]:
    """
    Load slow queries from the MySQL slow_log table (直连实例).

    This is a mock interface for testing, queries MySQL slow_log table directly.

    :param instance_config: Configuration for the database instance
    :param min_query_time: Minimum query time filter (seconds)
    :param window_days: Time window in days
    :return: List of WorkloadRow objects
    """
    from_start_time = time.time() - window_days * 86400

    with global_connection_manager.get_connection(instance_config) as conn:
        if not conn:
            raise ConnectionError(f"Failed to connect to the database with config: {instance_config}")
        # Ensure the slow_log table exists
        result = conn.execute(text("use mysql"))
        query = text("SHOW TABLES LIKE 'slow_log'")
        result = conn.execute(query)
        if not result.fetchone():
            raise ValueError("The slow_log table does not exist in the database.")
        query = text("""
            SELECT
                db,
                CAST(sql_text AS CHAR) AS sql_text,
                TIME_TO_SEC(query_time) + MICROSECOND(query_time) / 1000000.0 AS query_time,
                UNIX_TIMESTAMP(start_time)
            FROM mysql.slow_log
            WHERE db != '' and query_time >= :min_query_time and start_time > FROM_UNIXTIME(:from_start_time)
            ORDER BY start_time DESC
        """)
        result = conn.execute(query, {"min_query_time": min_query_time, "from_start_time": from_start_time})
        rows = result.fetchall()
        result_rows = [
            WorkloadRow.model_validate({
                "instance_id": instance_config.instance_id,
                "db": row[0],
                "sql_text": row[1],
                "query_time": row[2],
                "start_time": row[3],
            })
            for row in rows
        ]
        return result_rows
