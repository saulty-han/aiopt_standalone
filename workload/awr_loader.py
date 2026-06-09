from typing import List
from sqlalchemy import text
from data_models import InstanceConfig
from ai_config import TrainingParameters
from ai_exception import WorkloadLoadError
from ai_logger import aiopt_logger
from connection_manager import global_connection_manager
from data_models import ProductType, WorkloadRowAggregated
from clickhouse_driver import Client
from config.config import GetConf, GlobalConfig
from workload.clickhouse_utils import execute_with_retry


def load_workload_from_awr(
    instance_id: str,
    region: str,
    product_type: ProductType,
    cluster_id: int,
    node_uuid: str,
    inst_type: str = "master",
    *,
    min_query_time: float,
    window_days: int
) -> List[WorkloadRowAggregated]:
    """
    Load workload from AWR ClickHouse snap table.

    使用 snap 表按时间窗口聚合，按 (db, sql_hash, plan_id) 分组保留 plan_id 信息。

    :param instance_id: Instance ID for result rows
    :param region: Region for ClickHouse connection
    :param product_type: Product type (CDB or NCDB)
    :param cluster_id: Cluster ID for AWR table name
    :param node_uuid: Node UUID for filtering (CDB: inst_id, NCDB: node_uuid)
    :param inst_type: Instance type for CDB (master/ro), ignored for NCDB
    :param min_query_time: Minimum query time filter (seconds)
    :param window_days: Time window in days
    :return: List of WorkloadRowAggregated objects
    """
    if product_type == ProductType.CDB:
        return _load_workload_from_awr_cdb(
            instance_id, region, cluster_id, node_uuid, inst_type,
            min_query_time=min_query_time, window_days=window_days
        )
    else:
        return _load_workload_from_awr_ncdb(
            instance_id, region, cluster_id, node_uuid,
            min_query_time=min_query_time, window_days=window_days
        )


def _load_workload_from_awr_cdb(
    instance_id: str,
    region: str,
    cluster_id: int,
    node_uuid: str,
    inst_type: str,
    *,
    min_query_time: float,
    window_days: int
) -> List[WorkloadRowAggregated]:
    """
    Load workload from AWR snap table for CDB instances.

    按 (db, sql_hash, plan_id) 分组聚合，保留 plan_id 信息。
    """
    effective_cluster_id = GlobalConfig.get_effective_ck_cluster_id(region, cluster_id)
    table_name = f"cdblog.awr_sql_agg_by_digest_plan_id_snap__{effective_cluster_id}"

    # 使用 WITH 表达式聚合 snap 表，planid_t 作为聚合结果复用原查询结构
    # 注意：使用表别名 t 明确引用源列，避免与聚合别名冲突
    sql = f"""
        WITH planid_t AS (
            SELECT
                t.db,
                t.sql_hash,
                t.plan_id,
                argMin(t.sql_text_example, t.elapsed_time_avg) AS sql_text_example,
                sum(t.count_star) AS count_star_agg,
                sum(t.count_star * t.elapsed_time_avg) / sum(t.count_star) AS elapsed_time_avg_agg,
                min(t.elapsed_time_min) AS elapsed_time_min_agg,
                max(t.elapsed_time_max) AS elapsed_time_max_agg,
                max(t.last_query_start_time) AS last_query_start_time_agg,
                argMin(t.sql_text_min, t.elapsed_time_min) AS sql_text_min,
                argMax(t.sql_text_max, t.elapsed_time_max) AS sql_text_max
            FROM {table_name} AS t
            WHERE t.inst_id = %(inst_id)s
              AND t.inst_type = %(inst_type)s
              AND t.timestamp >= toDate(now()) - {window_days}
              AND t.db NOT IN ('null', 'information_schema', 'mysql', 'performance_schema', 'sys')
              AND length(t.sql_text_example) < %(max_allowed_sql_length)s
            GROUP BY t.db, t.sql_hash, t.plan_id
        )
        SELECT
            db,
            sql_hash,
            plan_id,
            sql_text_example AS sql_text,
            count_star_agg AS count_star,
            elapsed_time_avg_agg / 1000.0 AS elapsed_time_avg,
            elapsed_time_min_agg / 1000.0 AS elapsed_time_min,
            elapsed_time_max_agg / 1000.0 AS elapsed_time_max,
            last_query_start_time_agg / 1000000.0 AS last_start_time,
            sql_text_min,
            sql_text_max
        FROM planid_t
        WHERE elapsed_time_avg_agg / 1000.0 >= %(min_query_time)s
    """

    params = {
        "max_allowed_sql_length": TrainingParameters.max_allowed_sql_length,
        "min_query_time": min_query_time,
        "inst_id": node_uuid,
        "inst_type": inst_type
    }

    aiopt_logger.info(f"[AWR-CDB] Loading workload from snap table, inst_id={node_uuid}, window={window_days}d")
    return _execute_awr_query(instance_id, sql, params, region)


def _load_workload_from_awr_ncdb(
    instance_id: str,
    region: str,
    cluster_id: int,
    node_uuid: str,
    *,
    min_query_time: float,
    window_days: int
) -> List[WorkloadRowAggregated]:
    """
    Load workload from AWR snap table for NCDB instances.

    按 (db, sql_hash, plan_id) 分组聚合，保留 plan_id 信息。
    """
    effective_cluster_id = GlobalConfig.get_effective_ck_cluster_id(region, cluster_id)
    table_name = f"ncdblog.awr_sql_agg_by_digest_plan_id_snap__{effective_cluster_id}"

    # 使用 WITH 表达式聚合 snap 表，planid_t 作为聚合结果复用原查询结构
    # 注意：使用表别名 t 明确引用源列，避免与聚合别名冲突
    sql = f"""
        WITH planid_t AS (
            SELECT
                t.db,
                t.sql_hash,
                t.plan_id,
                argMin(t.sql_text_example, t.elapsed_time_avg) AS sql_text_example,
                sum(t.count_star) AS count_star_agg,
                sum(t.count_star * t.elapsed_time_avg) / sum(t.count_star) AS elapsed_time_avg_agg,
                min(t.elapsed_time_min) AS elapsed_time_min_agg,
                max(t.elapsed_time_max) AS elapsed_time_max_agg,
                max(t.last_query_start_time) AS last_query_start_time_agg,
                argMin(t.sql_text_min, t.elapsed_time_min) AS sql_text_min,
                argMax(t.sql_text_max, t.elapsed_time_max) AS sql_text_max
            FROM {table_name} AS t
            WHERE t.node_uuid = %(node_uuid)s
              AND t.timestamp >= toDate(now()) - {window_days}
              AND t.db NOT IN ('null', 'information_schema', 'mysql', 'performance_schema', 'sys')
              AND length(t.sql_text_example) < %(max_allowed_sql_length)s
            GROUP BY t.db, t.sql_hash, t.plan_id
        )
        SELECT
            db,
            sql_hash,
            plan_id,
            sql_text_example AS sql_text,
            count_star_agg AS count_star,
            elapsed_time_avg_agg / 1000.0 AS elapsed_time_avg,
            elapsed_time_min_agg / 1000.0 AS elapsed_time_min,
            elapsed_time_max_agg / 1000.0 AS elapsed_time_max,
            last_query_start_time_agg / 1000000.0 AS last_start_time,
            sql_text_min,
            sql_text_max
        FROM planid_t
        WHERE elapsed_time_avg_agg / 1000.0 >= %(min_query_time)s
    """

    params = {
        "max_allowed_sql_length": TrainingParameters.max_allowed_sql_length,
        "min_query_time": min_query_time,
        "node_uuid": node_uuid
    }

    aiopt_logger.info(f"[AWR-NCDB] Loading workload from snap table, node_uuid={node_uuid}, window={window_days}d")
    return _execute_awr_query(instance_id, sql, params, region)


def _execute_awr_query(
    instance_id: str,
    sql: str,
    params: dict,
    region: str
) -> List[WorkloadRowAggregated]:
    """
    Execute AWR query on ClickHouse and return results.
    
    :param instance_id: Instance ID for result rows
    :param sql: SQL query to execute
    :param params: Query parameters
    :param region: Region for ClickHouse connection
    :return: List of WorkloadRowAggregated objects
    """
    aiopt_logger.debug(sql)
    aiopt_logger.debug(params)
    
    if not region:
        raise ValueError("region is required for AWR loading")
    
    host = GetConf("awr_clickhouse", "%s_clickhouse_host" % region)
    user = GetConf("awr_clickhouse", "user")
    password = GetConf("awr_clickhouse", "password")
    aiopt_logger.debug("query clickhouse: %s", host)
    
    client = Client(host, user=user, password=password)
    try:
        result = execute_with_retry(client, sql, params, context="[AWR]")
    except Exception as e:
        aiopt_logger.error("query clickhouse failed, %s, err = %s" % (instance_id, e))
        raise WorkloadLoadError(f"Query ClickHouse failed for instance {instance_id}") from e
    
    aiopt_logger.debug(result)
    
    # 字段顺序: db, sql_hash, plan_id, sql_text, count_star, elapsed_time_avg,
    #           elapsed_time_min, elapsed_time_max, last_start_time, sql_text_min, sql_text_max
    result_rows = []
    for row in result:
        result_rows.append(WorkloadRowAggregated.model_validate({
            "instance_id": instance_id,
            "db": row[0],
            "digest": row[1],        # sql_hash
            "plan_id": row[2],
            "sql_text": row[3],
            "count_star": row[4],
            "elapsed_time_avg": row[5],
            "elapsed_time_min": row[6],
            "elapsed_time_max": row[7],
            "last_start_time": row[8],
            "sql_text_min": row[9],
            "sql_text_max": row[10],
        }))
    
    aiopt_logger.info(f"Loaded {len(result_rows)} workload rows from AWR")
    return result_rows


def diagnose_awr_data_availability(
    region: str,
    product_type: ProductType,
    cluster_id: int,
    node_uuid: str,
    inst_type: str = "master",
    *,
    window_days: int
) -> dict:
    """
    诊断 AWR 数据可用性，返回不同条件下的行数统计。

    仅在 load_workload 返回空时调用，用于区分 CK 无数据和查询过滤导致的空结果。

    :return: {"total_rows": int, "window_rows": int}
    """
    effective_cluster_id = GlobalConfig.get_effective_ck_cluster_id(region, cluster_id)

    if product_type == ProductType.CDB:
        table_name = f"cdblog.awr_sql_agg_by_digest_plan_id_snap__{effective_cluster_id}"
        instance_filter = "t.inst_id = %(inst_id)s AND t.inst_type = %(inst_type)s"
        params = {"inst_id": node_uuid, "inst_type": inst_type}
    else:
        table_name = f"ncdblog.awr_sql_agg_by_digest_plan_id_snap__{effective_cluster_id}"
        instance_filter = "t.node_uuid = %(node_uuid)s"
        params = {"node_uuid": node_uuid}

    host = GetConf("awr_clickhouse", "%s_clickhouse_host" % region)
    user = GetConf("awr_clickhouse", "user")
    password = GetConf("awr_clickhouse", "password")
    client = Client(host, user=user, password=password)

    # Level 1: 仅限定实例，无其他约束
    sql_total = f"SELECT count(*) FROM {table_name} AS t WHERE {instance_filter}"
    rows = execute_with_retry(client, sql_total, params, context="[AWR-Diag]")
    total_rows = rows[0][0] if rows else 0

    # Level 2: 实例 + 时间窗口
    sql_window = f"SELECT count(*) FROM {table_name} AS t WHERE {instance_filter} AND t.timestamp >= toDate(now()) - {window_days}"
    rows = execute_with_retry(client, sql_window, params, context="[AWR-Diag]")
    window_rows = rows[0][0] if rows else 0

    return {"total_rows": total_rows, "window_rows": window_rows}


def load_workload_from_awr_IS(
    instance_config: InstanceConfig,
    *,
    min_query_time: float,
    window_days: int
) -> List[WorkloadRowAggregated]:
    """
    Load workload from AWR IS tables (information_schema).

    This is a mock interface for local testing, does not support node_uuid filtering.

    :param instance_config: Configuration for the database instance (required for connection)
    :param min_query_time: Minimum query time filter (seconds)
    :param window_days: Time window in days
    :return: List of WorkloadRowAggregated objects
    """
    import time

    from_start_time = time.time() - window_days * 86400

    # Ensure the AWR table exists
    with global_connection_manager.get_connection(instance_config) as conn:
        if not conn:
            raise ConnectionError(f"Failed to connect to the database with config: {instance_config}")
        result = conn.execute(text("use information_schema"))
        query = text("SHOW TABLES LIKE 'txsql_awr_sql_by_digest_planid'")
        result = conn.execute(query)
        if not result.fetchone():
            raise ValueError("I_S table information_schema.txsql_awr_sql_by_digest_planid table does not exist.")
        query = text("""
            SELECT 
                db, sql_hash, CAST(sql_text AS CHAR), 
                count_star, elapsed_time / 1000.0, elapsed_time / 1000.0, elapsed_time / 1000.0, last_query_start_time / 1000000.0,
                CAST(sql_text_min AS CHAR), CAST(sql_text_max AS CHAR), PLANID
            FROM information_schema.txsql_awr_sql_by_digest_planid
            WHERE db not in ("null", "information_schema", "mysql", "performance_schema", "sys")
            AND length(sql_text) < 65535
            AND last_query_start_time > :from_start_time
            AND elapsed_time / 1000.0 >= :min_query_time
        """)
        result = conn.execute(query, {"min_query_time": min_query_time, "from_start_time": from_start_time})
        rows = result.fetchall()
        result_rows = [
            WorkloadRowAggregated.model_validate({
                "instance_id": instance_config.instance_id,
                "db": row[0],
                "digest": row[1],
                "sql_text": row[2],
                "count_star": row[3],
                "elapsed_time_avg": row[4],
                "elapsed_time_min": row[5],
                "elapsed_time_max": row[6],
                "last_start_time": row[7],
                "sql_text_min": row[8],
                "sql_text_max": row[9],
                "plan_id": row[10],
            })
            for row in rows
        ]
        return result_rows