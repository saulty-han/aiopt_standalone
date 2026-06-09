from typing import cast
from data_models import InstanceConfig
from ai_logger import aiopt_logger
from data_models import ProductType, SlowlogRowAggregated, WorkloadRow, WorkloadRowAggregated, WorkloadSource
from db_utils import is_select_statement
from .slowlog_loader import load_workload_from_slowlog, load_workload_from_slow_log_table, diagnose_slowlog_data_availability
from .awr_loader import load_workload_from_awr, load_workload_from_awr_IS, diagnose_awr_data_availability


def load_workload(
    workload_source: WorkloadSource,
    product_type: ProductType,
    cluster_id: int,
    instance_id: str,
    node_uuid: str,
    region: str,
    *,  # Force keyword-only arguments after this
    min_query_time: float,
    window_days: int,
    instance_config: InstanceConfig | None = None,
    inst_type: str = "master"
) -> list[WorkloadRow] | list[WorkloadRowAggregated] | list[SlowlogRowAggregated]:
    """
    Load workload data from the database.

    :param workload_source: Source of the workload data (e.g., slow log, slow log table, awr)
    :param product_type: Product type (CDB or NCDB)
    :param cluster_id: Cluster ID for AWR table name (required for AWR source)
    :param instance_id: Instance ID from upgrade_info (cluster identifier)
    :param node_uuid: Node UUID for workload filtering
    :param region: Region for the instance
    :param min_query_time: Minimum query time filter (seconds)
    :param window_days: Time window in days
    :param instance_config: Optional, only for mock data sources (AWR_TABLE, SLOW_LOG_TABLE)
    :param inst_type: Instance type for CDB (master/ro), defaults to "master"
    :return: A list of WorkloadRow/WorkloadRowAggregated objects
    :raises ValueError: If mock sources are used but not allowed by config
    """
    from config.config import GlobalConfig

    # Check mock sources permission
    mock_sources = {WorkloadSource.AWR_TABLE, WorkloadSource.SLOW_LOG_TABLE}
    if workload_source in mock_sources and not GlobalConfig.allow_mock_sources:
        raise ValueError(
            f"Mock data source '{workload_source.value}' is not allowed. "
            "Set 'allow_mock_sources = true' in [mock] config section to enable."
        )

    if workload_source == WorkloadSource.SLOW_LOG_TABLE:
        if instance_config is None:
            raise ValueError("instance_config is required for SLOW_LOG_TABLE source")
        rows = load_workload_from_slow_log_table(
            instance_config, min_query_time=min_query_time, window_days=window_days
        )
    elif workload_source == WorkloadSource.SLOW_LOG:
        rows = load_workload_from_slowlog(
            instance_id, region, product_type, node_uuid, inst_type,
            min_query_time=min_query_time, window_days=window_days
        )
    elif workload_source == WorkloadSource.AWR:
        if cluster_id is None:
            raise ValueError("cluster_id is required for AWR source")
        rows = load_workload_from_awr(
            instance_id, region, product_type, cluster_id, node_uuid, inst_type,
            min_query_time=min_query_time, window_days=window_days
        )
    elif workload_source == WorkloadSource.AWR_TABLE:
        # Mock interface for testing, needs instance_config for DB connection
        if instance_config is None:
            raise ValueError("instance_config is required for AWR_TABLE source")
        rows = load_workload_from_awr_IS(
            instance_config, min_query_time=min_query_time, window_days=window_days
        )
    elif workload_source == WorkloadSource.AUDIT_LOG:
        aiopt_logger.warning("AUDIT_LOG workload source is not yet implemented")
        raise NotImplementedError("AUDIT_LOG workload source is not yet implemented")
    else:
        raise ValueError(f"Unsupported workload source: {workload_source}")
    
    # verify if the rows are WorkloadRow or WorkloadRowAggregated
    if all(isinstance(row, WorkloadRow) for row in rows):
        rows = cast(list[WorkloadRow], rows)
        result_rows = [row for row in rows if is_select_statement(row.sql_text)]
        return result_rows
    elif all(isinstance(row, WorkloadRowAggregated) for row in rows):
        rows = cast(list[WorkloadRowAggregated], rows)
        result_rows = [row for row in rows if is_select_statement(row.sql_text)]
        return result_rows
    elif all(isinstance(row, SlowlogRowAggregated) for row in rows):
        rows = cast(list[SlowlogRowAggregated], rows)
        result_rows = [row for row in rows if is_select_statement(row.sql_text)]
        return result_rows
    else:
        raise TypeError("Loaded rows are not of type WorkloadRow or WorkloadRowAggregated.")


def diagnose_data_availability(
    workload_source: WorkloadSource,
    product_type: ProductType,
    cluster_id: int,
    node_uuid: str,
    region: str,
    *,
    window_days: int,
    inst_type: str = "master"
) -> dict | None:
    """
    诊断 ClickHouse 数据可用性。

    仅对 AWR 和 SLOW_LOG 数据源执行，mock 源返回 None。

    :return: {"total_rows": int, "window_rows": int} 或 None（mock 源）
    """
    if workload_source == WorkloadSource.AWR:
        return diagnose_awr_data_availability(
            region, product_type, cluster_id, node_uuid, inst_type,
            window_days=window_days
        )
    elif workload_source == WorkloadSource.SLOW_LOG:
        return diagnose_slowlog_data_availability(
            region, product_type, node_uuid, inst_type,
            window_days=window_days
        )
    else:
        return None