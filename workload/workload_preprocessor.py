"""
Workload 预处理模块

实现完整的预处理流程：日志读取 → 聚合 → 安全过滤 → 存储
"""

from typing import List, Tuple
from data_models import InstanceConfig
from ai_config import TrainingParameters
from ai_logger import aiopt_logger
from data_models import ProductType, WorkloadRowAggregated, WorkloadSource
from db_controller import DBController
from controller.workload_controller import WorkloadController
from workload.workload_loader import load_workload, diagnose_data_availability
import db_utils


class WorkloadPreprocessor:
    """Workload 预处理器"""

    def __init__(self, training_controller: DBController):
        """
        :param training_controller: 训练节点连接（用于 routine 列表查询和 digest 计算）
        """
        self.training_controller = training_controller
        self._routine_list: List[str] | None = None
        self.workload_diagnosis: dict | None = None  # CK 数据可用性诊断，用于区分空负载是 CK 无数据还是过滤导致

    @property
    def routine_list(self) -> List[str]:
        """延迟加载 routine 列表（从 training_server 获取）"""
        if self._routine_list is None:
            self._routine_list = db_utils.get_routine_list(self.training_controller)
        return self._routine_list
    
    def preprocess(
        self,
        task_id: str,
        meta_controller: DBController,
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
        workload_set: list[tuple[str, str]] | None = None,
        inst_type: str = "master"
    ) -> int:
        """
        完整预处理流程

        :param task_id: 任务ID
        :param meta_controller: 元数据库连接
        :param workload_source: 数据源类型
        :param product_type: 产品类型 (CDB/NCDB)
        :param cluster_id: 集群 ID (AWR 数据源必需)
        :param instance_id: 实例 ID (来自 upgrade_info，集群标识)
        :param node_uuid: 数据源节点 UUID
        :param region: 区域
        :param min_query_time: 最小查询时间过滤 (秒)
        :param window_days: 时间窗口 (天)
        :param instance_config: 可选，仅用于 mock 数据源 (AWR_TABLE, SLOW_LOG_TABLE)
        :param workload_set: 增量训练过滤条件 [(db, digest), ...]
        :param inst_type: 实例类型 (master/ro)，仅 CDB 使用
        :return: 保存的 workload 数量
        """
        # 0. 诊断 ClickHouse 数据可用性（在查真正负载之前，无条件执行）
        self.workload_diagnosis = self._query_data_availability(
            workload_source, product_type, cluster_id, node_uuid, region,
            instance_id, window_days=window_days, inst_type=inst_type
        )

        # 1. 加载 workload
        aiopt_logger.info(f"[Preprocessor] Loading workload from {workload_source.value}...")
        rows = load_workload(
            workload_source=workload_source,
            product_type=product_type,
            cluster_id=cluster_id,
            instance_id=instance_id,
            node_uuid=node_uuid,
            region=region,
            min_query_time=min_query_time,
            window_days=window_days,
            instance_config=instance_config,
            inst_type=inst_type
        )
        
        if not rows:
            aiopt_logger.warning(f"[Preprocessor] No workload loaded for instance {instance_id}")
            return 0
        
        aiopt_logger.info(f"[Preprocessor] Loaded {len(rows)} workload rows")
        
        # 2. 类型检查 (load_workload 已返回 WorkloadRowAggregated)
        if not isinstance(rows[0], WorkloadRowAggregated):
            aiopt_logger.error(f"[Preprocessor] Unexpected row type: {type(rows[0])}")
            return 0
        
        workload_rows: List[WorkloadRowAggregated] = rows  # type: ignore
        
        # 3. 安全过滤
        safe_rows = self._filter_safe_workloads(workload_rows)
        aiopt_logger.info(f"[Preprocessor] Filtered {len(safe_rows)}/{len(workload_rows)} safe workload rows")
        
        if not safe_rows:
            aiopt_logger.warning(f"[Preprocessor] No safe workload after filtering")
            return 0
        
        # 4. 计算 digest (Slow Log 数据源的 digest 字段为空)
        safe_rows = self._compute_missing_digests(safe_rows)
        
        # 5. workload_set 过滤
        # 
        # workload_set 过滤用于增量训练场景，仅保留指定的 (db, digest) 对应的行。
        # 
        # 由于 Slow Log 数据源在加载时没有 digest 字段（digest 是在上面 step 4 中计算的），
        # 所以 workload_set 过滤统一在这里执行，确保在 digest 计算完成后进行过滤。
        #
        if workload_set:
            safe_rows = self._filter_by_workload_set(safe_rows, workload_set)
        
        # 6. 保存到数据库
        saved_count = WorkloadController.save_workload_batch(
            meta_controller, task_id, safe_rows
        )
        
        return saved_count
    
    def _filter_by_workload_set(
        self,
        rows: List[WorkloadRowAggregated],
        workload_set: list[tuple[str, str]]
    ) -> List[WorkloadRowAggregated]:
        """
        根据 workload_set 过滤 workload 行
        
        用于增量训练场景，仅保留 workload_set 中指定的 (db, digest) 对应的行。
        
        :param rows: workload 行列表
        :param workload_set: 增量训练过滤条件 [(db, digest), ...]
        :return: 过滤后的 workload 行列表
        """
        filter_set = set(workload_set)
        before_count = len(rows)
        result = [row for row in rows if (row.db, row.digest) in filter_set]
        after_count = len(result)
        
        if before_count != after_count:
            aiopt_logger.info(
                f"[Preprocessor] workload_set filter (post-digest): {after_count}/{before_count} rows retained"
            )
        
        return result
    
    def _compute_missing_digests(
        self,
        rows: List[WorkloadRowAggregated],
    ) -> List[WorkloadRowAggregated]:
        """
        为缺少 digest 的行计算 digest

        Slow Log 数据源返回的 digest 为空字符串，需要使用 MySQL 的 statement_digest() 计算。
        使用 training_controller（训练节点连接）执行——statement_digest() 是纯函数，零 IO。
        """
        result = []
        computed_count = 0

        for row in rows:
            if not row.digest:  # 空字符串或 None
                try:
                    digest = db_utils.compute_statement_digest(self.training_controller, row.sql_text)
                    # 创建新对象，更新 digest 字段
                    result.append(row.model_copy(update={"digest": digest}))
                    computed_count += 1
                except Exception as e:
                    aiopt_logger.warning(f"[Preprocessor] Failed to compute digest for SQL: {e}")
                    # 跳过无法计算 digest 的行
                    continue
            else:
                result.append(row)
        
        if computed_count > 0:
            aiopt_logger.info(f"[Preprocessor] Computed digest for {computed_count} rows")
        
        return result
    
    def _query_data_availability(
        self,
        workload_source: WorkloadSource,
        product_type: ProductType,
        cluster_id: int,
        node_uuid: str,
        region: str,
        instance_id: str,
        *,
        window_days: int,
        inst_type: str = "master"
    ) -> dict | None:
        """查询 ClickHouse 中该实例的数据量，用于后续诊断空负载原因。"""
        try:
            diagnosis = diagnose_data_availability(
                workload_source, product_type, cluster_id, node_uuid, region,
                window_days=window_days, inst_type=inst_type
            )
        except Exception as e:
            aiopt_logger.warning(f"[Preprocessor] Workload diagnosis query failed: {e}")
            return None

        if diagnosis is None:
            return None

        result = {
            "rows_in_ck_all_time": diagnosis["total_rows"],
            "rows_in_ck_last_n_days": diagnosis["window_rows"],
            "window_days": window_days,
        }
        aiopt_logger.info(
            f"[Preprocessor] Workload diagnosis: instance={instance_id}, "
            f"rows_in_ck_all_time={diagnosis['total_rows']}, "
            f"rows_in_ck_last_{window_days}_days={diagnosis['window_rows']}"
        )
        return result

    def _filter_safe_workloads(
        self, 
        rows: List[WorkloadRowAggregated]
    ) -> List[WorkloadRowAggregated]:
        """
        安全过滤 - 过滤掉可能损害用户实例的 workload
        
        过滤规则（参考 ai_optimizer.pick_workload_rows）：
        0. 只保留 SELECT 语句（防御性检查）
        1. 过滤带 index hints 的 SQL
        2. 过滤带 index-level optimizer hints 的 SQL
        3. 过滤带 TxsqlAImarker 的 SQL
        4. 过滤带 procedure 调用的 SQL
        5. 过滤带 routine 的 SQL
        6. 过滤超长 SQL
        """
        safe_rows = rows
        
        # 0. 防御性检查：只保留 SELECT 语句
        filtered = []
        for row in safe_rows:
            if not db_utils.is_select_statement(row.sql_text):
                aiopt_logger.debug(f"[Filter] Skip non-SELECT SQL: {repr(row.sql_text)}")
            else:
                filtered.append(row)
        safe_rows = filtered
        
        # 1. 过滤带 index hints 的 SQL
        filtered = []
        for row in safe_rows:
            if db_utils.has_index_hints(row.sql_text):
                aiopt_logger.debug(f"[Filter] Skip SQL with index hints: {repr(row.sql_text)}")
            else:
                filtered.append(row)
        safe_rows = filtered
        
        # 2. 过滤带 index-level optimizer hints 的 SQL
        filtered = []
        for row in safe_rows:
            if db_utils.has_index_level_optimizer_hints(row.sql_text):
                aiopt_logger.debug(f"[Filter] Skip SQL with optimizer hints: {repr(row.sql_text)}")
            else:
                filtered.append(row)
        safe_rows = filtered
        
        # 3. 过滤带 TxsqlAImarker 的 SQL
        filtered = []
        for row in safe_rows:
            if "txsqlaimarker" in row.sql_text.lower():
                aiopt_logger.debug(f"[Filter] Skip SQL with TxsqlAImarker: {repr(row.sql_text)}")
            else:
                filtered.append(row)
        safe_rows = filtered
        
        # 4. 过滤带 procedure 调用的 SQL
        filtered = []
        for row in safe_rows:
            if db_utils.contains_procedure_call(row.sql_text):
                aiopt_logger.debug(f"[Filter] Skip SQL with procedure call: {repr(row.sql_text)}")
            else:
                filtered.append(row)
        safe_rows = filtered
        
        # 5. 过滤带 routine 的 SQL
        filtered = []
        for row in safe_rows:
            if any(routine in row.sql_text for routine in self.routine_list):
                aiopt_logger.debug(f"[Filter] Skip SQL with routine: {repr(row.sql_text)}")
            else:
                filtered.append(row)
        safe_rows = filtered
        
        # 6. 过滤超长 SQL
        max_len = TrainingParameters.max_allowed_sql_length
        filtered = []
        for row in safe_rows:
            if len(row.sql_text) >= max_len:
                aiopt_logger.debug(f"[Filter] Skip SQL exceeding max length ({max_len}): {repr(row.sql_text)}")
            else:
                filtered.append(row)
        safe_rows = filtered
        
        return safe_rows
