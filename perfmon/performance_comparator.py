"""
性能对比核心逻辑

支持多轮规则变更的性能对比
"""
from datetime import datetime, timedelta
from collections import defaultdict

from sqlalchemy import text, bindparam
from config.config import GlobalConfig
from ai_logger import perf_logger
from db_controller import DBController
from data_models import PerfMonInstanceInfo

from controller.rule_state_controller import RuleStateController, RuleStateChange
from controller.perf_metrics_controller import PerfMetricsController

from perfmon.models import PerfMetrics, PeriodMetrics
from perfmon.awr_metrics_loader import load_awr_metrics


class PerformanceComparator:
    """性能对比器 - 支持多轮规则变更和历史指标持久化"""
    
    def __init__(self, meta_controller: DBController):
        self.meta_controller = meta_controller
    
    def update_instance_metrics(self, instance_info: PerfMonInstanceInfo) -> tuple[int, int]:
        """
        定时任务入口：更新实例的性能指标

        流程:
        1. 获取所有规则变更历史（按时间排序）
        2. 按 (db, digest) 分组，对每个 SQL 模板识别所有周期
        3. 对于需要更新的周期，从 AWR 加载指标并 upsert
           - 跳过: 存储版本和新版本都已固化的周期
           - 更新: 存储版本未固化的周期（开放周期或刚固化的周期）
           - 插入: 尚未存储的新周期

        :param instance_info: 性能监控节点信息
        :return: (更新的记录数, 失败的模板数)
        """
        instance_id = instance_info.instance_id
        perf_logger.info(f"[PerfMon] Starting metrics update for instance {instance_id}")
        
        # 1. 获取所有规则变更历史
        all_changes = RuleStateController.get_all_changes_by_instance(
            self.meta_controller, instance_id
        )
        
        if not all_changes:
            perf_logger.info(f"[PerfMon] No rule changes found for instance {instance_id}")
            return 0, 0
        
        # 2. 按 (db, digest) 分组
        grouped = self._group_by_sql_template(all_changes)
        perf_logger.info(f"[PerfMon] Processing {len(grouped)} SQL templates")
        
        updated = 0
        failed = 0
        for (db, digest), changes in grouped.items():
            try:
                updated += self._update_template_metrics(instance_info, db, digest, changes)
            except Exception as e:
                perf_logger.error(f"[PerfMon] Error updating {db}.{digest[:8]}...: {e}")
                failed += 1

        perf_logger.info(
            f"[PerfMon] Instance {instance_id}: updated {updated} period records, "
            f"{failed} template(s) failed"
        )
        return updated, failed
    
    def _group_by_sql_template(
        self, 
        changes: list[RuleStateChange]
    ) -> dict[tuple[str, str], list[RuleStateChange]]:
        """按 (db, digest) 分组规则变更"""
        grouped = defaultdict(list)
        for change in changes:
            grouped[(change.db, change.digest)].append(change)
        return dict(grouped)
    
    def _update_template_metrics(
        self,
        instance_info: PerfMonInstanceInfo,
        db: str,
        digest: str,
        changes: list[RuleStateChange]
    ) -> int:
        """更新单个 SQL 模板的性能指标"""
        instance_id = instance_info.instance_id
        cluster_id = instance_info.cluster_id
        node_uuid = instance_info.node_uuid

        # 获取已存储的周期记录
        stored_periods = PerfMetricsController.get_periods(
            self.meta_controller, instance_id, node_uuid, db, digest
        )
        stored_map = {p.period_index: p for p in stored_periods}

        # 识别所有周期
        periods = self._identify_periods(changes)

        updated = 0
        for period in periods:
            stored = stored_map.get(period.period_index)

            # 跳过条件：存储版本已固化 AND 新版本也已固化
            # (真正的历史数据，无需更新)
            if stored and stored.is_finalized and period.is_finalized:
                continue

            # 需要更新的情况：
            # 1. 尚未存储 (新周期)
            # 2. 存储版本未固化 (开放周期或刚固化的周期，需要刷新 end_time 和 metrics)

            # 性能监控始终使用 AWR 数据源
            metrics = load_awr_metrics(
                db, digest,
                period.period_start_time, period.period_end_time,
                instance_info.region.value,
                instance_info.product_type,
                node_uuid,
                instance_info.cluster_id
            )

            # 如果无数据，使用零值（支持缺失语句）
            if metrics is None:
                metrics = PerfMetrics.zero()

            period.metrics = metrics

            # 解析 best_validation_log_id（物化到 perf_metrics_history）
            if period.period_index == 0:
                # baseline: 无 task，无 validation_logs
                period.best_validation_log_id = None
            else:
                change = changes[period.period_index - 1]
                if change.curr_plan_ids:
                    period.best_validation_log_id = self._resolve_best_validation_log_id(
                        instance_id, db, digest, change.task_id, change.curr_plan_ids
                    )
                else:
                    # reset / modify_timeout(无 curr_plan_ids) / 其他: 无样本
                    period.best_validation_log_id = None

            # 更新或插入 (upsert 会同时更新 end_time, metrics, is_finalized, best_validation_log_id)
            PerfMetricsController.upsert_period(
                self.meta_controller, instance_id, cluster_id, node_uuid, db, digest, period
            )
            updated += 1

        return updated

    def _resolve_best_validation_log_id(
        self,
        instance_id: str,
        db: str,
        digest: str,
        task_id: str,
        curr_plan_ids: list[str]
    ) -> int | None:
        """
        查询最佳验证样本 ID — 写入时一次性计算，避免 VIEW 每次查询重复计算

        :return: validation_logs.id 或 None（无匹配样本时）
        """
        self.meta_controller.use_db(GlobalConfig.ai_metadata_database)
        query = text("""
            SELECT v.id FROM validation_logs v
            WHERE v.instance_id = :instance_id AND v.db = :db
              AND v.digest = :digest AND v.task_id = :task_id
              AND v.plan_id IN :plan_ids
              AND v.elapsed_time > 0 AND v.elapsed_time < 1000000000
            ORDER BY v.speedup_ratio DESC LIMIT 1
        """).bindparams(bindparam("plan_ids", expanding=True))

        result = self.meta_controller.execute(query, {
            "instance_id": instance_id,
            "db": db,
            "digest": digest,
            "task_id": task_id,
            "plan_ids": curr_plan_ids
        })

        row = result.fetchone()
        return row[0] if row else None

    def _identify_periods(self, changes: list[RuleStateChange]) -> list[PeriodMetrics]:
        """
        根据规则变更历史识别所有周期
        
        示例：
        changes = [t1: setup_plan, t3: modify_plan, t5: reset]
        生成周期：
          - Period 0: [earliest_data, t1) - baseline
          - Period 1: [t1, t3) - first optimization
          - Period 2: [t3, t5) - modified optimization  
          - Period 3: [t5, now) - reset to default (open)
        """
        if not changes:
            return []
        
        periods = []
        
        # Period 0: baseline (首次规则应用前，回溯 30 天)
        first_change = changes[0]
        baseline_start = first_change.apply_time - timedelta(days=30)
        periods.append(PeriodMetrics(
            period_index=0,
            period_start_time=baseline_start,
            period_end_time=first_change.apply_time,
            task_id=None,
            operation=None,
            metrics=None,
            is_finalized=True  # baseline 总是固化的
        ))
        
        # Period 1 ~ N-1: 中间周期（均已固化）
        for i in range(len(changes) - 1):
            periods.append(PeriodMetrics(
                period_index=i + 1,
                period_start_time=changes[i].apply_time,
                period_end_time=changes[i + 1].apply_time,
                task_id=changes[i].task_id,
                operation=changes[i].operation.value,
                metrics=None,
                is_finalized=True
            ))
        
        # Period N: 当前开放周期
        last_change = changes[-1]
        periods.append(PeriodMetrics(
            period_index=len(changes),
            period_start_time=last_change.apply_time,
            period_end_time=datetime.now(),
            task_id=last_change.task_id,
            operation=last_change.operation.value,
            metrics=None,
            is_finalized=False  # 开放周期
        ))
        
        return periods

