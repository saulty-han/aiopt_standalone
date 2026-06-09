"""
Performance Metrics Controller

perf_metrics_history 表的 CRUD 操作
"""
from datetime import datetime
from sqlalchemy import text
from config.config import GlobalConfig
from db_controller import DBController
from ai_logger import aiopt_logger
from perfmon.models import PerfMetrics, PeriodMetrics


class PerfMetricsController:
    """性能指标历史表操作"""
    
    @staticmethod
    def get_periods(
        controller: DBController,
        instance_id: str,
        node_uuid: str,
        db: str,
        digest: str
    ) -> list[PeriodMetrics]:
        """获取已存储的周期记录"""
        controller.use_db(GlobalConfig.ai_metadata_database)
        
        query = text("""
            SELECT 
                period_index, period_start_time, period_end_time,
                task_id, operation, is_finalized, best_validation_log_id,
                execution_count, query_time_avg, query_time_min, query_time_max,
                rows_examined_avg, rows_examined_min, rows_examined_max
            FROM perf_metrics_history
            WHERE instance_id = :instance_id AND node_uuid = :node_uuid AND db = :db AND digest = :digest
            ORDER BY period_index ASC
        """)
        
        result = controller.execute(query, {
            "instance_id": instance_id,
            "node_uuid": node_uuid,
            "db": db,
            "digest": digest
        })
        
        periods = []
        for row in result.fetchall():
            metrics = PerfMetrics(
                execution_count=row[7],
                query_time_avg=row[8],
                query_time_min=row[9],
                query_time_max=row[10],
                rows_examined_avg=row[11],
                rows_examined_min=row[12],
                rows_examined_max=row[13]
            )
            periods.append(PeriodMetrics(
                period_index=row[0],
                period_start_time=row[1],
                period_end_time=row[2],
                task_id=row[3],
                operation=row[4],
                is_finalized=bool(row[5]),
                best_validation_log_id=row[6],
                metrics=metrics
            ))
        
        return periods
    
    @staticmethod
    def upsert_period(
        controller: DBController,
        instance_id: str,
        cluster_id: int,
        node_uuid: str,
        db: str,
        digest: str,
        period: PeriodMetrics
    ) -> None:
        """插入或更新周期记录"""
        controller.use_db(GlobalConfig.ai_metadata_database)
        
        metrics = period.metrics or PerfMetrics.zero()
        
        upsert_sql = text("""
            INSERT INTO perf_metrics_history (
                cluster_id, instance_id, node_uuid, db, digest, period_index,
                period_start_time, period_end_time,
                task_id, operation,
                execution_count, query_time_avg, query_time_min, query_time_max,
                rows_examined_avg, rows_examined_min, rows_examined_max,
                computed_at, is_finalized, best_validation_log_id
            ) VALUES (
                :cluster_id, :instance_id, :node_uuid, :db, :digest, :period_index,
                :period_start_time, :period_end_time,
                :task_id, :operation,
                :execution_count, :query_time_avg, :query_time_min, :query_time_max,
                :rows_examined_avg, :rows_examined_min, :rows_examined_max,
                :computed_at, :is_finalized, :best_validation_log_id
            )
            ON DUPLICATE KEY UPDATE
                period_start_time = VALUES(period_start_time),
                period_end_time = VALUES(period_end_time),
                task_id = VALUES(task_id),
                operation = VALUES(operation),
                execution_count = VALUES(execution_count),
                query_time_avg = VALUES(query_time_avg),
                query_time_min = VALUES(query_time_min),
                query_time_max = VALUES(query_time_max),
                rows_examined_avg = VALUES(rows_examined_avg),
                rows_examined_min = VALUES(rows_examined_min),
                rows_examined_max = VALUES(rows_examined_max),
                computed_at = VALUES(computed_at),
                is_finalized = VALUES(is_finalized),
                best_validation_log_id = VALUES(best_validation_log_id)
        """)
        
        controller.execute(upsert_sql, {
            "cluster_id": cluster_id,
            "instance_id": instance_id,
            "node_uuid": node_uuid,
            "db": db,
            "digest": digest,
            "period_index": period.period_index,
            "period_start_time": period.period_start_time,
            "period_end_time": period.period_end_time,
            "task_id": period.task_id,
            "operation": period.operation,
            "execution_count": metrics.execution_count,
            "query_time_avg": metrics.query_time_avg,
            "query_time_min": metrics.query_time_min,
            "query_time_max": metrics.query_time_max,
            "rows_examined_avg": metrics.rows_examined_avg,
            "rows_examined_min": metrics.rows_examined_min,
            "rows_examined_max": metrics.rows_examined_max,
            "computed_at": datetime.now(),
            "is_finalized": period.is_finalized,
            "best_validation_log_id": period.best_validation_log_id
        })
        
        aiopt_logger.debug(
            f"[PerfMetricsController] Upserted period {period.period_index} "
            f"for {db}.{digest[:8]}... (finalized={period.is_finalized})"
        )
