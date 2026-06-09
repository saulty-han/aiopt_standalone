"""
Validation Logs Controller

存储所有验证过的计划日志，用于后续分析和调试
"""

from sqlalchemy import text
from db_controller import DBController
from ai_logger import aiopt_logger
from data_models import ValidationLogEntry


class ValidationLogsController:
    """Validation Logs Controller - 存储验证过的计划日志"""
    
    @staticmethod
    def insert_validation_logs(controller: DBController, logs: list[ValidationLogEntry]) -> int:
        """
        批量插入 validation_logs 记录
        
        :param controller: 数据库控制器
        :param logs: 日志条目列表
        :return: 插入的记录数
        """
        if not logs:
            return 0
        
        insert_sql = text("""
            INSERT INTO validation_logs (
                task_id, instance_id, db, digest,
                hints_text, sql_text, sql_text_rewritten,
                default_plan_id, default_elapsed_time, default_explain_traditional, default_analyze_json, default_rows_examined,
                plan_id, elapsed_time, explain_traditional, analyze_json, rows_examined,
                is_best, is_better, comments
            ) VALUES (
                :task_id, :instance_id, :db, :digest,
                :hints_text, :sql_text, :sql_text_rewritten,
                :default_plan_id, :default_elapsed_time, :default_explain_traditional, :default_analyze_json, :default_rows_examined,
                :plan_id, :elapsed_time, :explain_traditional, :analyze_json, :rows_examined,
                :is_best, :is_better, :comments
            )
        """)
        
        inserted = 0
        for log in logs:
            try:
                controller.execute(insert_sql, {
                    "task_id": log.task_id,
                    "instance_id": log.instance_id,
                    "db": log.db,
                    "digest": log.digest,
                    "hints_text": log.hints_text,
                    "sql_text": log.sql_text,
                    "sql_text_rewritten": log.sql_text_rewritten,
                    "default_plan_id": log.default_plan_id,
                    "default_elapsed_time": log.default_elapsed_time,
                    "default_explain_traditional": log.default_explain_traditional,
                    "default_analyze_json": log.default_analyze_json,
                    "default_rows_examined": log.default_rows_examined,
                    "plan_id": log.plan_id,
                    "elapsed_time": log.elapsed_time,
                    "explain_traditional": log.explain_traditional,
                    "analyze_json": log.analyze_json,
                    "rows_examined": log.rows_examined,
                    "is_best": log.is_best,
                    "is_better": log.is_better,
                    "comments": log.comments
                })
                inserted += 1
            except Exception as e:
                aiopt_logger.warning(f"Failed to insert validation log: {e}")
        
        aiopt_logger.info(f"[ValidationLogsController] Inserted {inserted}/{len(logs)} validation logs")
        return inserted
