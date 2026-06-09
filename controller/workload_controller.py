from typing import List
from sqlalchemy import text, bindparam
from config.config import GlobalConfig
from data_models import WorkloadRowAggregated
from db_controller import DBController
from ai_logger import aiopt_logger


class WorkloadController:
    @staticmethod
    def save_workload_batch(
            db_controller: DBController,
            task_id: str,
            rows: List[WorkloadRowAggregated]
    ) -> int:
        """
        批量保存 workload 到数据库
        :return: 保存的行数
        """
        if not rows:
            return 0
        
        db_controller.use_db(GlobalConfig.ai_metadata_database)
        
        insert_sql = text("""
            INSERT INTO workload 
                (task_id, instance_id, db, digest, plan_id, sql_text, count_star, 
                 elapsed_time_avg, elapsed_time_min, elapsed_time_max, last_start_time,
                 md5, sql_text_min, sql_text_max)
            VALUES 
                (:task_id, :instance_id, :db, :digest, :plan_id, :sql_text, :count_star,
                 :elapsed_time_avg, :elapsed_time_min, :elapsed_time_max, :last_start_time,
                 :md5, :sql_text_min, :sql_text_max)
            ON DUPLICATE KEY UPDATE
                sql_text = VALUES(sql_text),
                count_star = VALUES(count_star),
                elapsed_time_avg = VALUES(elapsed_time_avg),
                elapsed_time_min = VALUES(elapsed_time_min),
                elapsed_time_max = VALUES(elapsed_time_max),
                last_start_time = VALUES(last_start_time),
                md5 = VALUES(md5),
                sql_text_min = VALUES(sql_text_min),
                sql_text_max = VALUES(sql_text_max)
        """)
        
        for row in rows:
            db_controller.execute(insert_sql, {
                "task_id": task_id,
                "instance_id": row.instance_id,
                "db": row.db,
                "digest": row.digest,
                "plan_id": row.plan_id or "",  # 空字符串而非 NULL，因为是主键的一部分
                "sql_text": row.sql_text,
                "count_star": row.count_star,
                "elapsed_time_avg": row.elapsed_time_avg,
                "elapsed_time_min": row.elapsed_time_min,
                "elapsed_time_max": row.elapsed_time_max,
                "last_start_time": row.last_start_time,
                "md5": row.md5,
                "sql_text_min": row.sql_text_min,
                "sql_text_max": row.sql_text_max
            })
        
        aiopt_logger.info(f"Saved {len(rows)} workload rows for task {task_id}")
        return len(rows)
    
    @staticmethod
    def load_workload_by_task(
            db_controller: DBController,
            task_id: str
    ) -> List[WorkloadRowAggregated]:
        """
        加载指定任务的 workload
        """
        db_controller.use_db(GlobalConfig.ai_metadata_database)
        
        sql = text("""
            SELECT instance_id, db, digest, plan_id, sql_text, count_star, 
                   elapsed_time_avg, elapsed_time_min, elapsed_time_max, last_start_time, 
                   md5, sql_text_min, sql_text_max
            FROM workload 
            WHERE task_id = :task_id
        """)
        result = db_controller.execute(sql, {"task_id": task_id})
        rows = result.fetchall()
        
        return [WorkloadRowAggregated(
            instance_id=row[0],
            db=row[1],
            digest=row[2],
            plan_id=row[3] if row[3] else None,
            sql_text=row[4],
            count_star=row[5],
            elapsed_time_avg=row[6],
            elapsed_time_min=row[7],
            elapsed_time_max=row[8],
            last_start_time=row[9],
            md5=row[10],
            sql_text_min=row[11],
            sql_text_max=row[12]
        ) for row in rows]

    @staticmethod
    def mark_processed(
            db_controller: DBController,
            task_id: str,
            db: str,
            digest: str
    ) -> None:
        """
        标记指定模板为已处理。
        不指定 plan_id，批量更新同一 (task_id, db, digest) 下所有行。
        """
        db_controller.use_db(GlobalConfig.ai_metadata_database)
        sql = text("""
            UPDATE workload SET is_processed = 1
            WHERE task_id = :task_id AND db = :db AND digest = :digest
        """)
        db_controller.execute(sql, {
            "task_id": task_id,
            "db": db,
            "digest": digest
        })

    @staticmethod
    def get_processed_templates(
            db_controller: DBController,
            task_ids: list[str],
            instance_id: str
    ) -> set[tuple[str, str]]:
        """
        批量查询多个 task 已处理的 (db, digest) 集合。
        用于链式续传时跳过整条链上所有已处理的模板。
        """
        if not task_ids:
            return set()
        db_controller.use_db(GlobalConfig.ai_metadata_database)
        query = text("""
            SELECT DISTINCT db, digest FROM workload
            WHERE task_id IN :task_ids AND instance_id = :instance_id
              AND is_processed = 1
        """).bindparams(bindparam("task_ids", expanding=True))
        result = db_controller.execute(query, {
            "task_ids": task_ids,
            "instance_id": instance_id
        })
        return {(row[0], row[1]) for row in result.fetchall()}