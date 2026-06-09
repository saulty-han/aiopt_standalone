"""
MCTS Result Controller

存储 MCTS 优化过程的完整结果（JSON格式）
"""

from sqlalchemy import text
from db_controller import DBController
from ai_logger import aiopt_logger
from config.config import GlobalConfig
import json


class MctsResultController:
    """MCTS Result Controller - 存储 MCTS 优化结果"""
    
    @staticmethod
    def insert_mcts_result(
        controller: DBController,
        task_id: str,
        instance_id: str,
        db: str,
        digest: str,
        result: list | dict
    ) -> bool:
        """
        插入 MCTS 优化结果记录
        
        :param controller: 数据库控制器（必须是meta_controller）
        :param task_id: 任务ID
        :param instance_id: 实例ID
        :param db: 数据库名
        :param digest: SQL模板digest
        :param result: MCTS优化结果（字典或字典列表，将被转换为JSON字符串存储为LONGTEXT）
        :return: 是否插入成功
        """
        try:
            controller.use_db(GlobalConfig.ai_metadata_database)
            
            insert_sql = text("""
                INSERT INTO mcts_result (
                    task_id, instance_id, db, digest, result
                ) VALUES (
                    :task_id, :instance_id, :db, :digest, :result
                )
            """)
            
            # 将result转换为JSON字符串（存储为LONGTEXT）
            result_json = json.dumps(result, ensure_ascii=False)
            
            controller.execute(insert_sql, {
                "task_id": task_id,
                "instance_id": instance_id,
                "db": db,
                "digest": digest,
                "result": result_json
            })
            
            aiopt_logger.info(
                f"[MctsResultController] Inserted MCTS result: "
                f"task_id={task_id}, db={db}, digest={digest[:8]}..."
            )
            return True
            
        except Exception as e:
            aiopt_logger.warning(
                f"[MctsResultController] Failed to insert MCTS result: {e}",
                exc_info=True
            )
            return False
