from config.config import GlobalConfig
from db_controller import DBController
from sqlalchemy import text


class TaskProgressController:
    @staticmethod
    def upsert(controller: DBController, task_id: str, instance_id: str, node_uuid: str,
               stage: str, total_templates: int | None = None, completed_templates: int | None = None):
        """Upsert task progress. None = 尚未统计，写入 DB 为 NULL。"""
        controller.use_db(GlobalConfig.ai_metadata_database)
        controller.execute(text("""
            INSERT INTO task_progress
                (task_id, instance_id, node_uuid, stage, total_templates, completed_templates)
            VALUES
                (:task_id, :instance_id, :node_uuid, :stage, :total, :completed)
            ON DUPLICATE KEY UPDATE
                stage=VALUES(stage),
                total_templates=VALUES(total_templates),
                completed_templates=VALUES(completed_templates)
        """), {
            "task_id": task_id, "instance_id": instance_id, "node_uuid": node_uuid,
            "stage": stage, "total": total_templates, "completed": completed_templates,
        })
