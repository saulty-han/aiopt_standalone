from sqlalchemy import text
from config.config import GlobalConfig
from data_models import DecidedRule, RuleAction
from db_controller import DBController


class RulesController:
    """规则控制器"""
    
    @staticmethod
    def insert_rules(db_controller: DBController, rules: list[DecidedRule]) -> None:
        """Insert rules into the database."""
        db_controller.use_db(GlobalConfig.ai_metadata_database)
        insert_sql = """
            INSERT INTO rules (
                task_id, cluster_id, instance_id, db, digest, 
                action, plan_id, hints_text,
                sql_text, sql_text_rewritten,
                feedback_timeout, md5, comments
            ) VALUES (
                :task_id, :cluster_id, :instance_id, :db, :digest, 
                :action, :plan_id, :hints_text,
                :sql_text, :sql_text_rewritten,
                :feedback_timeout, :md5, :comments
            )
        """
        # Convert action enum to string value for SQL
        data = []
        for rule in rules:
            rule_dict = rule.model_dump()
            rule_dict['action'] = rule.action.value
            data.append(rule_dict)
        db_controller.execute(text(insert_sql), data)
    
    # Alias
    store_rules = insert_rules

    @staticmethod
    def load_rules_by_task(
        db_controller: DBController, task_id: str, instance_id: str
    ) -> list[DecidedRule]:
        """
        加载该 task 所有已生成的规则。

        用于续传时合并历史规则和本次新生成的规则，统一应用。
        使用 instance_id + task_id 双条件防止误传 task_id 导致跨实例误操作。
        """
        db_controller.use_db(GlobalConfig.ai_metadata_database)
        query = text("""
            SELECT task_id, cluster_id, instance_id, db, digest,
                   action, plan_id, hints_text,
                   sql_text, sql_text_rewritten,
                   feedback_timeout, md5, comments
            FROM rules
            WHERE task_id = :task_id AND instance_id = :instance_id
        """)
        result = db_controller.execute(query, {
            "task_id": task_id, "instance_id": instance_id
        })
        rules = []
        for row in result.fetchall():
            rules.append(DecidedRule(
                task_id=row[0],
                cluster_id=row[1],
                instance_id=row[2],
                db=row[3],
                digest=row[4],
                action=RuleAction(row[5]),
                plan_id=row[6],
                hints_text=row[7],
                sql_text=row[8],
                sql_text_rewritten=row[9],
                feedback_timeout=row[10],
                md5=row[11],
                comments=row[12]
            ))
        return rules
