"""
SQL Templates Controller

管理 SQL 模板映射表 (cluster_id, instance_id, db, digest) → digest_text
存储 SQL 的归一化形式，便于人类阅读 digest 对应的 SQL 模式
"""

from sqlalchemy import text
from db_controller import DBController
from config.config import GlobalConfig


class SqlTemplatesController:
    """SQL Templates 控制器 — (cluster_id, instance_id, db, digest) → digest_text 映射"""

    @staticmethod
    def save_template(
        meta_controller: DBController,
        cluster_id: int,
        instance_id: str,
        db: str,
        digest: str,
        digest_text: str,
    ) -> None:
        """
        保存单条 SQL 模板。已存在则跳过 (INSERT IGNORE)。

        :param meta_controller: 元数据库连接
        :param cluster_id: 集群 ID
        :param instance_id: 实例 ID
        :param db: 数据库名
        :param digest: SQL digest (statement_digest() 输出)
        :param digest_text: 归一化 SQL 模板 (statement_digest_text() 输出)
        """
        meta_controller.use_db(GlobalConfig.ai_metadata_database)

        insert_sql = text("""
            INSERT IGNORE INTO sql_templates (cluster_id, instance_id, db, digest, digest_text)
            VALUES (:cluster_id, :instance_id, :db, :digest, :digest_text)
        """)
        meta_controller.execute(insert_sql, {
            "cluster_id": cluster_id,
            "instance_id": instance_id,
            "db": db,
            "digest": digest,
            "digest_text": digest_text,
        })

    @staticmethod
    def get_digest_text(
        meta_controller: DBController,
        cluster_id: int,
        instance_id: str,
        db: str,
        digest: str,
    ) -> str | None:
        """
        查询单条 digest 对应的 digest_text。

        :param meta_controller: 元数据库连接
        :param cluster_id: 集群 ID
        :param instance_id: 实例 ID
        :param db: 数据库名
        :param digest: SQL digest
        :return: digest_text 或 None (如果不存在)
        """
        meta_controller.use_db(GlobalConfig.ai_metadata_database)

        query = text("""
            SELECT digest_text FROM sql_templates
            WHERE cluster_id = :cluster_id AND instance_id = :instance_id
              AND db = :db AND digest = :digest
        """)
        result = meta_controller.execute(query, {
            "cluster_id": cluster_id,
            "instance_id": instance_id,
            "db": db,
            "digest": digest,
        })
        row = result.fetchone()
        return row[0] if row else None
