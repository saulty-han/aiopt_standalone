"""
Blacklist 控制器

管理 SQL 模板黑名单，禁止对指定 SQL 模板进行 AI 优化训练
blacklist 表由管控系统维护
"""

from typing import Set, Tuple
from sqlalchemy import text
from ai_logger import aiopt_logger
from db_controller import DBController
from config.config import GlobalConfig


class BlacklistController:
    """Blacklist 控制器"""

    @staticmethod
    def get_blacklist_set(
        db_controller: DBController,
        instance_id: str
    ) -> Set[Tuple[str, str]]:
        """
        获取指定实例的黑名单集合
        
        :return: Set of (db, digest) tuples
        """
        db_controller.use_db(GlobalConfig.ai_metadata_database)
        
        query = text("""
            SELECT db, digest FROM blacklist 
            WHERE instance_id = :instance_id AND enabled = TRUE
        """)
        result = db_controller.execute(query, {"instance_id": instance_id})
        
        blacklist_set = set()
        for row in result.fetchall():
            blacklist_set.add((row.db, row.digest))
        
        return blacklist_set

    @staticmethod
    def is_blacklisted(
        db_controller: DBController,
        instance_id: str,
        db: str,
        digest: str
    ) -> bool:
        """检查 SQL 模板是否在黑名单中"""
        db_controller.use_db(GlobalConfig.ai_metadata_database)
        
        query = text("""
            SELECT 1 FROM blacklist 
            WHERE instance_id = :instance_id 
            AND db = :db 
            AND digest = :digest 
            AND enabled = TRUE
            LIMIT 1
        """)
        result = db_controller.execute(query, {
            "instance_id": instance_id,
            "db": db,
            "digest": digest
        })
        return result.fetchone() is not None

