import json
from sqlalchemy import text


class RemoteCacheManager:
    """
    Cache manager that stores query cache in the remote MySQL database
    under the dbname_cache schema. Uses a DBController for database access.
    """

    TABLE_NAME = "query_cache"

    def __init__(self, controller, cache_db_name):
        """
        :param controller: DBController instance connected to the target MySQL instance
        :param cache_db_name: Name of the cache database (e.g., "tpcds_cache")

        Note: The cache database and query_cache table must be pre-created
        by running setup_cache_db.py before using this manager.
        """
        self.controller = controller
        self.cache_db_name = cache_db_name

    def get_by_digest(self, db_name, query_digest, plan_digest):
        """
        Get cached result by db_name, query_digest and plan_digest.
        Returns (plan_info, execution_time, extra_info, is_timeout) or None.
        """
        try:
            result = self.controller.execute(
                text(
                    f"SELECT plan_info, execution_time, extra_info, is_timeout, timeout_time "
                    f"FROM `{self.cache_db_name}`.`{self.TABLE_NAME}` "
                    f"WHERE db_name = :db_name AND query_digest = :query_digest AND plan_digest = :plan_digest "
                    f"ORDER BY created_at DESC LIMIT 1"
                ),
                {"db_name": db_name, "query_digest": query_digest, "plan_digest": plan_digest}
            )
            return result.fetchone()
        except Exception as e:
            print(f"Failed to get by digest from remote cache: {e}")
            return None

    def update_hints_by_digest(self, db_name, query_digest, plan_digest, hint_set):
        """
        Update the hint_set for a cached entry identified by db_name, query_digest and plan_digest.
        Appends the new hint_set to the existing list if not already present.
        A single (query_digest, plan_digest) pair corresponds to one entry holding all hint sets.

        :param hint_set: A single hint list, e.g. ["INDEX(t1 idx1)", "NO_INDEX(t2)"]
        """
        try:
            result = self.controller.execute(
                text(
                    f"SELECT id, hint_set "
                    f"FROM `{self.cache_db_name}`.`{self.TABLE_NAME}` "
                    f"WHERE db_name = :db_name AND query_digest = :query_digest AND plan_digest = :plan_digest "
                    f"LIMIT 1"
                ),
                {"db_name": db_name, "query_digest": query_digest, "plan_digest": plan_digest}
            )
            row = result.fetchone()
            if not row:
                return  # No entry to update

            entry_id = row[0]
            existing_hints = json.loads(row[1]) if row[1] else []

            # Deduplicate: only append if not already present
            if hint_set in existing_hints:
                return

            existing_hints.append(hint_set)
            new_hints_str = json.dumps(existing_hints)
            self.controller.execute(
                text(
                    f"UPDATE `{self.cache_db_name}`.`{self.TABLE_NAME}` "
                    f"SET hint_set = :hint_set "
                    f"WHERE id = :id"
                ),
                {"hint_set": new_hints_str, "id": entry_id}
            )
        except Exception as e:
            print(f"Failed to update hints by digest in remote cache: {e}")

    def update_result_by_digest(
        self, db_name, query_digest, plan_digest,
        plan_info, execution_time, extra_info, timeout_time, is_timeout,
    ):
        """
        Update the execution result (plan_info / execution_time / extra_info /
        timeout_time / is_timeout) for an existing cache entry identified by
        (db_name, query_digest, plan_digest). hint_set is NOT overwritten.

        Returns the number of rows updated (0 if no such entry).
        """
        try:
            result = self.controller.execute(
                text(
                    f"UPDATE `{self.cache_db_name}`.`{self.TABLE_NAME}` "
                    f"SET plan_info = :plan_info, "
                    f"    execution_time = :execution_time, "
                    f"    extra_info = :extra_info, "
                    f"    timeout_time = :timeout_time, "
                    f"    is_timeout = :is_timeout "
                    f"WHERE db_name = :db_name "
                    f"  AND query_digest = :query_digest "
                    f"  AND plan_digest = :plan_digest"
                ),
                {
                    "db_name": db_name,
                    "query_digest": query_digest,
                    "plan_digest": plan_digest,
                    "plan_info": plan_info,
                    "execution_time": execution_time,
                    "extra_info": extra_info,
                    "timeout_time": timeout_time,
                    "is_timeout": 1 if is_timeout else 0,
                },
            )
            return getattr(result, "rowcount", 0) or 0
        except Exception as e:
            print(f"Failed to update result by digest in remote cache: {e}")
            return 0

    def set(self, db_name, query_text, query_digest, plan_digest, plan_info, execution_time, extra_info, hint_set, timeout_time, is_timeout=False):
        """
        Insert a new cache entry.

        :param db_name: Database name
        :param query_text: Original SQL text (without hints)
        :param query_digest: Query digest string (unique key together with plan_digest)
        :param plan_digest: Plan digest string
        :param plan_info: EXPLAIN ANALYZE JSON result string
        :param execution_time: Execution time in seconds
        :param extra_info: Extra info JSON string
        :param hint_set: JSON string of hint sets list, e.g. '["INDEX(t1 idx1)"]'
        :param timeout_time: Timeout value used for execution
        :param is_timeout: Whether the execution timed out
        """
        try:
            self.controller.execute(
                text(
                    f"INSERT INTO `{self.cache_db_name}`.`{self.TABLE_NAME}` "
                    f"(db_name, query_text, query_digest, plan_digest, plan_info, execution_time, extra_info, hint_set, timeout_time, is_timeout) "
                    f"VALUES (:db_name, :query_text, :query_digest, :plan_digest, :plan_info, :execution_time, :extra_info, :hint_set, :timeout_time, :is_timeout)"
                ),
                {
                    "db_name": db_name,
                    "query_text": query_text,
                    "query_digest": query_digest,
                    "plan_digest": plan_digest,
                    "plan_info": plan_info,
                    "execution_time": execution_time,
                    "extra_info": extra_info,
                    "hint_set": hint_set,
                    "timeout_time": timeout_time,
                    "is_timeout": 1 if is_timeout else 0,
                }
            )
        except Exception as e:
            print(f"Failed to set remote cache: {e}")
