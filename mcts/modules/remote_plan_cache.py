"""
mcts.modules.remote_plan_cache - Optional remote plan cache.

Backs onto the ``{db}_cache``.``query_cache`` table on the target instance and
persists, across runs, the measured execution time of each (query, plan) pair.

This module is *entirely optional*. ``DBExecutor`` only touches it when a caller
passes a ``cache_key`` AND the cache initialised successfully; otherwise the
executor's core path never references anything in here. All schema setup, cache
key building, the lookup decision rules, and result storage live here so the
executor stays cache-agnostic.
"""
from __future__ import annotations

import json
import math
from dataclasses import dataclass
from typing import List, Optional

from sqlalchemy import text as sql_text

from mcts.types import DBExecutionResult
from mcts import logger

import db_utils


@dataclass
class RemoteCacheKey:
    """Identity of a cached (query, plan) pair."""
    db_name: str
    query_text: str
    query_digest: str
    plan_digest: str


@dataclass
class CacheRequest:
    """A caller's request to consult the remote plan cache for one measurement.

    ``query_sql`` is the original, hint-free SQL identifying the cache row;
    ``hints`` are the hints applied for this measurement (recorded against the
    entry). ``DBExecutor`` ignores this entirely when it has no remote cache
    attached — the caller can build one unconditionally without checking the
    cache switch.
    """
    query_sql: str
    hints: List[str]


@dataclass
class CacheLookup:
    """Outcome of a remote cache lookup.

    * ``result`` non-None  -> usable hit; the executor returns it directly.
    * ``rerun`` True       -> stale-timeout hit; re-execute and UPDATE the row.
    * both empty           -> miss; the executor executes and INSERTs.
    """
    result: Optional[DBExecutionResult] = None
    rerun: bool = False

    @property
    def is_hit(self) -> bool:
        return self.result is not None


class RemotePlanCache:
    """Optional remote plan cache over ``{db}_cache``.``query_cache``."""

    _QUERY_CACHE_TABLE_DDL = """
    CREATE TABLE IF NOT EXISTS `{cache_db}`.`query_cache` (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        db_name VARCHAR(255),
        query_text LONGTEXT,
        query_digest VARCHAR(255),
        plan_digest VARCHAR(255),
        plan_info LONGTEXT,
        execution_time DOUBLE,
        extra_info LONGTEXT,
        hint_set LONGTEXT,
        timeout_time DOUBLE,
        is_timeout TINYINT NOT NULL DEFAULT 0,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        INDEX idx_db_plan (db_name,query_digest, plan_digest)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """

    def __init__(self, controller, db_name: str, cache_timeout_seconds: int) -> None:
        self._controller = controller
        self._db_name = db_name
        self._cache_db_name = f"{db_name}_cache"
        # Hard wall-clock cap for a single EXPLAIN ANALYZE behind this cache, and
        # the value written to ``query_cache.timeout_time`` so an entry can be
        # reused across callers with different timeouts.
        self._cache_timeout_seconds = max(1, int(cache_timeout_seconds))
        self._manager = None
        self._init_schema()

    # ------------------------------------------------------------------
    # Lifecycle / config
    # ------------------------------------------------------------------

    @property
    def available(self) -> bool:
        """True only when the schema was set up and the manager is ready."""
        return self._manager is not None

    @property
    def cache_timeout_seconds(self) -> int:
        return self._cache_timeout_seconds

    def cap_timeout(self, seconds: float) -> int:
        """Tighten the cache timeout to ``min(current, seconds)`` (>= 1s)."""
        self._cache_timeout_seconds = max(1, math.ceil(min(self._cache_timeout_seconds, seconds)))
        return self._cache_timeout_seconds

    def _init_schema(self) -> None:
        """Create the cache database/table if needed and init the manager.

        On any failure the cache stays unavailable (``available`` is False);
        the executor then silently runs without it.
        """
        try:
            rows = self._controller.execute(
                sql_text(
                    "SELECT SCHEMA_NAME FROM information_schema.SCHEMATA "
                    "WHERE SCHEMA_NAME = :cache_db"
                ),
                {"cache_db": self._cache_db_name},
            ).fetchall()
            if not rows:
                logger.info(f"[RemotePlanCache] Creating cache database '{self._cache_db_name}'...")
                self._controller.execute(
                    sql_text(f"CREATE DATABASE IF NOT EXISTS `{self._cache_db_name}`")
                )

            self._controller.execute(
                sql_text(self._QUERY_CACHE_TABLE_DDL.format(cache_db=self._cache_db_name))
            )

            from remote_cache_manager import RemoteCacheManager
            self._manager = RemoteCacheManager(
                controller=self._controller, cache_db_name=self._cache_db_name
            )
            logger.info(f"[RemotePlanCache] Enabled: using {self._cache_db_name}")
        except Exception as e:
            logger.warning(f"[RemotePlanCache] Init failed, cache disabled: {e}")
            self._manager = None

    # ------------------------------------------------------------------
    # Key building
    # ------------------------------------------------------------------

    def make_key(self, query_text: Optional[str], plan_digest: Optional[str]) -> Optional[RemoteCacheKey]:
        """Build the cache key for a (query, plan) pair, or None if either
        digest is missing/uncomputable."""
        if not query_text or not plan_digest:
            return None
        try:
            query_digest = db_utils.compute_statement_digest(self._controller, query_text)
        except Exception as e:
            logger.debug(f"[RemotePlanCache] query digest failed: {e}")
            return None
        if not query_digest:
            return None
        return RemoteCacheKey(self._db_name, query_text, query_digest, plan_digest)

    # ------------------------------------------------------------------
    # Lookup
    # ------------------------------------------------------------------

    def lookup(self, key: RemoteCacheKey, hints: List[str], timeout_seconds: float) -> CacheLookup:
        """Look up a cached result and decide what to do (see ``CacheLookup``)."""
        try:
            cached = self._manager.get_by_digest(key.db_name, key.query_digest, key.plan_digest)
        except Exception as e:
            logger.debug(f"[RemotePlanCache] lookup failed: {e}")
            return CacheLookup()
        if not cached:
            return CacheLookup()

        plan_info_str, execution_time, _extra_info, is_timeout, timeout_time = cached
        execution_time = float(execution_time) if execution_time is not None else 0.0
        timeout_time = float(timeout_time) if timeout_time is not None else 0.0
        is_timeout = bool(is_timeout)
        plan_info_json = plan_info_str if isinstance(plan_info_str, str) else json.dumps(plan_info_str)

        # Record this hint combination against the existing entry.
        try:
            self._manager.update_hints_by_digest(key.db_name, key.query_digest, key.plan_digest, hints)
        except Exception as e:
            logger.debug(f"[RemotePlanCache] update hints failed: {e}")

        cap = self._cache_timeout_seconds
        logger.info(
            f"[RemotePlanCache] hit: digest={key.plan_digest}, time={execution_time:.4f}s, "
            f"is_timeout={is_timeout}, timeout_time={timeout_time:.4f}s, timeout={timeout_seconds:.4f}s"
        )

        # 决策规则：
        # 1) 缓存是 timeout 且 timeout_time < cache_timeout_seconds
        #    -> 旧记录是用更短的 timeout 截断的，真实耗时可能小于 cache_timeout_seconds
        #       重新以 cache_timeout_seconds 为上限执行并 UPDATE 原记录
        # 2) 缓存是 timeout 且 timeout_time >= cache_timeout_seconds
        #    -> 视为超时；返回时间取 min(timeout_time, timeout_seconds)
        # 3) 缓存是正常结果，但 execution_time >= timeout_seconds
        #    -> 按调用方 timeout 处理（超出 timeout）
        # 4) 否则正常返回缓存结果
        if is_timeout and timeout_time < cap:
            logger.info(
                f"[RemotePlanCache] cached timeout with shorter cap "
                f"({timeout_time}s < {cap}s), re-running."
            )
            return CacheLookup(rerun=True)

        if is_timeout:
            reported = min(timeout_time, timeout_seconds) if timeout_seconds > 0 else timeout_time
            return CacheLookup(result=DBExecutionResult(
                plan_digest=key.plan_digest,
                execution_time_seconds=reported,
                explain_analyze_json=plan_info_json,
                is_timeout=True,
                error=(
                    f"Execution hit max time (cached timeout, "
                    f"timeout_time={timeout_time}s, timeout={timeout_seconds}s)"
                ),
            ))

        if execution_time >= timeout_seconds:
            return CacheLookup(result=DBExecutionResult(
                plan_digest=key.plan_digest,
                execution_time_seconds=timeout_seconds,
                explain_analyze_json=plan_info_json,
                is_timeout=True,
                error=(
                    f"Execution hit max time (cached={execution_time}s "
                    f">= timeout={timeout_seconds}s)"
                ),
            ))

        return CacheLookup(result=DBExecutionResult(
            plan_digest=key.plan_digest,
            execution_time_seconds=execution_time,
            explain_analyze_json=plan_info_json,
            is_timeout=False,
        ))

    # ------------------------------------------------------------------
    # Store
    # ------------------------------------------------------------------

    def store(
        self,
        key: RemoteCacheKey,
        hints: List[str],
        plan_info: str,
        execution_time: float,
        is_timeout: Optional[bool] = None,
        force_update: bool = False,
    ) -> None:
        """Persist a measurement. ``timeout_time`` is always the cache timeout
        so the entry is reusable across timeouts. ``is_timeout`` defaults to
        whether the measurement reached the cap. ``force_update`` chooses UPDATE
        (refresh a stale-timeout row) over INSERT."""
        if is_timeout is None:
            is_timeout = execution_time >= self._cache_timeout_seconds
        plan_info_str = plan_info if isinstance(plan_info, str) else json.dumps(plan_info)
        try:
            if force_update:
                self._manager.update_result_by_digest(
                    key.db_name, key.query_digest, key.plan_digest,
                    plan_info=plan_info_str,
                    execution_time=execution_time,
                    extra_info="{}",
                    timeout_time=self._cache_timeout_seconds,
                    is_timeout=is_timeout,
                )
                logger.debug(
                    f"[RemotePlanCache] updated (rerun): digest={key.plan_digest}, "
                    f"time={execution_time:.4f}s, is_timeout={is_timeout}"
                )
            else:
                hint_set_str = json.dumps([hints]) if hints else json.dumps([[]])
                self._manager.set(
                    key.db_name, key.query_text, key.query_digest, key.plan_digest,
                    plan_info_str, execution_time, "{}",
                    hint_set=hint_set_str,
                    timeout_time=self._cache_timeout_seconds,
                    is_timeout=is_timeout,
                )
                logger.debug(
                    f"[RemotePlanCache] stored: digest={key.plan_digest}, "
                    f"time={execution_time:.4f}s, is_timeout={is_timeout}"
                )
        except Exception as e:
            logger.debug(f"[RemotePlanCache] store failed: {e}")


def build_remote_plan_cache(
    controller,
    db_name: Optional[str],
    cache_timeout_seconds: int,
    enabled: bool,
) -> Optional[RemotePlanCache]:
    """Construct a RemotePlanCache, or return None when it should be off.

    Returns None when ``enabled`` is False, no ``db_name`` is given, or schema
    setup fails — callers treat None as "no remote cache" with no extra checks.
    """
    if not enabled or not db_name:
        if db_name and not enabled:
            logger.info("[RemotePlanCache] Disabled via config; memory cache still active.")
        return None
    cache = RemotePlanCache(controller, db_name, cache_timeout_seconds)
    return cache if cache.available else None
