"""
mcts.modules.db_executor - Database execution layer.

Encapsulates all interactions with MySQL through a DBController and returns
typed ``DBExecutionResult`` — never raises across function boundaries.

The remote plan cache is an *optional* add-on (``RemotePlanCache``). The two
execution paths are fully separated:

  * No cache attached (or the caller passes no ``CacheRequest``) ->
    ``_execute_plain``: a bare capped EXPLAIN ANALYZE with no cache code at all.
  * Cache attached AND a ``CacheRequest`` supplied -> ``_execute_cached``: the
    request is handed straight to the remote-cache logic (lookup / execute /
    store). ``execute_and_measure`` only routes; it never unpacks the request.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from sqlalchemy import text as sql_text
from sqlalchemy.exc import OperationalError

from mcts.types import DBExecutionResult, MCTSRunMetrics
from mcts.modules.remote_plan_cache import RemotePlanCache, CacheRequest

from mcts import logger

import db_utils


@dataclass
class _ExecRun:
    """Raw outcome of one EXPLAIN ANALYZE, before the caller timeout is applied.

    Exactly one of these states holds:
      * ``error`` set         -> an unexpected DB error (not a timeout).
      * ``timed_out`` True     -> the run hit the wall-clock cap.
      * otherwise              -> success; ``execution_time`` / ``explain_json`` valid.

    It is returned (never raised) so the execute paths branch on plain fields
    instead of catching exceptions across method boundaries.
    """
    execution_time: Optional[float] = None
    explain_json: str = ""
    timed_out: bool = False
    error: Optional[str] = None


class DBExecutor:
    """Executes SQL against MySQL and returns structured results.

    All methods return ``DBExecutionResult``. Errors are captured in
    ``DBExecutionResult.error`` — no exceptions propagate to callers.
    """

    def __init__(
        self,
        controller: object,  # DBController
        explain_timeout_seconds: float = 30.0,
        metrics: Optional[MCTSRunMetrics] = None,
        remote_cache: Optional[RemotePlanCache] = None,
    ) -> None:
        self._controller = controller
        self._metrics = metrics
        self._explain_timeout_seconds = float(explain_timeout_seconds)
        # Optional. None (or an unavailable cache) => the executor never touches
        # the remote cache; the core path is identical to cache-off.
        self._remote_cache = remote_cache if (remote_cache and remote_cache.available) else None

    @property
    def remote_cache(self) -> Optional[RemotePlanCache]:
        return self._remote_cache

    @property
    def controller(self) -> object:
        return self._controller

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def get_plan_digest(self, sql_with_hints: str) -> DBExecutionResult:
        """Run EXPLAIN to get the plan digest only (no execution)."""
        try:
            plan_digest = self._explain_plan_digest(sql_with_hints)
            return DBExecutionResult(plan_digest=plan_digest)
        except Exception as e:
            logger.error(f"[DBExecutor] EXPLAIN failed: {type(e).__name__}: {e}")
            return DBExecutionResult(error=f"EXPLAIN error: {type(e).__name__}: {e}")

    def execute_and_measure(
        self,
        sql_with_hints: str,
        timeout_seconds: float,
        cache_request: Optional[CacheRequest] = None,
    ) -> DBExecutionResult:
        """Measure ``sql_with_hints`` via EXPLAIN ANALYZE.

        ``timeout_seconds`` is the caller's reporting timeout: a plan slower than
        it is reported as a timeout capped at ``timeout_seconds``.

        This method only ROUTES. When no remote cache is attached, or the caller
        passes no ``cache_request``, it runs the fully cache-free ``_execute_plain``
        path. Otherwise it hands the request straight to ``_execute_cached`` —
        the request is never unpacked here, so cache state never leaks onto the
        plain path.

        Args:
            sql_with_hints: SQL with optimizer hints already inserted.
            timeout_seconds: Caller reporting timeout in seconds.
            cache_request: Optional remote-cache request (original hint-free SQL
                + applied hints). Ignored when this executor has no cache, so
                callers may build it unconditionally.
        """
        # Plan digest first (cheap EXPLAIN, no execution). Best-effort: a failure
        # here just means no digest / no cache key, not a failed measurement.
        try:
            plan_digest = self._explain_plan_digest(sql_with_hints)
        except Exception as e:
            logger.warning(f"[DBExecutor] Failed to get plan digest: {e}")
            plan_digest = None

        if self._remote_cache is None or cache_request is None:
            return self._execute_plain(sql_with_hints, plan_digest, timeout_seconds)

        return self._execute_cached(
            sql_with_hints, plan_digest, timeout_seconds, cache_request
        )

    # ------------------------------------------------------------------
    # EXPLAIN helper (plan digest only — never EXPLAIN ANALYZE)
    # ------------------------------------------------------------------

    def _explain_plan_digest(self, sql_with_hints: str) -> str:
        plan_digest = db_utils.get_plan_id_only(
            self._controller, sql_with_hints,
            explain_timeout_seconds=self._explain_timeout_seconds,
        )
        if self._metrics:
            self._metrics.record_db_explain()
        return plan_digest

    # ------------------------------------------------------------------
    # Plain path (no remote cache — zero cache code on this path)
    # ------------------------------------------------------------------

    def _execute_plain(
        self,
        sql_with_hints: str,
        plan_digest: Optional[str],
        timeout_seconds: float,
    ) -> DBExecutionResult:
        """Bare capped EXPLAIN ANALYZE. The execution cap is ``timeout_seconds``."""
        run = self._run_explain_analyze(sql_with_hints, timeout_seconds)

        if run.error is not None:
            return DBExecutionResult(error=run.error)

        if run.timed_out:
            if self._metrics:
                self._metrics.record_db_execute(timeout_seconds)
            logger.error(f"[DBExecutor] Execution timeout ({timeout_seconds}s)")
            return DBExecutionResult(
                plan_digest=plan_digest,
                execution_time_seconds=timeout_seconds,
                error=f"Execution hit max time ({timeout_seconds}s)",
                is_timeout=True,
            )

        if self._metrics:
            self._metrics.record_db_execute(run.execution_time or 0.0)

        # Apply the caller timeout to the measurement.
        if run.execution_time is not None and run.execution_time >= timeout_seconds:
            logger.error(
                f"[DBExecutor] Over time limit ({timeout_seconds}s), actual={run.execution_time:.4f}s"
            )
            return DBExecutionResult(
                plan_digest=plan_digest,
                execution_time_seconds=timeout_seconds,
                explain_analyze_json=run.explain_json,
                error=f"Execution hit max time ({timeout_seconds}s)",
                is_timeout=True,
            )
        return DBExecutionResult(
            plan_digest=plan_digest,
            execution_time_seconds=run.execution_time,
            explain_analyze_json=run.explain_json,
            is_timeout=False,
        )

    # ------------------------------------------------------------------
    # Cached path (all remote-cache logic lives here)
    # ------------------------------------------------------------------

    def _execute_cached(
        self,
        sql_with_hints: str,
        plan_digest: Optional[str],
        timeout_seconds: float,
        request: CacheRequest,
    ) -> DBExecutionResult:
        """Lookup -> (hit | execute + store) against the remote cache.

        The execution cap here is the cache's timeout (not ``timeout_seconds``)
        so a stored ``timeout_time`` is reusable across callers with different
        timeouts; the caller ``timeout_seconds`` is then applied to whatever we
        measured.
        """
        hints = sorted(request.hints) if request.hints else []
        key = self._remote_cache.make_key(request.query_sql, plan_digest)

        # No usable key (digest uncomputable) -> degrade to the plain path.
        if key is None:
            return self._execute_plain(sql_with_hints, plan_digest, timeout_seconds)

        lookup = self._remote_cache.lookup(key, hints, timeout_seconds)
        if lookup.is_hit:
            if self._metrics:
                self._metrics.record_remote_cache_hit()
            return lookup.result
        force_update = lookup.rerun  # stale-timeout hit -> re-run then UPDATE

        cap_seconds = self._remote_cache.cache_timeout_seconds
        run = self._run_explain_analyze(sql_with_hints, cap_seconds)

        if run.error is not None:
            return DBExecutionResult(error=run.error)

        if run.timed_out:
            if self._metrics:
                self._metrics.record_db_execute(cap_seconds)
            logger.error(f"[DBExecutor] Execution timeout ({cap_seconds}s)")
            self._remote_cache.store(
                key, hints, "{}", cap_seconds,
                is_timeout=True, force_update=force_update,
            )
            return DBExecutionResult(
                plan_digest=plan_digest,
                execution_time_seconds=timeout_seconds,
                error=f"Execution hit max time ({timeout_seconds}s)",
                is_timeout=True,
            )

        if self._metrics:
            self._metrics.record_db_execute(run.execution_time or 0.0)
        if run.execution_time is not None:
            self._remote_cache.store(
                key, hints, run.explain_json, run.execution_time,
                is_timeout=None, force_update=force_update,
            )

        # Apply the caller timeout to the measurement.
        if run.execution_time is not None and run.execution_time >= timeout_seconds:
            logger.error(
                f"[DBExecutor] Over time limit ({timeout_seconds}s), actual={run.execution_time:.4f}s"
            )
            return DBExecutionResult(
                plan_digest=plan_digest,
                execution_time_seconds=timeout_seconds,
                explain_analyze_json=run.explain_json,
                error=f"Execution hit max time ({timeout_seconds}s)",
                is_timeout=True,
            )
        return DBExecutionResult(
            plan_digest=plan_digest,
            execution_time_seconds=run.execution_time,
            explain_analyze_json=run.explain_json,
            is_timeout=False,
        )

    # ------------------------------------------------------------------
    # Shared raw execution mechanics
    # ------------------------------------------------------------------

    def _run_explain_analyze(self, sql_with_hints: str, cap_seconds: float) -> _ExecRun:
        """Run a single capped EXPLAIN ANALYZE and return the raw measurement.

        Never raises: a hard timeout comes back as ``_ExecRun(timed_out=True)``
        and any other DB error as ``_ExecRun(error=...)``, so callers branch on
        fields rather than catching exceptions.
        """
        explain_sql = sql_text(f"EXPLAIN ANALYZE FORMAT=JSON {sql_with_hints}")
        try:
            # return_on_timeout=True keeps partial rows when execution hits the cap.
            elapsed_seconds, exec_rows = self._controller.evaluate_elapsed_time_with_result(
                explain_sql,
                timeout_seconds=cap_seconds,
                return_on_timeout=True,
            )
        except (ValueError, OperationalError) as exc:
            # Some hard timeouts still raise despite return_on_timeout=True. MySQL
            # error codes 3024/1317 are statement timeouts; a bare ValueError from
            # the controller is also a timeout signal.
            is_timeout = isinstance(exc, ValueError) or (
                isinstance(exc, OperationalError)
                and getattr(getattr(exc, "orig", None), "args", (None,))[0] in (3024, 1317)
            )
            if is_timeout:
                return _ExecRun(timed_out=True)
            logger.error(f"[DBExecutor] Execution error: {type(exc).__name__}: {exc}")
            return _ExecRun(error=f"Execution error: {type(exc).__name__}: {exc}")
        except Exception as exc:
            logger.error(f"[DBExecutor] Execution error: {type(exc).__name__}: {exc}")
            return _ExecRun(error=f"Execution error: {type(exc).__name__}: {exc}")

        # Extract the EXPLAIN ANALYZE JSON (drop the trailing "Outline Data:" tail).
        explain_json = ""
        if exec_rows and exec_rows[0][0]:
            explain_json = exec_rows[0][0]
            if isinstance(explain_json, str):
                marker = "\nOutline Data:"
                if marker in explain_json:
                    explain_json = explain_json[: explain_json.index(marker)]

        execution_time = float(elapsed_seconds) if elapsed_seconds is not None else None
        return _ExecRun(execution_time=execution_time, explain_json=explain_json)
