"""
mcts.rag.qconfig - Extract QConfig records from MCTS artifacts.

Two sources, one extraction core:
  - Offline: a record dict parsed from an ``eval_data/*.json`` file (the shape
    produced by ``mcts.solver.convert_search_result_to_dict``, wrapped in a
    one-element list).
  - Online:  an in-memory ``MCTSSearchResult`` produced at the end of a search.

TRUST FILTER (critical):
  In the source data, a plan whose runtime exceeds the baseline is interrupted
  and recorded with the baseline time. Therefore any solution whose runtime is
  not STRICTLY below the baseline is untrustworthy and is dropped. Only the
  baseline itself and plans strictly faster than baseline are kept. We require
  ``runtime < baseline * (1 - eps)`` to also exclude near-equal (== baseline)
  records caused by the interrupt-and-assign behaviour.
"""
from __future__ import annotations

import hashlib
from typing import Any, Dict, List, Optional

from mcts.rag.types import QConfig
from mcts.rag.schematic import normalize_sql
from mcts.utils.hint_utils import _extract_index_table

# Relative margin below baseline a record must clear to be trusted.
# A plan that merely "ties" the baseline is almost always an interrupted /
# baseline-assigned record, so exclude it.
_IMPROVE_EPS = 1e-3


def _query_identity(query_digest: str, query_text: str) -> str:
    """Stable identity for "the same query", used to aggregate records.

    The source artifacts frequently lack a statement digest (only a few files
    carry ``execution_info.query_digest``). Without a stable per-query key the
    retriever's link-following (pick the best record of the same query) breaks,
    treating every hint set as a different query.

    Resolution order:
      1. the real statement digest, if present;
      2. otherwise a hash of the whitespace-normalized SQL text, prefixed with
         ``qtext:`` so it can never be confused with a real digest.
    """
    digest = (query_digest or "").strip()
    if digest:
        return digest
    norm = normalize_sql(query_text or "")
    if not norm:
        return ""
    return "qtext:" + hashlib.sha1(norm.encode("utf-8")).hexdigest()[:16]


def _qconfig_id(query_digest: str, hints: List[str], plan_digest: Optional[str]) -> str:
    key = "|".join([query_digest or "", plan_digest or "", *sorted(hints or [])])
    return hashlib.sha1(key.encode("utf-8")).hexdigest()[:16]


def _tables_from_hints(hints: List[str]) -> List[str]:
    """Best-effort table names parsed from INDEX/NO_INDEX hints."""
    out: List[str] = []
    for h in hints or []:
        parsed = _extract_index_table(h)
        if parsed is not None:
            out.append(parsed[1])
    return out


def _collect_tables(
    index_info: Optional[Dict[str, Any]],
    hints: List[str],
) -> List[str]:
    tables: List[str] = []
    if isinstance(index_info, dict):
        tables.extend(str(t) for t in index_info.keys())
    tables.extend(_tables_from_hints(hints))
    # dedupe preserving order, case-insensitive
    seen = set()
    out: List[str] = []
    for t in tables:
        k = t.lower()
        if t and k not in seen:
            seen.add(k)
            out.append(t)
    return out


def _improvement_ratio(baseline: Optional[float], runtime: Optional[float]) -> Optional[float]:
    if not baseline or not runtime or baseline <= 0 or runtime <= 0:
        return None
    return baseline / runtime - 1.0


def _is_trustworthy(runtime: Optional[float], baseline: Optional[float]) -> bool:
    """True iff the runtime is strictly (with margin) below the baseline."""
    if runtime is None or baseline is None:
        return False
    if runtime <= 0 or baseline <= 0:
        return False
    return runtime < baseline * (1.0 - _IMPROVE_EPS)


# ---------------------------------------------------------------------------
# Offline extraction (from a parsed JSON record dict)
# ---------------------------------------------------------------------------

def qconfigs_from_record(
    record: Dict[str, Any],
    *,
    source_file: Optional[str] = None,
    min_reward: float = 0.0,
    max_per_query: int = 8,
) -> List[QConfig]:
    """Extract trustworthy QConfigs from one MCTS result record.

    Args:
        record: a dict with keys query / baseline_time / execution_info /
                index_info / solutions (the convert_search_result_to_dict shape).
        source_file: provenance tag stored on each QConfig.
        min_reward: drop solutions with reward below this (after the trust
                    filter). Default 0.0 keeps any genuine improvement.
        max_per_query: keep at most this many best (fastest) solutions per
                       query to bound store growth (link-following keeps the
                       best at retrieval time anyway).

    Returns:
        A list of QConfig (possibly empty).
    """
    query = str(record.get("query") or "")
    if not query:
        return []

    baseline = record.get("baseline_time")
    try:
        baseline = float(baseline) if baseline is not None else None
    except (TypeError, ValueError):
        baseline = None

    execution_info = record.get("execution_info") if isinstance(record.get("execution_info"), dict) else {}
    # raw_digest = str(execution_info.get("query_digest") or record.get("query_digest") or "")
    raw_digest = str("")  
    # Resolve a stable query identity (real digest, or normalized-SQL hash when
    # the source lacks a digest) so link-following can aggregate same-query
    # records correctly.
    query_digest = _query_identity(raw_digest, query)
    plan_cost = execution_info.get("accumulative_cost")
    try:
        plan_cost = float(plan_cost) if plan_cost is not None else None
    except (TypeError, ValueError):
        plan_cost = None

    index_info = record.get("index_info") if isinstance(record.get("index_info"), dict) else {}

    solutions = record.get("solutions") or []
    candidates: List[QConfig] = []
    seen_keys = set()
    for sol in solutions:
        if not isinstance(sol, dict):
            continue
        hints = [str(h) for h in (sol.get("executed_hints") or []) if h]
        if not hints:
            continue
        runtime = sol.get("execution_time_s")
        try:
            runtime = float(runtime) if runtime is not None else None
        except (TypeError, ValueError):
            runtime = None

        # TRUST FILTER: only strictly-faster-than-baseline records.
        if not _is_trustworthy(runtime, baseline):
            continue

        reward = sol.get("reward")
        try:
            reward = float(reward) if reward is not None else None
        except (TypeError, ValueError):
            reward = None
        if reward is not None and reward < min_reward:
            continue

        plan_digest = sol.get("plan_digest")
        qid = _qconfig_id(query_digest, hints, plan_digest)
        # de-dup identical (query, hints, plan) within the record
        dkey = (plan_digest, tuple(sorted(hints)))
        if dkey in seen_keys:
            continue
        seen_keys.add(dkey)

        candidates.append(
            QConfig(
                qconfig_id=qid,
                query_digest=query_digest,
                query_text=query,
                query_template="",  # filled lazily by the builder via schematic
                tables=_collect_tables(index_info, hints),
                executed_hints=hints,
                plan_digest=plan_digest,
                baseline_time=baseline,
                runtime_seconds=runtime,
                improvement_ratio=_improvement_ratio(baseline, runtime),
                reward=reward,
                plan_cost=plan_cost,
                source="offline_json",
                source_file=source_file,
            )
        )

    # keep the fastest max_per_query
    candidates.sort(key=lambda q: (q.runtime_seconds if q.runtime_seconds is not None else float("inf")))
    if max_per_query > 0:
        candidates = candidates[:max_per_query]
    return candidates


# ---------------------------------------------------------------------------
# Online extraction (from an MCTSSearchResult)
# ---------------------------------------------------------------------------

def qconfigs_from_result(
    result: Any,
    *,
    min_reward: float = 0.0,
    max_per_query: int = 8,
) -> List[QConfig]:
    """Extract trustworthy QConfigs from an in-memory MCTSSearchResult.

    Reuses the offline path by projecting the result into the record shape.
    ``execution_info`` is not retained on the result, so query_digest is taken
    from ``result.query_digest`` and plan_cost is left None.
    """
    if result is None:
        return []
    solutions = []
    for sol in getattr(result, "solutions", []) or []:
        solutions.append(
            {
                "executed_hints": list(getattr(sol, "executed_hints", []) or []),
                "execution_time_s": getattr(sol, "execution_time_seconds", None),
                "plan_digest": getattr(sol, "plan_digest", None),
                "reward": getattr(sol, "reward", None),
            }
        )
    record = {
        "query": getattr(result, "query", "") or "",
        "baseline_time": getattr(result, "baseline_time_seconds", None),
        "execution_info": {"query_digest": getattr(result, "query_digest", "") or ""},
        "index_info": {},
        "solutions": solutions,
    }
    out = qconfigs_from_record(
        record, source_file=None, min_reward=min_reward, max_per_query=max_per_query
    )
    for q in out:
        q.source = "online"
    return out
