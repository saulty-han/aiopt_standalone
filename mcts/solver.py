"""
mcts.solver - Top-level solver that runs MCTSSearch and returns results.

This module bridges the gap between the optimizer (which provides raw data)
and the MCTS search engine. It handles:
  - Config construction
  - LLM client and DB executor setup
  - Running the search
  - Returning structured results
"""
from __future__ import annotations

import json
from typing import Any, Dict, List, Optional

from mcts.types import (
    MCTSConfig,
    MCTSInputData,
    MCTSSearchResult,
)
from mcts.utils.utils import round6
from mcts.modules.llm_client import LLMClient
from mcts.modules.db_executor import DBExecutor
from mcts.search import MCTSSearch

from mcts import logger


def run_mcts_for_query(
    config: MCTSConfig,
    input_data: MCTSInputData,
    controller: object,  # DBController
    db_name: str,
    db_executor: DBExecutor,
) -> MCTSSearchResult:
    """Run MCTS search for a single query.

    Args:
        config: MCTS configuration.
        input_data: Prepared input data for the query. ``baseline_time_seconds``
            and ``default_plan_digest`` are trusted — the caller is expected to
            have measured them via ``DBExecutor.execute_and_measure`` (same code
            path / remote cache as MCTS rollouts).
        controller: DBController to use for database operations.
        db_name: Name of the database to use for database operations.
        db_executor: Pre-built DBExecutor to reuse. The caller is responsible
            for constructing it (with or without a remote cache) and for running
            the cache-aware baseline probe through it before calling here.

    Returns:
        MCTSSearchResult containing solutions and metrics.
    """
    # Create components
    llm_client = LLMClient(config)

    # Create and run search
    search = MCTSSearch(config, input_data, llm_client, db_executor)
    # Share the metrics object with db_executor
    db_executor._metrics = search._metrics

    result = search.run()
    return result


def convert_search_result_to_dict(
    result: MCTSSearchResult,
    config: Optional[MCTSConfig] = None,
) -> Dict[str, Any]:
    """Convert MCTSSearchResult to the dict format expected by the optimizer.

    This produces:
    {
        "solutions": [...],
        "plan_digest_cache": {...},
        "early_stopping_metrics": {...},           # opt-out via config
        "mcts_tree_nodes": {<tag>: {node info with llm Q&A}, ...},
        "performance_metrics": {...},
        "explain_analyze_info": {<plan_digest>: <EA JSON>, ...},  # opt-in via config
    }

    ``early_stopping_metrics`` and ``explain_analyze_info`` are gated by
    ``config.include_early_stopping_metrics`` (default True) and
    ``config.include_explain_analyze_info`` (default False) respectively.
    When ``config`` is None, both blocks are emitted for backward
    compatibility with callers that don't yet thread a config through.
    """
    # Build solutions list, deduplicated by plan_digest (keep the first, i.e.
    # highest-reward, solution per plan — solutions are sorted reward-desc).
    # Solutions without a plan_digest are always kept.
    solutions_list = []
    seen_digests = set()
    for sol in result.solutions:
        if sol.plan_digest is not None:
            if sol.plan_digest in seen_digests:
                continue
            seen_digests.add(sol.plan_digest)
        solutions_list.append({
            "executed_hints": sol.executed_hints,
            "execution_time_s": round6(sol.execution_time_seconds),
            "plan_digest": sol.plan_digest,
            "reward": round6(sol.reward),
            "q_value": round6(sol.q_value),
            "action_type": sol.action_type,
            "tag": sol.node_tag,
            "rollout_index": sol.rollout_index,
        })

    # Build metrics
    m = result.metrics
    metrics_dict = {
        "llm_call_count": m.llm_call_count,
        "llm_output_chars": m.llm_output_chars,
        "llm_output_seconds": round6(m.llm_output_seconds),
        "llm_chars_per_second": round6(m.llm_chars_per_second),
        "llm_input_chars": m.llm_input_chars,
        "db_explain_count": m.db_explain_count,
        "db_execute_count": m.db_execute_count,
        "db_execute_seconds": round6(m.db_execute_seconds),
        "memory_cache_hit_count": m.memory_cache_hit_count,
        "remote_cache_hit_count": m.remote_cache_hit_count,
        "mcts_e2e_seconds": round6(m.mcts_e2e_seconds),
    }

    if m.early_stop_reason:
        metrics_dict["mcts_early_stop_reason"] = m.early_stop_reason
        if m.early_stop_rollout is not None:
            metrics_dict["mcts_early_stop_rollout"] = m.early_stop_rollout
        if m.early_stop_detail:
            metrics_dict["mcts_early_stop_detail"] = m.early_stop_detail

    out: Dict[str, Any] = {
        "query_digest": result.query_digest,
        "solutions": solutions_list,
        "plan_digest_cache": dict(result.plan_digest_cache),
        "mcts_tree_nodes": dict(result.tree_dump),
        "performance_metrics": metrics_dict,
    }

    # Gated blocks — default to emitting both for backward compat when no
    # config is supplied.
    include_es = True if config is None else bool(config.include_early_stopping_metrics)
    include_ea = True if config is None else bool(config.include_explain_analyze_info)
    if include_es:
        out["early_stopping_metrics"] = dict(result.early_stopping_metrics)
    if include_ea:
        out["explain_analyze_info"] = dict(result.explain_analyze_info)
    return out
