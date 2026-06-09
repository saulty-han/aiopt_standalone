"""
LLM Optimizer using MCTS

Extends BasicOptimizer with MCTS-based LLM candidate generation.
MCTS candidates are evaluated through the standard BasicOptimizer pipeline
(multi-sample validation, relaxed intersection policy, reliability filtering).
"""

import json
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

from sqlalchemy import text as sql_text

from ai_logger import aiopt_logger
from optimizer.basic_optimizer import BasicOptimizer, OptimizationContext, CandidatePlan
from optimizer import query_prep
from ai_config import TrainingParameters
import db_utils
import hints_generator


class LLMOptimizer(BasicOptimizer):
    """
    LLM-based optimizer using MCTS.

    Extends BasicOptimizer by generating additional candidates through
    MCTS-based LLM reasoning. All candidates (Default + SPM + MCTS)
    go through the standard evaluation pipeline.
    """

    def __init__(self, context: OptimizationContext):
        super().__init__(context)

        from ai_config import MCTSConfig
        self.mcts_custom_cfg = MCTSConfig.custom_cfg
        self.mcts_output_dir = MCTSConfig.output_dir
        self.mcts_llm_api_url_key = MCTSConfig.llm_api_url_key
        self.mcts_iterations = MCTSConfig.iterations
        self.mcts_explain_timeout_seconds = MCTSConfig.explain_timeout_seconds
        self.mcts_stop_mcts_search_plan_time_threshold_seconds = MCTSConfig.stop_mcts_search_plan_time_threshold_seconds
        self.mcts_stop_mcts_search_estimated_tokens_budget = MCTSConfig.stop_mcts_search_estimated_tokens_budget

        self.mcts_base_dir = Path(__file__).parent.parent / "mcts"

        aiopt_logger.info(f"[LLMOptimizer] MCTS config: custom_cfg={self.mcts_custom_cfg}")

        # Store MCTS results for later retrieval (to be written to database by task_executor)
        self.mcts_results: Optional[List[Dict]] = None

        # Add project root to sys.path so mcts can be imported as a package
        project_root = Path(__file__).parent.parent
        if str(project_root) not in sys.path:
            sys.path.insert(0, str(project_root))

    def get_optimizer_name(self) -> str:
        return "LLMOptimizer"

    def _collect_additional_candidates(
        self,
        db: str,
        digest: str,
        sql_samples: list[str],
    ) -> list[CandidatePlan]:
        """Collect MCTS-generated candidate plans.

        Runs the MCTS pipeline, stores raw results in self.mcts_results
        for data collection, converts solutions to CandidatePlan objects
        for standard evaluation.
        """
        candidates: list[CandidatePlan] = []
        representative_sql = sql_samples[0]

        try:
            db_utils.set_explain_json_format_v2(self.context.training_controller)
        except Exception as e:
            aiopt_logger.warning(f"Failed to set explain_json_format_version=2: {e}")

        try:
            start_t = time.time()

            # 1. Run MCTS directly over the raw sql_samples — baseline /
            #    execution_info / plan_digest are obtained inside the loop via
            #    DBExecutor.execute_and_measure (cache-aware), so no separate
            #    qdf_builder step is needed.
            mcts_results = self._run_mcts(sql_samples, db, f"[{db}/{digest}]")
            if not mcts_results:
                aiopt_logger.warning(f"[{db}/{digest}] MCTS optimization returned no results")
                return []

            self.training_time += time.time() - start_t

            # 2. Store raw results
            self.mcts_results = mcts_results

            # 3. Save output JSON
            self._save_output_json(mcts_results, db, digest, f"[{db}/{digest}]")

            # 4. Filter and convert to CandidatePlan
            self.context.training_controller.use_db(db)
            supports_hints_extraction = self.context.feature_flags.supports_hints_extraction
            better_plan_ratio = TrainingParameters.better_plan_ratio
            max_candidates = 20

            # 5. Pre-filter solutions
            all_filtered = []
            for mcts_result in mcts_results:
                baseline_time = mcts_result.get("baseline_time")
                if baseline_time is None or baseline_time <= 0:
                    continue
                solutions = mcts_result.get("solutions", [])

                for solution in solutions:
                    executed_hints = solution.get("executed_hints", [])
                    if not executed_hints:
                        continue
                    execution_time = solution.get("execution_time_s")
                    if execution_time is None:
                        continue
                    improvement_ratio = (baseline_time - execution_time) / baseline_time
                    if improvement_ratio < better_plan_ratio:
                        continue
                    all_filtered.append((execution_time, solution))

            all_filtered.sort(key=lambda x: x[0])
            selected = all_filtered[:max_candidates]

            # 6. Convert to CandidatePlan
            seen_pids: set[str] = set()
            for exec_time, solution in selected:
                executed_hints = solution.get("executed_hints", [])
                hints_text = " ".join(executed_hints)
                if hints_text:
                    hints_text = f"/*+ {hints_text} */"

                try:
                    final_hints = None
                    if supports_hints_extraction:
                        rewritten_sql = hints_generator.insert_multiple_raw_hints(representative_sql, hints_text)
                        pid, extracted_outline = db_utils.get_plan_id_and_outline(
                            self.context.training_controller, rewritten_sql,
                            extract_outline=True, explain_timeout_seconds=10
                        )
                        if extracted_outline:
                            final_hints = extracted_outline

                    if not final_hints:
                        final_hints = hints_text
                        rewritten_sql = hints_generator.insert_raw_hints(representative_sql, hints_text)
                        pid, _ = db_utils.get_plan_id_and_outline(
                            self.context.training_controller, rewritten_sql,
                            extract_outline=False, explain_timeout_seconds=10
                        )

                    if pid and pid not in seen_pids:
                        seen_pids.add(pid)
                        candidates.append(CandidatePlan(
                            plan_id=pid,
                            hints_text=final_hints,
                            indexes_dict={},
                            source_sql=rewritten_sql,
                        ))
                except Exception as e:
                    aiopt_logger.debug(
                        f"[{db}/{digest}] Failed to get plan_id for MCTS solution: {e}"
                    )
                    continue

            aiopt_logger.info(
                f"[{db}/{digest}] MCTS: {len(all_filtered)} solutions passed filter "
                f"(ratio>={better_plan_ratio}), selected {len(selected)}, "
                f"produced {len(candidates)} unique candidate plans"
            )

        except Exception as e:
            aiopt_logger.error(f"[{db}/{digest}] MCTS candidate collection error: {e}", exc_info=True)

        return candidates

    # ========== MCTS-specific methods ==========

    def _run_mcts(self, sql_samples: List[str], db: str, log_prefix: str) -> Optional[List[Dict]]:
        """Run MCTS optimization for each sample SQL.

        For each SQL we:
          1. Build a fresh ``DBExecutor`` (cache-aware, scoped to ``db``).
          2. Use ``DBExecutor.execute_and_measure`` for the baseline probe — the
             same code path MCTS rollouts use — so the baseline plan is also
             written to / reused from the remote ``query_cache``.
          3. Collect candidate_hints / index_info via :mod:`optimizer.query_prep`.
          4. Hand the prepared ``MCTSInputData`` and the same DBExecutor to
             ``run_mcts_for_query``; ``MCTSSearch`` no longer probes again.
        """
        try:
            project_root = Path(__file__).parent.parent
            if str(project_root) not in sys.path:
                sys.path.insert(0, str(project_root))

            from mcts.types import MCTSInputData
            from mcts.config.config_loader import load_mcts_config
            from mcts.modules.db_executor import DBExecutor
            from mcts.modules.remote_plan_cache import build_remote_plan_cache, CacheRequest
            from mcts.solver import run_mcts_for_query, convert_search_result_to_dict

            controller = self.context.training_controller
            try:
                controller.use_db(db)
            except Exception as e:
                aiopt_logger.warning(f"{log_prefix} Failed to switch to db {db}: {e}")

            # Build config via config loader (YAML defaults → TOML → explicit overrides)
            config = load_mcts_config(
                custom_yaml_path=self.mcts_custom_cfg,
                toml_overrides={
                    "llm_api_url_key": self.mcts_llm_api_url_key or [],
                    "iterations": self.mcts_iterations,
                    "plan_time_threshold_seconds": self.mcts_stop_mcts_search_plan_time_threshold_seconds,
                    "estimated_tokens_budget": self.mcts_stop_mcts_search_estimated_tokens_budget,
                    "default_plan_timeout_seconds": TrainingParameters.default_plan_timeout_seconds,
                    "explain_timeout_seconds": self.mcts_explain_timeout_seconds,
                },
            )

            results: List[Dict] = []

            for idx, query in enumerate(sql_samples):
                query_log_prefix = f"{log_prefix}[{idx + 1}/{len(sql_samples)}]"

                # 1. Measure the baseline (no-hint) plan via DBExecutor. The
                #    timeout is the cache timeout when the remote cache is on
                #    (entries are shared across callers); otherwise
                #    default_plan_timeout_seconds, since this is the first time the
                #    baseline is measured.
                #    MCTSSearch later derives the per-rollout timeout from it.
                #    Only build the remote cache when it's enabled — when the
                #    switch is off we skip the call entirely instead of relying on
                #    build_remote_plan_cache's internal check.
                remote_cache = (
                    build_remote_plan_cache(
                        controller=controller,
                        db_name=db,
                        cache_timeout_seconds=config.remote_cache_timeout_seconds,
                        enabled=config.remote_cache_enabled,
                    )
                    if config.remote_cache_enabled
                    else None
                )
                db_executor = DBExecutor(
                    controller=controller,
                    explain_timeout_seconds=config.explain_timeout_seconds,
                    metrics=None,
                    remote_cache=remote_cache,
                )
                remote_cache_on = db_executor.remote_cache is not None
                baseline_timeout_seconds = (
                    db_executor.remote_cache.cache_timeout_seconds
                    if remote_cache_on
                    else TrainingParameters.default_plan_timeout_seconds
                )

                baseline_time: Optional[float] = None
                plan_digest: Optional[str] = None
                execution_info: Dict[str, Any] = {}
                try:
                    # Warm up the baseline plan once before the real measurement.
                    # Only serves to warm the buffer pool so the baseline timing
                    # below is not skewed by a cold cache — result is discarded,
                    # errors are swallowed (warmup must never block optimization).
                    try:
                        controller.evaluate_elapsed_time_with_result(
                            sql_text(f"EXPLAIN ANALYZE FORMAT=JSON {query}"),
                            timeout_seconds=baseline_timeout_seconds,
                            return_on_timeout=True,
                        )
                    except Exception as warmup_err:
                        aiopt_logger.warning(
                            f"{query_log_prefix} Baseline warmup failed (ignored): {warmup_err}"
                        )

                    baseline_result = db_executor.execute_and_measure(
                        query,
                        timeout_seconds=baseline_timeout_seconds,
                        cache_request=CacheRequest(query_sql=query, hints=[]),
                    )
                    if baseline_result.plan_digest:
                        plan_digest = baseline_result.plan_digest
                    if baseline_result.execution_time_seconds is not None:
                        baseline_time = float(baseline_result.execution_time_seconds)
                    execution_info = query_prep.parse_execution_info(
                        baseline_result.explain_analyze_json
                    )
                    if baseline_result.is_success:
                        digest_str = plan_digest[:16] if plan_digest else "N/A"
                        time_str = f"{baseline_time:.4f}s" if baseline_time is not None else "N/A"
                        aiopt_logger.info(
                            f"{query_log_prefix} Baseline ok: "
                            f"digest={digest_str}..., time={time_str}"
                        )
                    elif baseline_result.is_timeout:
                        digest_str = plan_digest[:16] if plan_digest else "N/A"
                        aiopt_logger.warning(
                            f"{query_log_prefix} Baseline timed out: "
                            f"digest={digest_str}..., time={baseline_time}s"
                        )
                    else:
                        aiopt_logger.warning(
                            f"{query_log_prefix} Baseline error: {baseline_result.error}"
                        )
                except Exception as e:
                    aiopt_logger.warning(
                        f"{query_log_prefix} Baseline measurement raised: {e}"
                    )

                if baseline_time is None or baseline_time <= 0:
                    aiopt_logger.warning(
                        f"{query_log_prefix} Skipping: no usable baseline_time"
                    )
                    continue

                ei_len = query_prep.execution_info_char_len(execution_info)
                if ei_len > config.max_execution_info_chars:
                    aiopt_logger.warning(
                        f"{query_log_prefix} Skipping: execution_info too large "
                        f"(len={ei_len} > {config.max_execution_info_chars})"
                    )
                    continue

                # 2. Build candidate hints and index info (no DB execution here
                #    beyond EXPLAIN-style metadata queries).
                try:
                    candidate_hints, table_names = query_prep.get_candidate_hints(
                        controller, db, query
                    )
                    index_info = query_prep.get_index_info(controller, table_names) or {}
                except Exception as e:
                    aiopt_logger.warning(
                        f"{query_log_prefix} Failed to build candidate hints / index info: {e}"
                    )
                    candidate_hints = {"index": [], "join_order": [], "config": []}
                    index_info = {}

                # 3. Per-query result skeleton (mirrors the previous qdf_data
                #    fields downstream tooling still consumes).
                result_row: Dict[str, Any] = {
                    "index": idx,
                    "db_name": db,
                    "query": query,
                    "baseline_time": baseline_time,
                    "execution_info": execution_info,
                    "candidate_hints": candidate_hints,
                    "index_info": index_info,
                }
                if plan_digest:
                    result_row["plan_digest"] = plan_digest

                # 4. Run MCTS, reusing the executor we already used for the
                #    baseline probe.
                try:
                    input_data = MCTSInputData(
                        query=query,
                        baseline_time_seconds=baseline_time,
                        execution_info_json=json.dumps(execution_info),
                        candidate_hints=candidate_hints,
                        default_plan_digest=plan_digest,
                        index_info=index_info,
                    )

                    aiopt_logger.info(
                        f"{query_log_prefix} Running MCTS for query: {query[:80]}..."
                    )

                    search_result = run_mcts_for_query(
                        config, input_data, controller, db, db_executor=db_executor,
                    )

                    result_dict = convert_search_result_to_dict(search_result)

                    if result_dict.get("query_digest"):
                        result_row["query_digest"] = result_dict["query_digest"]
                    result_row["mcts_tree_nodes"] = result_dict["mcts_tree_nodes"]
                    result_row["solutions"] = result_dict["solutions"]
                    result_row["plan_digest_cache"] = result_dict["plan_digest_cache"]
                    result_row["early_stopping_metrics"] = result_dict.get(
                        "early_stopping_metrics", {}
                    )
                    result_row["performance_metrics"] = result_dict["performance_metrics"]
                    results.append(result_row)

                    aiopt_logger.info(
                        f"{query_log_prefix} MCTS completed, "
                        f"metrics={result_dict['performance_metrics']}"
                    )

                except Exception as e:
                    aiopt_logger.error(
                        f"{query_log_prefix} Error running MCTS for query: {e}",
                        exc_info=True,
                    )
                    result_row["mcts_tree_nodes"] = {}
                    result_row["solutions"] = []
                    result_row["plan_digest_cache"] = {}
                    result_row["early_stopping_metrics"] = {}
                    result_row["performance_metrics"] = {
                        "error": str(e),
                        "llm_call_count": 0,
                        "llm_output_chars": 0,
                        "llm_output_seconds": 0.0,
                        "llm_chars_per_second": 0.0,
                        "llm_input_chars": 0,
                        "db_explain_count": 0,
                        "db_execute_count": 0,
                        "db_execute_seconds": 0.0,
                        "mcts_e2e_seconds": 0.0,
                    }
                    results.append(result_row)

            aiopt_logger.info(f"{log_prefix} MCTS completed, {len(results)} results")
            return results

        except Exception as e:
            aiopt_logger.error(f"{log_prefix} Error running MCTS: {e}", exc_info=True)
            return None

    def _save_output_json(
        self,
        mcts_results: List[Dict],
        db: str,
        digest: str,
        log_prefix: str
    ):
        """Save MCTS output JSON to specified output directory."""
        from datetime import datetime

        if not self.mcts_output_dir:
            return
        try:
            output_dir = Path(self.mcts_output_dir)
            output_dir.mkdir(parents=True, exist_ok=True)

            timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
            # digest 可能形如 "q0001_<hash>"（runner 的查询标记），文件名里保留
            # 完整标记，仅做文件系统安全过滤，方便按 qXXXX 定位结果文件。
            safe_digest = "".join(
                ch if ch.isalnum() or ch in ("-", "_") else "-"
                for ch in str(digest)
            ) or "na"
            output_file = output_dir / f"{db}_{safe_digest}_{timestamp}.json"

            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(mcts_results, f, indent=2, ensure_ascii=False)

            aiopt_logger.info(f"{log_prefix} Saved MCTS output to: {output_file}")
        except Exception as e:
            aiopt_logger.warning(f"{log_prefix} Failed to save output JSON to file: {e}")
