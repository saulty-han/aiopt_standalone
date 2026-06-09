"""
mcts.search - The MCTS search engine.

Orchestrates the full MCTS loop: selection → expansion → LLM generation →
DB execution → backpropagation. All side-effects (LLM calls, DB calls)
are injected via ``LLMClient`` and ``DBExecutor``.
"""
from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Optional, Tuple

from mcts.types import (
    ActionType,
    LLMStatus,
    MCTSConfig,
    MCTSInputData,
    MCTSRunMetrics,
    MCTSSearchResult,
    MCTSSolution,
    NodeStatus,
    ParsedLLMOutput,
    TR_FINAL_ANSWER,
    TR_DEPTH_EXCEEDED,
    TR_INEFFECTIVE_HINTS,
    TR_LLM_ERROR,
    TR_EXECUTION_ERROR,
)
from mcts.utils.utils import compute_reward
from mcts.tree import (
    TreeNode,
    collect_ancestor_hints,
    collect_partial_solution_text,
    collect_solutions,
    create_child_nodes,
    dump_tree,
    build_explain_analyze_info,
    get_allowed_actions,
    select_leaf,
)
from mcts.utils.prompts import build_action_prompt
from mcts.utils.hint_utils import (
    build_sql_with_hints,
    deduplicate_hints,
    extract_hints_from_text,
)
from mcts.modules.llm_client import LLMClient
from mcts.modules.db_executor import DBExecutor
from mcts.modules.remote_plan_cache import CacheRequest
from mcts.modules.memory_plan_cache import MemoryPlanCache

from mcts import logger

import db_utils

# Number of leading query_digest chars used as the per-query log tag.
# Kept in sync with the tpcds_runner query identifier (first 16 digest chars).
_QUERY_TAG_LEN = 16
# Number of leading SQL chars logged when the search starts.
_QUERY_SQL_PREVIEW_LEN = 200


# ---------------------------------------------------------------------------
# Output parser
# ---------------------------------------------------------------------------

def parse_llm_output(raw_text: str) -> ParsedLLMOutput:
    """Parse the raw LLM output text into structured form.

    Extracts hints from the text. If no hints are found, marks as
    continue-thinking (LLM produced reasoning but no actionable hints).
    """
    hints = extract_hints_from_text(raw_text)
    return ParsedLLMOutput(
        raw_text=raw_text,
        hints=hints,
        is_continue_thinking=len(hints) == 0,
    )


# ---------------------------------------------------------------------------
# MCTS Search
# ---------------------------------------------------------------------------

class MCTSSearch:
    """Execute MCTS search for a single SQL query.

    Usage::

        search = MCTSSearch(config, input_data, llm_client, db_executor)
        result = search.run()
    """

    def __init__(
        self,
        config: MCTSConfig,
        input_data: MCTSInputData,
        llm_client: LLMClient,
        db_executor: DBExecutor,
    ) -> None:
        self._config = config
        self._input = input_data
        self._llm = llm_client
        self._db = db_executor
        self._metrics = MCTSRunMetrics()

        # Per-query log tag: leading chars of the query_digest, used to prefix
        # every Rollout/Step/Node line so logs from concurrent queries are
        # distinguishable. Best-effort — falls back to "N/A" if the digest can't
        # be computed.
        self._query_digest = self._compute_query_digest()
        self._log_tag = (self._query_digest or "N/A")[:_QUERY_TAG_LEN]

        # Plan digest cache.
        #
        # The baseline is trusted to have been measured by the upstream
        # optimizer via ``DBExecutor.execute_and_measure`` (which goes through
        # the same remote query_cache as MCTS rollouts). We therefore use
        # ``input_data.baseline_time_seconds`` / ``input_data.default_plan_digest``
        # directly here to:
        #   1. pre-populate the in-memory MemoryPlanCache so nodes that land on
        #      the baseline plan short-circuit immediately;
        #   2. configure the rollout-time DB timeout off the known baseline;
        #   3. seed the root node's plan_digest / execution_time so the tree
        #      dump and reward computation see consistent values.
        self._plan_cache = MemoryPlanCache()

        baseline_time = self._input.baseline_time_seconds
        baseline_digest = self._input.default_plan_digest

        # Create root node and seed its baseline state.
        self._root = TreeNode(c_puct=config.c_puct)
        self._root.status = NodeStatus.EXPANDED
        self._root.state.plan_digest = baseline_digest
        self._root.state.execution_time_seconds = baseline_time

        # Whether the remote cache is in play for this search.
        self._remote_cache_on = self._db.remote_cache is not None

        # Per-query reporting timeout passed into every execute_and_measure call.
        # Derived from the baseline when known (baseline * amplifier). With no
        # baseline, fall back to the remote-cache cap if the cache is on, else
        # default_plan_timeout_seconds.
        if baseline_time is not None and baseline_time > 0:
            self._query_timeout_seconds = baseline_time * self._config.timeout_amplifier + 0.01
        elif self._remote_cache_on:
            self._query_timeout_seconds = float(self._db.remote_cache.cache_timeout_seconds)
        else:
            self._query_timeout_seconds = float(self._config.default_plan_timeout_seconds)

        # Optionally tighten the remote cache timeout so that any plan slower
        # than the baseline is not waited out for the full cache timeout. Only
        # applies when caching is on and a usable baseline was supplied.
        if (
            self._remote_cache_on
            and self._config.cap_cache_timeout_by_baseline
            and baseline_time is not None
            and baseline_time > 0
        ):
            capped = self._db.remote_cache.cap_timeout(baseline_time)
            logger.info(
                self._with_query_tag(
                    f"[MCTS] cap_cache_timeout_by_baseline=True: cache timeout capped to "
                    f"min({self._config.remote_cache_timeout_seconds}s, {baseline_time:.2f}s) = {capped}s"
                )
            )

        # Register the baseline (digest, time) into the in-memory plan cache.
        self._plan_cache.init_baseline(baseline_digest, baseline_time)

        # RAG (Booster-style retrieval augmentation). Inert unless enabled.
        # Built best-effort: any failure logs and degrades to no-RAG so the
        # search always proceeds.
        self._rag_store = None
        self._rag_embedder = None
        self._rag_context = None
        if getattr(config, "rag_enabled", False):
            self._setup_rag()

    def _resolve_store_path(self) -> str:
        """Resolve the RAG store path robustly w.r.t. the working directory.

        ``rag_store_path`` defaults to the relative ``mcts_scripts/rag_data/store``.
        Absolute paths are used as-is. For relative paths we prefer anchoring at
        the project root (the parent of the ``mcts`` package, i.e. the
        aiopt_standalone dir) so the store is found regardless of the process
        CWD; if that anchored path does not exist we fall back to the path as
        given (relative to CWD), preserving the original behaviour.
        """
        import os
        configured = self._config.rag_store_path
        if os.path.isabs(configured):
            return configured
        # mcts/search.py -> mcts/ -> project root
        project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        anchored = os.path.join(project_root, configured)
        if os.path.exists(anchored):
            return anchored
        return configured

    def _setup_rag(self) -> None:
        """Load the RAG store + embedder and retrieve the per-query context once.

        Called only when ``config.rag_enabled``. The retrieved context is reused
        across the entire tree (Phase II retrieval is per-query, not per-step).
        """
        try:
            from mcts.rag.store import RAGStore
            from mcts.rag.embedder import build_embedder
            from mcts.rag.retriever import Retriever
            from mcts.rag.qconfig import query_identity
        except Exception as e:  # noqa: BLE001
            logger.warning(self._with_query_tag(f"[RAG] import failed, disabling RAG: {e}"))
            return

        resolved_path = self._resolve_store_path()
        store = RAGStore.load(resolved_path)
        if store is None or store.num_rows == 0:
            logger.warning(
                self._with_query_tag(
                    f"[RAG] store unavailable/empty at {resolved_path}; RAG off"
                )
            )
            return

        try:
            embedder = build_embedder(self._config)
            if embedder.dim and store.dim and embedder.dim != store.dim:
                logger.warning(
                    self._with_query_tag(
                        f"[RAG] embedder dim {embedder.dim} != store dim {store.dim}; RAG off"
                    )
                )
                return
            retriever = Retriever(store, embedder)
            # Use OUR normalized-SQL identity hash (not the optimizer's
            # statement digest) so it matches the keys in the offline store.
            self._query_identity = query_identity(self._input.query)
            exclude = None if self._config.rag_allow_self_retrieval else self._query_identity
            self._rag_context = retriever.retrieve(
                self._input.query,
                self._parse_execution_info(),
                self._tables_from_index_info(),
                top_k=self._config.rag_top_k,
                min_similarity=self._config.rag_min_similarity,
                exclude_query_digest=exclude,
            )
            self._rag_store = store
            self._rag_embedder = embedder
            n = len(self._rag_context.refs) if self._rag_context else 0
            logger.info(self._with_query_tag(f"[RAG] retrieved {n} reference(s) for this query"))
        except Exception as e:  # noqa: BLE001
            logger.warning(self._with_query_tag(f"[RAG] retrieval failed, disabling RAG: {e}"))
            self._rag_context = None

    def _parse_execution_info(self):
        """Parse the input execution_info_json into a dict (best-effort)."""
        import json as _json
        try:
            data = _json.loads(self._input.execution_info_json or "{}")
            return data if isinstance(data, dict) else {}
        except Exception:  # noqa: BLE001
            return {}

    def _tables_from_index_info(self):
        """Referenced table names from index_info keys (best-effort)."""
        ii = self._input.index_info
        if isinstance(ii, dict):
            return [str(t) for t in ii.keys()]
        return []

    def _warm_start_from_rag(self) -> None:
        """Seed the root with DB-validated children from retrieved hint bundles.

        Each retrieved historical hint bundle becomes one root child with A5
        (rethink) semantics — A5 uses its own hints as a complete, fresh
        combination and stays expandable, which matches replaying a full
        historical bundle. The children are filled WITHOUT an LLM call, then run
        through the same DB execution + backpropagation as any node, so a stale
        bundle simply becomes a low-reward branch that PUCT abandons (this is
        exactly Booster's "seed, then let the search refine / discard").

        Best-effort: any failure logs and leaves the tree untouched.
        """
        try:
            refs = self._rag_context.refs if self._rag_context else []
            if not refs:
                return

            # Build unique hint bundles from the references.
            seen = set()
            bundles: List[List[str]] = []
            for ref in refs:
                hints = deduplicate_hints(list(ref.qconfig.executed_hints or []))
                if not hints:
                    continue
                key = tuple(hints)
                if key in seen:
                    continue
                seen.add(key)
                bundles.append(hints)
            if not bundles:
                return

            # One A5 child per bundle, attached to the root.
            children = create_child_nodes(
                self._root,
                [ActionType.A5_RETHINK] * len(bundles),
                rollout_index=0,
                c_puct=self._config.c_puct,
            )
            self._root.status = NodeStatus.EXPANDED

            for child, hints in zip(children, bundles):
                child.state.action_type = child.action_type
                child.state.parsed_output = ParsedLLMOutput(
                    raw_text="", hints=list(hints), is_continue_thinking=False
                )
                child.state.llm_response_text = (
                    "[RAG warm-start] replay historical improving hint bundle"
                )
                # A5 uses its own hints as the full combination (no ancestors at
                # the root anyway). Mark them all as new_hints so the node is
                # executed by _execute_children.
                child.state.new_hints = list(hints)
                child.state.deleted_hints = []
                child.state.executed_hints = list(hints)

            logger.info(
                self._with_query_tag(
                    f"[RAG] warm-start: seeding {len(children)} root child(ren) "
                    f"from retrieved bundles"
                )
            )

            # Validate against the DB and backpropagate, reusing the normal path.
            self._execute_children(children)
            self._backpropagate_children(children)
            self._plan_cache.finalize_stats(self._root)
        except Exception as e:  # noqa: BLE001
            logger.warning(self._with_query_tag(f"[RAG] warm-start failed: {e}"))

    def _compute_query_digest(self) -> Optional[str]:
        """Compute the statement digest of the (hint-free) query, best-effort."""
        try:
            return db_utils.compute_statement_digest(self._db.controller, self._input.query)
        except Exception as e:
            logger.debug(f"[MCTS] Failed to compute query_digest: {e}")
            return None

    def _with_query_tag(self, msg: str) -> str:
        """Prefix a log message with the per-query tag."""
        return f"[q={self._log_tag}] {msg}"

    # ------------------------------------------------------------------
    # Main entry point
    # ------------------------------------------------------------------

    def run(self) -> MCTSSearchResult:
        """Execute the full MCTS search and return the result."""
        start_time = time.time()

        baseline_str = (
            f"{self._input.baseline_time_seconds:.4f}s"
            if self._input.baseline_time_seconds is not None
            else "N/A"
        )
        logger.info(
            self._with_query_tag(
                f"[MCTS] Search started: query_digest={self._query_digest or 'N/A'}, "
                f"baseline={baseline_str}, "
                f"default_plan={self._input.default_plan_digest or 'N/A'}, "
                f"iterations={self._config.iterations}, max_depth={self._config.max_depth}"
                f"{' (global)' if self._config.limit_global_depth else ' (per-rollout only)'}, "
                f"sql={self._input.query[:_QUERY_SQL_PREVIEW_LEN]}"
            )
        )

        # Baseline is supplied by the upstream optimizer (which already routed
        # the no-hint measurement through ``DBExecutor.execute_and_measure``).
        # No baseline measurement is performed here — both the in-memory
        # MemoryPlanCache and the query timeout were configured in __init__.

        # RAG warm-start: seed the root with DB-validated children built from
        # retrieved historical hint bundles before normal rollouts begin.
        if getattr(self._config, "rag_warm_start", False) and self._rag_context is not None:
            self._warm_start_from_rag()

        for rollout_idx in range(self._config.iterations):
            logger.info(self._with_query_tag(f"--- Rollout {rollout_idx + 1}/{self._config.iterations} ---"))
            self._run_single_rollout(rollout_idx)

            # Early stopping checks
            stop = self._check_early_stop(rollout_idx)
            if stop is not None:
                self._metrics.early_stop_reason = stop[0]
                self._metrics.early_stop_rollout = rollout_idx
                self._metrics.early_stop_detail = stop[1]
                logger.info(self._with_query_tag(f"[MCTS] Early stop at rollout {rollout_idx + 1}: {stop[0]} — {stop[1]}"))
                break

        self._metrics.mcts_e2e_seconds = time.time() - start_time
        self._metrics.finalize()

        solutions = collect_solutions(self._root, self._input.baseline_time_seconds)

        # Log search summary
        m = self._metrics
        logger.info(
            self._with_query_tag(
                f"[MCTS] Search completed in {m.mcts_e2e_seconds:.1f}s: "
                f"{len(solutions)} solutions, "
                f"LLM calls={m.llm_call_count} ({m.llm_output_seconds:.1f}s), "
                f"DB explains={m.db_explain_count}, DB executes={m.db_execute_count} ({m.db_execute_seconds:.1f}s)"
            )
        )
        if solutions:
            best = solutions[0]
            logger.info(
                self._with_query_tag(
                    f"[MCTS] Best solution: reward={best.reward:.4f}, "
                    f"time={best.execution_time_seconds:.4f}s "
                    f"(baseline={self._input.baseline_time_seconds:.4f}s, "
                    f"speedup={self._input.baseline_time_seconds / best.execution_time_seconds:.2f}x), "
                    f"hints={best.executed_hints}"
                )
            )
            for i, sol in enumerate(solutions[1:5], 2):
                logger.debug(
                    self._with_query_tag(
                        f"[MCTS] Solution #{i}: reward={sol.reward:.4f}, "
                        f"time={sol.execution_time_seconds:.4f}s, "
                        f"hints={sol.executed_hints}"
                    )
                )
        else:
            logger.info(self._with_query_tag("[MCTS] No solutions found"))

        result = MCTSSearchResult(
            query=self._input.query,
            query_digest=self._query_digest,
            baseline_time_seconds=self._input.baseline_time_seconds,
            default_plan_digest=self._input.default_plan_digest,
            solutions=solutions,
            metrics=self._metrics,
            tree_dump=dump_tree(self._root, self._input.baseline_time_seconds),
            plan_digest_cache=self._plan_cache.to_dict(),
            early_stopping_metrics=self._plan_cache.to_early_stopping_metrics(),
            explain_analyze_info=build_explain_analyze_info(self._root),
        )

        # RAG online write-back: append this search's improving solutions to
        # the store so future searches can retrieve them. Gated + best-effort.
        if getattr(self._config, "rag_write_back", False):
            self._maybe_write_back(result)

        return result

    # ------------------------------------------------------------------
    # RAG online write-back
    # ------------------------------------------------------------------

    def _maybe_write_back(self, result: "MCTSSearchResult") -> None:
        """Append improving solutions from this search into the RAG store.

        Best-effort: any failure logs a warning and is swallowed so write-back
        never affects the search result. Uses the same store path as retrieval;
        if the store/embedder weren't loaded (RAG off, or load failed), it loads
        them lazily here so write-back works even when retrieval was skipped.
        """
        try:
            from mcts.rag.store import RAGStore
            from mcts.rag.embedder import build_embedder
            from mcts.rag.qconfig import qconfigs_from_result
            from mcts.rag.schematic import build_schematics, anonymize_sql
        except Exception as e:  # noqa: BLE001
            logger.warning(self._with_query_tag(f"[RAG] write-back import failed: {e}"))
            return

        qcs = qconfigs_from_result(
            result,
            min_reward=0.0,
            max_per_query=self._config.rag_top_k * 4 if self._config.rag_top_k else 8,
        )
        if not qcs:
            logger.info(self._with_query_tag("[RAG] write-back: no improving solutions to store"))
            return

        try:
            import os
            resolved_path = self._resolve_store_path()
            embedder = self._rag_embedder or build_embedder(self._config)
            store = self._rag_store or RAGStore.load(resolved_path)
            if store is None:
                # No existing store — create a fresh one matching the embedder.
                # Write it under the project-root-anchored path when the config
                # path is relative, so it lands in a stable, predictable place.
                dim = embedder.dim or embedder.encode_one("probe").shape[0]
                store = RAGStore(dim=dim, embedder_name=embedder.name)
                if not os.path.isabs(self._config.rag_store_path):
                    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
                    resolved_path = os.path.join(project_root, self._config.rag_store_path)
            if embedder.dim and store.dim and embedder.dim != store.dim:
                logger.warning(
                    self._with_query_tag(
                        f"[RAG] write-back skipped: embedder dim {embedder.dim} "
                        f"!= store dim {store.dim}"
                    )
                )
                return

            tables = self._tables_from_index_info()
            execution_info = self._parse_execution_info()
            rows = []
            for qc in qcs:
                if not qc.query_template:
                    qc.query_template = anonymize_sql(qc.query_text)
                schs = build_schematics(qc.query_text, execution_info, tables or qc.tables)
                texts = [s.text for s in schs.values()]
                stypes = list(schs.keys())
                vecs = embedder.encode(texts)
                for i, stype in enumerate(stypes):
                    rows.append((qc, stype, vecs[i]))

            added = store.upsert(rows)
            store.save(resolved_path)
            logger.info(
                self._with_query_tag(
                    f"[RAG] write-back: stored {len(qcs)} qconfig(s), "
                    f"{added} new row(s); store now rows={store.num_rows} "
                    f"qconfigs={store.num_qconfigs}"
                )
            )
        except Exception as e:  # noqa: BLE001
            logger.warning(self._with_query_tag(f"[RAG] write-back failed: {e}"))

    # ------------------------------------------------------------------
    # Single rollout
    # ------------------------------------------------------------------

    def _run_single_rollout(self, rollout_idx: int) -> None:
        """Execute one full rollout: walk a single chain from root to terminal.

        Each rollout explores exactly one path through the tree. Selection in
        subsequent steps starts from the node that was just expanded (not from
        root), so the rollout naturally grows deeper along a single chain.
        """
        current_node = self._root
        for step in range(self._config.max_depth):
            logger.info(self._with_query_tag(f"  Step {step + 1}/{self._config.max_depth}"))

            # 1. Selection: find a leaf node starting from current position
            leaf = select_leaf(current_node)
            if leaf is None:
                logger.info(self._with_query_tag("  No expandable leaf found, ending rollout"))
                break

            logger.debug(
                self._with_query_tag(
                    f"  Selected leaf: tag={leaf.tag}, depth={leaf.depth}, "
                    f"status={leaf.status.value}, action={leaf.action_type}"
                )
            )

            # 2. Expansion: create child nodes for allowed actions.
            #    Mark the leaf as EXPANDED — it now has children.
            actions = get_allowed_actions(leaf)
            children = create_child_nodes(
                leaf, actions, rollout_idx, self._config.c_puct
            )

            if not children:
                break

            leaf.status = NodeStatus.EXPANDED
            logger.info(
                self._with_query_tag(
                    f"  Expanded {leaf.tag}: {len(children)} children "
                    f"[{', '.join(c.action_type.value for c in children)}]"
                )
            )

            # 3. LLM generation: build prompts and call LLM in parallel
            self._generate_and_fill_children(children)

            # 4. DB execution: get execution times for all children
            self._execute_children(children)

            # 5. Backpropagation: only terminal nodes propagate rewards
            self._backpropagate_children(children)

            # 6. Finalize plan digest stats (root_children_stats) after backprop
            self._plan_cache.finalize_stats(self._root)

            # Log step summary
            status_summary = {}
            for c in children:
                key = f"{c.status.value}/{c.terminal_reason}" if c.terminal_reason else c.status.value
                status_summary[key] = status_summary.get(key, 0) + 1
            logger.info(
                self._with_query_tag(f"  Step {step + 1} done: children status={status_summary}")
            )

            # 7. Continue deeper from the expanded leaf (now has children).
            #    Next select_leaf(leaf) will pick the best child to go deeper.
            current_node = leaf

    # ------------------------------------------------------------------
    # Step 3: LLM generation (parallel)
    # ------------------------------------------------------------------

    def _generate_and_fill_children(self, children: List[TreeNode]) -> None:
        """Build prompts for children, call LLM in parallel, and fill node states."""
        prompts: List[Tuple[TreeNode, str]] = []
        for child in children:
            partial = collect_partial_solution_text(child.parent)
            prompt = build_action_prompt(
                action=child.action_type,
                query=self._input.query,
                execution_info=self._input.execution_info_json,
                candidate_hints=self._input.candidate_hints,
                index_info=self._input.index_info,
                partial_solution=partial,
                step_number=child.depth,
                rag_refs=self._rag_context,
            )
            prompts.append((child, prompt))

        # Parallel LLM calls
        with ThreadPoolExecutor(max_workers=min(len(prompts), 8)) as executor:
            futures = {
                executor.submit(self._llm.complete, prompt): (node, prompt)
                for node, prompt in prompts
            }
            for future in as_completed(futures):
                node, prompt_text = futures[future]
                completion = future.result()

                # Record metrics
                self._metrics.record_llm_call(
                    completion.input_chars,
                    completion.output_chars,
                    completion.latency_seconds,
                )

                # Parse output
                parsed = parse_llm_output(completion.text)
                node.state.llm_request_text = prompt_text
                node.state.llm_response_text = completion.text
                node.state.llm_input_chars = completion.input_chars
                node.state.llm_output_chars = completion.output_chars
                node.state.llm_latency_seconds = completion.latency_seconds
                node.state.parsed_output = parsed
                node.state.action_type = node.action_type

                # Compute accumulated hints
                ancestor_hints = collect_ancestor_hints(node)
                deduped_ancestor = deduplicate_hints(ancestor_hints)
                ancestor_set = set(deduped_ancestor)

                if node.action_type in (ActionType.A5_RETHINK, ActionType.A6_ANSWER):
                    # A5/A6: only use their own hints (fresh combination)
                    all_hints = deduplicate_hints(parsed.hints)
                else:
                    # A1-A4: accumulate with ancestor hints
                    all_hints = deduplicate_hints(ancestor_hints + parsed.hints)

                # new_hints = only the hints genuinely added at this step
                new_hints = [h for h in all_hints if h not in ancestor_set]
                # deleted_hints = ancestor hints dropped at this step (e.g. A5/A6 override)
                deleted_hints = [h for h in deduped_ancestor if h not in set(all_hints)]

                node.state.new_hints = new_hints
                node.state.deleted_hints = deleted_hints
                node.state.executed_hints = all_hints

                # Mark status: after LLM fills a node, it is not yet "expanded"
                # (expansion = creating child nodes). Nodes stay PENDING until
                # DB execution determines their fate. Only LLM error is set here.
                if completion.status in (LLMStatus.UNAVAILABLE, LLMStatus.HTTP_ERROR, LLMStatus.RATE_LIMIT_EXCEEDED):
                    node.status = NodeStatus.TERMINAL
                    node.terminal_reason = TR_LLM_ERROR
                    logger.error(
                        self._with_query_tag(
                            f"  Node {node.tag} ({node.action_type.value}): LLM error "
                            f"[{completion.status.value}] — {completion.text[:200]!r}"
                        )
                    )
                else:
                    logger.info(
                        self._with_query_tag(
                            f"  Node {node.tag} ({node.action_type.value}): "
                            f"LLM done in {completion.latency_seconds:.1f}s, "
                            f"request_chars={completion.input_chars}, output_chars={completion.output_chars}, "
                            f"total_hints={len(all_hints)}, "
                            f"continue_thinking={parsed.is_continue_thinking}"
                        )
                    )

    # ------------------------------------------------------------------
    # Step 4: DB execution (sequential)
    # ------------------------------------------------------------------

    def _execute_children(self, children: List[TreeNode]) -> None:
        """Execute DB operations for all children.

        Sets terminal status for nodes that don't need DB execution.
        Delegates to _execute_single_node for nodes with hints.
        """
        nodes_to_execute: List[TreeNode] = []
        for child in children:
            # LLM error already marked TERMINAL in _generate_and_fill_children
            if child.is_terminal:
                continue
            if not child.state.new_hints and not child.state.deleted_hints:
                if self._config.limit_global_depth and child.depth >= self._config.max_depth:
                    child.status = NodeStatus.TERMINAL
                    child.terminal_reason = TR_DEPTH_EXCEEDED
                    child.reward = 0
                    logger.debug(self._with_query_tag(f"  Node {child.tag}: no hints at max_depth → TERMINAL/{TR_DEPTH_EXCEEDED}"))
                # A6 (answer) is always terminal even without executed hints
                elif child.action_type == ActionType.A6_ANSWER:
                    child.status = NodeStatus.TERMINAL
                    child.terminal_reason = TR_FINAL_ANSWER
                    child.reward = self._config.negative_reward
                    child.state.execution_time_seconds = self._input.baseline_time_seconds
                    logger.debug(self._with_query_tag(f"  Node {child.tag}: A6 with no hints → TERMINAL/{TR_FINAL_ANSWER}"))
                else:
                    child.reward = 0
                    logger.debug(self._with_query_tag(f"  Node {child.tag}: no hints, stays PENDING"))
                # stays PENDING — can be selected and expanded later
                continue
            nodes_to_execute.append(child)

        if not nodes_to_execute:
            logger.info(self._with_query_tag("  No nodes to execute DB for (all filtered out)"))
            return

        logger.info(self._with_query_tag(f"  Executing DB for {len(nodes_to_execute)} nodes"))

        # Sequential DB execution (DBController uses thread-local sessions,
        # so parallel execution from worker threads would fail)
        for node in nodes_to_execute:
            self._execute_single_node(node)

    def _execute_single_node(self, node: TreeNode) -> None:
        """Execute DB for a single node: get plan digest and execution time."""
        sql, hints = self._input.query, node.state.executed_hints
        sql_with_hints = build_sql_with_hints(sql, hints)

        # Step 1: Get plan digest
        explain_result = self._db.get_plan_digest(sql_with_hints)
        if not explain_result.is_success:
            node.status = NodeStatus.TERMINAL
            node.terminal_reason = TR_EXECUTION_ERROR
            node.state.db_result = explain_result
            logger.error(
                self._with_query_tag(f"  Node {node.tag}: EXPLAIN failed — {explain_result.error}")
            )
            return

        plan_digest = explain_result.plan_digest
        node.state.plan_digest = plan_digest

        # Validate hint effectiveness: if plan_digest == parent's, hint had no effect
        parent_digest = node.parent.state.plan_digest
        # Mimic old mcts behaviour
        if node.depth > 1 and plan_digest and parent_digest and plan_digest == parent_digest:
            node.status = NodeStatus.TERMINAL
            node.terminal_reason = TR_INEFFECTIVE_HINTS
            node.reward = 0
            node.state.execution_time_seconds = node.parent.state.execution_time_seconds
            logger.debug(
                self._with_query_tag(
                    f"  Node {node.tag} ({node.action_type.value}): "
                    f"plan unchanged (digest={plan_digest[:16]}...) → TERMINAL/{TR_INEFFECTIVE_HINTS}"
                )
            )
            return

        # Step 2: Get execution time (from cache or real execution)
        exec_time: Optional[float] = None
        root_child_tag = self._get_root_child_tag(node)
        cached = self._plan_cache.lookup(plan_digest)

        if cached is not None:
            exec_time = self._plan_cache.record_hit(plan_digest, node.rollout_index)
            node.state.execution_time_seconds = exec_time
            node.state.db_result = explain_result
            if self._metrics:
                self._metrics.record_memory_cache_hit()
            logger.debug(
                self._with_query_tag(
                    f"  Node {node.tag}: plan cache hit, "
                    f"digest={plan_digest[:16]}..., cached_time={exec_time:.4f}s"
                )
            )
        else:
            # Real execution. Always describe the cache request; the executor
            # ignores it when it has no remote cache attached, so the routing
            # decision lives in one place (the executor), not here.
            exec_result = self._db.execute_and_measure(
                sql_with_hints,
                timeout_seconds=self._query_timeout_seconds,
                cache_request=CacheRequest(query_sql=sql, hints=hints),
            )
            node.state.db_result = exec_result
            if exec_result.is_success:
                exec_time = exec_result.execution_time_seconds
                node.state.execution_time_seconds = exec_time
                node.state.plan_digest = exec_result.plan_digest or plan_digest
                if plan_digest and exec_time is not None:
                    self._plan_cache.register_new(
                        plan_digest, exec_time, node.rollout_index,
                        root_child_tag, node,
                    )
                    node.state.new_plan_first_found = True
                logger.debug(
                    self._with_query_tag(
                        f"  Node {node.tag}: executed, "
                        f"digest={plan_digest[:16]}..., time={exec_time:.4f}s"
                    )
                )
            elif exec_result.is_timeout:
                # Mimic old mcts behaviour: treat timeout as a successful
                # execution whose time equals the timeout threshold. This keeps
                # the node PENDING (expandable) and lets reward calculation
                # produce a negative signal instead of discarding the branch.
                timeout_seconds = (
                    self._input.baseline_time_seconds * self._config.timeout_amplifier
                )
                exec_time = timeout_seconds
                node.state.execution_time_seconds = exec_time
                node.state.plan_digest = plan_digest
                if plan_digest:
                    self._plan_cache.register_new(
                        plan_digest, exec_time, node.rollout_index,
                        root_child_tag, node,
                    )
                    node.state.new_plan_first_found = True
                logger.info(
                    self._with_query_tag(
                        f"  Node {node.tag}: execution timeout, "
                        f"using timeout value as exec_time={exec_time:.4f}s"
                    )
                )
            else:
                node.status = NodeStatus.TERMINAL
                node.terminal_reason = TR_EXECUTION_ERROR
                logger.error(
                    self._with_query_tag(f"  Node {node.tag}: execution failed — {exec_result.error}")
                )
                return

        # Compute reward
        reward = compute_reward(self._input.baseline_time_seconds, exec_time)
        node.reward = reward if reward is not None else self._config.negative_reward

        # Determine terminal status
        if node.action_type == ActionType.A6_ANSWER:
            # A6 is the final-answer action — always terminal, no further expansion
            node.status = NodeStatus.TERMINAL
            node.terminal_reason = TR_FINAL_ANSWER
        elif self._config.limit_global_depth and node.depth >= self._config.max_depth:
            # At max depth with valid new plan (global-depth limit enabled)
            node.status = NodeStatus.TERMINAL
            node.terminal_reason = TR_DEPTH_EXCEEDED
        else:
            # Has valid execution with a new plan; stays PENDING (can be
            # selected and expanded later — EXPANDED is set only when
            # child nodes are actually created for this node).
            pass

        # Log the result at INFO level — this is a key event
        speedup = self._input.baseline_time_seconds / exec_time if exec_time and exec_time > 0 else 0
        logger.info(
            self._with_query_tag(
                f"  Node {node.tag} ({node.action_type.value}): "
                f"reward={node.reward:.4f}, time={exec_time:.4f}s, "
                f"speedup={speedup:.2f}x, status={node.status.value}, "
                f"hints={node.state.executed_hints}"
            )
        )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _get_root_child_tag(node: TreeNode) -> Optional[str]:
        """Walk up to find the direct child of root and return its tag."""
        current = node
        while current.parent is not None and not current.parent.is_root:
            current = current.parent
        return current.tag if current.parent is not None else None

    # ------------------------------------------------------------------
    # Step 5: Backpropagation
    # ------------------------------------------------------------------

    def _backpropagate_children(self, children: List[TreeNode]) -> None:
        """Update rewards for children and backpropagate terminal nodes.

        - TERMINAL nodes (except ``ineffective_hints``): backpropagate value up to root.
        - Non-terminal nodes with a reward (e.g. ``new_plan_first_found``):
          update only the node itself (no propagation to ancestors).
        - ``ineffective_hints`` nodes: skipped entirely.
        """
        propagated = 0
        for child in children:
            value = child.reward if child.reward is not None else None
            if value is None:
                continue
            # Mimic old mcts behaviour
            if (child.is_terminal and child.terminal_reason == TR_FINAL_ANSWER) or child.state.new_plan_first_found:
                child.backpropagate(value)
                propagated += 1
            else:
                # Non-terminal with reward: update only this node's stats
                child.update(value)
        if propagated > 0:
            logger.debug(
                self._with_query_tag(
                    f"  Backpropagated {propagated}/{len(children)} terminal nodes, "
                    f"root visit_count={self._root.visit_count}, q_value={self._root.q_value:.4f}"
                )
            )

    # ------------------------------------------------------------------
    # Early stopping
    # ------------------------------------------------------------------

    def _check_early_stop(self, rollout_idx: int) -> Optional[Tuple[str, str]]:
        """Check if MCTS should stop early. Returns (reason, detail) or None."""
        # 1. Plan time threshold
        threshold = self._config.plan_time_threshold_seconds
        if threshold > 0:
            solutions = collect_solutions(self._root, self._input.baseline_time_seconds)
            for sol in solutions:
                if sol.execution_time_seconds is not None and sol.execution_time_seconds < threshold:
                    return (
                        "plan_time",
                        f"best_plan_time={sol.execution_time_seconds:.4f}s < threshold={threshold}s",
                    )

        # 2. Token budget
        budget = self._config.estimated_tokens_budget
        if budget > 0:
            total_chars = self._metrics.llm_input_chars + self._metrics.llm_output_chars
            estimated_tokens = total_chars / 2.5
            if estimated_tokens > budget:
                return (
                    "estimated_tokens",
                    f"estimated_tokens={estimated_tokens:.0f} > budget={budget}",
                )

        return None
