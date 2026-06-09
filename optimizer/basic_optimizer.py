"""
Basic Optimizer

Base optimizer with shared infrastructure for SQL plan optimization.
Subclasses override _collect_additional_candidates() to provide specific
candidate plan generation strategies (hints enumeration, LLM/MCTS, etc.).

Supports:
- Dynamic feature detection (hints extraction, SPM, statement outline)
- Adaptive behavior based on instance capabilities
- Parallel execution via worker processes
"""

from dataclasses import dataclass
import json
import random
import time
from ai_logger import aiopt_logger
from data_models import WorkloadRowAggregated, DecidedRule, OutlineType, RuleAction, TrainingEnvType, InstanceInfo, FeatureFlags
from feature_detector import detect_features
from db_controller import DBController
from controller.validation_logs_controller import ValidationLogEntry
from sqlalchemy import text
import db_utils
import hints_generator
from ai_config import TrainingParameters
from config.config import GlobalConfig


@dataclass
class OptimizationContext:
    """
    Optimization context with feature flags and environment information.

    Attributes:
        task_id: Task ID
        instance_id: Instance ID
        outline_type: Outline type (statement_outline or spm)
        training_controller: Training environment DBController
        env_type: Training environment type
        feature_flags: Feature flags from dynamic detection
        instance_info: Instance info
    """
    task_id: str
    instance_id: str
    outline_type: OutlineType
    training_controller: DBController
    env_type: TrainingEnvType
    feature_flags: FeatureFlags
    instance_info: InstanceInfo


@dataclass
class CandidatePlan:
    """候选计划"""
    plan_id: str        # 对应的 PlanID (基于 representative SQL)
    hints_text: str     # hints 文本
    indexes_dict: dict  # 索引字典
    source_sql: str = "" # SQL used to generate this plan (for debugging)
    

def _is_better_plan(elapsed_time: float, default_elapsed_time: float) -> bool:
    """
    判断候选计划相比默认计划是否有显著改进。
    
    使用配置文件中的 better_plan_ratio 阈值判断，
    例如 0.2 表示需要至少 20% 的性能提升才算"显著改进"。
    
    :param elapsed_time: 候选计划执行时间
    :param default_elapsed_time: 默认计划执行时间
    :return: 是否有显著改进
    """
    if default_elapsed_time <= 0:
        return False
    
    improvement_ratio = (default_elapsed_time - elapsed_time) / default_elapsed_time
    return improvement_ratio >= TrainingParameters.better_plan_ratio


class BasicOptimizer:
    """
    Basic Optimizer: Shared infrastructure for SQL plan optimization.

    Provides the full optimization pipeline (validation, candidate collection,
    evaluation, decision, reliability filtering). Subclasses override
    _collect_additional_candidates() to inject specific candidate generation
    strategies.

    Behavior adapts based on feature detection:
    - If hints extraction supported: use extracted hints for SQL verification
    - If hints extraction NOT supported: use enumerated hints, skip extraction

    Note: Timeout control is handled by the main process via ParallelPoolManager.
    """

    def __init__(self, context: OptimizationContext):
        self.context = context
        self.training_time = 0.0

        # Initialize feature flags if not provided
        if self.context.feature_flags is None:
            # Detect features dynamically using feature_detector module
            self.context.feature_flags = detect_features(self.context.training_controller)

        # Verify training environment supports all features required by online
        self._verify_training_env_features()

        self._log_mode()
    
    def _verify_training_env_features(self):
        """验证训练环境支持所有 online 环境所需的 feature，否则抛出异常"""
        training_flags = detect_features(self.context.training_controller)
        if not training_flags.is_superset_of(self.context.feature_flags):
            raise RuntimeError(
                f"Training env {training_flags} missing features required by online {self.context.feature_flags}"
            )
        aiopt_logger.debug(f"[Optimizer] Training env feature verification passed: {training_flags}")
    
    def _log_mode(self):
        """Log the current operation mode based on feature flags."""
        flags = self.context.feature_flags
        if flags.supports_hints_extraction:
            aiopt_logger.info(
                "[Optimizer] Hints extraction supported: "
                "SQL verification with extracted hints enabled"
            )
        else:
            aiopt_logger.info(
                "[Optimizer] Hints extraction NOT supported: "
                "SQL verification uses enumerated hints instead"
            )
        
        aiopt_logger.info(f"[Optimizer] Training environment: {self.context.env_type.value}")

    def get_optimizer_name(self) -> str:
        return "BasicOptimizer"

    def get_optimizer_version(self) -> str:
        return "1.0.0"

    def _collect_sql_samples(self, workloads: list[WorkloadRowAggregated]) -> list[str]:
        """从 workloads 收集所有 SQL 样本（去重）"""
        samples = set()
        for w in workloads:
            if w.sql_text:
                samples.add(w.sql_text)
            if w.sql_text_min:
                samples.add(w.sql_text_min)
            if w.sql_text_max:
                samples.add(w.sql_text_max)
        return list(samples)
    
    def _collect_base_candidates(self, db: str, digest: str, sql_samples: list[str]) -> tuple[list[CandidatePlan], int]:
        """
        Collect Default (Set D) + SPM (Set S) candidate plans, deduplicated by PlanID.

        Returns:
            (candidates, default_count): candidates list and the number of distinct default plans |D|.
        """
        unique_candidates: dict[str, CandidatePlan] = {}
        feature_flags = self.context.feature_flags
        supports_hints_extraction = feature_flags.supports_hints_extraction

        # 1. Collect Defaults (Set D) - Always collected
        # If hints extraction is not supported, only get PlanID (no outline)
        aiopt_logger.debug(f"[Candidates] Collecting Default plans from {len(sql_samples)} samples (hints_extraction={supports_hints_extraction})")
        for idx, sample in enumerate(sql_samples):
            try:
                pid, outline = db_utils.get_plan_id_and_outline(
                    self.context.training_controller, sample,
                    extract_outline=supports_hints_extraction
                )
                if pid and pid not in unique_candidates:
                    unique_candidates[pid] = CandidatePlan(
                        plan_id=pid,
                        hints_text=outline,  # Will be empty if extract_outline=False
                        indexes_dict={},  # Default plan has no manual indexes dict
                        source_sql=sample  # Captured from sample
                    )
                    aiopt_logger.debug(f"[Candidates] Sample {idx+1}: new Default plan_id={pid}")
                else:
                    aiopt_logger.debug(f"[Candidates] Sample {idx+1}: duplicate plan_id={pid}, skipped")
            except Exception as e:
                aiopt_logger.debug(f"[Candidates] Sample {idx+1}: failed to extract default plan: {e}")

        aiopt_logger.debug(f"[Candidates] After Default collection: {len(unique_candidates)} unique plans")
        default_count = len(unique_candidates)  # |D|: record before SPM plans are added

        # 2. Collect SPM Captured Plans (Set S) - if SPM mode
        if self.context.outline_type == OutlineType.SPM:
            if not feature_flags.supports_spm:
                aiopt_logger.info("[Candidates] Skipping SPM plan collection (SPM not supported)")
            else:
                try:
                    spm_plans = self._get_spm_captured_plans(db, digest)
                    if spm_plans:
                        spm_added = 0
                        for plan in spm_plans:
                            if plan.plan_id not in unique_candidates:
                                unique_candidates[plan.plan_id] = plan
                                spm_added += 1
                            else:
                                # SPM plan_id already exists, check hints consistency
                                existing_plan = unique_candidates[plan.plan_id]
                                if existing_plan.hints_text != plan.hints_text:
                                    aiopt_logger.warning(
                                        f"[Candidates] SPM plan_id={plan.plan_id} already exists with different hints. "
                                        f"Existing hints: {existing_plan.hints_text[:100]}..., "
                                        f"SPM hints: {plan.hints_text[:100]}..."
                                    )
                        aiopt_logger.debug(f"[Candidates] SPM added {spm_added} new plans")
                except Exception as e:
                    aiopt_logger.warning(f"[Candidates] Failed to get SPM captured plans: {e}")

        return list(unique_candidates.values()), default_count

    def _collect_additional_candidates(
        self,
        db: str,
        digest: str,
        sql_samples: list[str],
    ) -> list[CandidatePlan]:
        """
        Collect additional candidate plans beyond Default + SPM.

        Subclasses override this method to provide specific candidate generation strategies:
        - BasicOptimizer: returns [] (only Default + SPM)
        - HintsEnumOptimizer: returns enumerated plans (index hints combinations)
        - LLMOptimizer: returns LLM-recommended plans (from MCTS)
        """
        return []

    def _collect_all_candidates(self, db: str, digest: str,
                                sql_samples: list[str]) -> tuple[list[CandidatePlan], int]:
        """
        Collect all candidate plans (Default + SPM + additional) and deduplicate by PlanID.

        Returns:
            (candidates, default_count): candidates list and the number of distinct default plans |D|.
        """
        candidates, default_count = self._collect_base_candidates(db, digest, sql_samples)
        additional = self._collect_additional_candidates(db, digest, sql_samples)

        # Cross-source dedup: base takes priority, skip additional plans with existing plan_id
        seen = {p.plan_id for p in candidates}
        additional_new = 0
        for plan in additional:
            if plan.plan_id and plan.plan_id not in seen:
                seen.add(plan.plan_id)
                candidates.append(plan)
                additional_new += 1
        if additional:
            aiopt_logger.debug(f"[Candidates] Additional candidates: {len(additional)} provided, {additional_new} new unique plans")

        aiopt_logger.debug(f"[Candidates] Final: {len(candidates)} unique candidates")
        return candidates, default_count

    def optimize_template(self, db: str, digest: str,
                          workloads: list[WorkloadRowAggregated],
                          sql_progress: str = "") -> tuple[list[DecidedRule], list[ValidationLogEntry], str | None]:
        """
        Optimize SQL template using Outline Data based Plan Selection.
        """
        if not workloads:
            return [], [], None
        
        log_prefix = f"{sql_progress}" if sql_progress else f"[{db}/{digest}]"
        
        # Extract md5 from workloads (all items in the same group should have the same md5)
        md5 = workloads[0].md5 if workloads else None
        
        # Extract max elapsed time from workloads for feedback_timeout calculation
        max_elapsed_time = max(w.elapsed_time_max for w in workloads) if workloads else 0
        
        # 1. Collect unique SQL samples
        sql_samples = self._collect_sql_samples(workloads)
        if not sql_samples:
            return [], [], None
            
        representative_sql = sql_samples[0]
        aiopt_logger.info(f"{log_prefix} Collected {len(sql_samples)} unique SQL samples")

        # 2. Validate Representative SQL (and Schema) with PlanID capture
        feature_flags = self.context.feature_flags
        self.context.training_controller.use_db(db)
        db_utils.get_plan_id_and_outline(
            self.context.training_controller,
            representative_sql,
            extract_outline=feature_flags.supports_hints_extraction
        )

        # 2.5 Compute digest_text (normalized SQL template)
        # Placed after step 2: use_db(db) already called, SQL validity verified
        digest_text = None
        try:
            digest_text = db_utils.compute_statement_digest_text(
                self.context.training_controller, representative_sql
            )
        except Exception as e:
            aiopt_logger.warning(f"{log_prefix} Failed to compute digest_text: {e}")
            
        # 3. Collect Candidates (D + S + additional)
        candidates, default_count = self._collect_all_candidates(db, digest, sql_samples)
        aiopt_logger.info(f"{log_prefix} Collected {len(candidates)} unique candidate plans (|D|={default_count})")
        if not candidates:
            return self._create_reset_rule(db, digest, representative_sql, md5=md5, reason="No candidates found"), [], digest_text

        # 3.1 Statement Outline: multiple default plans → DEFAULT
        # Different SQL samples produce different default plans, meaning the optimizer
        # cannot safely select a single plan for Statement Outline binding.
        if default_count > 1 and self.context.outline_type == OutlineType.STATEMENT_OUTLINE:
            aiopt_logger.info(
                f"{log_prefix} Statement Outline: {default_count} different default plans "
                f"detected, returning DEFAULT"
            )
            return self._create_reset_rule(
                db, digest, representative_sql, md5=md5,
                reason=f"Statement Outline: {default_count} different default plans",
                max_elapsed_time=max_elapsed_time
            ), [], digest_text
            
        # 4. Evaluation Loop
        sample_results: dict[str, dict[str, float]] = {} # sample -> {plan_id -> elapsed}
        sample_defaults: dict[str, str] = {} # sample -> default_plan_id
        evaluated_logs: list[ValidationLogEntry] = []
        
        # Store Default EXPLAIN results per sample for ValidationLogEntry
        sample_default_explains: dict[str, dict] = {}  # sample -> {plan_id, traditional, analyze_json}
        
        for sample_idx, sample_sql in enumerate(sql_samples, 1):
            sample_results[sample_sql] = {}
            
            # 4.1 Get Baseline (Default Plan)
            try:
                # Get Default PlanID first (lightweight, skip outline extraction if not supported)
                default_plan_id, _ = db_utils.get_plan_id_and_outline(
                    self.context.training_controller, sample_sql,
                    extract_outline=feature_flags.supports_hints_extraction
                )
                
                # Get EXPLAIN TRADITIONAL for default
                default_explain_traditional = db_utils.execute_explain(
                    self.context.training_controller, db, sample_sql,
                    fmt=db_utils.ExplainFormat.TRADITIONAL, mapping=True
                )
                
                # Measure Default Time and get ANALYZE JSON result
                db_utils.enable_planid_in_explain_and_set_json_format_v2(
                    self.context.training_controller,
                    enable_rows_examined=self.context.feature_flags.supports_rows_examined
                )
                default_explain_sql = f"EXPLAIN ANALYZE FORMAT=JSON {sample_sql}"
                aiopt_logger.debug(f"{log_prefix}[Sample {sample_idx}] Measuring Default, SQL: {repr(default_explain_sql)}")
                start_t = time.time()
                default_elapsed, default_analyze_result = self.context.training_controller.evaluate_elapsed_time_with_result(
                    text(default_explain_sql),
                    timeout_seconds=TrainingParameters.default_plan_timeout_seconds,
                    warmup_runs=1
                )
                self.training_time += time.time() - start_t

                # Get ANALYZE JSON result as string (MySQL will handle JSON type)
                default_analyze_json = None
                if default_analyze_result and len(default_analyze_result) > 0 and default_analyze_result[0][0]:
                    raw_result = str(default_analyze_result[0][0])
                    # Truncate "Outline Data:" suffix that breaks JSON validity
                    outline_marker = "\nOutline Data:"
                    if outline_marker in raw_result:
                        default_analyze_json = raw_result[:raw_result.index(outline_marker)]
                    else:
                        default_analyze_json = raw_result

                # Store Default EXPLAIN results
                sample_default_explains[sample_sql] = {
                    'plan_id': default_plan_id,
                    'traditional': json.dumps([dict(row) for row in default_explain_traditional]) if default_explain_traditional else None,
                    'analyze_json': default_analyze_json,
                }
                
                # Record Default
                sample_results[sample_sql][default_plan_id] = default_elapsed
                sample_defaults[sample_sql] = default_plan_id  # Store for decision phase
                aiopt_logger.debug(f"{log_prefix}[Sample {sample_idx}] Default ({default_plan_id}): {default_elapsed:.4f}s")
                
            except Exception as e:
                aiopt_logger.error(f"{log_prefix}[Sample {sample_idx}] Failed to measure default: {e}")
                aiopt_logger.warning(f"{log_prefix} Skipping entire SQL template due to Default measurement failure")
                raise
                
            # 4.2 Evaluate Candidates
            base_time = max(default_elapsed, 0.1)
            timeout_val = min(base_time * 1.5, base_time + 10.0)
            
            total_candidates = len(candidates)
            feature_flags = self.context.feature_flags
            
            # In basic feature set (no hints extraction), we proceed with candidate evaluation
            # but using enumerated hints directly and skipping SQL verification (PlanID check)
            if not feature_flags.supports_hints_extraction:
                aiopt_logger.info(f"{log_prefix} Hints extraction not supported: skipping SQL verification (PlanID check)")
            
            # Get Default EXPLAIN data for this sample (needed for logging)
            default_explains = sample_default_explains.get(sample_sql, {})

            for cand_idx, plan in enumerate(candidates, 1):
                # Optimization: If cand is obviously the default plan (by ID check), skip execution
                # But wait, plan.plan_id is the ID from extraction phase. 
                # Does (Sample + Outline) generate the same ID as (Enum + Rep + Outline)?
                # Yes, if outline is robust.
                # However, to be 100% sure we compare 'generated PlanID' on this sample.
                
                # Construct rewritten SQL with Canonical Hints
                rewritten_sql = hints_generator.insert_raw_hints(sample_sql, plan.hints_text)
                
                target_plan_id = plan.plan_id
                
                # Pre-check PlanID on THIS sample (always try to get current PlanID)
                # Use extract_outline only when hints extraction is supported
                current_plan_id, _ = db_utils.get_plan_id_and_outline(
                    self.context.training_controller, rewritten_sql,
                    extract_outline=feature_flags.supports_hints_extraction
                )
                
                # Log candidate context (for debugging FATAL or normal execution)
                aiopt_logger.debug(
                    f"{log_prefix}[Sample {sample_idx}] Evaluating candidate {cand_idx}/{total_candidates}: "
                    f"stored_plan_id={plan.plan_id}, actual_plan_id={current_plan_id}, "
                    f"rewritten_sql={repr(rewritten_sql)}"
                )
                    
                if current_plan_id == default_plan_id:
                    # Same as default, reuse default time
                    sample_results[sample_sql][plan.plan_id] = default_elapsed 
                    aiopt_logger.debug(f"{log_prefix}[Sample {sample_idx}] Candidate {cand_idx}/{total_candidates} same as Default, skipped")
                    
                    # Log as evaluated (equal to default)
                    try:
                        evaluated_logs.append(ValidationLogEntry(
                            task_id=self.context.task_id,
                            instance_id=self.context.instance_id,
                            db=db, digest=digest, plan_id=current_plan_id, sql_text=sample_sql,
                            sql_text_rewritten=rewritten_sql,
                            hints_text=plan.hints_text,
                            default_elapsed_time=default_elapsed, elapsed_time=default_elapsed,
                            default_plan_id=default_explains.get('plan_id'),
                            default_explain_traditional=default_explains.get('traditional'),
                            default_analyze_json=default_explains.get('analyze_json'),
                            default_rows_examined=db_utils.extract_rows_examined_from_json(default_explains.get('analyze_json')),
                            explain_traditional=None,  # Did not execute
                            analyze_json=None,         # Did not execute
                            rows_examined=db_utils.extract_rows_examined_from_json(default_explains.get('analyze_json')),  # Same plan as default
                            is_best=False, is_better=False,  # Equal is not better
                            comments="Same as default plan"
                        ))
                    except Exception as e_log:
                        aiopt_logger.error(f"{log_prefix}[Sample {sample_idx}] Failed to create log entry for default plan skip: {e_log}")

                    continue
                
                if current_plan_id and current_plan_id in sample_results[sample_sql]:
                     # Already evaluated this plan ID for this sample (deduplication per sample)
                     continue
                
                # Check plan_id consistency: stored vs actual (only if hints extraction supported)
                if feature_flags.supports_hints_extraction:
                    if current_plan_id and plan.plan_id and current_plan_id != plan.plan_id:
                        aiopt_logger.error(
                            f"{log_prefix}[Sample {sample_idx}] FATAL: Outline Data cannot reproduce original plan. "
                            f"stored_plan_id={plan.plan_id}, actual_plan_id={current_plan_id}"
                        )
                        aiopt_logger.warning(f"{log_prefix} Skipping entire SQL template due to unstable Outline Data")
                        raise RuntimeError(
                            f"Unstable Outline Data: stored_plan_id={plan.plan_id}, "
                            f"actual_plan_id={current_plan_id}"
                        )
                
                # Execute
                explain_sql = f"EXPLAIN ANALYZE FORMAT=JSON {rewritten_sql}"
                aiopt_logger.debug(f"{log_prefix}[Sample {sample_idx}] Executing candidate {cand_idx}/{total_candidates} (timeout={timeout_val:.2f}s): {repr(explain_sql)}")
                
                try:
                    # Get EXPLAIN TRADITIONAL for candidate
                    candidate_explain_traditional = db_utils.execute_explain(
                        self.context.training_controller, db, rewritten_sql,
                        fmt=db_utils.ExplainFormat.TRADITIONAL, mapping=True
                    )
                    
                    # Execute candidate and get ANALYZE JSON result
                    db_utils.enable_planid_in_explain_and_set_json_format_v2(
                        self.context.training_controller,
                        enable_rows_examined=self.context.feature_flags.supports_rows_examined
                    )
                    eval_start = time.time()
                    elapsed, candidate_analyze_result = self.context.training_controller.evaluate_elapsed_time_with_result(
                        text(explain_sql),
                        timeout_seconds=timeout_val,
                        warmup_runs=1
                    )
                    self.training_time += time.time() - eval_start

                    # Get candidate ANALYZE JSON result as string
                    candidate_analyze_json = None
                    if candidate_analyze_result and len(candidate_analyze_result) > 0 and candidate_analyze_result[0][0]:
                        raw_result = str(candidate_analyze_result[0][0])
                        # Truncate "Outline Data:" suffix that breaks JSON validity
                        outline_marker = "\nOutline Data:"
                        if outline_marker in raw_result:
                            candidate_analyze_json = raw_result[:raw_result.index(outline_marker)]
                        else:
                            candidate_analyze_json = raw_result

                    actual_plan_id = current_plan_id if current_plan_id else plan.plan_id
                    sample_results[sample_sql][plan.plan_id] = elapsed # Map candidate to result
                    is_better = _is_better_plan(elapsed, default_elapsed)
                    aiopt_logger.debug(f"{log_prefix}[Sample {sample_idx}] Candidate {cand_idx}/{total_candidates} result: {elapsed:.4f}s (default: {default_elapsed:.4f}s) {'BETTER' if is_better else 'WORSE'}")

                    # Log with EXPLAIN results
                    evaluated_logs.append(ValidationLogEntry(
                        task_id=self.context.task_id,
                        instance_id=self.context.instance_id,
                        db=db, digest=digest, plan_id=actual_plan_id, sql_text=sample_sql,
                        sql_text_rewritten=rewritten_sql,
                        hints_text=plan.hints_text,
                        default_elapsed_time=default_elapsed, elapsed_time=elapsed,
                        default_plan_id=default_explains.get('plan_id'),
                        default_explain_traditional=default_explains.get('traditional'),
                        default_analyze_json=default_explains.get('analyze_json'),
                        default_rows_examined=db_utils.extract_rows_examined_from_json(default_explains.get('analyze_json')),
                        explain_traditional=json.dumps([dict(row) for row in candidate_explain_traditional]) if candidate_explain_traditional else None,
                        analyze_json=candidate_analyze_json,
                        rows_examined=db_utils.extract_rows_examined_from_json(candidate_analyze_json),
                        is_best=False, is_better=_is_better_plan(elapsed, default_elapsed),
                        comments=None
                    ))
                    
                except Exception as e:
                    duration = time.time() - eval_start if 'eval_start' in dir() else 0
                    aiopt_logger.debug(f"{log_prefix}[Sample {sample_idx}] Candidate {cand_idx}/{total_candidates} TIMEOUT/ERROR after {duration:.1f}s: {e}", exc_info=True)
                    # Log failure with large elapsed value (indicates candidate is worse than default)
                    # Using 1e9 (MySQL-compatible) instead of float('inf')
                    TIMEOUT_ELAPSED = 1e9
                    evaluated_logs.append(ValidationLogEntry(
                        task_id=self.context.task_id,
                        instance_id=self.context.instance_id,
                        db=db, digest=digest, plan_id=target_plan_id, sql_text=sample_sql,
                        sql_text_rewritten=rewritten_sql,
                        hints_text=plan.hints_text,
                        default_elapsed_time=default_elapsed, elapsed_time=TIMEOUT_ELAPSED,
                        default_plan_id=default_explains.get('plan_id'),
                        default_explain_traditional=default_explains.get('traditional'),
                        default_analyze_json=default_explains.get('analyze_json'),
                        default_rows_examined=db_utils.extract_rows_examined_from_json(default_explains.get('analyze_json')),
                        explain_traditional=None,
                        analyze_json=None,
                        rows_examined=None,
                        is_best=False, is_better=False,
                        comments=f"Timeout/Error after {duration:.1f}s: {str(e)}"
                    ))
                    # Also store in sample_results so this candidate is marked as worse
                    sample_results[sample_sql][plan.plan_id] = TIMEOUT_ELAPSED

        # 4.3 Mark is_best for each sample (post-processing)
        # Group by sql_text, find best among is_better candidates
        from collections import defaultdict
        logs_by_sample = defaultdict(list)
        for log in evaluated_logs:
            logs_by_sample[log.sql_text].append(log)
        
        for sample_sql, logs in logs_by_sample.items():
            better_logs = [log for log in logs if log.is_better]
            if better_logs:
                best_log = min(better_logs, key=lambda x: x.elapsed_time)
                best_log.is_best = True

        # 5. Decision (Find best plan common across samples)
        # Strategy: For each candidate plan, check if it is better than default for ALL samples (or intersection)
        
        # Invert results: plan_id -> {sample -> elapsed}
        plan_performance = {}
        for sample, results in sample_results.items():
            # default_time = results.get(db_utils.get_plan_id(self.context.training_controller, sample)) 
            # Re-fetch default PID might be slow. 
            # We skip this logic block as it was empty pass anyway.
            pass
            # Better way: we stored default PID in step 4.1, but didn't save it cleanly.
            # Rework decision logic slightly.
        # ========== Decision Logic (Relaxed Intersection Policy v2) ==========
        # Step 1: Collect ValidSet per sample (Better OR Default)
        sample_valid_plans: dict[str, set[str]] = {}  # sample_sql -> set of valid PlanIDs
        
        for sample_sql, results in sample_results.items():
            default_pid = sample_defaults.get(sample_sql)
            if not default_pid or default_pid not in results:
                continue
            default_time = results[default_pid]
            
            valid_set = set()
            for plan_id, time_val in results.items():
                is_better = (plan_id != default_pid) and _is_better_plan(time_val, default_time)
                is_default = (plan_id == default_pid)
                if is_better or is_default:
                    valid_set.add(plan_id)
            
            sample_valid_plans[sample_sql] = valid_set
            aiopt_logger.debug(f"{log_prefix}[Sample]: default_pid={default_pid}, valid_plans={valid_set}")

        if not sample_valid_plans:
            return self._create_reset_rule(db, digest, representative_sql, md5=md5, reason="No valid plans for any sample"), evaluated_logs, digest_text

        # Step 2: Intersection = Safety Check (must be safe in ALL samples)
        qualified_set: set[str] = None
        for valid_set in sample_valid_plans.values():
            if qualified_set is None:
                qualified_set = valid_set.copy()
            else:
                qualified_set &= valid_set
        
        if not qualified_set:
            aiopt_logger.info(f"{log_prefix} No common safe plans across all samples")
            return self._create_reset_rule(db, digest, representative_sql, md5=md5, reason="No common safe plans"), evaluated_logs, digest_text

        # Step 3: Filter out plans with NO gain in ANY sample
        def has_gain(pid: str) -> bool:
            for sample_sql, results in sample_results.items():
                if pid not in results:
                    continue
                default_pid = sample_defaults.get(sample_sql)
                if default_pid and default_pid in results:
                    if pid != default_pid and _is_better_plan(results[pid], results[default_pid]):
                        return True
            return False
        
        qualified_plans = [pid for pid in qualified_set if has_gain(pid)]
        aiopt_logger.info(f"{log_prefix} Found {len(qualified_plans)} qualified plans (Relaxed Intersection)")
        
        if not qualified_plans:
            aiopt_logger.info(f"{log_prefix} No qualified plans with optimization gain")
            return self._create_reset_rule(db, digest, representative_sql, md5=md5, reason="No plans with gain"), evaluated_logs, digest_text

        # Step 4: Check for Global Best (fastest in EVERY sample, within qualified scope)
        def find_global_best() -> str | None:
            """Find a plan that is the fastest in every sample (among qualified plans only)."""
            for candidate_pid in qualified_plans:
                is_best_in_all = True
                for sample_sql, results in sample_results.items():
                    if candidate_pid not in results:
                        is_best_in_all = False
                        break
                    candidate_time = results[candidate_pid]
                    # Check if any OTHER qualified plan is faster in this sample
                    for other_pid in qualified_plans:
                        if other_pid == candidate_pid or other_pid not in results:
                            continue
                        if results[other_pid] < candidate_time:
                            is_best_in_all = False
                            break
                    if not is_best_in_all:
                        break
                if is_best_in_all:
                    return candidate_pid
            return None
        
        global_best = find_global_best()
        
        # Step 5: Final Selection
        if global_best:
            aiopt_logger.info(f"{log_prefix} Found Global Best plan: {global_best}")
            best_candidate = next((c for c in candidates if c.plan_id == global_best), None)
            if best_candidate:
                if self.context.outline_type == OutlineType.SPM:
                    decided_rule = self._create_optimize_rules_from_set(
                        db, digest, sql_samples, [best_candidate], sample_results, md5=md5, max_elapsed_time=max_elapsed_time
                    )
                else:
                    decided_rule = self._create_optimize_rules(
                        db, digest, sql_samples, best_candidate, sample_results, md5=md5, max_elapsed_time=max_elapsed_time
                    )
            else:
                decided_rule = self._create_reset_rule(db, digest, representative_sql, md5=md5, reason="Global best candidate not found", max_elapsed_time=max_elapsed_time)
        else:
            aiopt_logger.info(f"{log_prefix} No Global Best found among {len(qualified_plans)} qualified plans")
            if self.context.outline_type == OutlineType.SPM:
                # SPM: Select ALL qualified plans
                selected_candidates = [c for c in candidates if c.plan_id in qualified_plans]
                aiopt_logger.info(f"{log_prefix} SPM: Selecting all {len(selected_candidates)} qualified plans")
                decided_rule = self._create_optimize_rules_from_set(
                    db, digest, sql_samples, selected_candidates, sample_results, md5=md5, max_elapsed_time=max_elapsed_time
                )
            else:
                # Statement Outline: Cannot determine unique best, return Default
                aiopt_logger.info(f"{log_prefix} StatementOutline: No unique best, returning Default")
                decided_rule = self._create_reset_rule(db, digest, representative_sql, md5=md5, reason="No unique best plan", max_elapsed_time=max_elapsed_time)


        # Invariant: decided_rule is either all-OPTIMIZE or a single DEFAULT (see DESIGN_latest.md).
        assert (len(decided_rule) == 1 and decided_rule[0].action == RuleAction.DEFAULT) \
            or all(r.action == RuleAction.OPTIMIZE for r in decided_rule), \
            f"Invariant violated: expected single DEFAULT or all OPTIMIZE, got {[r.action for r in decided_rule]}"

        # 5.1 Reliability filter: discard OPTIMIZE rules where all validation
        # records have default_elapsed_time below min_query_time (unreliable noise).
        # Per the above invariant, decided_rule[0].action suffices to distinguish the two cases.
        if decided_rule[0].action == RuleAction.OPTIMIZE:
            min_query_time = GlobalConfig.workload_min_query_time

            reliable_rules = []
            unreliable_pids = []
            for rule in decided_rule:
                has_reliable_validation = any(
                    log.plan_id == rule.plan_id and log.default_elapsed_time is not None
                    and log.default_elapsed_time >= min_query_time
                    for log in evaluated_logs
                )
                if has_reliable_validation:
                    reliable_rules.append(rule)
                else:
                    unreliable_pids.append(rule.plan_id)
                    aiopt_logger.info(
                        f"{log_prefix} Discarding unreliable OPTIMIZE rule plan_id={rule.plan_id}: "
                        f"no validation with default_elapsed >= {min_query_time}s"
                    )

            if unreliable_pids:
                if reliable_rules:
                    decided_rule = reliable_rules
                    aiopt_logger.info(
                        f"{log_prefix} Filtered {len(unreliable_pids)} unreliable rules, "
                        f"{len(reliable_rules)} reliable rules remain"
                    )
                else:
                    aiopt_logger.info(
                        f"{log_prefix} All OPTIMIZE rules unreliable, reverting to DEFAULT"
                    )
                    decided_rule = self._create_reset_rule(
                        db, digest, representative_sql, md5=md5,
                        reason=f"All OPTIMIZE rules unreliable: no validation with default >= {min_query_time}s",
                        max_elapsed_time=max_elapsed_time
                    )

        return decided_rule, evaluated_logs, digest_text
    
    def _create_reset_rule(self, db: str, digest: str, sql_text: str, md5: str | None = None, reason: str = "No common better plan found", max_elapsed_time: float = 0) -> list[DecidedRule]:
        # Calculate feedback_timeout for Statement Outline mode
        feedback_timeout_ms = 0
        if self.context.outline_type == OutlineType.STATEMENT_OUTLINE and max_elapsed_time > 0:
            from config.config import GlobalConfig
            feedback_timeout_ms = int(max_elapsed_time * 1000 * GlobalConfig.feedback_timeout_rate)
        
        return [DecidedRule(
            task_id=self.context.task_id,
            cluster_id=self.context.instance_info.cluster_id,
            instance_id=self.context.instance_id,
            db=db, digest=digest, plan_id=None,
            sql_text=sql_text, sql_text_rewritten=sql_text,
            hints_text=None,
            feedback_timeout=feedback_timeout_ms, action=RuleAction.DEFAULT,
            md5=md5,
            comments=reason
        )]
    
    
    def _create_optimize_rules(self, db: str, digest: str, 
                               sql_samples: list[str],
                               best_plan: CandidatePlan,
                               sample_results: dict[str, dict[str, float]],
                               md5: str | None = None,
                               max_elapsed_time: float = 0) -> list[DecidedRule]:
        # Find contributing samples (where this plan is better than default)
        # and randomly select one
        contributing_samples = []
        for sample in sql_samples:
            result_map = sample_results.get(sample, {})
            if best_plan.plan_id in result_map:
                contributing_samples.append(sample)
        
        # Randomly select a contributing sample
        sample_sql = random.choice(contributing_samples) if contributing_samples else sql_samples[0]
        
        # Apply Outline Hints
        extended_sql = hints_generator.insert_raw_hints(sample_sql, best_plan.hints_text)
        
        # Calculate feedback_timeout for Statement Outline mode
        feedback_timeout_ms = 0
        if self.context.outline_type == OutlineType.STATEMENT_OUTLINE and max_elapsed_time > 0:
            from config.config import GlobalConfig
            feedback_timeout_ms = int(max_elapsed_time * 1000 * GlobalConfig.feedback_timeout_rate)
        
        return [DecidedRule(
            task_id=self.context.task_id,
            cluster_id=self.context.instance_info.cluster_id,
            instance_id=self.context.instance_id,
            db=db, digest=digest, plan_id=best_plan.plan_id,
            sql_text=sample_sql,
            sql_text_rewritten=extended_sql,
            hints_text=best_plan.hints_text,
            feedback_timeout=feedback_timeout_ms, action=RuleAction.OPTIMIZE,
            md5=md5,
            comments=f"Common better plan across {len(contributing_samples)} samples"
        )]
    
    def _create_optimize_rules_from_set(self, db: str, digest: str,
                                        sql_samples: list[str],
                                        candidate_plans: list[CandidatePlan],
                                        sample_results: dict[str, dict[str, float]],
                                        md5: str | None = None,
                                        max_elapsed_time: float = 0) -> list[DecidedRule]:
        rules = []
        for plan in candidate_plans:
            rules.extend(self._create_optimize_rules(db, digest, sql_samples, plan, sample_results, md5=md5, max_elapsed_time=max_elapsed_time))
        return rules


    def _get_spm_captured_plans(self, db: str, digest: str) -> list[CandidatePlan]:
        """
        获取 SPM 已捕获的计划
        
        使用 SPMOperator 查询该 SQL 模板的所有 SPM 计划，并转换为 CandidatePlan 对象
        
        :param db: 数据库名
        :param digest: SQL digest
        :return: SPM 捕获的候选计划列表
        """
        try:
            from optimizer.spm_operator import SPMOperator
            
            # 使用 SPMOperator 查询所有基线（包含 hints）
            baselines = SPMOperator.get_all_baselines(
                self.context.training_controller, db, digest
            )
            
            if not baselines:
                aiopt_logger.debug(f"No SPM plans found for db={db}, digest={digest}")
                return []
            
            # 转换为 CandidatePlan 对象
            spm_plans = []
            for baseline in baselines:
                plan_id = baseline["plan_id"]
                hints_text = baseline.get("hints")
                
                if hints_text:
                    candidate = CandidatePlan(
                        plan_id=plan_id,
                        hints_text=hints_text,
                        indexes_dict={},  # SPM 捕获的计划不需要 indexes_dict
                        source_sql=f"SPM (db={db}, digest={digest})"
                    )
                    spm_plans.append(candidate)
                    aiopt_logger.debug(f"Found SPM plan: plan_id={plan_id}, hints={hints_text[:50]}...")
                else:
                    aiopt_logger.warning(f"SPM plan {plan_id} has empty hints, skipped")
            
            aiopt_logger.info(f"Loaded {len(spm_plans)} SPM plans for db={db}, digest={digest}")
            return spm_plans
            
        except Exception as e:
            # 任何错误都应该抛出，因为调用方已经检查了 outline_type == SPM
            # 如果是 SPM 模式但表不存在，说明配置有问题
            aiopt_logger.error(f"Failed to query SPM plans for db={db}, digest={digest}: {e}")
            raise
