"""
Training Executor Module

Executes complete training workflow: Workload preprocessing → Blacklist filtering → AI optimization → Store rules

Now supports:
- Pre-allocated training environment (from scheduler)
- Dynamic feature detection
- Parallel optimization with process pool (configurable via parallel_workers)
- Signal-based task control (SIGUSR1 for immediate abort)
- Resume interrupted tasks (options.allow_resume=True, auto-detects previous INTERRUPTED task)
- Full SIGUSR1 lifecycle protection (handler active throughout execute())
- ExecutionStage tracking for observability
"""

import signal
import time
from collections import defaultdict
from ai_logger import aiopt_logger
from data_models import (
    ExecutorInput, ExecutorResult, ExecutionStatus, ExecutionStage, ExecutionStats,
    ExecutorOptions, OutlineType, DecidedRule, TrainingEnvType, RuleAction,
    OptimizationTaskPayload, OptimizationTaskResult
)
from db_controller import DBController
from controller.workload_controller import WorkloadController
from controller.blacklist_controller import BlacklistController
from controller.rules_controller import RulesController
from controller.validation_logs_controller import ValidationLogsController
from controller.execution_history_controller import ExecutionHistoryController
from controller.task_progress_controller import TaskProgressController
from workload.workload_preprocessor import WorkloadPreprocessor
from task.parallel_pool_manager import ParallelPoolManager
from task.parallel_worker import optimize_template_worker
from config.config import GlobalConfig


class TrainingExecutor:
    """Training Executor"""

    def __init__(self, meta_controller: DBController):
        """
        :param meta_controller: Metadata database connection
        """
        self.meta_controller = meta_controller

    def execute(self, executor_input: ExecutorInput) -> ExecutorResult:
        """
        Execute training task.

        SIGUSR1 lifecycle protection:
        - Handler installed at entry (sets flag, does not kill process)
        - Pool manager temporarily replaces with its own handler during pool execution
        - Pool manager restores executor handler after pool completes
        - Handler restored to original in finally block

        Args:
            executor_input: Executor input containing all necessary parameters

        Returns:
            ExecutorResult with success status and metrics
        """
        start_time = time.time()

        # Extract commonly used fields
        task_id = executor_input.task_id
        instance_info = executor_input.instance_info
        instance_id = instance_info.instance_id
        cluster_id = instance_info.cluster_id
        node_uuid = instance_info.node_uuid
        env_type = executor_input.env_type

        # Convert NodeConfig to InstanceConfig with explicit parameters
        # IMPORTANT: read_only depends on env_type
        # - CLONE: read_only=False (克隆环境可读写)
        # - SLAVE/RO: read_only=True (只读环境)
        env_read_only = env_type != TrainingEnvType.CLONE
        env_instance_config = executor_input.env_config.to_instance_config(
            read_only=env_read_only,
            with_ai_marker=True,      # 训练 SQL 需要标记
            allow_reconnect=True,     # 允许重连
            is_meta_server=False      # 非元数据库
        )
        online_server = executor_input.master_node.to_instance_config(
            read_only=False,          # 主节点可读写 (用于 outline 操作)
            with_ai_marker=True,      # SQL 需要标记
            allow_reconnect=True,     # 允许重连
            is_meta_server=False      # 非元数据库
        )

        # workload_set is already list[tuple[str, str]] from Pydantic validation
        workload_set = executor_input.workload_set

        # Initialize result
        result = ExecutorResult(
            task_id=task_id,
            status=ExecutionStatus.FAILED,  # Default to FAILED, set COMPLETED on success
        )

        # 1. Install executor-level SIGUSR1 handler (full lifecycle protection)
        _signal_received = False

        def _executor_signal_handler(signum, frame):
            nonlocal _signal_received
            _signal_received = True
            aiopt_logger.warning("[Executor] SIGUSR1 received (deferred)")

        old_handler = signal.signal(signal.SIGUSR1, _executor_signal_handler)

        stage = ExecutionStage.INITIALIZING
        from ai_logger import set_task_context
        set_task_context(task_id=task_id, cluster_id=cluster_id, instance_id=instance_id)

        def _update_progress(**kwargs):
            """Update task_progress, silently log on failure."""
            try:
                TaskProgressController.upsert(
                    self.meta_controller, task_id, instance_id, node_uuid, **kwargs
                )
            except Exception as e:
                aiopt_logger.warning(f"[Executor] Progress update failed: {e}")

        try:
            # Determine resume: query execution history for previous INTERRUPTED task
            options = executor_input.options
            prev_task_id = None
            if options and options.allow_resume:
                prev_task_id = ExecutionHistoryController.find_resumable_task(
                    self.meta_controller, instance_id, node_uuid,
                    options.resume_expiration_days
                )

            # Build interrupted chain for chain-based resume
            prev_task_ids: list[str] = []
            if prev_task_id:
                prev_task_ids = ExecutionHistoryController.get_interrupted_chain(
                    self.meta_controller, instance_id, node_uuid,
                    options.resume_expiration_days
                )
                result.extra["prev_task_ids"] = prev_task_ids

            aiopt_logger.info(f"[Executor] Starting task {task_id} for instance {instance_id}")
            aiopt_logger.info(f"[Executor] Env type: {env_type.value}, prev_task_ids: {prev_task_ids}")
            aiopt_logger.info(f"[Executor] Operator: {executor_input.operator}")

            # 2. Create training controller and WorkloadPreprocessor.
            # All operations before APPLYING (preprocessing, feature detection, optimization)
            # must use the training node. The online/master node is only accessed in APPLYING
            # for rule writing.
            training_controller = DBController(env_instance_config)
            preprocessor = WorkloadPreprocessor(training_controller=training_controller)

            # 3. Detect feature flags on the training node.
            # Feature detection probes which capabilities (hints extraction, SPM, Statement Outline)
            # are available on the training instance. The training environment is expected to mirror
            # the online instance's capabilities.
            from feature_detector import detect_features
            feature_flags = detect_features(training_controller)
            aiopt_logger.info(f"[Executor] Detected feature flags: {feature_flags}")

            # 4. Log workload_set info
            if workload_set:
                aiopt_logger.info(f"[Executor] Incremental training with {len(workload_set)} workload items")
            else:
                aiopt_logger.info(f"[Executor] Full training")

            stage = ExecutionStage.LOADING_WORKLOAD
            _update_progress(stage=stage.value)

            # 5. Execute Workload preprocessing (always, even on resume — loads fresh workload)
            min_query_time = GlobalConfig.workload_min_query_time
            window_days = GlobalConfig.workload_window_days

            saved_count = preprocessor.preprocess(
                task_id=task_id,
                meta_controller=self.meta_controller,
                workload_source=instance_info.workload_source,
                product_type=instance_info.product_type,
                cluster_id=cluster_id,
                instance_id=instance_id,
                node_uuid=instance_info.node_uuid,
                region=instance_info.region.value,
                min_query_time=min_query_time,
                window_days=window_days,
                instance_config=online_server,
                workload_set=workload_set
            )
            aiopt_logger.info(f"[Executor] Preprocessed {saved_count} workload rows")

            # 6. Load preprocessed workload
            workload_rows = WorkloadController.load_workload_by_task(
                self.meta_controller, task_id
            )

            # 7. Blacklist filtering
            blacklist_set = BlacklistController.get_blacklist_set(
                self.meta_controller, instance_id
            )
            if blacklist_set:
                before_count = len(workload_rows)
                filtered_rows = []
                for row in workload_rows:
                    if (row.db, row.digest) in blacklist_set:
                        aiopt_logger.debug(f"[Executor] Blacklist: skipped db={row.db}, digest={row.digest}")
                    else:
                        filtered_rows.append(row)
                workload_rows = filtered_rows
                filtered_count = before_count - len(workload_rows)
                aiopt_logger.info(f"[Executor] Blacklist filtered {filtered_count} rows, remaining {len(workload_rows)}")

            # 7.5 Count total unique templates (before resume filter) for accurate reporting
            all_template_keys = {(r.db, r.digest) for r in workload_rows}
            total_templates_for_task = len(all_template_keys)
            previously_processed = 0

            # 7.6 Resume: filter out templates already processed in the entire interrupted chain
            if prev_task_ids:
                processed_set = WorkloadController.get_processed_templates(
                    self.meta_controller, prev_task_ids, instance_id
                )
                if processed_set:
                    previously_processed = len(processed_set & all_template_keys)
                    workload_rows = [
                        r for r in workload_rows
                        if (r.db, r.digest) not in processed_set
                    ]
                    remaining_keys = {(r.db, r.digest) for r in workload_rows}
                    aiopt_logger.info(
                        f"[Executor] Resume chain={prev_task_ids}: "
                        f"{previously_processed} templates already processed, "
                        f"{len(remaining_keys)} templates remaining"
                    )

            # Signal checkpoint: preprocessing complete, workload stored
            # Even if signal was received, preprocessing ran to completion (workload integrity)
            if _signal_received:
                aiopt_logger.warning(
                    "[Executor] SIGUSR1 was received during preprocessing, "
                    "returning INTERRUPTED (workload stored, safe to resume)"
                )
                result.status = ExecutionStatus.INTERRUPTED
                result.stats.total_templates = total_templates_for_task
                result.stats.processed_templates = previously_processed
                result.extra["stage"] = stage.value
                result.extra["interrupt_stage"] = stage.value
                return result

            # Empty workload check (unified: both fresh and resume)
            if not workload_rows:
                aiopt_logger.info(f"[Executor] No workload to optimize")
                result.stats.total_templates = total_templates_for_task
                result.stats.processed_templates = previously_processed
                result.status = ExecutionStatus.COMPLETED
                result.extra["stage"] = ExecutionStage.FINISHED.value
                return result

            stage = ExecutionStage.OPTIMIZING

            # 8. Workload grouping: dict[(instance_id, db, digest) -> list[Workload]]
            workload_groups = defaultdict(list)
            for row in workload_rows:
                key = (row.instance_id, row.db, row.digest)
                workload_groups[key].append(row)

            remaining_templates = len(workload_groups)
            result.stats.total_templates = total_templates_for_task

            # 9. Sort templates by historical total elapsed time descending
            # so high-impact workloads are optimized first when training time is limited
            sorted_workload_groups = sorted(
                workload_groups.items(),
                key=lambda item: sum(r.count_star * r.elapsed_time_avg for r in item[1]),
                reverse=True,
            )

            for rank, ((_, db, digest), workloads) in enumerate(sorted_workload_groups, 1):
                score = sum(r.count_star * r.elapsed_time_avg for r in workloads)
                aiopt_logger.debug(f"[Executor] Template priority #{rank}: {db}/{digest} score={score}s")

            # 9.5 Apply per-task SQL template limit (only affects optimization, not workload archival)
            # Priority: task input > config file default
            templates_limit = (
                options.sql_template_limit
                if options and options.sql_template_limit is not None
                else GlobalConfig.sql_template_limit_per_task
            )
            if templates_limit > 0 and len(sorted_workload_groups) > templates_limit:
                aiopt_logger.info(
                    f"[Executor] SQL template limit: {len(sorted_workload_groups)} -> {templates_limit} "
                    f"(source: {'task input' if options and options.sql_template_limit is not None else 'config'})"
                )
                sorted_workload_groups = sorted_workload_groups[:templates_limit]
                remaining_templates = templates_limit

            # 10. Estimate training time
            from optimizer.training_estimator import estimate_training_time_minutes, is_light_workload
            estimated_minutes = estimate_training_time_minutes(workload_rows)
            workload_type = "Light" if is_light_workload(workload_rows) else "Heavy"
            aiopt_logger.info(
                f"[Executor] Estimated training time: {estimated_minutes:.1f} min "
                f"({workload_type} workload, {remaining_templates} SQL templates)"
            )

            # 11. Build optimization payloads
            payloads: list[OptimizationTaskPayload] = []
            for i, ((inst_id, db, digest), workloads) in enumerate(sorted_workload_groups, 1):
                sql_progress = f"[SQL {i}/{remaining_templates}]"
                payload = OptimizationTaskPayload(
                    task_id=task_id,
                    instance_id=instance_id,
                    cluster_id=cluster_id,
                    sql_progress=sql_progress,
                    db=db,
                    digest=digest,
                    workloads=workloads,
                    env_config=env_instance_config,
                    env_type=env_type,
                    instance_info=instance_info,
                    feature_flags=feature_flags,
                    outline_type=instance_info.outline_type
                )
                payloads.append(payload)

            aiopt_logger.info(f"[Executor] Created {len(payloads)} optimization payloads")
            _update_progress(
                stage=stage.value,
                total_templates=total_templates_for_task, completed_templates=previously_processed
            )

            # 12. Execute parallel optimization
            parallel_workers = GlobalConfig.parallel_workers
            aiopt_logger.info(f"[Executor] Using {parallel_workers} parallel workers")

            pool_manager = ParallelPoolManager(max_workers=parallel_workers)

            all_rules: list[DecidedRule] = []
            failed_counts: dict[str, int] = {}
            failed_templates = 0
            optimized_count = 0
            reset_count = 0
            total_training_time = 0.0
            completed_so_far = 0

            def on_result_callback(opt_result: OptimizationTaskResult):
                """Callback to store results as they complete."""
                nonlocal all_rules, failed_counts, failed_templates
                nonlocal optimized_count, reset_count, total_training_time, completed_so_far

                if not opt_result.success:
                    error_type = opt_result.error_type or "UnknownError"
                    failed_counts[error_type] = failed_counts.get(error_type, 0) + 1
                    failed_templates += 1

                if opt_result.success and opt_result.rules:
                    try:
                        RulesController.store_rules(self.meta_controller, opt_result.rules)
                        all_rules.extend(opt_result.rules)

                        # Update statistics
                        for rule in opt_result.rules:
                            if rule.action == RuleAction.OPTIMIZE:
                                optimized_count += 1
                            else:
                                reset_count += 1
                    except Exception as e:
                        aiopt_logger.error(f"[Executor] Failed to store rules for {opt_result.db}/{opt_result.digest}: {e}")
                        error_type = type(e).__name__
                        failed_counts[error_type] = failed_counts.get(error_type, 0) + 1
                        failed_templates += 1

                if opt_result.evaluated_logs:
                    try:
                        ValidationLogsController.insert_validation_logs(
                            self.meta_controller, opt_result.evaluated_logs
                        )
                    except Exception as e:
                        aiopt_logger.warning(f"[Executor] Failed to store validation logs for {opt_result.db}/{opt_result.digest}: {e}")

                if opt_result.mcts_results:
                    try:
                        from controller.mcts_result_controller import MctsResultController
                        MctsResultController.insert_mcts_result(
                            controller=self.meta_controller,
                            task_id=executor_input.task_id,
                            instance_id=instance_id,
                            db=opt_result.db,
                            digest=opt_result.digest,
                            result=opt_result.mcts_results
                        )
                    except Exception as e:
                        aiopt_logger.warning(f"[Executor] Failed to store MCTS results for {opt_result.db}/{opt_result.digest}: {e}")

                # Store digest_text to sql_templates (INSERT IGNORE)
                if opt_result.digest_text:
                    try:
                        from controller.sql_templates_controller import SqlTemplatesController
                        SqlTemplatesController.save_template(
                            self.meta_controller,
                            cluster_id=cluster_id,
                            instance_id=instance_id,
                            db=opt_result.db,
                            digest=opt_result.digest,
                            digest_text=opt_result.digest_text,
                        )
                    except Exception as e:
                        aiopt_logger.warning(
                            f"Failed to save sql_template for {opt_result.db}.{opt_result.digest}: {e}"
                        )

                total_training_time += opt_result.training_time
                completed_so_far += 1

                # Mark template as processed (unconditional — success or failure)
                try:
                    WorkloadController.mark_processed(
                        self.meta_controller, task_id, opt_result.db, opt_result.digest
                    )
                except Exception as e:
                    aiopt_logger.warning(f"[Executor] Failed to mark processed: {e}")

                _update_progress(
                    stage=stage.value,
                    total_templates=total_templates_for_task,
                    completed_templates=previously_processed + completed_so_far
                )

                # Log progress
                rules_count = len(opt_result.rules) if opt_result.rules else 0
                logs_count = len(opt_result.evaluated_logs) if opt_result.evaluated_logs else 0
                status = "SUCCESS" if opt_result.success else f"FAILED: {opt_result.error_message}"
                aiopt_logger.info(
                    f"{opt_result.sql_progress} Completed ({status}): "
                    f"{rules_count} rules, {logs_count} logs. "
                    f"Cumulative: {len(all_rules)} rules ({optimized_count} optimized, {reset_count} reset)"
                )

            # Execute with parallel pool
            # Pool manager temporarily replaces SIGUSR1 handler to terminate pool,
            # then restores executor handler when done
            results, was_aborted = pool_manager.execute_parallel(
                payloads=payloads,
                worker_fn=optimize_template_worker,
                on_result=on_result_callback
            )

            result.stats.processed_templates = previously_processed + len(results)

            if was_aborted:
                aiopt_logger.warning(
                    f"[Executor] Optimization interrupted (signal) after {len(results)}/{remaining_templates} SQLs. "
                    f"Proceeding to apply {len(all_rules)} generated rules."
                )

            aiopt_logger.info(
                f"[Executor] Stored {len(all_rules)} rules: "
                f"{optimized_count} optimized, {reset_count} reset"
            )

            total_rules_generated = len(all_rules)

            stage = ExecutionStage.APPLYING
            _update_progress(
                stage=stage.value,
                total_templates=total_templates_for_task,
                completed_templates=result.stats.processed_templates
            )

            # 13. Query latest master node info before applying rules
            try:
                from interfaces import query_master_node_info
                latest_master_node = query_master_node_info(
                    instance_info, mocked_master_node=executor_input.master_node
                )
                latest_online_config = latest_master_node.to_instance_config(
                    read_only=False,
                    with_ai_marker=True,
                    allow_reconnect=True,
                    is_meta_server=False
                )
                online_controller = DBController(latest_online_config)
                aiopt_logger.info(
                    f"[Executor] Refreshed master node: "
                    f"{latest_master_node.node_ip}:{latest_master_node.node_port}"
                )
            except Exception as e:
                aiopt_logger.warning(
                    f"[Executor] Failed to query master node info, aborting rule application: {e}"
                )
                raise

            # 14. Apply rules to user instance
            # Executor handler is active: SIGUSR1 sets flag but does not kill process
            applied_success = 0
            if all_rules and instance_info.outline_type.value == "spm":
                from optimizer.spm_operator import SPMRuleApplier

                aiopt_logger.info(f"[Executor] Applying {len(all_rules)} rules to user instance (SPM)...")
                applier = SPMRuleApplier(
                    online_controller=online_controller,
                    meta_controller=self.meta_controller
                )
                success, failed, skipped = applier.apply_rules(
                    rules=all_rules,
                    task_id=task_id,
                    cluster_id=cluster_id,
                    instance_id=instance_id
                )
                applied_success = success
                aiopt_logger.info(
                    f"[Executor] SPM Rule application completed: "
                    f"{success} success, {failed} failed, {skipped} skipped"
                )
            elif all_rules and instance_info.outline_type.value == "statement_outline":
                from optimizer.statement_outline_operator import StatementOutlineRuleApplier

                aiopt_logger.info(
                    f"[Executor] Applying {len(all_rules)} rules to user instance (Statement Outline)..."
                )
                applier = StatementOutlineRuleApplier(
                    online_controller=online_controller,
                    meta_controller=self.meta_controller
                )
                success, failed, skipped = applier.apply_rules(
                    rules=all_rules,
                    task_id=task_id,
                    cluster_id=cluster_id,
                    instance_id=instance_id
                )
                applied_success = success
                aiopt_logger.info(
                    f"[Executor] Statement Outline Rule application completed: "
                    f"{success} success, {failed} failed, {skipped} skipped"
                )

            result.stats.rules_applied = applied_success

            stage = ExecutionStage.FINISHED

            aiopt_logger.info(
                f"[Executor] Task {task_id} completed: "
                f"{remaining_templates} SQL templates, {total_rules_generated} rules, "
                f"training time: {total_training_time:.1f}s"
            )

            # Determine final status (stage and status are decoupled)
            result.extra["stage"] = stage.value  # FINISHED
            if was_aborted:
                result.status = ExecutionStatus.INTERRUPTED
                result.extra["interrupt_stage"] = ExecutionStage.OPTIMIZING.value
            else:
                result.status = ExecutionStatus.COMPLETED
            result.stats.rules_generated = total_rules_generated

        except Exception as e:
            result.extra["error"] = str(e)
            result.extra["stage"] = stage.value
            result.extra["interrupt_stage"] = stage.value
            # status remains FAILED (default)
            aiopt_logger.error(f"[Executor] Task {task_id} failed: {e}")
            import traceback
            traceback.print_exc()
        finally:
            try:
                result.stats.failed_templates = failed_templates
                if failed_counts:
                    result.extra["failed_templates_detail"] = failed_counts
            except NameError:
                pass  # variables not yet defined if exception before their declaration

            result.duration_seconds = time.time() - start_time

            # Restore original SIGUSR1 handler
            signal.signal(signal.SIGUSR1, old_handler)

            # Update final progress
            _update_progress(
                stage=result.extra.get("stage", stage.value),
                total_templates=result.stats.total_templates,
                completed_templates=result.stats.processed_templates
            )

            # Write workload diagnosis (CK data availability) to result
            try:
                if preprocessor.workload_diagnosis is not None:
                    result.extra["workload_diagnosis"] = preprocessor.workload_diagnosis
            except NameError:
                pass

            # Record execution history
            try:
                ExecutionHistoryController.record(
                    controller=self.meta_controller,
                    task_id=task_id,
                    instance_id=instance_id,
                    node_uuid=node_uuid,
                    executor_input=executor_input,
                    result=result
                )
            except Exception as e:
                aiopt_logger.warning(f"[Executor] Failed to record execution history: {e}")

            # Close controllers created by this method
            try:
                training_controller.close()
            except (NameError, Exception):
                pass
            try:
                online_controller.close()
            except (NameError, Exception):
                pass

        return result
