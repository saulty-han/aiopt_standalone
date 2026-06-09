"""
Parallel Worker Module

Worker process entry point for parallel SQL template optimization.
Recreates DBController and optimizer in worker process.

This module is designed to be used with multiprocessing, where each worker process
receives a serializable OptimizationTaskPayload and returns a serializable
OptimizationTaskResult.
"""

import time
from ai_logger import aiopt_logger
from data_models import (
    OptimizationTaskPayload, OptimizationTaskResult,
    DecidedRule, ValidationLogEntry
)
from db_controller import DBController
from optimizer.basic_optimizer import OptimizationContext
from optimizer.factory import OptimizerFactory
from config.config import GlobalConfig


def optimize_template_worker(payload: OptimizationTaskPayload) -> OptimizationTaskResult:
    """
    Worker function for parallel optimization.

    This function runs in a worker process. It:
    1. Creates new DBController instance for training environment
    2. Creates optimizer with new context (via OptimizerFactory)
    3. Executes optimize_template()
    4. Cleans up connections
    5. Returns serializable result

    Args:
        payload: Serializable task payload containing all necessary data

    Returns:
        OptimizationTaskResult with rules, logs, and execution status
    """
    log_prefix = payload.sql_progress
    start_time = time.time()

    env_controller = None

    from ai_logger import set_task_context
    set_task_context(
        task_id=payload.task_id,
        cluster_id=payload.cluster_id,
        instance_id=payload.instance_id
    )

    try:
        aiopt_logger.info(
            f"{log_prefix} Worker starting: db={payload.db}, digest={payload.digest}"
        )

        # 1. Create training environment DBController
        # env_config is already a complete InstanceConfig (pre-computed by main process)
        env_controller = DBController(
            payload.env_config,
            is_training_env=True,
            feature_flags=payload.feature_flags
        )

        # 2. Create OptimizationContext
        context = OptimizationContext(
            task_id=payload.task_id,
            instance_id=payload.instance_id,
            outline_type=payload.outline_type,
            training_controller=env_controller,
            env_type=payload.env_type,
            feature_flags=payload.feature_flags,
            instance_info=payload.instance_info
        )

        # 4. Create Optimizer using Factory
        aiopt_logger.info(f"{log_prefix} Using optimizer: {GlobalConfig.optimizer_type}")
        optimizer = OptimizerFactory.create_optimizer(GlobalConfig.optimizer_type, context)

        # 5. Execute optimization
        rules, evaluated_logs, digest_text = optimizer.optimize_template(
            db=payload.db,
            digest=payload.digest,
            workloads=payload.workloads,
            sql_progress=payload.sql_progress
        )

        training_time = optimizer.training_time

        # Get MCTS results if available (only for LLMOptimizer)
        mcts_results = None
        if hasattr(optimizer, 'mcts_results'):
            mcts_results = optimizer.mcts_results

        aiopt_logger.info(
            f"{log_prefix} Worker completed: {len(rules)} rules, "
            f"{len(evaluated_logs)} logs, {training_time:.1f}s training time"
        )

        return OptimizationTaskResult(
            db=payload.db,
            digest=payload.digest,
            sql_progress=payload.sql_progress,
            success=True,
            rules=rules,
            evaluated_logs=evaluated_logs,
            mcts_results=mcts_results,
            digest_text=digest_text,
            training_time=training_time,
            error_message=None
        )

    except Exception as e:
        import traceback
        error_msg = f"{type(e).__name__}: {str(e)}"
        aiopt_logger.error(f"{log_prefix} Worker failed: {error_msg}")
        aiopt_logger.debug(traceback.format_exc())

        return OptimizationTaskResult(
            db=payload.db,
            digest=payload.digest,
            sql_progress=payload.sql_progress,
            success=False,
            error_type=type(e).__name__,
            rules=[],
            evaluated_logs=[],
            training_time=time.time() - start_time,
            error_message=error_msg
        )

    finally:
        # 6. Cleanup connections
        if env_controller:
            try:
                env_controller.close()
            except Exception as e:
                aiopt_logger.warning(f"{log_prefix} Error closing env controller: {e}")
