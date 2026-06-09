"""
Parallel Pool Manager Module

Manages process pool for parallel SQL template optimization.
Handles signal-based termination for task control.

Features:
- Configurable parallelism (1-16 workers)
- SIGUSR1: Immediate abort - terminate all workers
- Result collection as tasks complete
- Centralized Logging: Workers send logs to main process via Queue
"""

import ctypes
import signal
import sys
from concurrent.futures import ProcessPoolExecutor, Future, as_completed, CancelledError
from typing import Callable
import multiprocessing

from ai_logger import aiopt_logger, setup_logging_queue, configure_worker_logger
from data_models import OptimizationTaskPayload, OptimizationTaskResult


def _set_pdeathsig():
    """Request kernel to send SIGKILL to this process when its parent dies (Linux only).

    Uses prctl(PR_SET_PDEATHSIG) so that orphaned worker processes are cleaned up
    immediately instead of lingering until their work completes or TCP timeouts expire.
    """
    if sys.platform != "linux":
        return
    try:
        PR_SET_PDEATHSIG = 1
        ctypes.CDLL(None, use_errno=True).prctl(PR_SET_PDEATHSIG, signal.SIGKILL, 0, 0, 0)
    except Exception:
        # Non-fatal: worst case is the old behavior (orphaned workers stay alive).
        pass


def _worker_init(log_queue):
    """Combined worker process initializer: pdeathsig + centralized logging."""
    _set_pdeathsig()
    configure_worker_logger(log_queue)


class ParallelPoolManager:
    """
    Manages process pool for parallel optimization.

    Task control is handled exclusively via SIGUSR1 signal.
    Timeout control is the responsibility of the management layer (外部管控).

    Usage:
        pool_manager = ParallelPoolManager(max_workers=4)
        results, was_aborted = pool_manager.execute_parallel(
            payloads=payloads,
            worker_fn=optimize_template_worker,
            on_result=callback
        )
    """

    def __init__(self, max_workers: int = 1):
        """
        Initialize pool manager.

        Args:
            max_workers: Number of parallel workers (1-16, default 1)
        """
        self.max_workers = max(1, min(16, max_workers))
        self._executor: ProcessPoolExecutor | None = None
        self._futures: dict[Future, OptimizationTaskPayload] = {}
        self._abort_requested = False
        self._old_sigusr1_handler = None
        
        # Setup Centralized Logging Queue (and start Dispatcher Thread)
        self.log_queue, self.log_dispatcher = setup_logging_queue()

        aiopt_logger.info(f"[PoolManager] Initialized with {self.max_workers} workers")

    def _sigusr1_handler(self, signum, frame):
        """Handle SIGUSR1 - immediate abort (terminate all workers)."""
        self._abort_requested = True
        aiopt_logger.warning("[PoolManager] SIGUSR1 received - aborting all workers")
        self._terminate_pool()

    def _terminate_pool(self):
        """Terminate the process pool immediately by killing worker processes."""
        if self._executor:
            # Cancel all pending futures
            for future in self._futures:
                future.cancel()

            # Kill all worker processes immediately
            # ProcessPoolExecutor._processes is a dict of {pid: process}
            if hasattr(self._executor, '_processes'):
                for pid, process in list(self._executor._processes.items()):
                    if process.is_alive():
                        aiopt_logger.info(f"[PoolManager] Terminating worker process {pid}")
                        try:
                            process.terminate()
                        except Exception as e:
                            aiopt_logger.warning(f"[PoolManager] Error terminating process {pid}: {e}")

            # Shutdown pool (cleanup)
            try:
                self._executor.shutdown(wait=False, cancel_futures=True)
            except Exception as e:
                aiopt_logger.warning(f"[PoolManager] Error during shutdown: {e}")

            self._executor = None
            aiopt_logger.info("[PoolManager] Pool terminated")

    def _setup_signal_handlers(self):
        """Setup signal handler for SIGUSR1 (abort)."""
        self._old_sigusr1_handler = signal.signal(signal.SIGUSR1, self._sigusr1_handler)

    def _restore_signal_handlers(self):
        """Restore original signal handler."""
        if self._old_sigusr1_handler is not None:
            try:
                signal.signal(signal.SIGUSR1, self._old_sigusr1_handler)
            except Exception as e:
                aiopt_logger.warning(f"[PoolManager] Error restoring SIGUSR1 handler: {e}")
            self._old_sigusr1_handler = None

    @property
    def is_aborted(self) -> bool:
        """Check if abort was requested."""
        return self._abort_requested

    def execute_parallel(
        self,
        payloads: list[OptimizationTaskPayload],
        worker_fn: Callable[[OptimizationTaskPayload], OptimizationTaskResult],
        on_result: Callable[[OptimizationTaskResult], None] | None = None
    ) -> tuple[list[OptimizationTaskResult], bool]:
        """
        Execute optimization tasks in parallel.

        Args:
            payloads: List of task payloads
            worker_fn: Worker function to execute
            on_result: Optional callback for each completed result

        Returns:
            Tuple of (completed results, was_aborted)
        """
        if not payloads:
            return [], False

        self._abort_requested = False
        self._setup_signal_handlers()

        results: list[OptimizationTaskResult] = []

        try:
            # Pass log_queue to workers via initializer
            # _worker_init also sets PR_SET_PDEATHSIG so workers are killed if parent dies unexpectedly
            self._executor = ProcessPoolExecutor(
                max_workers=self.max_workers,
                initializer=_worker_init,
                initargs=(self.log_queue,)
            )
            self._futures = {}

            # Submit all tasks
            for payload in payloads:
                if self._abort_requested:
                    aiopt_logger.info(
                        f"[PoolManager] Abort requested, submitted {len(self._futures)}/{len(payloads)} tasks"
                    )
                    break
                future = self._executor.submit(worker_fn, payload)
                self._futures[future] = payload

            aiopt_logger.info(
                f"[PoolManager] Submitted {len(self._futures)}/{len(payloads)} tasks to pool"
            )

            # Collect results as they complete
            for future in as_completed(self._futures):
                if self._abort_requested:
                    aiopt_logger.info(
                        f"[PoolManager] Abort requested, collected {len(results)} results"
                    )
                    break

                try:
                    result = future.result(timeout=1.0)
                except CancelledError:
                    payload = self._futures[future]
                    aiopt_logger.debug(f"[PoolManager] Task was cancelled: {payload.db}/{payload.digest}")
                    result = OptimizationTaskResult(
                        db=payload.db, digest=payload.digest, sql_progress=payload.sql_progress,
                        success=False, error_type="CancelledError", error_message="Task cancelled"
                    )
                except TimeoutError:
                    try:
                        result = future.result()
                    except Exception as e:
                        payload = self._futures[future]
                        aiopt_logger.error(f"[PoolManager] Task result error: {payload.db}/{payload.digest}: {e}")
                        result = OptimizationTaskResult(
                            db=payload.db, digest=payload.digest, sql_progress=payload.sql_progress,
                            success=False, error_type=type(e).__name__, error_message=f"Task result error: {e}"
                        )
                except Exception as e:
                    payload = self._futures[future]
                    aiopt_logger.error(f"[PoolManager] Future error: {payload.db}/{payload.digest}: {e}")
                    result = OptimizationTaskResult(
                        db=payload.db, digest=payload.digest, sql_progress=payload.sql_progress,
                        success=False, error_type=type(e).__name__, error_message=f"Future error: {e}"
                    )

                # callback 在 future try-except 外面 — 异常自然冒泡，不被吞掉
                results.append(result)
                if on_result:
                    on_result(result)

        except Exception as e:
            aiopt_logger.error(f"[PoolManager] Execution error: {e}")
            raise

        finally:
            self._restore_signal_handlers()
            if self._executor:
                try:
                    self._executor.shutdown(wait=True)
                except Exception as e:
                    aiopt_logger.warning(f"[PoolManager] Error during final shutdown: {e}")
                self._executor = None
            
            # Stop Log Dispatcher
            if self.log_dispatcher:
                self.log_dispatcher.stop()

        return results, self._abort_requested
