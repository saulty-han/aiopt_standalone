import logging
import os
import threading
import multiprocessing
from logging.handlers import RotatingFileHandler, QueueHandler
from config.config import GlobalConfig

# Thread-local storage for task context
_task_context = threading.local()

def set_task_context(task_id: str = None, cluster_id: str = None, instance_id: str = None):
    """Sets the task context for the current process/thread."""
    _task_context.task_id = task_id
    _task_context.cluster_id = cluster_id
    _task_context.instance_id = instance_id

# Save original factory
_old_factory = logging.getLogRecordFactory()

def context_factory(*args, **kwargs):
    record = _old_factory(*args, **kwargs)
    # Inject context into record
    record.task_id = getattr(_task_context, 'task_id', None)
    record.cluster_id = getattr(_task_context, 'cluster_id', None)
    record.instance_id = getattr(_task_context, 'instance_id', None)
    
    # Prefix only shows TaskID to keep logs concise
    record.ctx_prefix = f"[TaskID:{record.task_id}] " if record.task_id else ""
    return record

logging.setLogRecordFactory(context_factory)

# Default log format (now includes %(ctx_prefix)s)
LOG_FORMAT = "[%(asctime)s.%(msecs)03d - %(process)d - %(name)s][%(levelname)s] %(ctx_prefix)s%(message)s"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

class LogDispatcherThread(threading.Thread):
    """Dispatches Queue records to Main Process loggers."""
    def __init__(self, queue):
        super().__init__(name="LogDispatcherThread", daemon=True)
        self.queue = queue
        self._stop_event = threading.Event()

    def run(self):
        while not self._stop_event.is_set():
            try:
                record = self.queue.get(timeout=0.5)
                if record is None: break
                # Route back to naming-matched logger in Main process
                name = record.name if record.name != "root" else ""
                logging.getLogger(name).handle(record)
            except Exception: continue

    def stop(self): self._stop_event.set()

def _add_file_handler(logger, filename):
    """Creates and attaches a rotating file handler."""
    log_dir = GlobalConfig.log_dir
    os.makedirs(log_dir, exist_ok=True)
    handler = RotatingFileHandler(
        os.path.join(log_dir, filename), 
        maxBytes=GlobalConfig.log_max_bytes, 
        backupCount=GlobalConfig.log_backup_count, 
        encoding='utf-8'
    )
    handler.setFormatter(logging.Formatter(LOG_FORMAT, DATE_FORMAT))
    logger.addHandler(handler)
    return handler

def _init_main_handlers():
    """Initializes file handlers for the main process (lazily or at import)."""
    # Prevent double initialization
    aiopt = logging.getLogger("ai_optimizer")
    if any(isinstance(h, RotatingFileHandler) for h in aiopt.handlers):
        return

    level = logging.getLevelName(GlobalConfig.log_level or "INFO")
    aiopt_handler = _add_file_handler(aiopt, "ai_optimizer.log")
    _add_file_handler(logging.getLogger("perf_monitor"), "perf_monitor.log")
    _add_file_handler(logging.getLogger("mcts"), "mcts.log")

    # Main root also goes to ai_optimizer.log
    root = logging.getLogger()
    for h in root.handlers[:]: root.removeHandler(h)
    root.addHandler(aiopt_handler)
    
    for name in [None, "ai_optimizer", "perf_monitor", "mcts"]:
        l = logging.getLogger(name)
        l.setLevel(level)
        l.propagate = False # avoid double-logs via root propagation in Main

def setup_logging_queue():
    """Starts the LogDispatcherThread for multi-process support."""
    _init_main_handlers() # Ensure handlers are ready
    queue = multiprocessing.Queue(-1)
    dispatcher = LogDispatcherThread(queue)
    dispatcher.start()
    return queue, dispatcher

def configure_worker_logger(queue):
    """Worker setup: all logs go to Queue."""
    root = logging.getLogger()
    root.setLevel(logging.getLevelName(GlobalConfig.log_level or "INFO"))
    for h in root.handlers[:]: root.removeHandler(h)
    root.addHandler(QueueHandler(queue))
    # Ensure children propagate to root QueueHandler
    logging.getLogger("ai_optimizer").propagate = True
    logging.getLogger("perf_monitor").propagate = True
    logging.getLogger("mcts").propagate = True

# Global logger objects
aiopt_logger = logging.getLogger("ai_optimizer")
perf_logger = logging.getLogger("perf_monitor")
mcts_logger = logging.getLogger("mcts")

# Initialize main process logging immediately
_init_main_handlers()
