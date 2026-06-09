from ai_logger import aiopt_logger
from functools import wraps
import threading
from sqlalchemy import TextClause, text, Connection, Engine, create_engine
from sqlalchemy.engine import URL
from sqlalchemy.exc import OperationalError, PendingRollbackError
from data_models import InstanceConfig

from ai_exception import InstanceNonReadonlyException, DBConnectionError
from utils import sql_utils


def retry_on_connection_loss(func):
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        last_exception = None
        for attempt in range(2):
            try:
                return func(self, *args, **kwargs)
            except PendingRollbackError as e:
                last_exception = e
                if self.config.allow_reconnect:
                    aiopt_logger.warning(
                        f"Database connection ({self.config}) has pending rollback, "
                        f"discarding and retrying... (attempt {attempt + 1})"
                    )
                    if hasattr(self.thd_local, 'conn') and self.thd_local.conn:
                        try:
                            self.thd_local.conn.close()
                        except Exception:
                            pass
                        self.thd_local.conn = None
                    continue
                raise
            except OperationalError as e:
                last_exception = e
                if self.config.allow_reconnect:
                    if hasattr(e, 'orig') and hasattr(e.orig, 'args') and e.orig.args and e.orig.args[0] in (2006, 2013):
                        aiopt_logger.warning(f"Database connection ({self.config}) lost, retrying... (attempt {attempt + 1})")
                        if hasattr(self.thd_local, 'conn') and self.thd_local.conn:
                            self.thd_local.conn.close()
                            self.thd_local.conn = None
                        continue
                raise
        raise last_exception
    return wrapper


class DBController():
    _instances = set()
    
    def __new__(cls, *args, **kwargs):
        instance = super().__new__(cls)
        cls._instances.add(instance)
        return instance

    def __del__(self):
        self._disconnect()
        self._dispose()
        type(self)._instances.discard(self)

    def __init__(self, config: InstanceConfig, db: str = "mysql", is_training_env: bool = False, feature_flags: "FeatureFlags" = None):
        # 训练环境必须提供 feature_flags
        if is_training_env and feature_flags is None:
            raise ValueError("feature_flags is required when is_training_env=True")
        
        self.config = config
        self.is_training_env = is_training_env
        self.feature_flags = feature_flags
        self.connection_list = []
        self.known_connection_ids = set()
        self.thd_local = threading.local()
        self.thd_local.conn = None
        self.thd_local.db = db if db else "mysql"
        
        # Create private engine for this instance
        self._engine = self._create_engine()

    def _create_engine(self) -> Engine:
        """
        Creates a private SQLAlchemy engine for this DBController instance.
        :return: SQLAlchemy Engine object
        """
        database_url = URL.create(
            drivername="mysql+pymysql",
            username=self.config.user,
            password=self.config.password,
            host=self.config.ip,
            port=self.config.port,
        )
        
        engine = create_engine(
            database_url,
            echo=False,
            pool_size=64,
            max_overflow=64,
            pool_timeout=1800,
            pool_recycle=3600,
            isolation_level="AUTOCOMMIT"
        )
        
        return engine

    def _connect_if_loss(self):
        if not self._is_connect():
            # Ensure engine is available
            if not hasattr(self, '_engine') or self._engine is None:
                self._engine = self._create_engine()
                
            # Create new connection using private engine
            try:
                self.thd_local.conn = self._engine.connect()
            except OperationalError as e:
                self.thd_local.conn = None
                aiopt_logger.error(f"Failed to connect to the database: {e}")
                raise

            conn_id = id(self.thd_local.conn)

            conn = self.thd_local.conn
            if conn_id not in self.known_connection_ids:
                # Apply the session settings directly without using self.execute()
                # to avoid recursive calls to _connect_if_loss

                try:
                    # Always use the specified database
                    conn.execute(text(f"USE `{self.thd_local.db}`"))
                    
                    # Skip session optimizations for metadata server (only needed for business DBs)
                    if not self.config.is_meta_server:
                        # Apply session optimizations for business databases
                        settings = [
                            "SET SESSION long_query_time = 31536000",  # do not log slow query
                            "SET SESSION sql_log_off = 1",  # disable general log
                            # "SET SESSION sql_log_bin = 0",  # disable binlog for this session
                            "SET SESSION sql_notes = 0",  # disable warnings
                        ]
                        
                        # Execute all settings
                        for setting in settings:
                            conn.execute(text(setting))
                        
                        # For training environments, disable SPM and/or Statement Outline
                        if self.is_training_env:
                            self._disable_plan_intervention_features(conn)

                    # Only add to known connections if all settings were successfully applied
                    self.connection_list.append(self.thd_local.conn)
                    self.known_connection_ids.add(conn_id)

                except Exception as e:
                    self.thd_local.conn.close()
                    self.thd_local.conn = None
                    aiopt_logger.error(f"Failed to apply preset: {e}")
                    raise DBConnectionError(f"Session setup failed: {e}") from e
    
    @retry_on_connection_loss
    def use_db(self, db: str):
        """
        Use a specific database in the connection if it is not already set.
        :param db: Database name to use
        """
        if not db:
            raise ValueError("Database name cannot be empty.")
        if db == self.thd_local.db:
            return

        self._connect_if_loss()
        try:
            self.thd_local.conn.execute(text(f"USE `{db}`"))
            self.thd_local.db = db
        except Exception as e:
            aiopt_logger.error(f"Failed to use database `{db}`: {e}")
            raise

    def _disconnect(self):
        """
        Disconnect from the database.
        This function closes the connection if it is already established.
        """
        if self._is_connect():
            self.thd_local.conn.close()
            self.thd_local.conn = None

    def _dispose(self):
        """
        Dispose of all connections and clean up resources.
        """
        self.known_connection_ids.clear()
        for conn in self.connection_list:
            if conn is not None:
                try:
                    conn.close()
                except Exception as e:
                    aiopt_logger.error(f"Error closing connection: {e}")
        self.connection_list.clear()
        
        # Dispose of the private engine
        if hasattr(self, '_engine') and self._engine:
            try:
                self._engine.dispose()
            except Exception as e:
                aiopt_logger.error(f"Error disposing engine: {e}")

    def close(self):
        """
        Close the connection and dispose of all resources.
        Public API for cleaning up DBController instances.
        """
        self._disconnect()
        self._dispose()

    def _is_connect(self):
        return getattr(self.thd_local, 'conn', None) is not None
    
    def _disable_session_variable(self, conn: Connection, variable_name: str, feature_name: str):
        """
        禁用指定的 session 变量
        
        :param conn: 数据库连接
        :param variable_name: 变量名（如 'txsql_spm_use_plan_baseline'）
        :param feature_name: 功能名称（用于日志，如 'SPM'）
        """
        try:
            conn.execute(text(f"SET SESSION {variable_name} = OFF"))
            aiopt_logger.debug(f"Training environment: disabled {feature_name} for {self.config.instance_id}")
        except Exception as e:
            aiopt_logger.error(f"Training environment: Failed to disable {feature_name}: {e}")
            raise DBConnectionError(f"Failed to disable {feature_name} ({variable_name})") from e
    
    def _disable_spm_feature(self, conn: Connection):
        """禁用 SPM plan baseline 功能"""
        self._disable_session_variable(conn, 'txsql_spm_use_plan_baseline', 'SPM')
    
    def _disable_outline_feature(self, conn: Connection):
        """禁用 Statement Outline 功能"""
        self._disable_session_variable(conn, 'statement_outline_enable_apply', 'Statement Outline')
    
    def _disable_plan_intervention_features(self, conn: Connection):
        """
        根据 feature_flags 禁用相应的计划干预功能
        
        原则：如果实例支持某个 feature，就必须禁用它
        要求：必须传入 feature_flags，禁用必须成功
        """
        if not self.feature_flags:
            raise ValueError("feature_flags is required for training environment")
        
        if self.feature_flags.supports_spm:
            self._disable_spm_feature(conn)
        if self.feature_flags.supports_statement_outline:
            self._disable_outline_feature(conn)




    def _assert_readonly_if_required(self, conn: Connection):
        if self.config.read_only:
            try:
                result = conn.execute(text("select @@global.read_only"))
            except Exception:
                raise  # 连接级异常直接穿透，让 retry_on_connection_loss 处理
            if result.scalar() != 1:
                raise InstanceNonReadonlyException("Current connection is not read-only.")

    def _prefix_sql_with_ai_marker_if_needed(self, statement: TextClause) -> TextClause:
        if not isinstance(statement, TextClause):
            aiopt_logger.error("Only TextClause statements are supported.")
            raise ValueError("Only TextClause statements are supported.")
        
        if self.config.with_ai_marker:
            return text(f"/* TxsqlAImarker */ {str(statement)}")
        else:
            return statement

    @retry_on_connection_loss
    def execute(self, statement, *args, **kwargs):
        """
        Execute a SQL query. All parameters are passed to the underlying SQLAlchemy connection.
        If 'timeout_seconds' is present in kwargs, the session max_execution_time is set for the
        duration of the query and restored afterwards.
        """
        if isinstance(statement, TextClause) and sql_utils.is_use_statement(str(statement)):
            aiopt_logger.error("Use statement is not allowed in execute method. Use use_db method instead.")
            raise ValueError("Use statement is not allowed in execute method. Use use_db method instead.")

        timeout_seconds = kwargs.pop('timeout_seconds', None)
        statement_rewritten = self._prefix_sql_with_ai_marker_if_needed(statement)

        self._connect_if_loss()
        conn: Connection = self.thd_local.conn
        self._assert_readonly_if_required(conn)

        if timeout_seconds is None:
            return conn.execute(statement_rewritten, *args, **kwargs)

        # --- timeout path: save → set → execute → restore ---
        original_timeout = 0
        try:
            res = conn.execute(text("SELECT @@SESSION.max_execution_time"))
            val = res.scalar()
            if val is not None:
                original_timeout = val
        except Exception as e:
            aiopt_logger.warning(f"Failed to capture current max_execution_time: {e}")

        try:
            conn.execute(text("SET SESSION max_execution_time = :max_execution_time"),
                         {"max_execution_time": int(timeout_seconds * 1000)})
            return conn.execute(statement_rewritten, *args, **kwargs)
        finally:
            if self.thd_local.conn:
                try:
                    conn.execute(text(f"SET SESSION max_execution_time = {original_timeout}"))
                except Exception as e:
                    error_msg = str(e).lower()
                    if 'invalid transaction' in error_msg:
                        try:
                            conn.rollback()
                            conn.execute(text(f"SET SESSION max_execution_time = {original_timeout}"))
                        except Exception as retry_err:
                            aiopt_logger.warning(f"Failed to restore max_execution_time after rollback: {retry_err}")
                            self.thd_local.conn = None
                    else:
                        aiopt_logger.warning(f"Failed to restore max_execution_time: {e}")
                        self.thd_local.conn = None

    @retry_on_connection_loss
    def _evaluate_elapsed_time_with_result_by_profiling(self, *args, **kwargs):
        """
        Execute a SQL query with profiling. All parameters are passed to the underlying SQLAlchemy connection.
        :param args: Positional arguments for the SQL query.
        :param kwargs: Keyword arguments for the SQL query.
        :return: The result of the query execution, if applicable.
        :raises ValueError: If query was interrupted (warning 1317/3024) or no profiling data.
        """
        # Extract custom arguments (timeout_seconds: caller specifies in seconds, converted to ms for MySQL)
        timeout_seconds = kwargs.pop('timeout_seconds', None)
        return_on_timeout = kwargs.pop('return_on_timeout', False)
        
        self._connect_if_loss()
        conn: Connection = self.thd_local.conn
        self._assert_readonly_if_required(conn)
        
        # 1. Capture current max_execution_time
        original_timeout = 0
        try:
            # Query session variable. Note: returns value in milliseconds usually
            res = conn.execute(text("SELECT @@SESSION.max_execution_time"))
            val = res.scalar()
            if val is not None:
                original_timeout = val
        except Exception as e:
            aiopt_logger.warning(f"Failed to capture current max_execution_time: {e}")
            
        try:
            conn.execute(text("SET profiling = 1"))
            
            # 2. Set max_execution_time if caller specified timeout
            if timeout_seconds is not None:
                conn.execute(text("set session max_execution_time = :max_execution_time"),
                             {"max_execution_time": int(timeout_seconds * 1000)})
                         
            result = conn.execute(*args, **kwargs)
            exec_result = result.fetchall()
        
            # 显式执行 SHOW WARNINGS 检查是否有执行中断
            # 不再依赖 cursor.warning_count，直接执行以确保稳健性
            # SHOW WARNINGS 会创建一个 profile 条目
            # 检查的错误码：1317=Query execution was interrupted, 3024=max_execution_time exceeded
            INTERRUPTED_CODES = {1317, 3024}
            warnings_result = conn.execute(text("SHOW WARNINGS"))
            warnings = warnings_result.fetchall()
            if not return_on_timeout:
                for warning in warnings:
                    if len(warning) >= 2 and warning[1] in INTERRUPTED_CODES:
                        raise ValueError(f"Query execution was interrupted (code={warning[1]}): {warning[2] if len(warning) > 2 else 'unknown'}")
            
            # 获取 profiling 结果
            # 因为总是执行了 SHOW WARNINGS，它会在 profiles 中产生一条记录
            # 所以主查询的时间总是 profiles[-2]
            profiling_result = conn.execute(text("SHOW PROFILES"))
            if profiling_result.rowcount is not None and profiling_result.rowcount == 0:
                # 注意: 某些驱动 rowcount 可能不准确，但这里通常有效
                 # 如果确实为空 list，下面 fetchall 会是空
                 pass
                 
            profiles = profiling_result.fetchall()
            if not profiles or len(profiles) < 2:
                aiopt_logger.warning(f"Insufficient profiling data: {profiles}")
                raise ValueError(f"Insufficient profiling data: expected at least 2 entries, got {len(profiles) if profiles else 0}")
                
            elapsed_time = profiles[-2][1]
                
            return (elapsed_time, exec_result)
        
        except ValueError:
            # ValueError is raised by our own code when we detect timeout warnings via SHOW WARNINGS.
            # In this case, the query did complete (just interrupted), and the connection is still valid.
            # No rollback is needed - just re-raise to let caller handle.
            raise
            
        except Exception as e:
            # Real execution errors (connection lost, syntax error, etc.)
            # Don't attempt rollback here - let finally block handle cleanup if needed
            raise
            
        finally:
            # 3. Restore original max_execution_time
            if self.thd_local.conn:  # Only try if connection is still valid
                try:
                    conn.execute(text(f"set session max_execution_time = {original_timeout}"))
                except Exception as e:
                    error_msg = str(e).lower()
                    # Only attempt rollback if error specifically indicates invalid transaction state
                    if 'invalid transaction' in error_msg:
                        try:
                            conn.rollback()
                            conn.execute(text(f"set session max_execution_time = {original_timeout}"))
                        except Exception as retry_err:
                            aiopt_logger.warning(f"Failed to restore max_execution_time after rollback: {retry_err}")
                            self.thd_local.conn = None
                    else:
                        # Other errors - just invalidate connection
                        aiopt_logger.warning(f"Failed to restore max_execution_time: {e}")
                        self.thd_local.conn = None

    
    def evaluate_elapsed_time_with_result(self, statement, *args, **kwargs):
        """
        Execute a SQL query and return it's elapsed_time. All parameters are passed to the underlying SQLAlchemy connection.
        :param args: Positional arguments for the SQL query.
        :param kwargs: Keyword arguments for the SQL query.
        :return: The result of the query execution, if applicable.
        """
        warmup_runs = kwargs.pop('warmup_runs', 0)
        statement_rewritten = self._prefix_sql_with_ai_marker_if_needed(statement)
        
        if warmup_runs > 0:
            for _ in range(warmup_runs):
                try:
                    # Use internal profiling method for warmup to ensure same timeout/error checks
                    # Pass a copy of kwargs because the method pops 'timeout_seconds'
                    self._evaluate_elapsed_time_with_result_by_profiling(statement_rewritten, *args, **kwargs.copy())
                except Exception as e:
                    # Decide whether to abort or continue. 
                    # If execution fails, likely main execution will too.
                    raise
                    
        return self._evaluate_elapsed_time_with_result_by_profiling(statement_rewritten, *args, **kwargs)
