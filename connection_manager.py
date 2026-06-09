from sqlalchemy import Engine, create_engine, Connection, text
from sqlalchemy.engine import URL
from typing import Dict
from data_models import InstanceConfig
import logging


class ConnectionManager:
    """
    Manages database connections per instance using SQLAlchemy.
    """
    _engines: Dict[str, Engine] = {}

    def _get_database_url(self, config: InstanceConfig) -> URL:
        """
        Constructs the database URL from the instance configuration.
        :param config: InstanceConfig object containing connection details
        :return: Database URL object
        """
        return URL.create(
            drivername="mysql+pymysql",
            username=config.user,
            password=config.password,
            host=config.ip,
            port=config.port,
        )
    
    def _is_connection_available(self, obj: Connection) -> bool:
        """
        Check if the Connection object is available
        :param obj: Connection object
        :return: True if available, False otherwise
        """
        try:
            obj.execute(text("SELECT 1"))
            logging.error(f"Connection is available: {obj}")
            return True
        except Exception as e:
            logging.error(f"Connection is not available: {obj}, exception occurred: {e}")
            return False
    
    def _engine_id(self, config: InstanceConfig) -> str:
        """
        Generates a unique identifier for the engine based on instance configuration.
        :param config: InstanceConfig object containing connection details
        :return: Unique engine identifier string
        """
        return f"{config.instance_id}_{config.ip}_{config.port}"
    
    def _create_engine(self, config: InstanceConfig) -> None:
        """
        Creates a new SQLAlchemy engine for the given instance configuration.
        :param config: InstanceConfig object containing connection details
        :return: None
        """
        engine_id = self._engine_id(config)
        database_url = self._get_database_url(config)
        engine = create_engine(
            database_url,
            echo=False,
            # echo_pool="debug",
            pool_size=64,
            max_overflow=64,
            pool_timeout=1800,
            pool_recycle=3600,
            isolation_level="AUTOCOMMIT"
        )

        self._engines[engine_id] = engine

    def get_connection(self, config: InstanceConfig) -> Connection:
        """
        Retrieves a database session for the given instance configuration.
        If the session does not exist, it creates a new one.
        :param config: InstanceConfig object containing connection details
        :return: SQLAlchemy Session object
        """
        engine_id = self._engine_id(config)

        if engine_id not in self._engines:
            self._create_engine(config)
        engine = self._engines[engine_id]
        
        try:
            conn = engine.connect()
        except:
            raise

        return conn

global_connection_manager = ConnectionManager()
