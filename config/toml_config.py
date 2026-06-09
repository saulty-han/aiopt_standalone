# coding:utf-8
"""
TOML Configuration Loader

Provides a singleton TomlConfig class for loading and accessing TOML configuration.
"""
import os
import tomli  # For Python < 3.11; use tomllib for 3.11+
from typing import Any, Optional


class TomlConfig:
    """
    TOML 配置加载器（单例模式）
    
    Usage:
        config = TomlConfig.get_instance()
        value = config.get("section", "key")
    """
    _instance: Optional["TomlConfig"] = None
    _config: dict
    _config_path: Optional[str] = None

    def __init__(self, config_path: str):
        """
        Initialize TomlConfig with the given config file path.
        
        Args:
            config_path: Path to the TOML configuration file
        """
        self._config_path = config_path
        self._config = self._load_config(config_path)

    @staticmethod
    def _load_config(config_path: str) -> dict:
        """Load TOML configuration from file."""
        with open(config_path, "rb") as f:
            return tomli.load(f)

    @classmethod
    def get_instance(cls) -> "TomlConfig":
        """
        Get the singleton instance of TomlConfig.
        
        If not initialized, will load from default path: etc/aiopt_conf.toml
        """
        if cls._instance is None:
            # Default config path relative to project root
            current_dir = os.path.dirname(os.path.abspath(__file__))
            project_root = os.path.dirname(current_dir)
            default_path = os.path.join(project_root, "etc", "aiopt_conf.toml")
            cls._instance = cls(default_path)
        return cls._instance

    @classmethod
    def reload(cls, config_path: Optional[str] = None) -> "TomlConfig":
        """
        Reload configuration from file.
        
        Args:
            config_path: Optional new config path. If None, reloads from current path.
            
        Returns:
            The reloaded TomlConfig instance
        """
        if config_path is not None:
            cls._instance = cls(config_path)
        elif cls._instance is not None:
            cls._instance = cls(cls._instance._config_path)
        else:
            cls._instance = cls.get_instance()
        return cls._instance

    def get(self, section: str, key: str) -> Any:
        """
        Get a configuration value (with native TOML type).
        
        Args:
            section: The TOML section (table) name
            key: The key within the section
            
        Returns:
            The configuration value with its native TOML type
            
        Raises:
            KeyError: If section or key not found (no default to avoid masking errors)
        """
        try:
            return self._config[section][key]
        except KeyError:
            raise KeyError(f"Configuration key '{section}.{key}' not found")

    def get_section(self, section: str) -> dict:
        """
        Get an entire section as a dictionary.
        
        Raises:
            KeyError: If section not found
        """
        if section not in self._config:
            raise KeyError(f"Configuration section '{section}' not found")
        return self._config[section]

    @property
    def config(self) -> dict:
        """Get the entire configuration dictionary."""
        return self._config
