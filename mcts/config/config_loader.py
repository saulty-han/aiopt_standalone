"""
mcts.config.config_loader - Configuration loading for mcts.

Loading priority (highest wins):
  1. Explicit overrides from caller (e.g. LLMOptimizer)
  2. TOML ``[mcts]`` section (from etc/aiopt_conf.toml)
  3. YAML config file (mcts/config/mcts_defaults.yaml or custom path)
  4. MCTSConfig built-in defaults
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Dict, Optional

import yaml

from mcts.types import MCTSConfig

from mcts import logger

# Default YAML path relative to this file
_DEFAULT_YAML = Path(__file__).parent / "mcts_defaults.yaml"


def _load_yaml(path: str) -> Dict[str, Any]:
    """Load a YAML file, return empty dict on failure."""
    try:
        with open(path, "r") as f:
            data = yaml.safe_load(f)
        return data if isinstance(data, dict) else {}
    except Exception as e:
        logger.warning(f"Failed to load YAML config from {path}: {e}")
        return {}


def _load_toml_section(section: str) -> Dict[str, Any]:
    """Load a section from the global TOML config (etc/aiopt_conf.toml).

    Returns empty dict if TOML config is unavailable.
    """
    try:
        from config.toml_config import TomlConfig
        toml_cfg = TomlConfig.get_instance()
        return dict(toml_cfg.get_section(section))
    except Exception:
        return {}


# Field name mapping: TOML/YAML key → MCTSConfig field
# Handles cases where TOML uses different names than MCTSConfig
_FIELD_ALIASES: Dict[str, str] = {
    "stop": "stop_tokens",
    "stop_mcts_search_plan_time_threshold_seconds": "plan_time_threshold_seconds",
    "stop_mcts_search_estimated_tokens_budget": "estimated_tokens_budget",
}


def _apply_overrides(config: MCTSConfig, overrides: Dict[str, Any]) -> None:
    """Apply a dict of overrides to the config, respecting field aliases."""
    for key, value in overrides.items():
        field_name = _FIELD_ALIASES.get(key, key)
        if hasattr(config, field_name):
            try:
                setattr(config, field_name, value)
            except Exception as e:
                logger.debug(f"Cannot set config.{field_name}={value!r}: {e}")


def load_mcts_config(
    custom_yaml_path: Optional[str] = None,
    toml_overrides: Optional[Dict[str, Any]] = None,
) -> MCTSConfig:
    """Build an MCTSConfig by layering defaults, YAML, and TOML.

    Args:
        custom_yaml_path: Optional path to a custom YAML config file.
            If None, uses the built-in mcts_defaults.yaml.
        toml_overrides: Optional dict of overrides (e.g. from LLMOptimizer).
            Applied last (highest priority).

    Returns:
        A fully populated MCTSConfig.
    """
    config = MCTSConfig()

    # Layer 1: Built-in YAML defaults
    if _DEFAULT_YAML.exists():
        defaults = _load_yaml(str(_DEFAULT_YAML))
        _apply_overrides(config, defaults)

    # Layer 2: Custom YAML (if provided and different from defaults)
    if custom_yaml_path and os.path.exists(custom_yaml_path):
        custom = _load_yaml(custom_yaml_path)
        _apply_overrides(config, custom)

    # Layer 3: TOML [mcts] section
    toml_mcts = _load_toml_section("mcts")
    _apply_overrides(config, toml_mcts)

    # Layer 4: Explicit overrides from caller
    if toml_overrides:
        _apply_overrides(config, toml_overrides)

    return config
