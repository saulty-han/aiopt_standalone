"""Per-query preparation helpers shared by LLM / SLM optimizers.

Scope (intentionally narrow):
  * ``get_candidate_hints`` — build the index/join_order/config hint pool
    plus referenced table names.
  * ``get_index_info``      — fetch index metadata for those tables.
  * ``execution_info_char_len`` — utility for budget checks.

Baseline / execution_info / plan_digest are **no longer** prepared here —
they are obtained inside the optimizer loop via ``DBExecutor.execute_and_measure``
so the same cache-aware code path serves both the optimizer's baseline probe
and MCTS rollouts.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from ai_logger import aiopt_logger
import db_utils


# Skip rows whose serialized execution_info exceeds this many chars (mirrors
# agent ``json.dumps(execution_info)`` size budget).
EXECUTION_INFO_MAX_CHARS = 128 * 1024


def execution_info_char_len(execution_info) -> int:
    """Return the serialized length of an execution_info payload."""
    if execution_info is None:
        return 0
    if isinstance(execution_info, str):
        return len(execution_info)
    return len(json.dumps(execution_info))


def get_candidate_hints(controller, db: str, sql: str) -> Tuple[Dict, List[str]]:
    """Collect candidate hints (index/join_order/config) and referenced tables."""
    # Ensure project root is in sys.path (sqlglot / hints_generator live there).
    project_root = Path(__file__).parent.parent
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))

    import sqlglot
    from sqlglot import exp
    from hints_generator import is_temporary_table

    alias_to_table: Dict[str, str] = {}
    try:
        parsed = sqlglot.parse_one(sql, read="mysql")
        for table in parsed.find_all(exp.Table):
            if table.alias_or_name not in alias_to_table:
                alias_to_table[table.alias_or_name] = table.name
    except Exception as e:
        aiopt_logger.debug(f"Failed to parse table aliases: {e}")

    possible_keys_with_queryblock = db_utils.get_possible_keys(
        controller, db, sql, with_empty_key=True, explain_timeout_seconds=10,
    )

    possible_keys: Dict[str, List[str]] = {}
    for (_queryblock_id, table_name), keys in possible_keys_with_queryblock.items():
        if table_name is None:
            continue
        if table_name in possible_keys:
            possible_keys[table_name] = list(set(possible_keys[table_name]) | set(keys))
        else:
            possible_keys[table_name] = keys

    index_hints: List[str] = []
    prefix_hints: List[str] = []
    suffix_hints: List[str] = []
    real_table_names: set[str] = set()

    for alias_or_table, indexes in possible_keys.items():
        if alias_or_table is None or is_temporary_table(alias_or_table):
            continue

        real_table_name = alias_to_table.get(alias_or_table, alias_or_table)
        real_table_names.add(real_table_name)

        index_hints.append(f"NO_INDEX({alias_or_table})")
        prefix_hints.append(f"JOIN_PREFIX({alias_or_table})")
        suffix_hints.append(f"JOIN_SUFFIX({alias_or_table})")

        if indexes is not None:
            for index in indexes:
                index_hints.append(f"INDEX({alias_or_table} {index})")

    candidate_hints = {
        "index": index_hints,
        "join_order": prefix_hints + suffix_hints,
        "config": [
            "SET_VAR(optimizer_switch='materialization=off')",
            "SET_VAR(optimizer_switch='derived_merge=off')",
            "SET_VAR(optimizer_switch='semijoin=off')",
            "SET_VAR(optimizer_switch='loosescan=off')",
            "SET_VAR(optimizer_switch='firstmatch=off')",
            "SET_VAR(optimizer_switch='duplicateweedout=off')",
            "SET_VAR(optimizer_switch='subquery_materialization_cost_based=off')",
            "SET_VAR(optimizer_switch='subquery_to_derived=on')",
        ],
    }
    return candidate_hints, list(real_table_names)


def get_index_info(controller, table_names: List[str]) -> Optional[Dict]:
    if not table_names:
        return None
    try:
        return db_utils.get_all_tables_indexes_info(controller, table_names=table_names)
    except Exception as e:
        aiopt_logger.debug(f"Failed to get index info: {e}")
        return None


def parse_execution_info(explain_analyze_json: Optional[str]) -> Dict:
    """Parse the JSON string returned by ``EXPLAIN ANALYZE FORMAT=JSON``.

    Falls back to ``{"raw": ...}`` when the payload is not valid JSON, mirroring
    the prior behaviour of ``qdf_builder.get_execution_info``.
    """
    if not explain_analyze_json:
        return {}
    if isinstance(explain_analyze_json, dict):
        return explain_analyze_json
    try:
        return json.loads(explain_analyze_json)
    except (json.JSONDecodeError, TypeError):
        return {"raw": explain_analyze_json}
