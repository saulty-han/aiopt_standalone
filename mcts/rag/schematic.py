"""
mcts.rag.schematic - Build schematic texts from a query and its plan.

A *schematic* is a text representation of a query that an embedder turns into a
vector. We build three flavours (Booster Sec. 4.1 builds multiple schematics
per QConfig and embeds each):

  - SQL  : the raw SQL with whitespace collapsed. Captures literal lexical
           structure (table/column names, keywords).
  - ANON : an anonymized SQL "template" — numeric/string literals replaced with
           placeholders. Captures query shape independent of parameters, which
           is what makes drift (same template, different params) retrievable.
  - PLAN : a compact textual digest of the execution plan: referenced tables,
           access types, and operator keywords. Captures *how* the DBMS runs
           the query, independent of SQL phrasing.

All functions are pure and have no external dependencies.
"""
from __future__ import annotations

import re
from typing import Any, Dict, List, Optional

from mcts.rag.types import Schematic, SchematicType


# ---------------------------------------------------------------------------
# SQL normalization / anonymization
# ---------------------------------------------------------------------------

_WS_RE = re.compile(r"\s+")
# String literals: '...'  (handles escaped '' inside)
_STR_LIT_RE = re.compile(r"'(?:[^']|'')*'")
# Numeric literals (integer / decimal / scientific)
_NUM_LIT_RE = re.compile(r"\b\d+(?:\.\d+)?(?:[eE][+-]?\d+)?\b")
# IN (...) lists collapse to IN (?)
_IN_LIST_RE = re.compile(r"\bIN\s*\([^()]*\)", re.IGNORECASE)


def normalize_sql(sql: str) -> str:
    """Collapse whitespace; keep the SQL otherwise intact."""
    if not sql:
        return ""
    return _WS_RE.sub(" ", sql).strip()


def anonymize_sql(sql: str) -> str:
    """Produce a parameter-agnostic SQL template.

    Replaces string and numeric literals with placeholders and collapses
    ``IN (...)`` lists. The result is lower-cased so that templates differing
    only in literals / case map to the same text.
    """
    if not sql:
        return ""
    text = normalize_sql(sql)
    text = _STR_LIT_RE.sub("'?'", text)
    text = _IN_LIST_RE.sub("IN (?)", text)
    text = _NUM_LIT_RE.sub("?", text)
    return text.lower()


# ---------------------------------------------------------------------------
# Plan digest text
# ---------------------------------------------------------------------------

# Operator / access keywords worth surfacing from execution_info nodes.
_OP_KEYS = ("operation", "access_type", "table_name", "table", "index_name", "join_type")


def _walk_plan(node: Any, sink: List[str]) -> None:
    """Depth-first walk over an execution_info plan tree collecting op tokens."""
    if isinstance(node, dict):
        for k in _OP_KEYS:
            v = node.get(k)
            if isinstance(v, str) and v.strip():
                sink.append(f"{k}={v.strip()}")
        # Recurse into known child containers and any nested dict/list values.
        for v in node.values():
            if isinstance(v, (dict, list)):
                _walk_plan(v, sink)
    elif isinstance(node, list):
        for item in node:
            _walk_plan(item, sink)


def build_plan_text(execution_info: Any, tables: Optional[List[str]] = None) -> str:
    """Build a compact textual digest of the plan structure."""
    tokens: List[str] = []
    if tables:
        tokens.append("tables=" + ",".join(sorted({t for t in tables if t})))
    if isinstance(execution_info, dict):
        _walk_plan(execution_info, tokens)
    # Deduplicate while preserving order; cap length to keep embeddings stable.
    seen = set()
    out: List[str] = []
    for tok in tokens:
        if tok not in seen:
            seen.add(tok)
            out.append(tok)
    return " ".join(out[:512])


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def build_schematics(
    query: str,
    execution_info: Any = None,
    tables: Optional[List[str]] = None,
) -> Dict[SchematicType, Schematic]:
    """Build all schematic flavours for a query.

    Args:
        query: raw SQL text.
        execution_info: parsed execution plan dict (may be None / missing).
        tables: optional list of referenced table names (improves PLAN text).

    Returns:
        Mapping of SchematicType -> Schematic. Empty schematics are still
        included so the embedder always receives a consistent set of types.
    """
    sql_text = normalize_sql(query)
    anon_text = anonymize_sql(query)
    plan_text = build_plan_text(execution_info, tables)
    # Plan text alone can be sparse; prepend the template so PLAN retrieval
    # still has lexical signal when the plan tree is shallow.
    if anon_text and plan_text:
        plan_text = f"{plan_text} || {anon_text}"
    elif anon_text:
        plan_text = anon_text

    return {
        SchematicType.SQL: Schematic(schematic_type=SchematicType.SQL, text=sql_text),
        SchematicType.ANON: Schematic(schematic_type=SchematicType.ANON, text=anon_text),
        SchematicType.PLAN: Schematic(schematic_type=SchematicType.PLAN, text=plan_text),
    }
