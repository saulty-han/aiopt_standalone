"""
mcts.rag.types - Data models for the RAG subsystem.

Mirrors the style of ``mcts.types``: every structured payload is an explicit
pydantic model, no raw dicts cross function boundaries.

Key concepts (Booster terminology):
  - QConfig:      one historical "query under a configuration" record. For our
                  single-query hint optimizer this is a (query, hint-set, plan,
                  measured runtime) tuple distilled from an MCTS solution.
  - Schematic:    a textual representation of a query used to derive an
                  embedding. Multiple schematic *types* are produced per query
                  (raw SQL, plan structure, anonymized) and retrieval is done
                  per-type so distance is only compared within the same type.
  - RetrievedRef: a QConfig returned by retrieval, with its similarity score.
  - RagContext:   the full set of references for one target query, computed
                  once per search and reused across the whole tree.
"""
from __future__ import annotations

from enum import Enum
from typing import List, Optional

from pydantic import BaseModel, Field


# ---------------------------------------------------------------------------
# Schematic types
# ---------------------------------------------------------------------------

class SchematicType(str, Enum):
    """The schematic flavours we embed and retrieve against.

    Retrieval always compares a query vector against stored vectors of the
    *same* schematic type (Booster Sec. 4.2), so a SQL-text query vector is
    never ranked against a plan-structure stored vector.
    """
    SQL = "sql"        # raw (lightly normalized) SQL text
    PLAN = "plan"      # plan structure: tables, access types, operators
    ANON = "anon"      # anonymized SQL template (literals/params stripped)


class Schematic(BaseModel):
    """One schematic text plus the type it represents."""
    schematic_type: SchematicType
    text: str = ""


# ---------------------------------------------------------------------------
# QConfig — one historical experience record
# ---------------------------------------------------------------------------

class QConfig(BaseModel):
    """A single historical query-configuration experience.

    Only records that genuinely *improved* over baseline are stored (see
    ``mcts.rag.qconfig``): in the source data a plan slower than baseline is
    interrupted and assigned the baseline time, so any record whose runtime is
    not strictly below baseline is untrustworthy and excluded at build time.
    """
    qconfig_id: str
    query_digest: str = ""           # execution_info.query_digest (statement id)
    query_text: str = ""
    query_template: str = ""         # anonymized SQL (literals stripped)
    tables: List[str] = Field(default_factory=list)

    executed_hints: List[str] = Field(default_factory=list)
    plan_digest: Optional[str] = None

    baseline_time: Optional[float] = None
    runtime_seconds: Optional[float] = None
    improvement_ratio: Optional[float] = None  # baseline/runtime - 1 (>0 only)
    reward: Optional[float] = None
    plan_cost: Optional[float] = None          # execution_info.accumulative_cost

    source: str = "offline_json"               # offline_json | online
    source_file: Optional[str] = None


# ---------------------------------------------------------------------------
# Retrieval results
# ---------------------------------------------------------------------------

class RetrievedRef(BaseModel):
    """A QConfig returned by retrieval with its similarity score."""
    qconfig: QConfig
    similarity: float = 0.0
    schematic_type: SchematicType = SchematicType.SQL


class RagContext(BaseModel):
    """All references retrieved for one target query.

    Computed once at the start of a search and reused across every node, so the
    embedder is invoked at most once per query (Booster Phase II is per-query,
    not per-step).
    """
    query_digest: str = ""
    refs: List[RetrievedRef] = Field(default_factory=list)

    @property
    def is_empty(self) -> bool:
        return len(self.refs) == 0
