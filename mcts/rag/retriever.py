"""
mcts.rag.retriever - Phase II retrieval over the RAG store.

Given a target query (SQL + plan), retrieve the most relevant historical
QConfigs and package them into a RagContext for prompt enrichment.

Pipeline (Booster Sec. 4.2):
  1. Build the target query's schematics (same builder/embedder as Phase I).
  2. For each schematic type, search the store within that type only.
  3. Aggregate hits by query_digest and keep, per query, the single most
     performant QConfig (link-following: the most similar historical record is
     often a stock/slow config, so we follow to the best record of the same
     query). For us, "follow links" == pick the QConfig with the best
     improvement among the same query_digest.
  4. Drop hits below ``min_similarity`` and any non-improving record (defensive;
     the store should already be improvement-only).
  5. Rank the surviving references and truncate to ``top_k``.

Retrieval is best-effort: any failure logs a warning and yields an empty
RagContext so a search never crashes because of RAG.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

from mcts.rag import logger
from mcts.rag.embedder import Embedder
from mcts.rag.schematic import build_schematics
from mcts.rag.store import RAGStore
from mcts.rag.types import QConfig, RagContext, RetrievedRef, SchematicType


# Schematic types to search, in priority order. We weight plan/anon higher than
# raw SQL because they capture structure/template (robust to literal drift).
_SCHEMATIC_WEIGHTS: Dict[SchematicType, float] = {
    SchematicType.ANON: 1.0,
    SchematicType.PLAN: 1.0,
    SchematicType.SQL: 0.9,
}


class Retriever:
    """Retrieve relevant historical QConfigs for a target query."""

    def __init__(self, store: RAGStore, embedder: Embedder) -> None:
        self._store = store
        self._embedder = embedder

    def retrieve(
        self,
        query: str,
        execution_info: Any = None,
        tables: Optional[List[str]] = None,
        *,
        top_k: int = 2,
        min_similarity: float = 0.5,
        exclude_query_digest: Optional[str] = None,
        per_type_k: int = 8,
    ) -> RagContext:
        """Return a RagContext of up to ``top_k`` references.

        Args:
            query: target SQL text.
            execution_info: target query's parsed plan dict (optional).
            tables: referenced tables (improves PLAN schematic; optional).
            top_k: max references to return (Booster default 2).
            min_similarity: cosine threshold; hits below are dropped.
            exclude_query_digest: if set, drop refs from this query_digest
                (used online to avoid retrieving the query's own past records;
                set to None to allow self-retrieval, e.g. for warm-start).
            per_type_k: candidates pulled per schematic type before merge.
        """
        if self._store is None or self._store.num_rows == 0:
            return RagContext()

        try:
            schematics = build_schematics(query, execution_info, tables)
        except Exception as e:  # noqa: BLE001
            logger.warning(f"[RAG] schematic build failed: {e}")
            return RagContext()

        # query_digest -> best (RetrievedRef) seen across all schematic types
        best_by_query: Dict[str, RetrievedRef] = {}

        for stype, schematic in schematics.items():
            if not schematic.text:
                continue
            weight = _SCHEMATIC_WEIGHTS.get(stype, 1.0)
            try:
                q_vec = self._embedder.encode_one(schematic.text)
                hits = self._store.search(q_vec, stype, per_type_k)
            except Exception as e:  # noqa: BLE001
                logger.warning(f"[RAG] search failed for type={stype.value}: {e}")
                continue

            for qconfig, sim in hits:
                weighted_sim = sim * weight
                if weighted_sim < min_similarity:
                    continue
                if exclude_query_digest and qconfig.query_digest == exclude_query_digest:
                    continue
                # defensive: only improving records (store should guarantee this)
                if qconfig.improvement_ratio is not None and qconfig.improvement_ratio <= 0:
                    continue

                key = qconfig.query_digest or qconfig.qconfig_id
                existing = best_by_query.get(key)
                # link-following: among the same query, prefer the most
                # performant record; break ties by similarity.
                if existing is None or _is_better_ref(qconfig, weighted_sim, existing):
                    best_by_query[key] = RetrievedRef(
                        qconfig=qconfig,
                        similarity=weighted_sim,
                        schematic_type=stype,
                    )

        refs = list(best_by_query.values())
        # Rank by similarity first, then improvement (most useful refs on top).
        refs.sort(
            key=lambda r: (
                r.similarity,
                r.qconfig.improvement_ratio or 0.0,
            ),
            reverse=True,
        )
        refs = refs[: max(0, top_k)]

        return RagContext(
            query_digest=str(exclude_query_digest or ""),
            refs=refs,
        )


def _is_better_ref(
    candidate_qc: QConfig,
    candidate_sim: float,
    existing: RetrievedRef,
) -> bool:
    """Whether candidate should replace the existing best ref for a query.

    Link-following preference: better improvement wins; if improvement is
    comparable, higher similarity wins.
    """
    cand_impr = candidate_qc.improvement_ratio or 0.0
    exist_impr = existing.qconfig.improvement_ratio or 0.0
    if abs(cand_impr - exist_impr) > 1e-6:
        return cand_impr > exist_impr
    return candidate_sim > existing.similarity
