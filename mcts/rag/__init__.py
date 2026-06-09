"""
mcts.rag - Booster-style retrieval-augmented generation for MCTS hint search.

Phase I  (offline): mine MCTS output JSON -> QConfig -> embeddings -> RAGStore.
Phase II (online):  retrieve similar historical QConfigs and enrich the
                    per-action prompt; optionally warm-start the search tree.

The whole subsystem is inert unless ``MCTSConfig.rag_enabled`` is True, so the
baseline MCTS behaviour is unchanged when RAG is off.

Design constraints:
  - No new third-party dependencies beyond numpy (already required).
  - No external vector database; an in-memory numpy flat index is used.
  - The embedder is pluggable: a deterministic local embedder is the default
    fallback, and an API-backed embedder can be dropped in via config.
"""
from mcts import logger  # reuse the shared "mcts" logger hierarchy

__all__ = ["logger"]
