"""
mcts.rag.store - In-memory numpy vector store with disk persistence.

No external vector database: for our scale (a few thousand QConfigs at most)
an exact flat index is faster and more accurate than ANN. A query vector is
compared against stored vectors of the SAME schematic type via a single
matrix-vector product (rows are L2-normalized, so dot == cosine).

On-disk layout (a directory):
  <path>/manifest.json   - dim, embedder name, version, counts
  <path>/qconfigs.jsonl  - one QConfig JSON per row (row r in embeddings.npz)
  <path>/embeddings.npz  - 'embeddings' (N,dim) float32, 'types' (N,) int8

Each stored row is (QConfig, schematic_type, vector): a single QConfig
contributes multiple rows (one per schematic type). Retrieval dedups back to
QConfigs.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

import numpy as np

from mcts.rag import logger
from mcts.rag.types import QConfig, SchematicType

_STORE_VERSION = 1

# Stable int code per schematic type for the npz 'types' array.
_TYPE_TO_CODE: Dict[SchematicType, int] = {
    SchematicType.SQL: 0,
    SchematicType.ANON: 1,
    SchematicType.PLAN: 2,
}
_CODE_TO_TYPE: Dict[int, SchematicType] = {v: k for k, v in _TYPE_TO_CODE.items()}


def _model_to_dict(model) -> dict:
    if hasattr(model, "model_dump"):
        return model.model_dump()
    return model.dict()


class RAGStore:
    """Flat numpy vector store keyed by schematic type."""

    def __init__(self, dim: int, embedder_name: str = "") -> None:
        self.dim = int(dim)
        self.embedder_name = embedder_name
        self._embeddings = np.zeros((0, self.dim), dtype=np.float32)
        self._types = np.zeros((0,), dtype=np.int8)
        self._qconfigs: List[QConfig] = []  # parallel to rows

    # ------------------------------------------------------------------
    # Stats
    # ------------------------------------------------------------------
    @property
    def num_rows(self) -> int:
        return self._embeddings.shape[0]

    @property
    def num_qconfigs(self) -> int:
        return len({q.qconfig_id for q in self._qconfigs})

    # ------------------------------------------------------------------
    # Mutation
    # ------------------------------------------------------------------
    def add(
        self,
        rows: Sequence[Tuple[QConfig, SchematicType, np.ndarray]],
    ) -> int:
        """Append (qconfig, schematic_type, vector) rows. Returns rows added."""
        if not rows:
            return 0
        vecs = []
        types = []
        qcs: List[QConfig] = []
        for qc, stype, vec in rows:
            v = np.asarray(vec, dtype=np.float32).reshape(-1)
            if v.shape[0] != self.dim:
                raise ValueError(f"vector dim {v.shape[0]} != store dim {self.dim}")
            vecs.append(v)
            types.append(_TYPE_TO_CODE[stype])
            qcs.append(qc)
        self._embeddings = np.vstack([self._embeddings, np.array(vecs, dtype=np.float32)])
        self._types = np.concatenate([self._types, np.array(types, dtype=np.int8)])
        self._qconfigs.extend(qcs)
        return len(qcs)

    def upsert(
        self,
        rows: Sequence[Tuple[QConfig, SchematicType, np.ndarray]],
    ) -> int:
        """Add rows whose (qconfig_id, schematic_type) is not already present.

        Used for online write-back so re-running the same query/plan does not
        bloat the store. Returns the number of rows actually added.
        """
        if not rows:
            return 0
        existing = {
            (q.qconfig_id, int(self._types[i]))
            for i, q in enumerate(self._qconfigs)
        }
        fresh = [
            (qc, stype, vec)
            for (qc, stype, vec) in rows
            if (qc.qconfig_id, _TYPE_TO_CODE[stype]) not in existing
        ]
        return self.add(fresh)

    # ------------------------------------------------------------------
    # Search
    # ------------------------------------------------------------------
    def search(
        self,
        query_vec: np.ndarray,
        schematic_type: SchematicType,
        top_k: int,
    ) -> List[Tuple[QConfig, float]]:
        """Return up to top_k (QConfig, cosine_similarity) within one type."""
        if self.num_rows == 0 or top_k <= 0:
            return []
        q = np.asarray(query_vec, dtype=np.float32).reshape(-1)
        if q.shape[0] != self.dim:
            raise ValueError(f"query dim {q.shape[0]} != store dim {self.dim}")

        code = _TYPE_TO_CODE[schematic_type]
        mask = self._types == code
        idxs = np.nonzero(mask)[0]
        if idxs.size == 0:
            return []

        # NumPy 2.x can emit spurious divide/overflow/invalid warnings from the
        # BLAS matmul path on some platforms even for finite, normalized inputs.
        # Inputs are validated finite at insert/encode time, so suppress them.
        sub = np.ascontiguousarray(self._embeddings[idxs])
        with np.errstate(divide="ignore", over="ignore", invalid="ignore"):
            sims = sub @ q  # cosine (rows normalized)
        k = min(top_k, idxs.size)
        # argpartition for top-k then sort that slice descending
        part = np.argpartition(-sims, k - 1)[:k]
        order = part[np.argsort(-sims[part])]
        return [(self._qconfigs[int(idxs[j])], float(sims[j])) for j in order]

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------
    def save(self, path: str) -> None:
        """Persist atomically: write to temp dir then rename into place."""
        target = Path(path)
        target.mkdir(parents=True, exist_ok=True)

        manifest = {
            "version": _STORE_VERSION,
            "dim": self.dim,
            "embedder_name": self.embedder_name,
            "num_rows": int(self.num_rows),
            "num_qconfigs": int(self.num_qconfigs),
        }

        # embeddings.npz
        emb_tmp = target / "embeddings.npz.tmp"
        with emb_tmp.open("wb") as fh:
            np.savez_compressed(fh, embeddings=self._embeddings, types=self._types)
        emb_tmp.replace(target / "embeddings.npz")

        # qconfigs.jsonl
        qc_tmp = target / "qconfigs.jsonl.tmp"
        with qc_tmp.open("w", encoding="utf-8") as fh:
            for qc in self._qconfigs:
                fh.write(json.dumps(_model_to_dict(qc), ensure_ascii=False))
                fh.write("\n")
        qc_tmp.replace(target / "qconfigs.jsonl")

        # manifest.json
        mf_tmp = target / "manifest.json.tmp"
        with mf_tmp.open("w", encoding="utf-8") as fh:
            json.dump(manifest, fh, ensure_ascii=False, indent=2, sort_keys=True)
            fh.write("\n")
        mf_tmp.replace(target / "manifest.json")

        logger.info(
            f"[RAG] store saved path={path} rows={self.num_rows} "
            f"qconfigs={self.num_qconfigs} dim={self.dim}"
        )

    @classmethod
    def load(cls, path: str) -> Optional["RAGStore"]:
        """Load a store directory. Returns None if absent/corrupt."""
        target = Path(path)
        manifest_path = target / "manifest.json"
        emb_path = target / "embeddings.npz"
        qc_path = target / "qconfigs.jsonl"
        if not (manifest_path.exists() and emb_path.exists() and qc_path.exists()):
            logger.warning(f"[RAG] store not found or incomplete at {path}")
            return None
        try:
            with manifest_path.open("r", encoding="utf-8") as fh:
                manifest = json.load(fh)
            store = cls(dim=int(manifest["dim"]), embedder_name=manifest.get("embedder_name", ""))
            with np.load(emb_path) as data:
                store._embeddings = data["embeddings"].astype(np.float32)
                store._types = data["types"].astype(np.int8)
            qcs: List[QConfig] = []
            with qc_path.open("r", encoding="utf-8") as fh:
                for line in fh:
                    line = line.strip()
                    if not line:
                        continue
                    qcs.append(QConfig(**json.loads(line)))
            store._qconfigs = qcs
            if store._embeddings.shape[0] != len(qcs):
                logger.warning(
                    f"[RAG] store row/qconfig mismatch: "
                    f"{store._embeddings.shape[0]} vs {len(qcs)}"
                )
            logger.info(
                f"[RAG] store loaded path={path} rows={store.num_rows} "
                f"qconfigs={store.num_qconfigs} dim={store.dim} "
                f"embedder={store.embedder_name}"
            )
            return store
        except Exception as e:  # noqa: BLE001 - load must never crash a search
            logger.warning(f"[RAG] failed to load store at {path}: {e}")
            return None
