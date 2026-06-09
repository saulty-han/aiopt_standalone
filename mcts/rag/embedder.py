"""
mcts.rag.embedder - Pluggable text embedders.

Two implementations:
  - LocalFeatureEmbedder: deterministic, dependency-free (numpy only) hashing
    embedder. Token -> md5 bucket -> accumulate -> L2 normalize. Reproducible
    and good enough to find structurally similar queries. This is the default
    fallback when no embedding API is configured.
  - ApiEmbedder: skeleton for an external embedding service (e.g. Voyage,
    OpenAI). Wire in the HTTP/SDK call where marked; until then it raises a
    clear error so misconfiguration fails loudly rather than silently.

``build_embedder(config)`` picks the implementation:
  - rag_embedder == "api" AND an endpoint is configured -> ApiEmbedder
  - otherwise -> LocalFeatureEmbedder (fallback)

All embedders return an (N, dim) float32 numpy array with L2-normalized rows,
so a dot product equals cosine similarity.
"""
from __future__ import annotations

import hashlib
import re
from abc import ABC, abstractmethod
from typing import Any, List, Optional

import numpy as np

from mcts.rag import logger


_TOKEN_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_]*|[<>=!]+|[(),.*]")


def _tokenize(text: str) -> List[str]:
    """Split schematic text into coarse tokens (identifiers, operators)."""
    if not text:
        return []
    return _TOKEN_RE.findall(text.lower())


# ---------------------------------------------------------------------------
# Abstract base
# ---------------------------------------------------------------------------

class Embedder(ABC):
    """Abstract text embedder. Rows of the returned matrix are L2-normalized."""

    name: str = "abstract"
    dim: int = 0

    @abstractmethod
    def encode(self, texts: List[str]) -> np.ndarray:
        """Encode a list of texts into an (N, dim) float32 normalized matrix."""
        raise NotImplementedError

    def encode_one(self, text: str) -> np.ndarray:
        """Encode a single text into a (dim,) float32 normalized vector."""
        return self.encode([text])[0]

    @staticmethod
    def _l2_normalize(mat: np.ndarray) -> np.ndarray:
        norms = np.linalg.norm(mat, axis=1, keepdims=True)
        norms[norms == 0.0] = 1.0
        return (mat / norms).astype(np.float32)


# ---------------------------------------------------------------------------
# Local deterministic fallback
# ---------------------------------------------------------------------------

class LocalFeatureEmbedder(Embedder):
    """Dependency-free hashing embedder (hashing trick).

    Each token is hashed to a bucket in [0, dim) with a signed contribution.
    Bigrams of adjacent tokens are added too, giving a little word-order signal.
    Deterministic across runs and machines (md5-based), so a store built once
    stays consistent with online query encodings.
    """

    def __init__(self, dim: int = 256) -> None:
        if dim <= 0:
            raise ValueError("LocalFeatureEmbedder dim must be positive")
        self.dim = int(dim)
        self.name = f"local_hash_{self.dim}"

    def _hash(self, token: str) -> tuple[int, float]:
        h = hashlib.md5(token.encode("utf-8")).digest()
        bucket = int.from_bytes(h[:4], "little") % self.dim
        sign = 1.0 if (h[4] & 1) else -1.0
        return bucket, sign

    def encode(self, texts: List[str]) -> np.ndarray:
        mat = np.zeros((len(texts), self.dim), dtype=np.float32)
        for i, text in enumerate(texts):
            tokens = _tokenize(text)
            for tok in tokens:
                b, s = self._hash(tok)
                mat[i, b] += s
            # adjacent bigrams for coarse ordering signal
            for a, b_ in zip(tokens, tokens[1:]):
                bucket, sign = self._hash(a + "\x1f" + b_)
                mat[i, bucket] += sign * 0.5
        return self._l2_normalize(mat)


# ---------------------------------------------------------------------------
# API-backed embedder (skeleton — wire in your provider here)
# ---------------------------------------------------------------------------

class ApiEmbedder(Embedder):
    """External embedding-service client.

    This is intentionally a thin skeleton. To enable it, implement ``_call_api``
    with your provider's request (the ``openai`` package is already a project
    dependency and exposes an embeddings endpoint; Voyage/others are similar).

    Construction does NOT make any network call, so ``build_embedder`` can
    instantiate it cheaply; the first ``encode`` is where the call happens.
    """

    def __init__(
        self,
        *,
        api_url: Optional[str] = None,
        api_key: Optional[str] = None,
        model: Optional[str] = None,
        dim: int = 0,
    ) -> None:
        self.api_url = api_url
        self.api_key = api_key
        self.model = model or "unknown"
        # dim may be known a priori (provider-documented) or discovered on the
        # first call; store builds record the actual dim from the matrix.
        self.dim = int(dim)
        self.name = f"api:{self.model}"

    def _call_api(self, texts: List[str]) -> np.ndarray:
        """Return a raw (N, D) float matrix from the provider.

        TODO(api): implement the actual request here. Example shape with the
        openai SDK:

            from openai import OpenAI
            client = OpenAI(base_url=self.api_url, api_key=self.api_key)
            resp = client.embeddings.create(model=self.model, input=texts)
            return np.array([d.embedding for d in resp.data], dtype=np.float32)
        """
        raise NotImplementedError(
            "ApiEmbedder._call_api is not implemented. Configure an embedding "
            "provider here, or set rag_embedder='local' to use the fallback."
        )

    def encode(self, texts: List[str]) -> np.ndarray:
        if not texts:
            return np.zeros((0, max(1, self.dim)), dtype=np.float32)
        raw = np.asarray(self._call_api(texts), dtype=np.float32)
        if raw.ndim != 2:
            raise ValueError(f"ApiEmbedder expected 2D output, got shape {raw.shape}")
        if self.dim and raw.shape[1] != self.dim:
            raise ValueError(
                f"ApiEmbedder dim mismatch: configured {self.dim}, got {raw.shape[1]}"
            )
        self.dim = raw.shape[1]
        return self._l2_normalize(raw)


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------

def build_embedder(config: Any) -> Embedder:
    """Pick an embedder from an MCTSConfig-like object.

    Selection:
      - rag_embedder == "api" and an api endpoint is configured -> ApiEmbedder
      - otherwise -> LocalFeatureEmbedder (deterministic fallback)

    Never raises on misconfig: if "api" is requested but unconfigured, falls
    back to local with a warning, so searches keep working.
    """
    kind = str(getattr(config, "rag_embedder", "local") or "local").lower()

    if kind == "api":
        api_url = getattr(config, "rag_embedder_api_url", None)
        api_key = getattr(config, "rag_embedder_api_key", None)
        model = getattr(config, "rag_embedder_model", None)
        dim = int(getattr(config, "rag_embedder_dim", 0) or 0)
        if api_url:
            logger.info(f"[RAG] using ApiEmbedder model={model}")
            return ApiEmbedder(api_url=api_url, api_key=api_key, model=model, dim=dim)
        logger.warning(
            "[RAG] rag_embedder='api' but no rag_embedder_api_url configured; "
            "falling back to LocalFeatureEmbedder"
        )

    dim = int(getattr(config, "rag_embedder_dim", 0) or 256)
    return LocalFeatureEmbedder(dim=dim)
