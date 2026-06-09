#!/usr/bin/env python3
"""
build_store.py — Phase I offline builder for the MCTS RAG store.

Scans a directory of MCTS output JSON files (e.g. mcts_scripts/eval_data/),
extracts trustworthy QConfigs (only plans strictly faster than baseline —
see mcts.rag.qconfig), embeds their schematics, and writes a RAGStore.

Usage
  # build from eval_data into the default store path
  python mcts_scripts/rag_build/build_store.py \
      --input-dir mcts_scripts/eval_data \
      --out       mcts_scripts/rag_data/store

  # tune filters / embedder
  python mcts_scripts/rag_build/build_store.py \
      --input-dir mcts_scripts/eval_data \
      --out mcts_scripts/rag_data/store \
      --embedder local --embedder-dim 256 \
      --min-reward 0.0 --max-per-query 8

The script depends only on the (numpy-only) mcts.rag subsystem and standard
library; it does NOT require a DB connection or the optimizer.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from typing import Any, Dict, List

# Make the aiopt_standalone root importable (this file lives at
# mcts_scripts/rag_build/build_store.py, so the root is two levels up).
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

import numpy as np  # noqa: E402

from mcts.rag.embedder import build_embedder, LocalFeatureEmbedder  # noqa: E402
from mcts.rag.qconfig import qconfigs_from_record  # noqa: E402
from mcts.rag.schematic import build_schematics, anonymize_sql  # noqa: E402
from mcts.rag.store import RAGStore  # noqa: E402


def _iter_records(input_dir: str):
    """Yield (filename, record_dict) for each query JSON in input_dir.

    Each file is a list with (typically) one element — the result record dict.
    """
    for name in sorted(os.listdir(input_dir)):
        if not name.endswith(".json"):
            continue
        path = os.path.join(input_dir, name)
        try:
            with open(path, "r", encoding="utf-8") as fh:
                data = json.load(fh)
        except Exception as e:  # noqa: BLE001
            print(f"  [skip] {name}: failed to parse ({e})")
            continue
        records: List[Dict[str, Any]] = data if isinstance(data, list) else [data]
        for rec in records:
            if isinstance(rec, dict):
                yield name, rec


class _EmbedderCfg:
    """Tiny config shim so build_embedder() can read attributes."""
    def __init__(self, args: argparse.Namespace) -> None:
        self.rag_embedder = args.embedder
        self.rag_embedder_dim = args.embedder_dim
        self.rag_embedder_api_url = args.embedder_api_url
        self.rag_embedder_api_key = args.embedder_api_key
        self.rag_embedder_model = args.embedder_model


def main() -> int:
    ap = argparse.ArgumentParser(description="Build the MCTS RAG store from MCTS output JSON.")
    ap.add_argument("--input-dir", required=True, help="Directory of MCTS output JSON files")
    ap.add_argument("--out", default="mcts_scripts/rag_data/store", help="Output store directory")
    ap.add_argument("--embedder", default="local", choices=["local", "api"], help="Embedder kind")
    ap.add_argument("--embedder-dim", type=int, default=256, help="Local embedder dimension")
    ap.add_argument("--embedder-api-url", default=None, help="API embedder endpoint (api mode)")
    ap.add_argument("--embedder-api-key", default=None, help="API embedder key (api mode)")
    ap.add_argument("--embedder-model", default=None, help="API embedder model name (api mode)")
    ap.add_argument("--min-reward", type=float, default=0.0, help="Drop solutions below this reward")
    ap.add_argument("--max-per-query", type=int, default=8, help="Keep at most N fastest solutions per query")
    args = ap.parse_args()

    if not os.path.isdir(args.input_dir):
        print(f"ERROR: input dir not found: {args.input_dir}")
        return 2

    embedder = build_embedder(_EmbedderCfg(args))
    # The local embedder reports its dim immediately; the API embedder may only
    # know its dim after the first encode, so probe with a tiny call.
    if embedder.dim == 0:
        embedder.dim = embedder.encode_one("probe").shape[0]
    store = RAGStore(dim=embedder.dim, embedder_name=embedder.name)
    print(f"Embedder: {embedder.name} (dim={embedder.dim})")

    n_files = 0
    n_records = 0
    n_qconfigs = 0
    # Batch all schematic texts per record, encode once, then add rows.
    for fname, rec in _iter_records(args.input_dir):
        n_records += 1
        qcs = qconfigs_from_record(
            rec,
            source_file=fname,
            min_reward=args.min_reward,
            max_per_query=args.max_per_query,
        )
        if not qcs:
            continue
        execution_info = rec.get("execution_info") if isinstance(rec.get("execution_info"), dict) else {}
        texts: List[str] = []
        meta: List[tuple] = []  # (qconfig, schematic_type)
        for q in qcs:
            # fill the template once for storage/inspection
            if not q.query_template:
                q.query_template = anonymize_sql(q.query_text)
            schs = build_schematics(q.query_text, execution_info, q.tables)
            for stype, schematic in schs.items():
                texts.append(schematic.text)
                meta.append((q, stype))
        vectors = embedder.encode(texts)
        rows = [(meta[i][0], meta[i][1], vectors[i]) for i in range(len(meta))]
        store.add(rows)
        n_qconfigs += len(qcs)

    # track distinct files seen
    n_files = len({m for m, _ in _iter_records(args.input_dir)})

    store.save(args.out)
    print("-" * 60)
    print(f"Files scanned : {n_files}")
    print(f"Records read  : {n_records}")
    print(f"QConfigs kept : {n_qconfigs}")
    print(f"Store rows    : {store.num_rows} (qconfigs={store.num_qconfigs})")
    print(f"Saved to      : {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
