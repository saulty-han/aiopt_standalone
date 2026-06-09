#!/usr/bin/env python3
"""
validate_mcts_results.py — 重跑 MCTS JSON 里的 best-hints，验证执行时间。

给定一个 MCTS 输出目录（mcts_scripts/tpcds_json 之类），对每个 JSON：
  1) 按 rollout_index 分组，取每一轮 reward 最大的 solution 作为"该轮最优"；
  2) 按 plan_digest 去重（同一份文件里多轮命中同一个 plan_digest 只跑一次，
     结果回填给所有命中它的行）；
  3) 用真实 DB 执行 ``EXPLAIN ANALYZE``（实际是 controller 的
     ``evaluate_elapsed_time_with_result``，走 profiling 路径），测量新的执行时间；
  4) 可选：加 ``--validate-baseline`` 会**额外**把 query 以无 hints 形式跑一次，
     拿到"重新测量的 baseline"；默认不跑，只跑 best hints。
  5) 输出 JSONL，每行一个 solution 的对照信息，包括 recorded/remeasured 对比。

Usage
  # 只重测 best hints（默认）
  python mcts_scripts/validate_mcts_results.py \\
      --input-dir mcts_scripts/tpcds_json \\
      --output    tmp/validation_tpcds.jsonl \\
      --host  1.2.3.4 --port 3306 --user root --password xxx --db tpcds

  # 同时重测 baseline（无 hints）
  python mcts_scripts/validate_mcts_results.py \\
      --input-dir ... --output ... --validate-baseline \\
      --host ... --port ... --user ... --password ... --db tpcds
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import re
import sys
import time
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import text as sql_text

_PROJECT_ROOT = os.path.abspath(
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")
)
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from mcts.utils.hint_utils import build_sql_with_hints  # noqa: E402

try:
    from tqdm import tqdm  # noqa: E402
    HAS_TQDM = True
except Exception:
    HAS_TQDM = False


# ============================================================================
# 文件解析 / best-per-rollout 抽取
# ============================================================================

# Accept either the 8-hex digest (llm_optimizer._save_output_json default) or
# the 4-hex variant used by some older TPC-DS runners.
_FILENAME_RE = re.compile(r"^(?P<db>.+)_(?P<digest>[0-9a-fA-F]{4,8})_\d{14}\.json$")


def _parse_db_from_filename(fname: str) -> Optional[str]:
    """从 ``{db}_{digest}_{ts}.json`` 里抠出 db 名。

    时间戳固定 14 位数字，digest 通常是 4 或 8 位十六进制，从右边锚定
    这两段后剩下的就是 db 名；这样 db 名本身含下划线（如 ``tpcds_100``）
    也能正确解析。
    """
    m = _FILENAME_RE.match(fname)
    return m.group("db") if m else None


@dataclass
class RolloutBest:
    """Best solution at one rollout index within one MCTS JSON entry."""
    rollout_index: int
    plan_digest: Optional[str]
    hints: List[str]
    recorded_time_s: Optional[float]
    reward: Optional[float]


def _best_per_rollout(mcts_tree_nodes: Dict[str, Any]) -> List[RolloutBest]:
    """从 ``mcts_tree_nodes`` (完整、未去重的搜索树) 抽取每个 rollout 的最优节点。

    之前是从 ``solutions`` 抽取, 但 ``solutions`` 在落盘前按 plan_digest 去重过
    (convert_search_result_to_dict): 当某轮最优计划与更早一轮的计划 digest 相同时,
    该轮的 solution 会被丢弃, 导致 best-per-rollout 找不到对应 plan_digest (为空)。

    改为直接读 tree nodes —— 它保留了所有节点 (含 plan_digest / reward / hints /
    execution_time), 不受 solutions 去重影响。每个 rollout_index 内:
      * 优先在**有 plan_digest 的节点**里按 reward 取最大;
      * 若该轮没有任何带 plan_digest 的节点, 再退回所有节点里 reward 最大的。
    """
    by_r: Dict[int, List[Dict[str, Any]]] = defaultdict(list)
    for tag, entry in (mcts_tree_nodes or {}).items():
        if not isinstance(entry, dict):
            continue
        ni = entry.get("node_info", {}) or {}
        db = entry.get("db_response", {}) or {}
        ri = ni.get("rollout_index")
        if ri is None:
            continue
        by_r[int(ri)].append({
            "plan_digest": db.get("plan_digest"),
            "execution_time_s": db.get("execution_time_s"),
            "executed_hints": ni.get("executed_hints") or [],
            "reward": ni.get("reward"),
        })

    def _reward_key(s: Dict[str, Any]) -> float:
        rw = s.get("reward")
        return rw if rw is not None else -1e18

    out: List[RolloutBest] = []
    for ri in sorted(by_r.keys()):
        cands = by_r[ri]
        with_pd = [c for c in cands if c.get("plan_digest")]
        best = max(with_pd or cands, key=_reward_key)
        out.append(RolloutBest(
            rollout_index=int(ri),
            plan_digest=best.get("plan_digest"),
            hints=list(best.get("executed_hints") or []),
            recorded_time_s=best.get("execution_time_s"),
            reward=best.get("reward"),
        ))
    return out


@dataclass
class FileEntry:
    """One MCTS result entry (a single JSON file typically holds one)."""
    path: str
    db: Optional[str]
    query: str
    recorded_baseline_s: Optional[float]
    default_plan_digest: Optional[str]
    rollout_bests: List[RolloutBest]

    @property
    def filename(self) -> str:
        return Path(self.path).name


def _iter_entries(input_dir: str) -> List[FileEntry]:
    path = Path(input_dir)
    if not path.is_dir():
        print(f"错误: 目录不存在 {input_dir}", file=sys.stderr)
        sys.exit(1)

    entries: List[FileEntry] = []
    for fp in sorted(path.glob("*.json")):
        try:
            with open(fp, "r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception as e:
            print(f"  ⚠ 跳过损坏文件 {fp.name}: {e}", file=sys.stderr)
            continue
        items = data if isinstance(data, list) else [data]
        for it in items:
            if not isinstance(it, dict):
                continue
            q = it.get("query") or ""
            if not q:
                continue
            db = _parse_db_from_filename(fp.name)
            entries.append(FileEntry(
                path=str(fp),
                db=db,
                query=q,
                recorded_baseline_s=it.get("baseline_time"),
                default_plan_digest=it.get("plan_digest"),
                rollout_bests=_best_per_rollout(it.get("mcts_tree_nodes") or {}),
            ))
    return entries


# ============================================================================
# DB 执行 —— 复用 DBController.evaluate_elapsed_time_with_result
# ============================================================================

def _build_controller(args: argparse.Namespace):
    """Build one DBController bound to --db; mirrors test_tpcds_queries_parallel."""
    # Late imports so --help doesn't need the whole aiopt stack loaded.
    from data_models import InstanceConfig  # noqa: E402
    from db_controller import DBController  # noqa: E402

    cfg = InstanceConfig(
        instance_id=f"validate_mcts_{args.host}_{args.port}",
        ip=args.host,
        port=args.port,
        user=args.user,
        password=args.password or "",
        read_only=False,
        with_ai_marker=True,
        allow_reconnect=True,
    )
    return DBController(cfg, db=args.db)


def _run_once(
    controller,
    sql_with_hints: str,
    timeout_seconds: float,
    warmup_runs: int,
) -> Tuple[Optional[float], Optional[str], bool]:
    """Run a single EXPLAIN ANALYZE and return (elapsed_seconds, error_str, is_timeout).

    On timeout: elapsed_seconds = timeout_seconds, is_timeout = True.
    On other errors: elapsed_seconds = None, is_timeout = False.
    Never raises.
    """
    from sqlalchemy.exc import OperationalError as _OpError
    try:
        stmt = sql_text(f"EXPLAIN ANALYZE {sql_with_hints}")
        elapsed, _rows = controller.evaluate_elapsed_time_with_result(
            stmt,
            timeout_seconds=timeout_seconds,
            warmup_runs=warmup_runs,
        )
        if elapsed is None:
            return None, "evaluate_elapsed_time returned None", False
        return float(elapsed), None, False
    except (ValueError, _OpError) as e:
        # ValueError is raised by evaluate_elapsed_time_with_result on timeout;
        # OperationalError with errno 3024/1317 is a MySQL query-interrupted timeout.
        is_timeout = isinstance(e, ValueError) or (
            isinstance(e, _OpError)
            and hasattr(e, "orig")
            and getattr(e.orig, "args", (None,))[0] in (3024, 1317)
        )
        if is_timeout:
            return float(timeout_seconds), f"timeout({timeout_seconds}s)", True
        return None, f"{type(e).__name__}: {e}", False
    except Exception as e:
        return None, f"{type(e).__name__}: {e}", False


# ============================================================================
# 主流程：per-entry 验证（带 plan_digest 缓存）
# ============================================================================

@dataclass
class RemeasureResult:
    elapsed_s: Optional[float]
    error: Optional[str] = None
    cached: bool = False
    is_timeout: bool = False
    # When True, this row's elapsed_s was not actually measured against the
    # DB — it was substituted from the recorded baseline because the MCTS
    # trace already showed the rollout had no improvement over baseline.
    # Saves wall-clock time and avoids needlessly hitting the server.
    skipped_as_baseline: bool = False
    # When True, skipped because the rollout's recorded best is already
    # worse than (or equal to) the per-file running best we've accumulated
    # across earlier rollouts. Running it would not improve the running
    # best — measured time goes toward wall clock but contributes nothing.
    skipped_worse_than_running_best: bool = False


def _build_row(
    entry: FileEntry,
    rb: RolloutBest,
    res_row: RemeasureResult,
    baseline_res: Optional[RemeasureResult],
    validate_baseline: bool,
) -> Dict[str, Any]:
    """Assemble one output JSONL row for a single (entry, rollout) pair."""
    rec_b = entry.recorded_baseline_s
    rec_t = rb.recorded_time_s
    sp_rec = (rec_b / rec_t) if rec_b and rec_t and rec_t > 0 else None
    sp_new = (
        (rec_b / res_row.elapsed_s)
        if rec_b and res_row.elapsed_s and res_row.elapsed_s > 0
        else None
    )
    row: Dict[str, Any] = {
        "file": entry.filename,
        "db": entry.db,
        "rollout_index": rb.rollout_index,
        "plan_digest": rb.plan_digest,
        "hints": rb.hints,
        "reward_recorded": rb.reward,
        "recorded_baseline_s": rec_b,
        "recorded_best_s": rec_t,
        "remeasured_best_s": res_row.elapsed_s,
        "remeasured_best_error": res_row.error,
        "remeasured_best_from_cache": res_row.cached,
        "remeasured_best_skipped_as_baseline": res_row.skipped_as_baseline,
        "remeasured_best_skipped_worse_than_running_best": res_row.skipped_worse_than_running_best,
        "speedup_recorded": sp_rec,
        "speedup_remeasured": sp_new,
    }
    if validate_baseline and baseline_res is not None:
        row["remeasured_baseline_s"] = baseline_res.elapsed_s
        row["remeasured_baseline_error"] = baseline_res.error
    return row


# CSV column order — matches analyze_tpcds_results.py format.
_CSV_COLUMNS = [
    "file",
    "db",
    "recorded_baseline_s",
    "recorded_best_s",
    "remeasured_best_s",
    "speedup_recorded",
    "speedup_remeasured",
    "best_hints",
    "plan_digest",
    "rollout_index",
    "remeasured_baseline_s",
    "remeasured_baseline_error",
]


def _csv_rollout_columns(max_rollout: int) -> List[str]:
    """Return per-rollout column names for R0..Rmax."""
    cols = []
    for r in range(max_rollout + 1):
        cols += [
            f"R{r}_plan_digest",
            f"R{r}_hints",
            f"R{r}_hints_count",
            f"R{r}_remeasured_best_s",
            f"R{r}_speedup_remeasured",
        ]
    return cols


def _write_wide_csv(
    csv_path: Path,
    pending_entries: "List[FileEntry]",
    csv_accumulator: "Dict[str, Dict[str, Any]]",
    baseline_results: "Dict[str, RemeasureResult]",
    max_rollout: int,
    validate_baseline: bool,
) -> None:
    """Write CSV in analyze_tpcds format: one row per query, best result across rollouts.

    Columns match analyze_tpcds_results.py format with remeasured_best_s replacing best_time_s.
    """
    # Main columns (analyze_tpcds-compatible)
    main_cols = [
        "file", "db", "recorded_baseline_s", "recorded_best_s",
        "remeasured_best_s", "speedup_recorded", "speedup_remeasured",
        "best_plan_digest", "best_hints", "best_rollout_index",
    ]
    if validate_baseline:
        main_cols += ["remeasured_baseline_s", "remeasured_baseline_error"]
    # Per-rollout columns
    rollout_cols = _csv_rollout_columns(max_rollout)
    all_cols = main_cols + rollout_cols

    with open(csv_path, "w", encoding="utf-8", newline="") as fcsv:
        writer = csv.DictWriter(fcsv, fieldnames=all_cols, extrasaction="ignore")
        writer.writeheader()

        for entry in pending_entries:
            rec_b = entry.recorded_baseline_s
            acc = csv_accumulator.get(entry.filename, {})

            # Find best rollout (lowest remeasured_best_s)
            best_rollout_idx = None
            best_remeasured = None
            best_hints_str = ""
            best_plan_digest = ""
            for r_idx, rd in acc.items():
                elapsed = rd.get("remeasured_best_s")
                if elapsed is not None:
                    if best_remeasured is None or elapsed < best_remeasured:
                        best_remeasured = elapsed
                        best_rollout_idx = r_idx
                        best_hints_str = rd.get("hints", "")
                        best_plan_digest = rd.get("plan_digest", "")

            # Also find recorded best (from rollout_bests)
            recorded_best = None
            for rb in entry.rollout_bests:
                if rb.recorded_time_s is not None:
                    if recorded_best is None or rb.recorded_time_s < recorded_best:
                        recorded_best = rb.recorded_time_s

            sp_rec = (rec_b / recorded_best) if rec_b and recorded_best and recorded_best > 0 else None
            sp_new = (rec_b / best_remeasured) if rec_b and best_remeasured and best_remeasured > 0 else None

            row: Dict[str, Any] = {
                "file": entry.filename,
                "db": entry.db or "",
                "recorded_baseline_s": f"{rec_b:.6f}" if rec_b is not None else "",
                "recorded_best_s": f"{recorded_best:.6f}" if recorded_best is not None else "",
                "remeasured_best_s": f"{best_remeasured:.6f}" if best_remeasured is not None else "",
                "speedup_recorded": f"{sp_rec:.4f}" if sp_rec else "",
                "speedup_remeasured": f"{sp_new:.4f}" if sp_new else "",
                "best_plan_digest": best_plan_digest,
                "best_hints": best_hints_str,
                "best_rollout_index": best_rollout_idx if best_rollout_idx is not None else "",
            }

            if validate_baseline:
                bl = baseline_results.get(entry.filename)
                if bl is not None:
                    row["remeasured_baseline_s"] = (
                        f"{bl.elapsed_s:.6f}" if bl.elapsed_s is not None else ""
                    )
                    row["remeasured_baseline_error"] = bl.error or ""

            # Fill per-rollout columns
            for r in range(max_rollout + 1):
                rd = acc.get(r, {})
                elapsed = rd.get("remeasured_best_s")
                sp = (
                    f"{rec_b / elapsed:.6f}"
                    if rec_b and elapsed and elapsed > 0
                    else ""
                )
                row[f"R{r}_plan_digest"]        = rd.get("plan_digest", "")
                row[f"R{r}_hints"]              = rd.get("hints", "")
                row[f"R{r}_hints_count"]        = rd.get("hints_count", "")
                row[f"R{r}_remeasured_best_s"]  = f"{elapsed:.6f}" if elapsed is not None else ""
                row[f"R{r}_speedup_remeasured"] = sp

            writer.writerow(row)


def _measure_rollout(
    entry: FileEntry,
    rb: RolloutBest,
    plan_cache: Dict[str, RemeasureResult],
    running_best_s: Optional[float],
    controller,
    timeout_seconds: float,
    warmup_runs: int,
) -> RemeasureResult:
    """Measure (or retrieve cached / skip) a single rollout best for one file.

    ``plan_cache`` is the **per-file** plan_digest cache that persists across
    rollouts, so later rollouts hitting a plan already measured in R0..R(r-1)
    (common on smaller queries) skip the DB call.

    ``running_best_s`` is the per-file running best (min elapsed across
    earlier rollouts, or the baseline if this is the first rollout). If
    the rollout's **recorded** best already fails to beat the running best,
    measuring it can't improve anything — skip the DB call and keep the
    running best. This is the default aggressive skip; relies on the
    assumption that MCTS measured the same plan on the same instance, so
    recorded and remeasured times are in the same ballpark.
    """
    rec_b = entry.recorded_baseline_s
    rec_t = rb.recorded_time_s

    # Fast path 1: MCTS already shows this rollout couldn't beat baseline.
    # Don't bother the DB — the remeasured best is, by construction, the
    # baseline. We still emit the row so the R0..Rn trace is complete.
    no_improvement = (
        rec_b is not None
        and rec_b > 0
        and rec_t is not None
        and rec_t >= rec_b
    )
    if no_improvement:
        return RemeasureResult(
            elapsed_s=rec_b,
            error=None,
            cached=False,
            skipped_as_baseline=True,
        )

    # Fast path 2: recorded best is already worse than the per-file running
    # best established by earlier rollouts. Measuring can't beat that, so
    # skip the DB call and echo the running best into this row.
    if (
        running_best_s is not None
        and rec_t is not None
        and rec_t >= running_best_s
    ):
        return RemeasureResult(
            elapsed_s=running_best_s,
            error=None,
            cached=False,
            skipped_worse_than_running_best=True,
        )

    pd = rb.plan_digest
    if pd and pd in plan_cache:
        cached_res = plan_cache[pd]
        return RemeasureResult(
            elapsed_s=cached_res.elapsed_s,
            error=cached_res.error,
            cached=True,
            skipped_as_baseline=cached_res.skipped_as_baseline,
            skipped_worse_than_running_best=cached_res.skipped_worse_than_running_best,
        )

    sql_with_hints = build_sql_with_hints(entry.query, rb.hints)
    elapsed, err, is_timeout = _run_once(
        controller,
        sql_with_hints=sql_with_hints,
        timeout_seconds=timeout_seconds,
        warmup_runs=warmup_runs,
    )
    res = RemeasureResult(elapsed_s=elapsed, error=err, cached=False, is_timeout=is_timeout)
    if pd:
        plan_cache[pd] = res
    return res


# ============================================================================
# 入口
# ============================================================================

def main() -> None:
    parser = argparse.ArgumentParser(
        description="重跑 MCTS JSON 里的 best-per-rollout hints 并测量执行时间（带 plan_digest 缓存）",
    )
    parser.add_argument("--input-dir", required=True, help="MCTS 输出 JSON 目录")
    parser.add_argument("--output", required=True, help="输出 JSONL 路径")
    parser.add_argument(
        "--output-csv", default="",
        help="同时输出 CSV 路径；留空则自动用 --output 同名替换扩展名为 .csv",
    )
    parser.add_argument(
        "--validate-baseline", action="store_true",
        help="同时重测 baseline（无 hints）；默认不跑",
    )
    parser.add_argument(
        "--timeout-seconds", type=float, default=0,
        help="单次 EXPLAIN ANALYZE 的超时（秒）。0=自动（baseline×1.1），>0=固定值",
    )
    parser.add_argument(
        "--warmup-runs", type=int, default=0,
        help="每条 SQL 正式计时前的预热轮数（默认 0）",
    )
    parser.add_argument(
        "--limit", type=int, default=0,
        help="仅处理前 N 个文件（0=全部），用于小规模抽测",
    )
    parser.add_argument(
        "--progress", action="store_true",
        help="启用 tqdm 进度条",
    )

    # DB 连接（对齐 test_tpcds_queries_parallel.py）
    parser.add_argument("--host", required=True, help="数据库 IP")
    parser.add_argument("--port", type=int, required=True, help="数据库端口")
    parser.add_argument("--user", default="root", help="数据库用户名")
    parser.add_argument("--password", default="", help="数据库密码")
    parser.add_argument(
        "--db", required=True,
        help="目标数据库（DBController 初始 USE 的库）",
    )

    args = parser.parse_args()

    entries = _iter_entries(args.input_dir)
    if args.limit and args.limit > 0:
        entries = entries[: args.limit]

    # Quick summary of what we're about to do. Simulate the two skip rules
    # (baseline, running-best) in rollout order per file so the estimate
    # matches the actual main loop behaviour.
    total_bests = sum(len(e.rollout_bests) for e in entries)
    skipped_as_baseline = 0
    skipped_worse_than_run = 0
    executing_plans: int = 0
    for e in entries:
        rec_b = e.recorded_baseline_s
        # plan_cache + running_best per file, just like the main loop.
        sim_plan_cache: set = set()
        sim_running_best: Optional[float] = (
            float(rec_b) if rec_b is not None else None
        )
        for rb in sorted(e.rollout_bests, key=lambda x: x.rollout_index):
            rec_t = rb.recorded_time_s
            if (
                rec_b is not None and rec_b > 0
                and rec_t is not None and rec_t >= rec_b
            ):
                skipped_as_baseline += 1
                continue
            if (
                sim_running_best is not None
                and rec_t is not None
                and rec_t >= sim_running_best
            ):
                skipped_worse_than_run += 1
                continue
            if rb.plan_digest and rb.plan_digest in sim_plan_cache:
                # Cached — not a new DB execute.
                continue
            if rb.plan_digest:
                sim_plan_cache.add(rb.plan_digest)
            executing_plans += 1
            # Optimistically assume measurement ~= recorded best, so running
            # best updates accordingly (min-clamped against recorded).
            if rec_t is not None and (
                sim_running_best is None or rec_t < sim_running_best
            ):
                sim_running_best = rec_t

    est_executes = executing_plans + (
        sum(1 for e in entries if e.recorded_baseline_s is not None)
        if args.validate_baseline else 0
    )
    print("=" * 72)
    print(f"[load] files                      = {len(entries)}")
    print(f"[load] best-per-rollout           = {total_bests}")
    print(f"[load] skip (recorded ≥ baseline) = {skipped_as_baseline}")
    print(f"[load] skip (worse than running)  = {skipped_worse_than_run}   (新默认跳过规则)")
    print(f"[load] executing plans            = {executing_plans}   (去重 + 两级 skip 后剩余)")
    if args.validate_baseline:
        print(f"[load] baseline executes          = {sum(1 for e in entries if e.recorded_baseline_s is not None)}")
    print(f"[load] estimated DB runs ≈ {est_executes}  "
          f"(timeout={'auto(baseline×1.1)' if args.timeout_seconds == 0 else f'{args.timeout_seconds}s'}, warmup={args.warmup_runs})")
    print("=" * 72)

    try:
        controller = _build_controller(args)
    except Exception as e:
        print(f"错误: 连接数据库失败: {type(e).__name__}: {e}", file=sys.stderr)
        sys.exit(2)

    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    csv_path = Path(
        args.output_csv if args.output_csv
        else out_path.with_suffix(".csv")
    )
    csv_path.parent.mkdir(parents=True, exist_ok=True)

    # Always start fresh: no resume, no reading disk. Cache (plan_digest →
    # result, running best per file) lives entirely in memory for this run.
    if out_path.exists():
        out_path.unlink()
    if csv_path.exists():
        csv_path.unlink()

    pending_entries = [e for e in entries if e.rollout_bests]
    if not pending_entries:
        print("没有可处理样本，退出。")
        return

    max_rollout = max(
        rb.rollout_index
        for e in pending_entries
        for rb in e.rollout_bests
    )

    # Per-file plan_digest cache persisting across rollouts — later rollouts
    # hitting a plan already measured in an earlier rollout skip the DB call.
    plan_caches: Dict[str, Dict[str, RemeasureResult]] = {
        e.filename: {} for e in pending_entries
    }

    # Per-file baseline result (only populated when --validate-baseline and
    # the baseline pre-pass actually ran for this file).
    baseline_results: Dict[str, RemeasureResult] = {}

    # Per-file running best across rollouts: for each file we keep
    # ``min(elapsed_s)`` seen in R0..R_current. Seeded from the file's
    # recorded baseline so the running best never exceeds baseline — a
    # rollout that regresses doesn't "pollute" the running min.
    running_best_s: Dict[str, float] = {}
    for e in pending_entries:
        if e.recorded_baseline_s is not None:
            running_best_s[e.filename] = float(e.recorded_baseline_s)

    # Fixed baseline total across all pending files — the denominator for
    # any per-rollout speedup number. Computed once.
    total_baseline_s = sum(
        float(e.recorded_baseline_s)
        for e in pending_entries
        if e.recorded_baseline_s is not None
    )

    t0 = time.time()
    stats = {
        "entries_processed": 0,
        "rows_emitted": 0,
        "best_executed": 0,
        "best_from_cache": 0,
        "best_skipped_as_baseline": 0,
        "best_skipped_worse_than_running_best": 0,
        "best_errors": 0,
        "baseline_executed": 0,
        "baseline_errors": 0,
    }

    fout = open(out_path, "a", encoding="utf-8")
    # csv_accumulator[filename][rollout_index] = {plan_digest, hints, hints_count, remeasured_best_s}
    # Populated during the rollout loop; written to CSV in one pass at the end.
    csv_accumulator: Dict[str, Dict[int, Dict[str, Any]]] = {
        e.filename: {} for e in pending_entries
    }
    try:
        # ---- Baseline pre-pass (optional) -----------------------------
        if args.validate_baseline:
            print()
            print(f"[baseline] 重测 baseline (无 hints)  —— {len(pending_entries)} 文件")
            b_iter = pending_entries
            if args.progress and HAS_TQDM:
                b_iter = tqdm(b_iter, desc="baseline")
            for entry in b_iter:
                if entry.recorded_baseline_s is None:
                    baseline_results[entry.filename] = RemeasureResult(
                        elapsed_s=None, error="no recorded baseline",
                    )
                    stats["baseline_errors"] += 1
                    continue
                elapsed, err, is_timeout = _run_once(
                    controller,
                    sql_with_hints=entry.query,
                    timeout_seconds=(args.timeout_seconds if args.timeout_seconds > 0
                                     else (entry.recorded_baseline_s * 1.1 if entry.recorded_baseline_s else 600.0)),
                    warmup_runs=args.warmup_runs,
                )
                baseline_results[entry.filename] = RemeasureResult(
                    elapsed_s=elapsed, error=err, is_timeout=is_timeout,
                )
                if err is None and elapsed is not None:
                    stats["baseline_executed"] += 1
                    if args.progress:
                        line = f"  [baseline] {entry.filename}  elapsed={elapsed:.4f}s"
                        if args.progress and HAS_TQDM:
                            tqdm.write(line)
                        else:
                            print(line)
                else:
                    stats["baseline_errors"] += 1
                    if args.progress:
                        if is_timeout:
                            line = f"  [baseline] {entry.filename}  timeout({args.timeout_seconds}s)"
                        else:
                            line = f"  [baseline] {entry.filename}  error={err}"
                        if args.progress and HAS_TQDM:
                            tqdm.write(line)
                        else:
                            print(line)

        # ---- Rollout outer loop ---------------------------------------
        # For each rollout index 0..N, walk every file that has that rollout
        # and measure / cache / skip. Print a short per-rollout summary as
        # soon as the rollout finishes so the operator sees progress in
        # real time instead of waiting for the full job.
        for r in range(max_rollout + 1):
            per_rollout = {
                "measured": 0, "cached": 0,
                "skipped_baseline": 0, "skipped_worse": 0,
                "errors": 0, "missing": 0,
            }
            # Count how many files this rollout actually improved the running
            # best for (a tiny sanity signal — monotonic decrease expected).
            improved_this_rollout = 0

            files_this_r = [
                e for e in pending_entries
                if any(rb.rollout_index == r for rb in e.rollout_bests)
            ]
            label = f"R{r}"
            print()
            print(f"[{label}] 开始  —— 触达文件 {len(files_this_r)} / {len(pending_entries)}")
            it = files_this_r
            if args.progress and HAS_TQDM:
                it = tqdm(it, desc=label)

            for entry in it:
                rb = next(
                    (x for x in entry.rollout_bests if x.rollout_index == r),
                    None,
                )
                if rb is None:
                    per_rollout["missing"] += 1
                    continue
                # Per-entry timeout: baseline × 1.1 (anything slower is useless)
                # Falls back to global --timeout-seconds if baseline unknown
                if args.timeout_seconds > 0:
                    entry_timeout = args.timeout_seconds
                elif entry.recorded_baseline_s and entry.recorded_baseline_s > 0:
                    entry_timeout = entry.recorded_baseline_s * 1.1
                else:
                    entry_timeout = 600.0
                res = _measure_rollout(
                    entry=entry,
                    rb=rb,
                    plan_cache=plan_caches[entry.filename],
                    running_best_s=running_best_s.get(entry.filename),
                    controller=controller,
                    timeout_seconds=entry_timeout,
                    warmup_runs=args.warmup_runs,
                )

                row = _build_row(
                    entry=entry,
                    rb=rb,
                    res_row=res,
                    baseline_res=baseline_results.get(entry.filename),
                    validate_baseline=args.validate_baseline,
                )
                fout.write(json.dumps(row, ensure_ascii=False) + "\n")
                fout.flush()

                stats["rows_emitted"] += 1

                # Per-query detail line when --progress is active
                if args.progress:
                    rec_b = entry.recorded_baseline_s
                    rec_t = rb.recorded_time_s
                    new_t = res.elapsed_s
                    # Status tag
                    if res.skipped_as_baseline:
                        status = "skip(≥base)"
                    elif res.skipped_worse_than_running_best:
                        status = "skip(worse)"
                    elif res.cached:
                        status = "cached"
                    elif res.is_timeout:
                        status = f"timeout({args.timeout_seconds}s)"
                    elif res.error:
                        status = f"error:{res.error[:40]}"
                    else:
                        status = "measured"
                    # Speedup vs recorded baseline
                    sp_str = ""
                    if rec_b and new_t and new_t > 0:
                        sp_str = f"  speedup={rec_b/new_t:.3f}x"
                    # Hints summary (show up to 2, then count)
                    if rb.hints:
                        if len(rb.hints) <= 2:
                            hints_str = " ".join(rb.hints)
                        else:
                            hints_str = " ".join(rb.hints[:2]) + f" (+{len(rb.hints)-2})"
                    else:
                        hints_str = "(no hints)"
                    rec_t_str = f"{rec_t:.4f}s" if rec_t is not None else "?"
                    new_t_str = f"{new_t:.4f}s" if new_t is not None else "?"
                    base_str  = f"{rec_b:.4f}s" if rec_b is not None else "?"
                    line = (
                        f"  [{label}] {entry.filename}"
                        f"  base={base_str}  rec={rec_t_str}  new={new_t_str}"
                        f"  [{status}]{sp_str}"
                        f"  hints={hints_str}"
                    )
                    if args.progress and HAS_TQDM:
                        tqdm.write(line)
                    else:
                        print(line)

                if res.skipped_as_baseline:
                    per_rollout["skipped_baseline"] += 1
                    stats["best_skipped_as_baseline"] += 1
                elif res.skipped_worse_than_running_best:
                    per_rollout["skipped_worse"] += 1
                    stats["best_skipped_worse_than_running_best"] += 1
                elif res.cached:
                    per_rollout["cached"] += 1
                    stats["best_from_cache"] += 1
                else:
                    per_rollout["measured"] += 1
                    stats["best_executed"] += 1
                if res.error:
                    per_rollout["errors"] += 1
                    stats["best_errors"] += 1

                # Running best across R0..R_current for this file. Seed is
                # the recorded baseline (above) so a regression never raises
                # the running min.
                if res.elapsed_s is not None:
                    prev = running_best_s.get(entry.filename)
                    new_best = res.elapsed_s if prev is None else min(prev, res.elapsed_s)
                    if prev is None or new_best < prev:
                        improved_this_rollout += 1
                    running_best_s[entry.filename] = new_best

                # Accumulate per-rollout data for wide CSV (written after all rollouts).
                # Use cumulative running best up to this rollout, not just this rollout's value.
                cum_best = running_best_s.get(entry.filename, res.elapsed_s)
                csv_accumulator[entry.filename][r] = {
                    "plan_digest": rb.plan_digest or "",
                    "hints": "; ".join(rb.hints) if rb.hints else "",
                    "hints_count": len(rb.hints),
                    "remeasured_best_s": cum_best,
                }

            # Per-rollout summary line: cumulative-best sum across ALL files
            # (not just those that participated in this rollout). Compared
            # against the fixed total_baseline_s so numbers are comparable
            # across R0..Rn.
            best_sum_cum = sum(running_best_s.values())
            speedup = (
                total_baseline_s / best_sum_cum
                if best_sum_cum > 0 else None
            )
            sp_txt = f"{speedup:.3f}x" if speedup is not None else "-"
            print(
                f"[{label}] 完成  "
                f"measured={per_rollout['measured']} cached={per_rollout['cached']} "
                f"skip_base={per_rollout['skipped_baseline']} "
                f"skip_worse={per_rollout['skipped_worse']} "
                f"errors={per_rollout['errors']}  "
                f"improved={improved_this_rollout}  "
                f"BestSum(cum)={best_sum_cum:.3f}s  "
                f"(BaselineSum={total_baseline_s:.3f}s, Ovr≈{sp_txt})"
            )
    finally:
        fout.close()
        # Write wide CSV (one row per query, one column group per rollout)
        _write_wide_csv(
            csv_path=csv_path,
            pending_entries=pending_entries,
            csv_accumulator=csv_accumulator,
            baseline_results=baseline_results,
            max_rollout=max_rollout,
            validate_baseline=args.validate_baseline,
        )

    stats["entries_processed"] = len(pending_entries)

    dt = time.time() - t0
    print()
    print(f"[done] 耗时 {dt:.1f}s")
    print(f"  entries                    = {stats['entries_processed']}")
    print(f"  rows                       = {stats['rows_emitted']}")
    print(f"  best 实测                  = {stats['best_executed']}  (errors={stats['best_errors']})")
    print(f"  best 缓存                  = {stats['best_from_cache']}   (同一文件 plan_digest 重复命中)")
    print(f"  best 跳过 (=baseline)      = {stats['best_skipped_as_baseline']}   (recorded ≥ baseline)")
    print(f"  best 跳过 (worse than run) = {stats['best_skipped_worse_than_running_best']}   "
          f"(recorded ≥ 当前 running best)")
    if args.validate_baseline:
        print(f"  baseline                   = {stats['baseline_executed']}  (errors={stats['baseline_errors']})")
    print(f"  out:   {out_path}")
    print(f"  csv:   {csv_path}")


if __name__ == "__main__":
    main()
