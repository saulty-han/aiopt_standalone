from __future__ import annotations

"""
rollout_validation.py — 优化结束后的单线程验证阶段（被 benchmark_runner /
tpcds_runner 复用）。

核心思想与 mcts/validate_mcts_results.py 完全一致, 区别在于:
  * 这里直接吃**内存里的 mcts_results**（runner 优化阶段已经拿到的结构），
    不需要从磁盘 JSON 重新解析。
  * 单线程串行重跑, 避免并发对计时的干扰。

对每个 query(每个 mcts_results entry):
  1) 按 rollout_index 分组, 取每轮 reward 最大的 solution 作为"该轮最优";
  2) 同一 query 内按 plan_digest 去重（多轮命中同一 plan 只真跑一次, 回填结果）;
  3) 两级 skip:
       - recorded_best >= baseline      → 不可能改善, 直接记 baseline;
       - recorded_best >= running_best   → 跑了也超不过当前最优, 跳过;
  4) 其余用真实 DB 跑 EXPLAIN ANALYZE（走 controller.evaluate_elapsed_time_with_result
     的 profiling 路径）测真实耗时, 支持 warmup;
  5) 产出一行/查询的宽表 CSV（每个 rollout 一组列 + 跨轮最优汇总）。

入口: validate_results(entries, controller, csv_path, ...) -> ValidationStats
"""

import csv
import time
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple


# ---------------------------------------------------------------------------
# 数据结构
# ---------------------------------------------------------------------------

@dataclass
class RolloutBest:
    rollout_index: int
    plan_digest: Optional[str]
    hints: List[str]
    recorded_time_s: Optional[float]
    reward: Optional[float]


@dataclass
class ValidationEntry:
    """一个待验证 query。由 runner 从 mcts_results 转换而来。"""
    key: str                      # 文件名/标识（CSV 第一列）
    db: Optional[str]
    query: str
    recorded_baseline_s: Optional[float]
    default_plan_digest: Optional[str]
    rollout_bests: List[RolloutBest]
    instance_id: Optional[str] = None
    benchmark_id: Optional[str] = None


@dataclass
class RemeasureResult:
    elapsed_s: Optional[float]
    error: Optional[str] = None
    cached: bool = False
    is_timeout: bool = False
    skipped_as_baseline: bool = False
    skipped_worse_than_running_best: bool = False
    # 本次真跑前后探测到的"外部正在执行"的查询数峰值 (processlist executing - 1)。
    external_exec: int = 0


@dataclass
class ValidationStats:
    entries: int = 0
    rows: int = 0
    best_executed: int = 0
    best_from_cache: int = 0
    best_skipped_as_baseline: int = 0
    best_skipped_worse_than_running_best: int = 0
    best_errors: int = 0
    baseline_executed: int = 0
    baseline_errors: int = 0
    elapsed_s: float = 0.0
    csv_path: Optional[str] = None
    total_baseline_s: float = 0.0
    total_best_s: float = 0.0
    # 验证阶段探测到的"外部并发执行"峰值 (理想应为 0, >0 说明计时可能受干扰)。
    external_exec_peak: int = 0

    @property
    def overall_speedup(self) -> Optional[float]:
        if self.total_best_s and self.total_best_s > 0:
            return self.total_baseline_s / self.total_best_s
        return None


# ---------------------------------------------------------------------------
# mcts_results(内存) → ValidationEntry
# ---------------------------------------------------------------------------

def _best_per_rollout(mcts_tree_nodes: Dict[str, Any]) -> List[RolloutBest]:
    """从 mcts_tree_nodes (完整搜索树, 未去重) 抽取每个 rollout 的最优节点。

    不再从 solutions 抽取: solutions 落盘前按 plan_digest 去重, 某轮最优计划与更早
    一轮 digest 相同会被丢弃, 导致 best-per-rollout 的 plan_digest 为空。tree nodes
    保留所有节点。每个 rollout 内: 优先在有 plan_digest 的节点里按 reward 取最大,
    整轮都没有带 digest 的节点时再退回所有节点里 reward 最大的。
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


def entries_from_mcts_results(
    mcts_results: Optional[List[Dict[str, Any]]],
    *,
    key: str,
    db: Optional[str],
    instance_id: Optional[str] = None,
    benchmark_id: Optional[str] = None,
) -> List[ValidationEntry]:
    """把单个 query 的 mcts_results(可能含多个 sample entry)转成 ValidationEntry 列表。"""
    entries: List[ValidationEntry] = []
    if not mcts_results:
        return entries
    for i, mr in enumerate(mcts_results):
        if not isinstance(mr, dict):
            continue
        q = mr.get("query") or ""
        if not q:
            continue
        rb = _best_per_rollout(mr.get("mcts_tree_nodes") or {})
        if not rb:
            continue
        entry_key = key if len(mcts_results) == 1 else f"{key}#{i}"
        entries.append(ValidationEntry(
            key=entry_key,
            db=db or mr.get("db_name"),
            query=q,
            recorded_baseline_s=mr.get("baseline_time"),
            default_plan_digest=mr.get("plan_digest"),
            rollout_bests=rb,
            instance_id=instance_id,
            benchmark_id=benchmark_id,
        ))
    return entries


# ---------------------------------------------------------------------------
# 单次 DB 执行
# ---------------------------------------------------------------------------

def _count_external_executing(controller) -> int:
    """探测当前实例上"外部"正在执行的查询数。

    SELECT COUNT(*) FROM information_schema.processlist WHERE State='executing'
    本身在统计时也会把自己这条 COUNT 查询算进去 (State=executing)，所以减 1，
    结果即为除本会话外其它正在执行的查询数。探测失败/异常时返回 0 (不阻塞验证)。
    """
    from sqlalchemy import text as sql_text
    try:
        res = controller.execute(sql_text(
            "SELECT COUNT(*) FROM information_schema.processlist "
            "WHERE State = 'executing'"
        ))
        val = res.scalar()
        if val is None:
            return 0
        return max(0, int(val) - 1)
    except Exception:
        return 0


def _run_once(
    controller,
    sql_with_hints: str,
    timeout_seconds: float,
    warmup_runs: int,
) -> Tuple[Optional[float], Optional[str], bool]:
    """跑一次 EXPLAIN ANALYZE, 返回 (elapsed_s, error, is_timeout); 永不抛异常。"""
    from sqlalchemy import text as sql_text
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


def _measure_rollout(
    entry: ValidationEntry,
    rb: RolloutBest,
    plan_cache: Dict[str, RemeasureResult],
    running_best_s: Optional[float],
    controller,
    timeout_seconds: float,
    warmup_runs: int,
    build_sql_with_hints: Callable[[str, List[str]], str],
) -> RemeasureResult:
    rec_b = entry.recorded_baseline_s
    rec_t = rb.recorded_time_s

    # Fast path 1: 记录显示该轮没超过 baseline → 直接记 baseline。
    if rec_b is not None and rec_b > 0 and rec_t is not None and rec_t >= rec_b:
        return RemeasureResult(elapsed_s=rec_b, skipped_as_baseline=True)

    # Fast path 2: 记录最优已劣于当前 running best → 跳过。
    if running_best_s is not None and rec_t is not None and rec_t >= running_best_s:
        return RemeasureResult(elapsed_s=running_best_s, skipped_worse_than_running_best=True)

    pd = rb.plan_digest
    if pd and pd in plan_cache:
        c = plan_cache[pd]
        return RemeasureResult(
            elapsed_s=c.elapsed_s, error=c.error, cached=True,
            is_timeout=c.is_timeout,
            skipped_as_baseline=c.skipped_as_baseline,
            skipped_worse_than_running_best=c.skipped_worse_than_running_best,
        )

    sql_with_hints = build_sql_with_hints(entry.query, rb.hints)
    # 真跑前后各探测一次外部并发执行数, 取峰值记到本条结果上。
    ext_before = _count_external_executing(controller)
    elapsed, err, is_timeout = _run_once(
        controller, sql_with_hints, timeout_seconds, warmup_runs,
    )
    ext_after = _count_external_executing(controller)
    res = RemeasureResult(
        elapsed_s=elapsed, error=err, cached=False, is_timeout=is_timeout,
        external_exec=max(ext_before, ext_after),
    )
    if pd:
        plan_cache[pd] = res
    return res


# ---------------------------------------------------------------------------
# CSV（宽表：一行一 query，每个 rollout 一组列）
# ---------------------------------------------------------------------------

def _write_wide_csv(
    csv_path: Path,
    entries: List[ValidationEntry],
    accumulator: Dict[str, Dict[int, Dict[str, Any]]],
    baseline_results: Dict[str, RemeasureResult],
    max_rollout: int,
    validate_baseline: bool,
    entry_external_exec: Optional[Dict[str, int]] = None,
) -> None:
    entry_external_exec = entry_external_exec or {}
    main_cols = [
        "key", "instance_id", "benchmark_id", "db",
        "recorded_baseline_s", "recorded_best_s",
        "remeasured_best_s", "speedup_recorded", "speedup_remeasured",
        "external_exec_peak", "external_exec_note",
        "best_plan_digest", "best_hints", "best_rollout_index",
    ]
    if validate_baseline:
        main_cols += ["remeasured_baseline_s", "remeasured_baseline_error"]
    rollout_cols: List[str] = []
    for r in range(max_rollout + 1):
        # 列名用 1-based 展示 (R1..RN)，内部索引仍 0-based。
        rcol = r + 1
        rollout_cols += [
            f"R{rcol}_plan_digest", f"R{rcol}_hints", f"R{rcol}_hints_count",
            f"R{rcol}_remeasured_best_s", f"R{rcol}_speedup_remeasured",
        ]
    all_cols = main_cols + rollout_cols

    with open(csv_path, "w", encoding="utf-8", newline="") as fcsv:
        writer = csv.DictWriter(fcsv, fieldnames=all_cols, extrasaction="ignore")
        writer.writeheader()
        for entry in entries:
            rec_b = entry.recorded_baseline_s
            acc = accumulator.get(entry.key, {})

            best_rollout_idx = None
            best_remeasured = None
            best_hints_str = ""
            best_plan_digest = ""
            for r_idx, rd in acc.items():
                elapsed = rd.get("remeasured_best_s")
                if elapsed is not None and (best_remeasured is None or elapsed < best_remeasured):
                    best_remeasured = elapsed
                    best_rollout_idx = r_idx
                    best_hints_str = rd.get("hints", "")
                    best_plan_digest = rd.get("plan_digest", "")

            recorded_best = None
            for rb in entry.rollout_bests:
                if rb.recorded_time_s is not None and (recorded_best is None or rb.recorded_time_s < recorded_best):
                    recorded_best = rb.recorded_time_s

            sp_rec = (rec_b / recorded_best) if rec_b and recorded_best and recorded_best > 0 else None
            sp_new = (rec_b / best_remeasured) if rec_b and best_remeasured and best_remeasured > 0 else None

            ext_peak = int(entry_external_exec.get(entry.key, 0) or 0)
            ext_note = (
                f"测量时检测到 {ext_peak} 条外部执行(计时可能受干扰)"
                if ext_peak > 0 else ""
            )

            row: Dict[str, Any] = {
                "key": entry.key,
                "instance_id": entry.instance_id or "",
                "benchmark_id": entry.benchmark_id or "",
                "db": entry.db or "",
                "recorded_baseline_s": f"{rec_b:.6f}" if rec_b is not None else "",
                "recorded_best_s": f"{recorded_best:.6f}" if recorded_best is not None else "",
                "remeasured_best_s": f"{best_remeasured:.6f}" if best_remeasured is not None else "",
                "speedup_recorded": f"{sp_rec:.4f}" if sp_rec else "",
                "speedup_remeasured": f"{sp_new:.4f}" if sp_new else "",
                "external_exec_peak": ext_peak,
                "external_exec_note": ext_note,
                "best_plan_digest": best_plan_digest,
                "best_hints": best_hints_str,
                "best_rollout_index": best_rollout_idx if best_rollout_idx is not None else "",
            }
            if validate_baseline:
                bl = baseline_results.get(entry.key)
                if bl is not None:
                    row["remeasured_baseline_s"] = f"{bl.elapsed_s:.6f}" if bl.elapsed_s is not None else ""
                    row["remeasured_baseline_error"] = bl.error or ""

            for r in range(max_rollout + 1):
                rcol = r + 1
                rd = acc.get(r, {})
                elapsed = rd.get("remeasured_best_s")
                sp = f"{rec_b / elapsed:.6f}" if rec_b and elapsed and elapsed > 0 else ""
                row[f"R{rcol}_plan_digest"] = rd.get("plan_digest", "")
                row[f"R{rcol}_hints"] = rd.get("hints", "")
                row[f"R{rcol}_hints_count"] = rd.get("hints_count", "")
                row[f"R{rcol}_remeasured_best_s"] = f"{elapsed:.6f}" if elapsed is not None else ""
                row[f"R{rcol}_speedup_remeasured"] = sp
            writer.writerow(row)


# ---------------------------------------------------------------------------
# 主入口
# ---------------------------------------------------------------------------

class InstanceRolloutValidator:
    """单实例的 rollout 验证状态机。

    把"一个实例所有 query 的 rollout 验证"拆成可单步推进的形式:
      * 构造时完成 baseline 预跑(可选)与状态初始化;
      * ``run_rollout(r)`` 跑第 r 轮(实例内单线程串行), 返回该轮的计数/累计和;
      * ``finalize(csv_path)`` 写宽表 CSV 并返回 ``ValidationStats``。

    这样既能被单实例的 ``validate_results`` 直接 for 循环驱动 (TPC-DS),
    也能被 benchmark 的"按实例并行 + 每轮 barrier"协调器驱动 —— 多个实例的同一轮
    全部跑完后再由协调器把各实例的累计和加总, 与 baseline 对比后输出, 再放行下一轮。
    """

    def __init__(
        self,
        entries: List[ValidationEntry],
        controller,
        *,
        timeout_seconds: float = 0.0,
        warmup_runs: int = 0,
        validate_baseline: bool = False,
        log: Callable[[str], None] = print,
    ) -> None:
        from mcts.utils.hint_utils import build_sql_with_hints
        self._build_sql_with_hints = build_sql_with_hints

        self.controller = controller
        self.timeout_seconds = timeout_seconds
        self.warmup_runs = warmup_runs
        self.validate_baseline = validate_baseline
        self.log = log

        self.entries = [e for e in entries if e.rollout_bests]
        self.stats = ValidationStats()
        self.max_rollout = (
            max(rb.rollout_index for e in self.entries for rb in e.rollout_bests)
            if self.entries else -1
        )

        self.plan_caches: Dict[str, Dict[str, RemeasureResult]] = {e.key: {} for e in self.entries}
        self.baseline_results: Dict[str, RemeasureResult] = {}
        self.running_best_s: Dict[str, float] = {
            e.key: float(e.recorded_baseline_s)
            for e in self.entries if e.recorded_baseline_s is not None
        }
        self.running_recorded_s: Dict[str, float] = {
            e.key: float(e.recorded_baseline_s)
            for e in self.entries if e.recorded_baseline_s is not None
        }
        self.total_baseline_s = sum(
            float(e.recorded_baseline_s) for e in self.entries if e.recorded_baseline_s is not None
        )
        self.accumulator: Dict[str, Dict[int, Dict[str, Any]]] = {e.key: {} for e in self.entries}
        self.entry_external_exec: Dict[str, int] = {e.key: 0 for e in self.entries}
        self._cur_db = {"name": None}
        self._t0 = time.time()

        if self.validate_baseline:
            self._run_baseline()

    # -- helpers ----------------------------------------------------------
    def _entry_timeout(self, e: ValidationEntry) -> float:
        if self.timeout_seconds > 0:
            return self.timeout_seconds
        if e.recorded_baseline_s and e.recorded_baseline_s > 0:
            return e.recorded_baseline_s * 1.1
        return 600.0

    def _ensure_db(self, e: ValidationEntry) -> None:
        # 不同 query 可能落在不同 db（benchmark 同实例跨库），重跑前切到对应库。
        if e.db and e.db != self._cur_db["name"]:
            try:
                self.controller.use_db(e.db)
                self._cur_db["name"] = e.db
            except Exception as exc:
                self.log(f"  [validate] use_db({e.db}) 失败: {exc}")

    def _run_baseline(self) -> None:
        for e in self.entries:
            if e.recorded_baseline_s is None:
                self.baseline_results[e.key] = RemeasureResult(elapsed_s=None, error="no recorded baseline")
                self.stats.baseline_errors += 1
                continue
            self._ensure_db(e)
            elapsed, err, is_to = _run_once(
                self.controller, e.query, self._entry_timeout(e), self.warmup_runs,
            )
            self.baseline_results[e.key] = RemeasureResult(elapsed_s=elapsed, error=err, is_timeout=is_to)
            if err is None and elapsed is not None:
                self.stats.baseline_executed += 1
            else:
                self.stats.baseline_errors += 1

    # -- per-rollout step -------------------------------------------------
    def run_rollout(self, r: int) -> Dict[str, Any]:
        """跑第 r 轮; 返回该轮计数与本实例累计和。r 超过本实例 max_rollout 时为空操作
        (累计和保持不变), 方便协调器用全局 max_rollout 统一驱动。"""
        files_this_r = [e for e in self.entries if any(rb.rollout_index == r for rb in e.rollout_bests)]
        measured = cached = skip_base = skip_worse = timeout = 0
        ext_peak_r = 0
        for e in files_this_r:
            rb = next((x for x in e.rollout_bests if x.rollout_index == r), None)
            if rb is None:
                continue
            self._ensure_db(e)
            res = _measure_rollout(
                entry=e, rb=rb,
                plan_cache=self.plan_caches[e.key],
                running_best_s=self.running_best_s.get(e.key),
                controller=self.controller,
                timeout_seconds=self._entry_timeout(e),
                warmup_runs=self.warmup_runs,
                build_sql_with_hints=self._build_sql_with_hints,
            )
            self.stats.rows += 1
            if res.skipped_as_baseline:
                skip_base += 1; self.stats.best_skipped_as_baseline += 1
            elif res.skipped_worse_than_running_best:
                skip_worse += 1; self.stats.best_skipped_worse_than_running_best += 1
            elif res.cached:
                cached += 1; self.stats.best_from_cache += 1
            else:
                measured += 1; self.stats.best_executed += 1
            if res.is_timeout:
                timeout += 1
            if res.error:
                self.stats.best_errors += 1

            # 外部并发执行: 累计本 query 峰值 + 全阶段峰值。
            if res.external_exec and not res.cached:
                if res.external_exec > self.entry_external_exec.get(e.key, 0):
                    self.entry_external_exec[e.key] = res.external_exec
                if res.external_exec > self.stats.external_exec_peak:
                    self.stats.external_exec_peak = res.external_exec
                if res.external_exec > ext_peak_r:
                    ext_peak_r = res.external_exec

            if res.elapsed_s is not None:
                prev = self.running_best_s.get(e.key)
                self.running_best_s[e.key] = res.elapsed_s if prev is None else min(prev, res.elapsed_s)

            # 直接读取(MCTS 记录)的累计最优, 仅看记录值, 不真跑。
            if rb.recorded_time_s is not None:
                prev_rec = self.running_recorded_s.get(e.key)
                self.running_recorded_s[e.key] = (
                    rb.recorded_time_s if prev_rec is None
                    else min(prev_rec, rb.recorded_time_s)
                )

            cum_best = self.running_best_s.get(e.key, res.elapsed_s)
            self.accumulator[e.key][r] = {
                "plan_digest": rb.plan_digest or "",
                "hints": "; ".join(rb.hints) if rb.hints else "",
                "hints_count": len(rb.hints),
                "remeasured_best_s": cum_best,
            }

        return {
            "measured": measured,
            "cached": cached,
            "skip_base": skip_base,
            "skip_worse": skip_worse,
            "timeout": timeout,
            "ext_peak_r": ext_peak_r,
            "recorded_sum_cum": sum(self.running_recorded_s.values()),
            "best_sum_cum": sum(self.running_best_s.values()),
            "baseline_sum": self.total_baseline_s,
        }

    # -- finalize ---------------------------------------------------------
    def finalize(self, csv_path: str) -> ValidationStats:
        csv_out = Path(csv_path)
        csv_out.parent.mkdir(parents=True, exist_ok=True)
        _write_wide_csv(
            csv_out, self.entries, self.accumulator, self.baseline_results,
            max(self.max_rollout, 0), self.validate_baseline, self.entry_external_exec,
        )
        self.stats.entries = len(self.entries)
        self.stats.elapsed_s = time.time() - self._t0
        self.stats.csv_path = str(csv_out)
        self.stats.total_baseline_s = self.total_baseline_s
        self.stats.total_best_s = sum(self.running_best_s.values())
        return self.stats


def validate_results(
    entries: List[ValidationEntry],
    controller,
    csv_path: str,
    *,
    timeout_seconds: float = 0.0,
    warmup_runs: int = 0,
    validate_baseline: bool = False,
    log: Callable[[str], None] = print,
) -> ValidationStats:
    """单线程验证 entries, 写宽表 CSV, 返回统计。

    timeout_seconds: 0 = 自动(每条 baseline×1.1); >0 = 固定上限。
    """
    entries = [e for e in entries if e.rollout_bests]
    if not entries:
        log("  [validate] 没有可验证样本, 跳过验证阶段。")
        stats = ValidationStats()
        stats.csv_path = csv_path
        # 仍写一个空 CSV 表头, 方便下游统一处理
        Path(csv_path).parent.mkdir(parents=True, exist_ok=True)
        _write_wide_csv(Path(csv_path), [], {}, {}, 0, validate_baseline)
        return stats

    v = InstanceRolloutValidator(
        entries, controller,
        timeout_seconds=timeout_seconds, warmup_runs=warmup_runs,
        validate_baseline=validate_baseline, log=log,
    )

    log("=" * 72)
    log(f"  [validate] 单线程验证阶段: {len(v.entries)} queries, "
        f"max_rollout={v.max_rollout}, warmup={warmup_runs}, "
        f"timeout={'auto(base×1.1)' if timeout_seconds == 0 else f'{timeout_seconds}s'}, "
        f"validate_baseline={validate_baseline}")
    log("=" * 72)

    for r in range(v.max_rollout + 1):
        s = v.run_rollout(r)
        ext_txt = f"  ⚠ 外部并发峰值={s['ext_peak_r']}" if s["ext_peak_r"] > 0 else ""
        log(f"  [validate][R{r + 1}] measured={s['measured']} cached={s['cached']} "
            f"skip_base={s['skip_base']} skip_worse={s['skip_worse']} timeout={s['timeout']}  "
            f"原结果Sum {s['recorded_sum_cum']:.3f}s -> 单线程Sum {s['best_sum_cum']:.3f}s{ext_txt}")

    stats = v.finalize(csv_path)

    log("-" * 72)
    log(f"  [validate] 完成 耗时 {stats.elapsed_s:.1f}s | "
        f"实测 {stats.best_executed} 缓存 {stats.best_from_cache} "
        f"跳过(≥base) {stats.best_skipped_as_baseline} "
        f"跳过(worse) {stats.best_skipped_worse_than_running_best} "
        f"错误 {stats.best_errors}")

    # 外部并发执行峰值提示: 理想为 0; >0 说明验证期间该实例上有外部查询在跑,
    # 计时可能受干扰。
    affected = sum(1 for x in v.entry_external_exec.values() if x > 0)
    if stats.external_exec_peak > 0:
        log(f"  [validate] ⚠ 外部并发执行峰值={stats.external_exec_peak} "
            f"(有 {affected} 条 query 测量时检测到外部执行, 计时可能受干扰)")
    else:
        log("  [validate] 外部并发执行峰值=0 (测量期间无外部查询干扰)")

    ov = stats.overall_speedup
    summary_line = (f"  [validate] BaselineSum={stats.total_baseline_s:.2f}s "
                    f"BestSum={stats.total_best_s:.2f}s")
    if ov:
        summary_line += f" Overall≈{ov:.3f}x"
    log(summary_line)
    log(f"  [validate] CSV: {stats.csv_path}")
    return stats
