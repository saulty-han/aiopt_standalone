"""opt_result_analysis.py — 优化阶段结果分析 (被 tpcds_runner / benchmark_runner 复用)。

优化阶段跑完后, 对每条 query 的 mcts_results 提取关键指标并写 CSV:
  * 最优时间 (best_time_s)      —— 该 query 所有 solution 里最小的 execution_time_s
  * unique 计划数 (unique_plans) —— plan_digest_cache 的大小 (去重后的执行计划数)
  * explain 次数 (db_explain_count)
  * 大模型调用次数 (llm_call_count)
  * 大模型输入字符数 (llm_input_chars)
  * 大模型输出字符数 (llm_output_chars)
  * e2e 时间 (mcts_e2e_seconds)

并对所有 query 统计平均值: 其中"最优时间"按**累计最优**口径求和后再平均
(即把每条 query 的 best_time 加起来 / 条数; 没有 best 的按 baseline 兜底)。
"""

import csv
from pathlib import Path
from typing import Any, Dict, List, Optional


CSV_COLUMNS = [
    "key",
    "instance_id",
    "db",
    "baseline_time_s",
    "best_time_s",
    "speedup",
    "unique_plans",
    "db_explain_count",
    "llm_call_count",
    "llm_input_chars",
    "llm_output_chars",
    "mcts_e2e_seconds",
]


def _to_float(value: Any) -> Optional[float]:
    try:
        f = float(value)
    except (TypeError, ValueError):
        return None
    return f


def extract_query_metrics(
    mcts_results: Optional[List[Dict[str, Any]]],
    *,
    key: str,
    instance_id: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """把单条 query 的 mcts_results (可能含多个 sample run) 提取为指标行列表。"""
    rows: List[Dict[str, Any]] = []
    if not mcts_results:
        return rows

    multi = len(mcts_results) > 1
    for i, mr in enumerate(mcts_results):
        if not isinstance(mr, dict):
            continue
        metrics = mr.get("performance_metrics", {}) or {}
        baseline = _to_float(mr.get("baseline_time"))

        # 最优时间: 所有 solution 里最小的 execution_time_s。
        best_time: Optional[float] = None
        for sol in mr.get("solutions", []) or []:
            et = _to_float(sol.get("execution_time_s"))
            if et is None:
                continue
            if best_time is None or et < best_time:
                best_time = et

        # unique 计划数: plan_digest_cache 去重后的执行计划数; 兜底用 solutions 里
        # 出现过的 plan_digest 去重。
        cache = mr.get("plan_digest_cache")
        if isinstance(cache, dict) and cache:
            unique_plans = len(cache)
        else:
            digests = {
                sol.get("plan_digest")
                for sol in (mr.get("solutions", []) or [])
                if sol.get("plan_digest")
            }
            unique_plans = len(digests)

        speedup = (
            baseline / best_time
            if baseline and best_time and best_time > 0
            else None
        )

        rows.append({
            "key": key if not multi else f"{key}#{i}",
            "instance_id": instance_id or mr.get("instance_id") or "",
            "db": mr.get("db_name") or "",
            "baseline_time_s": baseline,
            "best_time_s": best_time,
            "speedup": speedup,
            "unique_plans": unique_plans,
            "db_explain_count": int(metrics.get("db_explain_count", 0) or 0),
            "llm_call_count": int(metrics.get("llm_call_count", 0) or 0),
            "llm_input_chars": int(metrics.get("llm_input_chars", 0) or 0),
            "llm_output_chars": int(metrics.get("llm_output_chars", 0) or 0),
            "mcts_e2e_seconds": _to_float(metrics.get("mcts_e2e_seconds")) or 0.0,
        })
    return rows


def _fmt(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float):
        return f"{value:.6f}"
    return str(value)


def write_optimization_csv(
    rows: List[Dict[str, Any]],
    csv_path: str,
    *,
    log=print,
) -> Dict[str, Any]:
    """写每条 query 的指标 CSV + 末尾平均值行; 返回平均值汇总 dict。

    平均值口径:
      * best_time_s: 各 query 的累计最优值之和 / 条数 (无 best 用 baseline 兜底);
      * 其余指标: 算术平均。
    """
    csv_out = Path(csv_path)
    csv_out.parent.mkdir(parents=True, exist_ok=True)

    n = len(rows)
    # 累计最优: 每条取 best_time, 没有则用 baseline 兜底 (都没有则跳过计入)。
    best_sum = 0.0
    best_counted = 0
    baseline_sum = 0.0
    baseline_counted = 0
    sums = {
        "unique_plans": 0,
        "db_explain_count": 0,
        "llm_call_count": 0,
        "llm_input_chars": 0,
        "llm_output_chars": 0,
        "mcts_e2e_seconds": 0.0,
    }
    for r in rows:
        bt = r.get("best_time_s")
        if bt is None:
            bt = r.get("baseline_time_s")
        if bt is not None:
            best_sum += float(bt)
            best_counted += 1
        if r.get("baseline_time_s") is not None:
            baseline_sum += float(r["baseline_time_s"])
            baseline_counted += 1
        for k in sums:
            sums[k] += r.get(k, 0) or 0

    avg: Dict[str, Any] = {
        "best_time_s": (best_sum / best_counted) if best_counted else None,
        "baseline_time_s": (baseline_sum / baseline_counted) if baseline_counted else None,
        "best_sum_s": best_sum,
        "baseline_sum_s": baseline_sum,
        "count": n,
    }
    for k, v in sums.items():
        avg[k] = (v / n) if n else 0
    avg["overall_speedup"] = (
        baseline_sum / best_sum if best_sum > 0 else None
    )

    with open(csv_out, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS, extrasaction="ignore")
        writer.writeheader()
        for r in rows:
            writer.writerow({c: _fmt(r.get(c)) for c in CSV_COLUMNS})
        # 平均值行 (key=__AVERAGE__)
        avg_row = {c: "" for c in CSV_COLUMNS}
        avg_row["key"] = "__AVERAGE__"
        avg_row["baseline_time_s"] = _fmt(avg["baseline_time_s"])
        avg_row["best_time_s"] = _fmt(avg["best_time_s"])
        avg_row["speedup"] = _fmt(avg["overall_speedup"])
        avg_row["unique_plans"] = f"{avg['unique_plans']:.2f}"
        avg_row["db_explain_count"] = f"{avg['db_explain_count']:.2f}"
        avg_row["llm_call_count"] = f"{avg['llm_call_count']:.2f}"
        avg_row["llm_input_chars"] = f"{avg['llm_input_chars']:.1f}"
        avg_row["llm_output_chars"] = f"{avg['llm_output_chars']:.1f}"
        avg_row["mcts_e2e_seconds"] = f"{avg['mcts_e2e_seconds']:.3f}"
        writer.writerow(avg_row)

    # 控制台汇总
    log("-" * 72)
    log(f"  [analyze] 优化结果分析: {n} 条 query -> {csv_out}")
    if n:
        bt = avg["best_time_s"]
        bl = avg["baseline_time_s"]
        ov = avg["overall_speedup"]
        log(f"  [analyze] 平均: best_time={bt:.4f}s baseline={bl:.4f}s "
            f"(累计最优Sum={avg['best_sum_s']:.2f}s / BaselineSum={avg['baseline_sum_s']:.2f}s"
            f"{f', Overall≈{ov:.3f}x' if ov else ''})"
            if bt is not None and bl is not None else
            f"  [analyze] 平均: best_time={bt} baseline={bl}")
        log(f"  [analyze] 平均: unique_plans={avg['unique_plans']:.2f} "
            f"explain={avg['db_explain_count']:.2f} llm_calls={avg['llm_call_count']:.2f} "
            f"llm_in_chars={avg['llm_input_chars']:.0f} llm_out_chars={avg['llm_output_chars']:.0f} "
            f"e2e={avg['mcts_e2e_seconds']:.2f}s")
    return avg
