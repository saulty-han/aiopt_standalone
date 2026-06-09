#!/usr/bin/env python3
"""
MCTS 结果分析脚本

读取指定文件夹下所有 MCTS JSON 结果文件，分析每个查询的优化效果，汇总成 CSV 表格。

Usage:
    python scripts/analyze_mcts_results.py /path/to/mcts/output
    python scripts/analyze_mcts_results.py /path/to/mcts/output -o report.csv
    python scripts/analyze_mcts_results.py /path/to/mcts/output --sort speedup
"""
import argparse
import csv
import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional


def load_mcts_jsons(input_dir: str) -> List[Dict[str, Any]]:
    """加载目录下所有 MCTS JSON 文件，按修改时间排序。

    每个 JSON 文件是一个 list（可能含多条查询），展平后返回。
    """
    json_files = sorted(
        [f for f in Path(input_dir).glob("*.json")],
        key=lambda f: f.stat().st_mtime,
    )

    if not json_files:
        print(f"错误: {input_dir} 下没有找到 JSON 文件")
        sys.exit(1)

    all_entries = []
    for jf in json_files:
        try:
            with open(jf, 'r', encoding='utf-8') as f:
                data = json.load(f)
            # 文件内容是 list（一个或多个查询结果）
            if isinstance(data, list):
                for item in data:
                    item["_source_file"] = jf.name
                    all_entries.append(item)
            elif isinstance(data, dict):
                data["_source_file"] = jf.name
                all_entries.append(data)
        except Exception as e:
            print(f"  ⚠ 跳过 {jf.name}: {e}")

    return all_entries


def analyze_entry(entry: Dict[str, Any]) -> Dict[str, Any]:
    """分析单条查询结果。"""
    query = entry.get("query", "")
    baseline_time = entry.get("baseline_time")
    if baseline_time is None:
        # 尝试从根节点获取
        tree_nodes = entry.get("mcts_tree_nodes", {})
        root = tree_nodes.get("0", {})
        baseline_time = root.get("db_response", {}).get("execution_time_s")

    solutions = entry.get("solutions", [])
    metrics = entry.get("performance_metrics", {})
    pdc = entry.get("plan_digest_cache", {})
    esm = entry.get("early_stopping_metrics", {})

    # 最优 solution
    best_time = None
    best_reward = None
    best_hints = None
    if solutions:
        best = solutions[0]  # 已按 reward 降序排列
        best_time = best.get("execution_time_s")
        best_reward = best.get("reward")
        best_hints = best.get("executed_hints", [])

    # 加速比
    speedup = None
    if baseline_time and best_time and best_time > 0:
        speedup = baseline_time / best_time

    # 最优时间 = min(baseline, best)
    optimal_time = None
    if baseline_time is not None and best_time is not None:
        optimal_time = min(baseline_time, best_time)
    elif baseline_time is not None:
        optimal_time = baseline_time
    elif best_time is not None:
        optimal_time = best_time

    # 新计划数量 = plan_digest_cache 中非 baseline 的条目数
    # baseline 条目在 early_stopping_metrics 中 first_rollout == -1
    new_plan_count = 0
    if esm:
        new_plan_count = sum(1 for v in esm.values() if v.get("first_rollout", -1) >= 0)
    elif pdc:
        # fallback: plan_digest_cache 条目数 - 1 (减去 baseline)
        new_plan_count = max(0, len(pdc) - 1)

    # LLM 调用次数
    llm_calls = metrics.get("llm_call_count", 0)
    db_executes = metrics.get("db_execute_count", 0)
    e2e_seconds = metrics.get("mcts_e2e_seconds", 0)
    early_stop = metrics.get("mcts_early_stop_reason")

    return {
        "query_prefix": query[:80],
        "baseline_time_s": baseline_time,
        "best_time_s": best_time,
        "optimal_time_s": optimal_time,
        "speedup": speedup,
        "best_reward": best_reward,
        "solutions_count": len(solutions),
        "new_plan_count": new_plan_count,
        "llm_calls": llm_calls,
        "db_executes": db_executes,
        "e2e_seconds": e2e_seconds,
        "early_stop": early_stop or "",
        "best_hints": " ".join(best_hints) if best_hints else "",
        "source_file": entry.get("_source_file", ""),
    }


def fmt(v, decimals=4):
    if v is None:
        return ""
    if isinstance(v, float):
        return f"{v:.{decimals}f}"
    return str(v)


def main():
    parser = argparse.ArgumentParser(description="MCTS 结果分析")
    parser.add_argument("input_dir", help="MCTS JSON 文件目录")
    parser.add_argument("-o", "--output", default=None, help="输出 CSV 文件路径 (默认: 打印到终端)")
    parser.add_argument("--sort", default="id", choices=["id", "speedup", "baseline", "best", "llm"],
                        help="排序方式 (默认: id)")
    args = parser.parse_args()

    entries = load_mcts_jsons(args.input_dir)
    print(f"加载 {len(entries)} 条查询结果\n")

    rows = []
    for i, entry in enumerate(entries, 1):
        result = analyze_entry(entry)
        result["id"] = i
        rows.append(result)

    # 排序
    sort_keys = {
        "id": lambda r: r["id"],
        "speedup": lambda r: -(r["speedup"] or 0),
        "baseline": lambda r: -(r["baseline_time_s"] or 0),
        "best": lambda r: (r["best_time_s"] or float("inf")),
        "llm": lambda r: -r["llm_calls"],
    }
    rows.sort(key=sort_keys[args.sort])

    # CSV 列
    columns = [
        ("id", "No."),
        ("baseline_time_s", "Baseline(s)"),
        ("best_time_s", "Best(s)"),
        ("optimal_time_s", "Optimal(s)"),
        ("speedup", "Speedup"),
        ("best_reward", "Reward"),
        ("solutions_count", "Solutions"),
        ("new_plan_count", "NewPlans"),
        ("llm_calls", "LLM Calls"),
        ("db_executes", "DB Execs"),
        ("e2e_seconds", "E2E(s)"),
        ("early_stop", "EarlyStop"),
        ("best_hints", "BestHints"),
        ("query_prefix", "Query"),
    ]

    # 输出
    if args.output:
        with open(args.output, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow([col[1] for col in columns])
            for row in rows:
                writer.writerow([fmt(row.get(col[0]), 4) for col in columns])
        print(f"CSV 已保存: {args.output}")
    else:
        # 终端表格输出
        headers = [col[1] for col in columns[:12]]  # 不打印 hints 和 query 的完整内容
        col_widths = [max(len(h), 10) for h in headers]

        # 计算实际宽度
        formatted_rows = []
        for row in rows:
            vals = [
                str(row["id"]),
                fmt(row["baseline_time_s"]),
                fmt(row["best_time_s"]),
                fmt(row["optimal_time_s"]),
                f"{row['speedup']:.2f}x" if row["speedup"] else "",
                fmt(row["best_reward"]),
                str(row["solutions_count"]),
                str(row["new_plan_count"]),
                str(row["llm_calls"]),
                str(row["db_executes"]),
                fmt(row["e2e_seconds"], 1),
                row["early_stop"][:15],
            ]
            formatted_rows.append(vals)
            for j, v in enumerate(vals):
                col_widths[j] = max(col_widths[j], len(v))

        # 打印表头
        header_line = " | ".join(h.rjust(w) for h, w in zip(headers, col_widths))
        print(header_line)
        print("-" * len(header_line))

        # 打印数据行
        for vals in formatted_rows:
            print(" | ".join(v.rjust(w) for v, w in zip(vals, col_widths)))

        # 汇总统计
        print("-" * len(header_line))
        total = len(rows)
        improved = sum(1 for r in rows if r["speedup"] and r["speedup"] > 1.01)
        total_llm = sum(r["llm_calls"] for r in rows)
        total_plans = sum(r["new_plan_count"] for r in rows)
        total_solutions = sum(r["solutions_count"] for r in rows)
        total_e2e = sum(r["e2e_seconds"] for r in rows)
        speedups = [r["speedup"] for r in rows if r["speedup"] and r["speedup"] > 1.0]
        avg_speedup = sum(speedups) / len(speedups) if speedups else 0

        print(f"\n汇总:")
        print(f"  查询总数:     {total}")
        print(f"  优化成功:     {improved} ({improved/total*100:.0f}%)" if total else "")
        print(f"  平均加速比:   {avg_speedup:.2f}x (仅统计加速>1x的查询)")
        print(f"  总 Solutions: {total_solutions}")
        print(f"  总新计划数:   {total_plans}")
        print(f"  总 LLM 调用:  {total_llm}")
        print(f"  总耗时:       {total_e2e:.1f}s")


if __name__ == "__main__":
    main()
