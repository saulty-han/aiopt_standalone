#!/usr/bin/env python3
"""
TPC-DS MCTS 结果分析脚本

读取 MCTS JSON 结果目录，根据文件名中的 q{N} 标识映射到 TPC-DS 99 条查询。
缺失的查询留空显示，便于全局对比。

Usage:
    python mcts_scripts/tpcds_runner/analyze_tpcds_results.py /path/to/mcts/output
    python mcts_scripts/tpcds_runner/analyze_tpcds_results.py /path/to/mcts/output -o report.csv
    python mcts_scripts/tpcds_runner/analyze_tpcds_results.py /path/to/mcts/output --sort speedup
    python mcts_scripts/tpcds_runner/analyze_tpcds_results.py /path/to/mcts/output --detailed-output detailed.csv
"""
import argparse
import csv
import json
import re
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

TPCDS_TOTAL = 99


def extract_query_number(filename: str) -> Optional[int]:
    """从文件名中提取 TPC-DS 查询编号。

    文件名格式: {db}_q{N}_{hash}_{timestamp}.json
    例如: tpcds_q42_abcd_20260330120000.json -> 42
    """
    m = re.search(r'_q(\d+)_', filename)
    if m:
        num = int(m.group(1))
        if 1 <= num <= TPCDS_TOTAL:
            return num
    return None


def load_mcts_jsons(input_dir: str) -> Dict[int, Dict[str, Any]]:
    """加载目录下所有 MCTS JSON 文件，按 q{N} 映射到查询编号。

    同一编号有多个文件时取最新的（按修改时间）。
    返回 {query_number: entry}。
    """
    json_files = sorted(
        [f for f in Path(input_dir).glob("*.json")],
        key=lambda f: f.stat().st_mtime,
    )

    if not json_files:
        print(f"错误: {input_dir} 下没有找到 JSON 文件")
        sys.exit(1)

    result: Dict[int, Tuple[float, Dict[str, Any]]] = {}

    for jf in json_files:
        qnum = extract_query_number(jf.name)
        if qnum is None:
            print(f"  ⚠ 跳过 {jf.name}: 无法识别查询编号")
            continue

        try:
            with open(jf, 'r', encoding='utf-8') as f:
                data = json.load(f)

            # 文件内容可能是 list 或 dict
            if isinstance(data, list):
                entries = data
            elif isinstance(data, dict):
                entries = [data]
            else:
                continue

            for entry in entries:
                entry["_source_file"] = jf.name
                mtime = jf.stat().st_mtime
                # 同一查询编号取最新文件
                if qnum not in result or mtime > result[qnum][0]:
                    result[qnum] = (mtime, entry)

        except Exception as e:
            print(f"  ⚠ 跳过 {jf.name}: {e}")

    return {qnum: entry for qnum, (_, entry) in result.items()}


def analyze_entry(entry: Dict[str, Any]) -> Dict[str, Any]:
    """分析单条查询结果。"""
    query = entry.get("query", "")
    baseline_time = entry.get("baseline_time")
    if baseline_time is None:
        tree_nodes = entry.get("mcts_tree_nodes", {})
        root = tree_nodes.get("0", {})
        baseline_time = root.get("db_response", {}).get("execution_time_s")

    solutions = entry.get("solutions", [])
    metrics = entry.get("performance_metrics", {})
    pdc = entry.get("plan_digest_cache", {})
    esm = entry.get("early_stopping_metrics", {})

    best_time = None
    best_reward = None
    best_hints = None
    if solutions:
        best = solutions[0]
        best_time = best.get("execution_time_s")
        best_reward = best.get("reward")
        best_hints = best.get("executed_hints", [])

    speedup = None
    if baseline_time and best_time and best_time > 0:
        speedup = baseline_time / best_time

    min_time = None
    if baseline_time is not None and best_time is not None:
        min_time = min(baseline_time, best_time)
    elif baseline_time is not None:
        min_time = baseline_time
    elif best_time is not None:
        min_time = best_time

    new_plan_count = 0
    if esm:
        new_plan_count = sum(1 for v in esm.values() if v.get("first_rollout", -1) >= 0)
    elif pdc:
        new_plan_count = max(0, len(pdc) - 1)

    llm_calls = metrics.get("llm_call_count", 0)
    db_executes = metrics.get("db_execute_count", 0)
    e2e_seconds = metrics.get("mcts_e2e_seconds", 0)
    early_stop = metrics.get("mcts_early_stop_reason")
    llm_input_chars = metrics.get("llm_input_chars", 0)
    llm_output_chars = metrics.get("llm_output_chars", 0)
    total_chars = llm_input_chars + llm_output_chars
    est_tokens = total_chars / 2.5 if total_chars > 0 else 0
    est_price = est_tokens / 1_000_000 * 2  # 2元/100万tokens

    return {
        "query_prefix": query[:80],
        "baseline_time_s": baseline_time,
        "best_time_s": best_time,
        "min_time_s": min_time,
        "speedup": speedup,
        "best_reward": best_reward,
        "solutions_count": len(solutions),
        "new_plan_count": new_plan_count,
        "llm_calls": llm_calls,
        "db_executes": db_executes,
        "e2e_seconds": e2e_seconds,
        "llm_input_chars": llm_input_chars,
        "llm_output_chars": llm_output_chars,
        "total_chars": total_chars,
        "est_tokens": est_tokens,
        "est_price": est_price,
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


def compute_rollout_stats(entry: Dict[str, Any]) -> Dict[int, Dict[str, Any]]:
    """按 rollout 维度统计累计最优计划时间和 tokens。

    返回 {rollout_index: {...}}，每个值表示"累计到该 rollout（含）时的状态"：
    - best_time_s:          最优执行时间（solutions 中）
    - cumulative_tokens:    所有树节点产生的 tokens 之和
    - cumulative_nodes:     树节点数量
    - cumulative_plans:     唯一 plan_digest 数量
    - cumulative_llm_calls: LLM 调用次数（input_length > 0 的节点）
    """
    nodes = entry.get("mcts_tree_nodes", {})
    solutions = entry.get("solutions", [])

    # 收集所有出现的 rollout_index
    all_rollouts = set()
    for v in nodes.values():
        ri = v.get("node_info", {}).get("rollout_index")
        if ri is not None:
            all_rollouts.add(ri)
    for sol in solutions:
        ri = sol.get("rollout_index")
        if ri is not None:
            all_rollouts.add(ri)

    if not all_rollouts:
        return {}

    max_rollout = max(all_rollouts)

    # 按 rollout 分组统计节点级数据
    node_tokens_by_rollout: Dict[int, float] = {}
    node_count_by_rollout: Dict[int, int] = {}
    llm_calls_by_rollout: Dict[int, int] = {}
    plans_by_rollout: Dict[int, set] = {}

    for v in nodes.values():
        ri = v.get("node_info", {}).get("rollout_index")
        if ri is None:
            continue
        llm = v.get("llm_response", {})
        input_len = llm.get("input_length", 0)
        output_len = llm.get("output_length", 0)
        node_tokens_by_rollout[ri] = node_tokens_by_rollout.get(ri, 0) + input_len + output_len
        node_count_by_rollout[ri] = node_count_by_rollout.get(ri, 0) + 1
        if input_len > 0:
            llm_calls_by_rollout[ri] = llm_calls_by_rollout.get(ri, 0) + 1
        plan = v.get("db_response", {}).get("plan_digest")
        if plan:
            plans_by_rollout.setdefault(ri, set()).add(plan)

    # 每个 solution 的执行时间和 rollout_index
    sol_times_by_rollout: Dict[int, List[float]] = {}
    for sol in solutions:
        ri = sol.get("rollout_index")
        if ri is None:
            continue
        t = sol.get("execution_time_s")
        if t is not None:
            sol_times_by_rollout.setdefault(ri, []).append(t)

    # 按 rollout 累计
    result = {}
    cumulative_tokens = 0.0
    cumulative_nodes = 0
    cumulative_plans: set = set()
    cumulative_llm_calls = 0
    running_best_time = None

    for r in range(max_rollout + 1):
        delta_nodes = node_count_by_rollout.get(r, 0)
        delta_plans = len(plans_by_rollout.get(r, set()) - cumulative_plans)
        cumulative_tokens += node_tokens_by_rollout.get(r, 0)
        cumulative_nodes += delta_nodes
        cumulative_llm_calls += llm_calls_by_rollout.get(r, 0)
        cumulative_plans |= plans_by_rollout.get(r, set())

        times_this_rollout = sol_times_by_rollout.get(r, [])
        if times_this_rollout:
            rollout_best = min(times_this_rollout)
            if running_best_time is None:
                running_best_time = rollout_best
            else:
                running_best_time = min(running_best_time, rollout_best)
        result[r] = {
            "best_time_s": running_best_time,
            "cumulative_tokens": cumulative_tokens,
            "cumulative_nodes": cumulative_nodes,
            "cumulative_plans": len(cumulative_plans),
            "cumulative_llm_calls": cumulative_llm_calls,
            "delta_nodes": delta_nodes,
            "delta_plans": delta_plans,
        }

    return result


def main():
    parser = argparse.ArgumentParser(description="TPC-DS MCTS 结果分析")
    parser.add_argument("input_dir", help="MCTS JSON 文件目录")
    parser.add_argument("-o", "--output", default=None, help="输出 CSV 文件路径 (默认: 打印到终端)")
    parser.add_argument("--detailed-output", default=None, help="输出 detailed result CSV 路径（按 rollout 维度）")
    parser.add_argument("--sort", default="id", choices=["id", "speedup", "baseline", "best", "llm"],
                        help="排序方式 (默认: id)")
    args = parser.parse_args()

    entries_map = load_mcts_jsons(args.input_dir)
    print(f"加载 {len(entries_map)}/{TPCDS_TOTAL} 条查询结果\n")

    # 构建 99 行，缺失查询留空
    rows = []
    for qnum in range(1, TPCDS_TOTAL + 1):
        if qnum in entries_map:
            result = analyze_entry(entries_map[qnum])
            result["id"] = qnum
        else:
            result = {
                "id": qnum,
                "query_prefix": "",
                "baseline_time_s": None,
                "best_time_s": None,
                "min_time_s": None,
                "speedup": None,
                "best_reward": None,
                "solutions_count": 0,
                "new_plan_count": 0,
                "llm_calls": 0,
                "db_executes": 0,
                "e2e_seconds": 0,
                "llm_input_chars": 0,
                "llm_output_chars": 0,
                "total_chars": 0,
                "est_tokens": 0,
                "est_price": 0,
                "early_stop": "",
                "best_hints": "",
                "source_file": "",
            }
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

    columns = [
        ("id", "Q#"),
        ("baseline_time_s", "Baseline(s)"),
        ("best_time_s", "Best(s)"),
        ("min_time_s", "Min(s)"),
        ("speedup", "Speedup"),
        ("best_reward", "Reward"),
        ("solutions_count", "Solutions"),
        ("new_plan_count", "NewPlans"),
        ("llm_calls", "LLM Calls"),
        ("db_executes", "DB Execs"),
        ("e2e_seconds", "E2E(s)"),
        ("total_chars", "Chars"),
        ("est_tokens", "Tokens"),
        ("est_price", "Price(¥)"),
        ("early_stop", "EarlyStop"),
        ("best_hints", "BestHints"),
        ("query_prefix", "Query"),
    ]

    if args.output:
        with open(args.output, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow([col[1] for col in columns])
            for row in rows:
                writer.writerow([fmt(row.get(col[0]), 4) for col in columns])
        print(f"CSV 已保存: {args.output}")
    else:
        # 终端表格输出（不打印 hints 和 query 完整内容）
        headers = [col[1] for col in columns[:15]]
        col_widths = [max(len(h), 10) for h in headers]

        formatted_rows = []
        for row in rows:
            has_data = row["baseline_time_s"] is not None
            vals = [
                f"q{row['id']}",
                fmt(row["baseline_time_s"]) if has_data else "-",
                fmt(row["best_time_s"]) if has_data else "-",
                fmt(row["min_time_s"]) if has_data else "-",
                f"{row['speedup']:.2f}x" if row["speedup"] else ("-" if not has_data else ""),
                fmt(row["best_reward"]) if has_data else "-",
                str(row["solutions_count"]) if has_data else "-",
                str(row["new_plan_count"]) if has_data else "-",
                str(row["llm_calls"]) if has_data else "-",
                str(row["db_executes"]) if has_data else "-",
                fmt(row["e2e_seconds"], 1) if has_data else "-",
                str(row["total_chars"]) if has_data else "-",
                str(int(row["est_tokens"])) if has_data else "-",
                fmt(row["est_price"], 4) if has_data else "-",
                row["early_stop"][:15] if has_data else "-",
            ]
            formatted_rows.append(vals)
            for j, v in enumerate(vals):
                col_widths[j] = max(col_widths[j], len(v))

        header_line = " | ".join(h.rjust(w) for h, w in zip(headers, col_widths))
        print(header_line)
        print("-" * len(header_line))

        for vals in formatted_rows:
            print(" | ".join(v.rjust(w) for v, w in zip(vals, col_widths)))

        # 汇总统计（只统计有数据的行）
        print("-" * len(header_line))
        present = [r for r in rows if r["baseline_time_s"] is not None]
        total = len(present)
        missing = TPCDS_TOTAL - total
        improved = sum(1 for r in present if r["speedup"] and r["speedup"] > 1.01)
        total_llm = sum(r["llm_calls"] for r in present)
        total_plans = sum(r["new_plan_count"] for r in present)
        total_solutions = sum(r["solutions_count"] for r in present)
        total_e2e = sum(r["e2e_seconds"] for r in present)
        speedups = [r["speedup"] for r in present if r["speedup"] and r["speedup"] > 1.0]
        avg_speedup = sum(speedups) / len(speedups) if speedups else 0

        # min_time 汇总
        min_times = [r["min_time_s"] for r in present if r["min_time_s"] is not None]
        baseline_times = [r["baseline_time_s"] for r in present if r["baseline_time_s"] is not None]
        sum_min = sum(min_times) if min_times else 0
        sum_baseline = sum(baseline_times) if baseline_times else 0

        print(f"\n汇总 (TPC-DS {TPCDS_TOTAL} 条):")
        print(f"  已完成:       {total} 条")
        print(f"  缺失:         {missing} 条")
        print(f"  优化成功:     {improved} ({improved/total*100:.0f}%)" if total else "")
        print(f"  平均加速比:   {avg_speedup:.2f}x (仅统计加速>1x的查询)")
        print(f"  Baseline总和: {sum_baseline:.2f}s")
        print(f"  Min总和:      {sum_min:.2f}s")
        if sum_baseline > 0:
            print(f"  整体加速比:   {sum_baseline/sum_min:.2f}x (Baseline总和/Min总和)")
        print(f"  总 Solutions: {total_solutions}")
        print(f"  总新计划数:   {total_plans}")
        print(f"  总 LLM 调用:  {total_llm}")
        print(f"  总耗时:       {total_e2e:.1f}s")

        # 字符 / Tokens / 价格统计
        total_input_chars = sum(r["llm_input_chars"] for r in present)
        total_output_chars = sum(r["llm_output_chars"] for r in present)
        total_all_chars = total_input_chars + total_output_chars
        total_tokens = total_all_chars / 2.5
        total_price = total_tokens / 1_000_000 * 2

        print(f"\n  LLM 消耗统计:")
        print(f"  输入字符:     {total_input_chars:,}")
        print(f"  输出字符:     {total_output_chars:,}")
        print(f"  总字符:       {total_all_chars:,}")
        print(f"  估算 Tokens:  {total_tokens:,.0f} (字符/2.5)")
        print(f"  估算价格:     ¥{total_price:.4f} (2元/100万tokens)")

        # ---- Detailed result 终端汇总 ----
        _print_detailed_summary(entries_map)

    # ---- Detailed result CSV 输出 ----
    if args.detailed_output:
        _write_detailed_csv(entries_map, args.detailed_output)


def _print_detailed_summary(entries_map: Dict[int, Dict[str, Any]]) -> None:
    """终端打印 detailed result 汇总（按 rollout 维度）。"""
    rollout_stats_map: Dict[int, Dict[int, Dict[str, Any]]] = {}
    max_rollout = 0
    for qnum, entry in entries_map.items():
        stats = compute_rollout_stats(entry)
        rollout_stats_map[qnum] = stats
        if stats:
            max_rollout = max(max_rollout, max(stats.keys()))

    if max_rollout < 0 or not rollout_stats_map:
        return

    # 汇总每个 rollout 的统计
    print(f"\n--- Detailed Result (按 Rollout 累计汇总，共 {len(entries_map)} 条查询) ---")
    header_parts = ["Rollout", "有改善查询数", "累计BestTime总和(s)", "累计Tokens总和", "累计节点数", "累计计划数", "累计LLM调用", "本轮新增节点", "本轮新增计划", "新增计划/节点", "首达全局最优", "首达全局最优%", "首达全局最优(>5%)", "首达全局最优(>5%)%", "平均提升比例"]
    print("  ".join(h.rjust(20) for h in header_parts))
    print("-" * 328)

    # 预计算每条查询的全局最优时间
    global_bests: Dict[int, Optional[float]] = {}
    for qnum, stats in rollout_stats_map.items():
        if stats:
            last_r = max(stats.keys())
            global_bests[qnum] = stats[last_r]["best_time_s"]
        else:
            global_bests[qnum] = None

    for r in range(max_rollout + 1):
        improved = 0
        sum_best_time = 0.0
        sum_tokens = 0.0
        sum_nodes = 0
        sum_plans = 0
        sum_llm_calls = 0
        sum_delta_nodes = 0
        sum_delta_plans = 0
        first_reach = 0
        first_reach_5pct = 0
        queries_with_data = 0
        sum_speedup_ratio = 0.0
        speedup_count = 0
        for qnum, entry in entries_map.items():
            stats = rollout_stats_map.get(qnum, {})
            if not stats:
                continue
            queries_with_data += 1
            base = entry.get("baseline_time")
            if base is None:
                tn = entry.get("mcts_tree_nodes", {})
                root = tn.get("0", {})
                base = root.get("db_response", {}).get("execution_time_s")
            # carry-forward：取 <= r 的最后一个有效轮次的累计数据
            if r in stats:
                snap = stats[r]
                is_new = True
            else:
                last_r = max(k for k in stats if k < r) if any(k < r for k in stats) else None
                if last_r is None:
                    continue
                snap = stats[last_r]
                is_new = False
            bt = snap["best_time_s"]
            ct = snap["cumulative_tokens"]
            cn = snap["cumulative_nodes"]
            cp = snap["cumulative_plans"]
            cl = snap["cumulative_llm_calls"]
            if bt is not None and base is not None and bt < base:
                improved += 1
            sum_best_time += bt if bt is not None else (base or 0)
            sum_tokens += ct
            sum_nodes += cn
            sum_plans += cp
            sum_llm_calls += cl
            # delta 和 first_reach 仅在本轮真正有新数据时计入
            if is_new:
                sum_delta_nodes += snap["delta_nodes"]
                sum_delta_plans += snap["delta_plans"]
                gb = global_bests.get(qnum)
                if gb is not None and bt == gb:
                    prev_bt = stats[r - 1]["best_time_s"] if r > 0 and (r - 1) in stats else None
                    if prev_bt is None or prev_bt != gb:
                        first_reach += 1
                        prev_ref = prev_bt if prev_bt is not None else base
                        if prev_ref is not None and prev_ref > 0 and gb <= prev_ref * 0.95:
                            first_reach_5pct += 1
            # 提升比例用当前最优状态计算
            if bt is not None and base is not None and base > 0:
                sum_speedup_ratio += (base - bt) / base
                speedup_count += 1
        ratio = f"{sum_delta_plans/sum_delta_nodes:.3f}" if sum_delta_nodes > 0 else "-"
        first_reach_pct = f"{first_reach/queries_with_data*100:.1f}%" if queries_with_data > 0 else "-"
        first_reach_5pct_str = f"{first_reach_5pct/queries_with_data*100:.1f}%" if queries_with_data > 0 else "-"
        avg_speedup = f"{sum_speedup_ratio/speedup_count*100:.1f}%" if speedup_count > 0 else "-"
        print("  ".join([
            f"R{r+1}".rjust(20),
            str(improved).rjust(20),
            f"{sum_best_time:.4f}".rjust(20),
            f"{sum_tokens:.0f}".rjust(20),
            str(sum_nodes).rjust(20),
            str(sum_plans).rjust(20),
            str(sum_llm_calls).rjust(20),
            str(sum_delta_nodes).rjust(20),
            str(sum_delta_plans).rjust(20),
            ratio.rjust(20),
            str(first_reach).rjust(20),
            first_reach_pct.rjust(20),
            str(first_reach_5pct).rjust(20),
            first_reach_5pct_str.rjust(20),
            avg_speedup.rjust(20),
        ]))


def _write_detailed_csv(entries_map: Dict[int, Dict[str, Any]], output_path: str) -> None:
    """将 detailed result 写入 CSV 文件，按 rollout 维度展开列。"""
    rollout_stats_map: Dict[int, Dict[int, Dict[str, Any]]] = {}
    max_rollout = 0
    for qnum, entry in entries_map.items():
        stats = compute_rollout_stats(entry)
        rollout_stats_map[qnum] = stats
        if stats:
            max_rollout = max(max_rollout, max(stats.keys()))

    rollout_range = list(range(max_rollout + 1))

    header = ["Q#", "Baseline(s)"]
    for r in rollout_range:
        header.append(f"R{r+1}_BestTime(s)")
        header.append(f"R{r+1}_CumTokens")
        header.append(f"R{r+1}_CumNodes")
        header.append(f"R{r+1}_CumPlans")
        header.append(f"R{r+1}_CumLLMCalls")

    with open(output_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(header)

        for qnum in range(1, TPCDS_TOTAL + 1):
            if qnum not in entries_map:
                # 缺失查询：填空行
                row = [f"q{qnum}", ""]
                for r in rollout_range:
                    row.extend(["", "", "", "", ""])
                writer.writerow(row)
                continue

            entry = entries_map[qnum]
            stats = rollout_stats_map.get(qnum, {})

            baseline_time = entry.get("baseline_time")
            if baseline_time is None:
                tn = entry.get("mcts_tree_nodes", {})
                root = tn.get("0", {})
                baseline_time = root.get("db_response", {}).get("execution_time_s")

            row = [f"q{qnum}", fmt(baseline_time, 4) if baseline_time is not None else ""]
            for r in rollout_range:
                if r in stats:
                    bt = stats[r]["best_time_s"]
                    ct = stats[r]["cumulative_tokens"]
                    cn = stats[r]["cumulative_nodes"]
                    cp = stats[r]["cumulative_plans"]
                    cl = stats[r]["cumulative_llm_calls"]
                    row.append(f"{bt:.4f}" if bt is not None else "")
                    row.append(f"{ct:.0f}")
                    row.append(str(cn))
                    row.append(str(cp))
                    row.append(str(cl))
                else:
                    row.extend(["", "", "", "", ""])
            writer.writerow(row)

    print(f"Detailed CSV 已保存: {output_path}")


if __name__ == "__main__":
    main()
