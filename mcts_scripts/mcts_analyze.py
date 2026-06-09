#!/usr/bin/env python3
"""
MCTS 结果分析脚本 (通用负载 - 增强版)

功能：
    - 提取 query_digest 作为唯一标识。
    - 提取 best_plan_id (来自 solutions->plan_digest)。
    - 计算 min(baseline, best) 耗时。
    - 导出完整 SQL 内容。
    - 生成 detailed result：按 rollout 维度统计累计最优计划时间和 tokens 消耗。

Usage:
    python analyze_mcts_results.py /path/to/mcts/output -o report.csv
"""
import argparse
import csv
import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

def load_mcts_results(input_dir: str) -> List[Dict[str, Any]]:
    """加载目录下所有 MCTS JSON 文件，根据 Digest 去重并保留最新文件。"""
    path = Path(input_dir)
    if not path.is_dir():
        print(f"错误: 目录不存在 {input_dir}")
        sys.exit(1)

    # 按文件修改时间排序
    json_files = sorted(path.glob("*.json"), key=lambda f: f.stat().st_mtime)
    unique_results: Dict[str, Dict[str, Any]] = {}

    for jf in json_files:
        try:
            with open(jf, 'r', encoding='utf-8') as f:
                data = json.load(f)
            entries = data if isinstance(data, list) else [data]
            for entry in entries:
                # 核心逻辑：从 execution_info -> query_digest 获取 digest
                exec_info = entry.get("execution_info", {})
                digest = exec_info.get("query_digest") or entry.get("sql_digest") or jf.stem
                
                entry["_source_file"] = jf.name
                entry["_derived_digest"] = digest
                unique_results[digest] = entry
        except Exception as e:
            print(f"  ⚠ 跳过损坏文件 {jf.name}: {e}")

    return list(unique_results.values())

def analyze_entry(entry: Dict[str, Any]) -> Dict[str, Any]:
    """解析单条 MCTS 结果。"""
    full_sql = entry.get("query", "Unknown SQL").strip()
    
    # 1. 提取 Baseline 时间
    baseline_time = entry.get("baseline_time")
    if baseline_time is None:
        tree_nodes = entry.get("mcts_tree_nodes", {})
        root = tree_nodes.get("0", {})
        baseline_time = root.get("db_response", {}).get("execution_time_s")

    solutions = entry.get("solutions", [])
    metrics = entry.get("performance_metrics", {})

    # 2. 提取最优解及 Plan ID
    best_time = None
    best_reward = None
    best_hints = []
    best_plan_id = "N/A"
    
    if solutions:
        best = solutions[0]
        best_time = best.get("execution_time_s")
        best_reward = best.get("reward")
        best_hints = best.get("executed_hints", [])
        # 核心逻辑：从 solutions -> plan_digest 拿到 best_plan_id
        best_plan_id = best.get("plan_digest") or "N/A"

    # 3. 计算 min(baseline, best)
    min_time = None
    if baseline_time is not None and best_time is not None:
        min_time = min(baseline_time, best_time)
    elif baseline_time is not None:
        min_time = baseline_time
    elif best_time is not None:
        min_time = best_time

    # 4. 计算加速比
    speedup = None
    if baseline_time and best_time and best_time > 0:
        speedup = baseline_time / best_time

    # 统计探索出的新执行计划数
    new_plan_count = len(entry.get("plan_digest_cache", {}))
    if new_plan_count > 0:
        new_plan_count -= 1 

    total_chars = metrics.get("llm_input_chars", 0) + metrics.get("llm_output_chars", 0)
    est_tokens = total_chars / 2.5
    est_price = (est_tokens / 1_000_000) * 2.0 

    return {
        "digest": entry.get("_derived_digest", "N/A"),
        "best_plan_id": best_plan_id,
        "baseline_s": baseline_time,
        "best_s": best_time,
        "min_s": min_time,
        "speedup": speedup,
        "reward": best_reward,
        "new_plans": new_plan_count,
        "llm_calls": metrics.get("llm_call_count", 0),
        "db_execs": metrics.get("db_execute_count", 0),
        "e2e_s": metrics.get("mcts_e2e_seconds", 0),
        "cost_yuan": est_price,
        "best_hints": " ".join(best_hints) if best_hints else "None",
        "full_sql": full_sql
    }

def compute_rollout_stats(entry: Dict[str, Any]) -> Dict[int, Dict[str, Any]]:
    """按 rollout 维度统计累计最优计划时间和 tokens。

    返回 {rollout_index: {...}}，每个值表示"累计到该 rollout（含）时的状态"：
    - best_time_s:         最优执行时间（solutions 中）
    - cumulative_tokens:   所有树节点产生的 tokens 之和
    - cumulative_nodes:    树节点数量
    - cumulative_plans:    唯一 plan_digest 数量
    - cumulative_llm_calls: LLM 调用次数（input_length > 0 的节点）
    """
    nodes = entry.get("mcts_tree_nodes", {})
    solutions = entry.get("solutions", [])

    # 收集所有出现的 rollout_index（来自 node_info）
    all_rollouts = set()
    for v in nodes.values():
        ri = v.get("node_info", {}).get("rollout_index")
        if ri is not None:
            all_rollouts.add(ri)
    # 也从 solutions 中收集
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
    parser = argparse.ArgumentParser(description="MCTS 结果汇总分析工具")
    parser.add_argument("input_dir", help="JSON 结果目录")
    parser.add_argument("-o", "--output", help="输出 CSV 路径（brief result）")
    parser.add_argument("--detailed-output", help="输出 detailed result CSV 路径（按 rollout 维度）")
    parser.add_argument("--sort", default="speedup", choices=["speedup", "baseline", "min"], help="排序方式")
    args = parser.parse_args()

    raw_data = load_mcts_results(args.input_dir)
    results = [analyze_entry(d) for d in raw_data]

    # 排序逻辑
    if args.sort == "speedup":
        results.sort(key=lambda x: x["speedup"] or 0, reverse=True)
    elif args.sort == "baseline":
        results.sort(key=lambda x: x["baseline_s"] or 0, reverse=True)
    elif args.sort == "min":
        results.sort(key=lambda x: x["min_s"] or 0)

    # 定义列标题（brief）
    columns = [
        ("digest", "Query Digest"),
        ("best_plan_id", "Best Plan ID"),
        ("baseline_s", "Baseline(s)"),
        ("best_s", "Best(s)"),
        ("min_s", "Min(Base,Best)"),
        ("speedup", "Speedup"),
        ("reward", "Reward"),
        ("new_plans", "NewPlans"),
        ("llm_calls", "LLM"),
        ("db_execs", "DB"),
        ("e2e_s", "E2E(s)"),
        ("cost_yuan", "Cost(¥)"),
        ("best_hints", "BestHints"),
        ("full_sql", "Full SQL")
    ]

    if args.output:
        with open(args.output, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow([c[1] for c in columns])
            for r in results:
                row_data = []
                for key, _ in columns:
                    val = r[key]
                    if key == "full_sql":
                        # 处理换行符，防止破坏 CSV 结构
                        val = val.replace('\n', ' ').replace('\r', '')
                    if isinstance(val, float):
                        row_data.append(f"{val:.4f}")
                    else:
                        row_data.append(val)
                writer.writerow(row_data)
        print(f"分析完成，报告已生成: {args.output}")
    else:
        # 终端简易预览
        head = " | ".join(c[1][:12].rjust(12) for c in columns[:6]) + " | SQL"
        print(head)
        print("-" * len(head))
        for r in results:
            line = [f"{str(r[c[0]]):>12.12}" if not isinstance(r[c[0]], float) else f"{r[c[0]]:>12.2f}" for c in columns[:6]]
            print(" | ".join(line) + f" | {r['full_sql'][:80]}...")

    # ---- Detailed result（按 rollout 维度）----
    if args.detailed_output:
        # 确定所有文件中最大 rollout 数
        all_rollout_stats = []
        max_rollout = 0
        for entry in raw_data:
            stats = compute_rollout_stats(entry)
            all_rollout_stats.append((entry, stats))
            if stats:
                max_rollout = max(max_rollout, max(stats.keys()))

        rollout_range = list(range(max_rollout + 1))

        # 构建列：digest, baseline_s, best_plan_rollout,
        #   rollout=1 best_time, rollout=1 tokens, nodes, plans, llm_calls, ...
        header = ["Query Digest", "Baseline(s)"]
        for r in rollout_range:
            header.append(f"R{r+1}_BestTime(s)")
            header.append(f"R{r+1}_CumTokens")
            header.append(f"R{r+1}_CumNodes")
            header.append(f"R{r+1}_CumPlans")
            header.append(f"R{r+1}_CumLLMCalls")

        with open(args.detailed_output, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(header)
            for entry, stats in all_rollout_stats:
                digest = entry.get("_derived_digest", "N/A")
                baseline_time = entry.get("baseline_time")
                if baseline_time is None:
                    tree_nodes = entry.get("mcts_tree_nodes", {})
                    root = tree_nodes.get("0", {})
                    baseline_time = root.get("db_response", {}).get("execution_time_s")
                row = [digest, f"{baseline_time:.4f}" if baseline_time is not None else ""]
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
                        row.append("")
                        row.append("")
                        row.append("")
                        row.append("")
                        row.append("")
                writer.writerow(row)
        print(f"Detailed 分析完成，报告已生成: {args.detailed_output}")
    elif not args.output:
        # 终端打印 detailed 汇总（仅汇总行，不逐行打印）
        all_rollout_stats = []
        max_rollout = 0
        for entry in raw_data:
            stats = compute_rollout_stats(entry)
            all_rollout_stats.append((entry, stats))
            if stats:
                max_rollout = max(max_rollout, max(stats.keys()))

        if max_rollout >= 0 and all_rollout_stats:
            print("\n--- Detailed Result (按 Rollout 累计汇总) ---")
            header_parts = ["Rollout", "有改善查询数", "累计BestTime总和(s)", "累计Tokens总和", "累计节点数", "累计计划数", "累计LLM调用", "本轮新增节点", "本轮新增计划", "新增计划/节点", "首达全局最优", "首达全局最优%", "首达全局最优(>5%)", "首达全局最优(>5%)%", "平均提升比例"]
            print("  ".join(h.rjust(18) for h in header_parts))
            print("-" * 260)

            # 收集所有查询的 baseline
            baselines = []
            for entry, _ in all_rollout_stats:
                bt = entry.get("baseline_time")
                if bt is None:
                    tn = entry.get("mcts_tree_nodes", {})
                    root = tn.get("0", {})
                    bt = root.get("db_response", {}).get("execution_time_s")
                baselines.append(bt)

            # 预计算每条查询的全局最优时间（所有 rollout 累计后的最终最优）
            global_bests = []
            for entry, stats in all_rollout_stats:
                if stats:
                    last_r = max(stats.keys())
                    global_bests.append(stats[last_r]["best_time_s"])
                else:
                    global_bests.append(None)

            total_queries = len(all_rollout_stats)

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
                for i, (entry, stats) in enumerate(all_rollout_stats):
                    if not stats:
                        continue
                    queries_with_data += 1
                    base = baselines[i]
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
                        gb = global_bests[i]
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
                    f"R{r+1}".rjust(18),
                    str(improved).rjust(18),
                    f"{sum_best_time:.4f}".rjust(18),
                    f"{sum_tokens:.0f}".rjust(18),
                    str(sum_nodes).rjust(18),
                    str(sum_plans).rjust(18),
                    str(sum_llm_calls).rjust(18),
                    str(sum_delta_nodes).rjust(18),
                    str(sum_delta_plans).rjust(18),
                    ratio.rjust(18),
                    str(first_reach).rjust(18),
                    first_reach_pct.rjust(18),
                    str(first_reach_5pct).rjust(18),
                    first_reach_5pct_str.rjust(18),
                    avg_speedup.rjust(18),
                ]))

if __name__ == "__main__":
    main()