#!/usr/bin/env python3
"""
MCTS 节点详细分析工具

分析 MCTS JSON 结果文件中每个搜索树节点的动作分布、改进效果、
Hint 类型统计、Index/Join/Config 探索覆盖率、A5/A6 纠错行为和幻觉 Hint 等。

Usage:
    python mcts_scripts/mcts_detailed_analyzer/analyze_mcts_nodes.py \
        --input-dir /path/to/mcts/output

    python mcts_scripts/mcts_detailed_analyzer/analyze_mcts_nodes.py \
        --input-dir /path/to/mcts/output --workers 4
"""
import argparse
import json
import os
import re
import sys
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor, as_completed
from typing import Any, Dict, List, Optional, Set, Tuple

try:
    from tqdm import tqdm
    HAS_TQDM = True
except ImportError:
    HAS_TQDM = False
    print("提示: 未安装 tqdm，将使用简单进度输出。可通过 pip install tqdm 安装。")

ACTION_TYPES = ["A1", "A2", "A3", "A4", "A5", "A6"]

STATS_CONFIG = {
    "index": ["INDEX", "NO_INDEX"],
    "join": ["JOIN_PREFIX", "JOIN_SUFFIX"],
    "set_var": [
        "materialization=off", "derived_merge=off", "semijoin=off",
        "loosescan=off", "firstmatch=off", "duplicateweedout=off",
        "subquery_materialization_cost_based=off", "subquery_to_derived=on",
    ],
}


# ---------------------------------------------------------------------------
# 工具函数
# ---------------------------------------------------------------------------

def iter_eval_files(eval_dir: str) -> List[str]:
    """收集 .jsonl 和 .json 文件路径。"""
    file_paths = []
    for root, _, files in os.walk(eval_dir):
        for name in files:
            if name.endswith(".jsonl") or name.endswith(".json"):
                file_paths.append(os.path.join(root, name))
    return file_paths


def get_time_value(data: Dict[str, Any], time_key: str) -> Optional[float]:
    for key in (f"{time_key}_s", time_key):
        value = data.get(key)
        if value is not None:
            return value
    return None


def get_parent_id(node_id: str) -> Optional[str]:
    if "." not in node_id:
        return None
    return node_id.rsplit(".", 1)[0]


def get_ancestor_ids(node_id: str) -> List[str]:
    parts = node_id.split(".")
    return [".".join(parts[:i]) for i in range(1, len(parts))]


def normalize_hints(node: Dict[str, Any]) -> List[str]:
    hints = node.get("executed_hints")
    if hints is None:
        hints = node.get("action_input")
    if hints is None:
        return []
    if isinstance(hints, list):
        return [str(hint) for hint in hints]
    return [str(hints)]


def parse_action_type(node: Dict[str, Any]) -> Optional[str]:
    # 新格式: node_info.action_type 已为 "A1" 等字符串
    at = node.get("action_type")
    if at and re.match(r"A\d", str(at)):
        return str(at)
    # 旧格式: 从 text 中解析 [A1] 前缀
    text = node.get("text")
    if not text:
        return None
    m = re.match(r"\[A(\d)\]", text.strip())
    return f"A{m.group(1)}" if m else None


def _flatten_new_format_node(node: Dict[str, Any]) -> Dict[str, Any]:
    """将新格式节点 (node_info/llm_response/db_response) 展平为旧格式兼容 dict。

    新格式:
        {"node_info": {...}, "llm_response": {...}, "db_response": {...}}
    旧格式:
        {"text": ..., "cur_time": ..., "plan_digest": ..., "executed_hints": ..., ...}

    如果节点已是旧格式（无 node_info key），则原样返回。
    """
    if "node_info" not in node:
        return node
    ni = node.get("node_info", {})
    llm = node.get("llm_response", {})
    db = node.get("db_response", {})
    flat = {}
    # node_info 字段
    flat["action_type"] = ni.get("action_type")
    flat["executed_hints"] = ni.get("executed_hints", [])
    flat["new_hints"] = ni.get("new_hints", [])
    flat["deleted_hints"] = ni.get("deleted_hints", [])
    flat["status"] = ni.get("status")
    flat["terminal_reason"] = ni.get("terminal_reason")
    flat["reward"] = ni.get("reward")
    flat["q_value"] = ni.get("q_value")
    flat["visit_count"] = ni.get("visit_count")
    flat["rollout_index"] = ni.get("rollout_index")
    flat["depth"] = ni.get("depth")
    # llm_response 字段
    flat["text"] = llm.get("response")
    # db_response 字段
    flat["cur_time"] = db.get("execution_time_s")
    flat["cur_time_s"] = db.get("execution_time_s")
    flat["plan_digest"] = db.get("plan_digest")
    flat["step_improvement"] = db.get("step_improvement")
    return flat


def _get_tree_nodes(item: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    """从 item 中提取树节点 dict，兼容新旧格式。

    新格式: item["mcts_tree_nodes"]
    旧格式: item["deepthink"] (或 item.get("list", item)["deepthink"])
    """
    # 新格式优先
    tree = item.get("mcts_tree_nodes")
    if isinstance(tree, dict) and tree:
        return tree
    # 旧格式 fallback
    base = item.get("list", item) if isinstance(item, dict) else item
    if isinstance(base, dict):
        dt = base.get("deepthink")
        if isinstance(dt, dict):
            return dt
    return {}


def _get_plan_cache(item: Dict[str, Any]) -> Dict[str, Any]:
    """获取 plan_digest_cache，兼容新旧格式。"""
    # 新格式: 顶层
    pdc = item.get("plan_digest_cache")
    if isinstance(pdc, dict) and pdc:
        return pdc
    # 旧格式: deepthink 内
    base = item.get("list", item) if isinstance(item, dict) else item
    if isinstance(base, dict):
        dt = base.get("deepthink", {})
        if isinstance(dt, dict):
            return dt.get("plan_digest_cache", {})
    return {}


def has_better_or_same_in_chain(
    node_id: str,
    node_map: Dict[str, Dict[str, Any]],
    cur_time: float,
    cur_digest: str,
) -> bool:
    for ancestor_id in get_ancestor_ids(node_id):
        ancestor = node_map.get(ancestor_id)
        if not ancestor:
            continue
        ancestor_time = get_time_value(ancestor, "cur_time")
        if ancestor_time is not None and ancestor_time < cur_time:
            return True
        ancestor_digest = ancestor.get("plan_digest")
        if ancestor_digest and ancestor_digest == cur_digest:
            return True
    return False


def _improvement_ratio(node: Dict[str, Any]) -> Optional[float]:
    """
    ratio = parent_time / cur_time - 1 (相对父节点的加速比)。
    当 hints 与父节点完全相同且 cur_time 缺失时视为无变化返回 0.0。
    """
    pt = node.get("parent_cur_time")
    ct = node.get("cur_time")
    if ct is not None and ct > 0 and pt is not None and pt > 0:
        return pt / ct - 1.0
    if set(node.get("executed_hints", [])) == set(node.get("parent_executed_hints", [])):
        return 0.0
    return None


# ---------------------------------------------------------------------------
# hint 分类辅助
# ---------------------------------------------------------------------------

def _is_index_hint(h: str) -> bool:
    u = h.upper().strip()
    return u.startswith("INDEX(") or u.startswith("NO_INDEX(")


def _is_join_hint(h: str) -> bool:
    u = h.upper().strip()
    return u.startswith("JOIN_PREFIX(") or u.startswith("JOIN_SUFFIX(")


def _is_config_hint(h: str) -> bool:
    return h.upper().strip().startswith("SET_VAR(")


# ---------------------------------------------------------------------------
# Join / Set_var 覆盖逻辑
# ---------------------------------------------------------------------------

def _parse_join_tables(h: str) -> Set[str]:
    """从 JOIN_PREFIX(a,b) 或 JOIN_SUFFIX(x) 提取表名集合。"""
    m = re.match(r"JOIN_(PREFIX|SUFFIX)\s*\(\s*(.+?)\s*\)", h.upper().strip())
    if not m:
        return set()
    inner = m.group(2).strip()
    return {t.strip() for t in re.split(r"[,&\s]+", inner) if t.strip()}


def _join_candidate_covered_by_executed(cand: str, executed_set: Set[str]) -> bool:
    cand_tables = _parse_join_tables(cand)
    if not cand_tables:
        return True
    cand_type = "PREFIX" if "PREFIX" in cand.upper() else "SUFFIX"
    for ex in executed_set:
        if cand_type not in ex.upper():
            continue
        ex_tables = _parse_join_tables(ex)
        if cand_tables <= ex_tables:
            return True
    return False


def _join_executed_covered_by_candidates(executed: str, cand_join_list: List[str]) -> bool:
    ex_tables = _parse_join_tables(executed)
    if not ex_tables:
        return True
    ex_type = "PREFIX" if "PREFIX" in executed.upper() else "SUFFIX"
    cand_by_type = [
        c for c in cand_join_list
        if (ex_type in c.upper()) and _parse_join_tables(c)
    ]
    for t in ex_tables:
        if not any(t in _parse_join_tables(c) for c in cand_by_type):
            return False
    return True


def _set_var_candidate_covered_by_executed(cand: str, executed_set: Set[str]) -> bool:
    cand_lower = str(cand).lower()
    for key in STATS_CONFIG["set_var"]:
        if key.lower() in cand_lower:
            key_part = key.split("=")[0].lower()
            if any(key_part in ex.lower() for ex in executed_set):
                return True
    return False


def _set_var_executed_covered_by_candidates(executed: str, cand_config_list: List[str]) -> bool:
    ex_lower = executed.lower()
    for key in STATS_CONFIG["set_var"]:
        key_part = key.split("=")[0].lower()
        if key_part in ex_lower:
            if not any(key_part in str(c).lower() for c in cand_config_list):
                return False
    return True


def _stats_for_ratios(rs: List[Optional[float]]) -> Tuple[float, float, float, int]:
    """返回 (avg, median, p90, n_valid)。"""
    valid = [x for x in rs if x is not None]
    n = len(valid)
    if n == 0:
        return 0.0, 0.0, 0.0, 0
    sorted_v = sorted(valid)
    avg = sum(valid) / n
    median = sorted_v[n // 2] if n % 2 else (sorted_v[n // 2 - 1] + sorted_v[n // 2]) / 2
    p90_idx = int((n - 1) * 0.9) if n > 1 else 0
    p90 = sorted_v[p90_idx]
    return avg, median, p90, n


# ---------------------------------------------------------------------------
# 数据提取
# ---------------------------------------------------------------------------

def extract_from_item(
    item: Dict[str, Any], file_path: str
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], Dict[str, Any]]:
    """返回 (全量节点, SFT 筛选节点, item 级信息)"""
    all_nodes: List[Dict[str, Any]] = []
    sft_nodes: List[Dict[str, Any]] = []

    base_node = item.get("list", item) if isinstance(item, dict) else item
    if not isinstance(base_node, dict):
        return [], [], {}

    default_baseline_time = get_time_value(base_node, "baseline_time")
    default_plan_digest = base_node.get("plan_digest")
    candidate_hints = base_node.get("candidate_hints", {})

    item_info: Dict[str, Any] = {
        "candidate_hints": candidate_hints,
        "baseline_time": default_baseline_time,
    }

    # 兼容新旧格式获取树节点
    tree_nodes = _get_tree_nodes(base_node)
    if not tree_nodes:
        return [], [], item_info

    # 排除非节点的顶层 key，展平新格式节点
    _exclude_keys = {
        "solutions", "plan_digest_cache", "mcts_metrics",
        "performance_metrics", "early_stopping_metrics",
    }
    node_map = {}
    for nid, nd in tree_nodes.items():
        if nid in _exclude_keys or not isinstance(nd, dict):
            continue
        node_map[nid] = _flatten_new_format_node(nd)

    for node_id, node in node_map.items():
        action_type = parse_action_type(node)

        hints = normalize_hints(node)
        parent_id = get_parent_id(node_id)
        parent = node_map.get(parent_id) if parent_id else None
        parent_hints = normalize_hints(parent) if parent else []
        new_hints = list(set(hints) - set(parent_hints))

        parent_cur_time = get_time_value(parent, "cur_time") if parent else None
        if parent_cur_time is None:
            parent_cur_time = default_baseline_time

        prefix = node_id + "."
        child_count = sum(
            1 for nid in node_map
            if nid.startswith(prefix) and "." not in nid[len(prefix):]
        )

        node_data = {
            "file_path": file_path,
            "node_id": node_id,
            "child_count": child_count,
            "action_type": action_type,
            "cur_time": get_time_value(node, "cur_time"),
            "baseline_time": default_baseline_time,
            "parent_cur_time": parent_cur_time,
            "new_hints": new_hints,
            "executed_hints": hints,
            "parent_executed_hints": parent_hints,
            "answer": node.get("text"),
        }

        if action_type:
            all_nodes.append(node_data)

        # SFT 筛选
        cur_time = node_data["cur_time"]
        cur_digest = node.get("plan_digest")
        if default_plan_digest is None or default_baseline_time is None:
            continue
        if cur_time is None or cur_time >= default_baseline_time:
            continue
        if not cur_digest or cur_digest == default_plan_digest:
            continue
        if has_better_or_same_in_chain(node_id, node_map, cur_time, cur_digest):
            continue
        if not new_hints:
            continue
        if not action_type:
            continue
        sft_nodes.append(node_data)

    return all_nodes, sft_nodes, item_info


def _node_id_sort_key(node_id: str) -> Tuple[int, ...]:
    try:
        return tuple(int(x) for x in node_id.split("."))
    except ValueError:
        return (0,)


def extract_best_action_by_round(
    item: Dict[str, Any],
) -> Optional[Tuple[float, List[Tuple[Optional[str], float]]]]:
    """
    按 1-9 轮每轮取最佳 hints 所在计划，追溯该计划由哪个动作产生。
    返回 (baseline_time, [(action_1, best_t_1), ..., (action_9, best_t_9)])。
    """
    base_node = item.get("list", item) if isinstance(item, dict) else item
    if not isinstance(base_node, dict):
        return None

    baseline_time = base_node.get("baseline_time")
    try:
        base_t_float = float(baseline_time) if baseline_time is not None else float("inf")
    except (ValueError, TypeError):
        base_t_float = float("inf")

    # 兼容新旧格式
    tree_nodes = _get_tree_nodes(base_node)
    if not tree_nodes:
        return None

    plan_cache = _get_plan_cache(base_node)
    plans_with_digest = []
    for plan_digest, plan_info in plan_cache.items():
        plan_obj = dict(plan_info) if isinstance(plan_info, dict) else {}
        plan_obj["plan_digest"] = plan_digest
        plans_with_digest.append(plan_obj)

    if len(plans_with_digest) <= 1:
        return None

    mcts_plans = plans_with_digest[1:]

    # 排除非节点的顶层 key，展平新格式节点
    _exclude_keys = {
        "solutions", "plan_digest_cache", "mcts_metrics",
        "performance_metrics", "early_stopping_metrics",
    }
    node_map = {}
    for nid, nd in tree_nodes.items():
        if nid in _exclude_keys or not isinstance(nd, dict):
            continue
        node_map[nid] = _flatten_new_format_node(nd)

    digest_to_action: Dict[str, str] = {}
    for pd in {n.get("plan_digest") for n in node_map.values() if n.get("plan_digest")}:
        candidates = [(nid, parse_action_type(nd)) for nid, nd in node_map.items() if nd.get("plan_digest") == pd]
        candidates = [(nid, at) for nid, at in candidates if at]
        if not candidates:
            continue
        best_nid = min(candidates, key=lambda x: _node_id_sort_key(x[0]))[0]
        digest_to_action[pd] = parse_action_type(node_map[best_nid]) or "OTHER"

    def safe_get_time(x: Dict[str, Any]) -> float:
        t = x.get("execution_time_s")
        if t is None:
            return base_t_float
        try:
            return float(t)
        except (ValueError, TypeError):
            return base_t_float

    result: List[Tuple[Optional[str], float]] = []
    for i in range(1, 10):
        k = max(1, round(i * len(mcts_plans) / 9.0))
        current_window = mcts_plans[:k]
        best_plan = min(current_window, key=safe_get_time)
        best_t = safe_get_time(best_plan)
        best_digest = best_plan.get("plan_digest")

        if best_t >= base_t_float:
            result.append((None, best_t))
        else:
            result.append((digest_to_action.get(best_digest, "OTHER"), best_t))

    return (base_t_float, result)


# ===================================================================
# Section 1 — 动作分布 (A1-A6)
# ===================================================================

def run_action_distribution(nodes: List[Dict[str, Any]], title: str) -> None:
    total = len(nodes)
    counts: Dict[str, int] = defaultdict(int)
    for n in nodes:
        at = n.get("action_type")
        if at:
            counts[at] += 1

    print("\n" + "=" * 60)
    print(f"[动作分布] {title} (总节点数: {total})")
    print("-" * 60)
    if total == 0:
        print("无数据。")
        return
    for at in ACTION_TYPES:
        c = counts.get(at, 0)
        print(f"  {at}: {c:>6} ({c / total * 100:>6.2f}%)")
    other = total - sum(counts.get(at, 0) for at in ACTION_TYPES)
    if other > 0:
        print(f"  其他: {other:>6} ({other / total * 100:>6.2f}%)")
    print("-" * 60)


def run_action_child_count(nodes: List[Dict[str, Any]], title: str) -> None:
    total = len(nodes)
    if total == 0:
        return
    by_action: Dict[str, List[int]] = defaultdict(list)
    for n in nodes:
        at = n.get("action_type")
        if at:
            by_action[at].append(n.get("child_count", 0))

    print("\n" + "=" * 60)
    print(f"[动作平均子节点数] {title}")
    print("-" * 60)
    for at in ACTION_TYPES:
        counts = by_action.get(at, [])
        avg = sum(counts) / len(counts) if counts else 0.0
        print(f"  {at}: {avg:.2f} (n={len(counts)})")
    all_counts = [n.get("child_count", 0) for n in nodes if n.get("action_type")]
    if all_counts:
        print(f"  全部: {sum(all_counts) / len(all_counts):.2f} (n={len(all_counts)})")
    print("-" * 60)


# ===================================================================
# Section 2 — 动作改进比例 (A1-A6)
# ===================================================================

def run_action_improvement(nodes: List[Dict[str, Any]], title: str) -> None:
    ratios_by_action: Dict[str, List[Optional[float]]] = {at: [] for at in ACTION_TYPES}
    ratios_all: List[Optional[float]] = []

    for n in nodes:
        at = n.get("action_type")
        if not at:
            continue
        r = _improvement_ratio(n)
        ratios_all.append(r)
        ratios_by_action.setdefault(at, []).append(r)

    tiers = [(0.2, ">20%"), (1.0, ">100%"), (5.0, ">500%")]
    CW, CN, CA, CM, CP = 12, 8, 10, 10, 10

    print("\n" + "=" * 100)
    print(f"[动作改进] {title}")
    print("-" * 100)

    header = (
        f"{'动作':<{CW}}{'n':>{CN}}{'avg':>{CA}}{'median':>{CM}}{'P90':>{CP}}"
        + "".join(f"{t[1]:>{CP}}" for t in tiers)
    )
    sep = "-" * len(header)

    def _row(label: str, rs: List[Optional[float]]) -> str:
        n = len(rs)
        if n == 0:
            return (
                f"{label:<{CW}}{'0':>{CN}}{'-':>{CA}}{'-':>{CM}}{'-':>{CP}}"
                + "".join(f"{'-':>{CP}}" for _ in tiers)
            )
        avg, median, p90, _ = _stats_for_ratios(rs)
        parts = [f"{label:<{CW}}", f"{n:>{CN}}", f"{avg:>{CA}.4f}", f"{median:>{CM}.4f}", f"{p90:>{CP}.4f}"]
        for thr, _ in tiers:
            c = sum(1 for x in rs if x is not None and x > thr)
            parts.append(f"{c / n * 100:>{CP}.2f}%")
        return "".join(parts)

    if not nodes:
        print("无数据。")
        return

    print(sep)
    print(header)
    print(sep)
    print(_row("全部", ratios_all))
    for at in ACTION_TYPES:
        print(_row(at, ratios_by_action.get(at, [])))
    print(sep)
    print(
        "说明: ratio = parent_time/cur_time - 1 (相对上一步/父节点); "
        "avg/median/P90 仅计有效值; 各档占比分母为 n。"
    )


# ===================================================================
# Section 2b — 按树深度分层的动作统计
# ===================================================================

def _depth_from_node_id(node_id: str) -> int:
    return node_id.count(".")


def run_action_by_depth(nodes: List[Dict[str, Any]], title: str) -> None:
    tiers = [(0.2, ">20%"), (1.0, ">100%"), (5.0, ">500%")]
    by_depth: Dict[int, List[Dict[str, Any]]] = defaultdict(list)
    for n in nodes:
        at = n.get("action_type")
        if not at:
            continue
        node_id = n.get("node_id", "")
        d = _depth_from_node_id(node_id)
        by_depth[d].append(n)

    depths = sorted(by_depth.keys())
    if not depths:
        print("\n" + "=" * 60)
        print(f"[按深度分层动作统计] {title}")
        print("-" * 60)
        print("无数据。")
        return

    W = 8
    header = (
        f"{'深度':<{W}}{'动作':<{W}}{'层节点数':>{W}}{'动作数':>{W}}{'占比':>{W}}"
        + "".join(f"{t[1]:>{W}}" for t in tiers)
    )

    print("\n" + "=" * 80)
    print(f"[按深度分层动作统计] {title}")
    print("-" * 80)
    print(header)
    print("-" * 80)

    for d in depths:
        layer = by_depth[d]
        total = len(layer)
        counts: Dict[str, int] = defaultdict(int)
        ratios_by_action: Dict[str, List[Optional[float]]] = {at: [] for at in ACTION_TYPES}
        for n in layer:
            at = n.get("action_type")
            if at:
                counts[at] += 1
                r = _improvement_ratio(n)
                ratios_by_action.setdefault(at, []).append(r)

        for at in ACTION_TYPES:
            c = counts.get(at, 0)
            pct = c / total * 100 if total else 0
            rs = ratios_by_action.get(at, [])
            n_rs = len(rs)
            r20 = sum(1 for x in rs if x is not None and x > 0.2) / n_rs * 100 if n_rs else 0
            r100 = sum(1 for x in rs if x is not None and x > 1.0) / n_rs * 100 if n_rs else 0
            r500 = sum(1 for x in rs if x is not None and x > 5.0) / n_rs * 100 if n_rs else 0
            row = (
                f"{d:<{W}}{at:<{W}}{total:>{W}}{c:>{W}}{pct:>{W-1}.1f}%"
                f"{r20:>{W-1}.1f}%{r100:>{W-1}.1f}%{r500:>{W-1}.1f}%"
            )
            print(row)
    print("-" * 80)
    print("说明: 深度 = node_id 中 '.' 的数量; 占比 = 该动作数/该层节点数; 改进率 = 该动作中 ratio>阈值的占比。")


# ===================================================================
# Section 2c — 按 1-9 轮最佳 hints 追溯动作比例
# ===================================================================

def run_action_proportion_by_round(
    round_data_list: List[Tuple[float, List[Tuple[Optional[str], float]]]],
) -> None:
    if not round_data_list:
        print("\n" + "=" * 80)
        print("[按轮次最佳 hints 动作比例] 无数据。")
        return

    by_round: Dict[int, Dict[str, int]] = {i: defaultdict(int) for i in range(1, 10)}
    for baseline, rounds_data in round_data_list:
        if len(rounds_data) != 9:
            continue
        prev_best_t = baseline
        for i, (action, best_t) in enumerate(rounds_data):
            r = i + 1
            if best_t < prev_best_t:
                if action is None:
                    by_round[r]["BASELINE"] += 1
                elif action in ACTION_TYPES:
                    by_round[r][action] += 1
                else:
                    by_round[r]["OTHER"] += 1
            prev_best_t = best_t

    W = 8
    header = (
        f"{'轮次':<{W}}{'样本数':>{W}}"
        + "".join(f"{at:>{W}}" for at in ACTION_TYPES)
        + f" {'BASELINE':>{W-1}}{'OTHER':>{W}}"
    )
    print("\n" + "=" * 100)
    print("[按轮次最佳 hints 动作比例] 仅统计当轮出现更优计划的样本 (A1-A6 比例)")
    print("-" * 100)
    print(header)
    print("-" * 100)

    for r in range(1, 10):
        counts = by_round[r]
        total = sum(counts.values())
        if total == 0:
            row = f"{r:<{W}}{0:>{W}}" + "".join(f"{'-':>{W}}" for _ in ACTION_TYPES) + f"{'-':>{W}}{'-':>{W}}"
            print(row)
            continue
        parts = [f"{r:<{W}}", f"{total:>{W}}"]
        for at in ACTION_TYPES:
            c = counts.get(at, 0)
            pct = c / total * 100
            parts.append(f"{pct:.1f}%")
        parts.append(f"{counts.get('BASELINE', 0) / total * 100:.1f}%")
        parts.append(f"{counts.get('OTHER', 0) / total * 100:.1f}%")
        print("".join(f"{p:>{W}}" for p in parts))
    print("-" * 100)
    print("说明: 仅统计当轮 best_time < 上一轮 best_time 的样本；比例=该动作产生更优 hints 的样本占比。")


# ===================================================================
# Section 3 — hint 大类统计
# ===================================================================

def _subkeys_hit(new_hints: List[str]) -> List[str]:
    upper = " ".join(new_hints).upper()
    hit: List[str] = []
    for _cat, keys in STATS_CONFIG.items():
        for key in keys:
            if key.upper() in upper:
                hit.append(key)
    return hit


def run_hint_category_stats(nodes: List[Dict[str, Any]], title: str) -> None:
    total = len(nodes)
    stats = {"join": 0, "index": 0, "set_var": 0}
    sub_stats = {cat: {key: 0 for key in keys} for cat, keys in STATS_CONFIG.items()}

    for node in nodes:
        nh_upper = " ".join(node.get("new_hints", [])).upper()
        found = {"join": False, "index": False, "set_var": False}
        for cat, keys in STATS_CONFIG.items():
            for key in keys:
                if key.upper() in nh_upper:
                    sub_stats[cat][key] += 1
                    found[cat] = True
        for cat in found:
            if found[cat]:
                stats[cat] += 1

    print("\n" + "=" * 60)
    print(f"[Hint 分类] {title} (样本总数: {total})")
    print("-" * 60)
    if total == 0:
        print("无数据。")
        return
    for cat in ("index", "join", "set_var"):
        ct = stats[cat]
        print(f"[{cat.upper()}] 大类占比: {ct:>5} ({ct / total * 100:>5.2f}%)")
        for key, cnt in sub_stats[cat].items():
            print(f"  - {key:<40}: {cnt:>5} ({cnt / total * 100:>5.2f}%)")
        print("-" * 60)


def run_hint_improvement(nodes: List[Dict[str, Any]], title: str) -> None:
    ratios_all: List[Optional[float]] = []
    for n in nodes:
        if n.get("new_hints"):
            ratios_all.append(_improvement_ratio(n))

    all_subkeys = [k for _c, ks in STATS_CONFIG.items() for k in ks]
    ratios_by_sub: Dict[str, List[Optional[float]]] = {k: [] for k in all_subkeys}
    for n in nodes:
        nh = n.get("new_hints") or []
        if not nh:
            continue
        r = _improvement_ratio(n)
        hit = set(_subkeys_hit(nh))
        for k in all_subkeys:
            if k in hit:
                ratios_by_sub[k].append(r)

    tiers = [(0.2, ">20%"), (1.0, ">100%"), (5.0, ">500%")]
    CW, CN, CA, CM, CP = 44, 8, 10, 10, 10

    print("\n" + "=" * 130)
    print(f"[Hint 改进] {title}  (ratio = pt/ct - 1)")

    header = (
        f"{'hint_type':<{CW}}{'n':>{CN}}{'avg':>{CA}}{'median':>{CM}}{'P90':>{CP}}"
        + "".join(f"{t[1]:>{CP}}" for t in tiers)
    )
    sep = "-" * len(header)

    def _row(label: str, rs: List[Optional[float]]) -> str:
        n = len(rs)
        if n == 0:
            return (
                f"{label:<{CW}}{'0':>{CN}}{'-':>{CA}}{'-':>{CM}}{'-':>{CP}}"
                + "".join(f"{'-':>{CP}}" for _ in tiers)
            )
        avg, median, p90, _ = _stats_for_ratios(rs)
        parts = [f"{label:<{CW}}", f"{n:>{CN}}", f"{avg:>{CA}.4f}", f"{median:>{CM}.4f}", f"{p90:>{CP}.4f}"]
        for thr, _ in tiers:
            c = sum(1 for x in rs if x is not None and x > thr)
            parts.append(f"{c / n * 100:>{CP}.2f}%")
        return "".join(parts)

    if not nodes:
        print("无数据。")
        return

    print(sep)
    print(header)
    print(sep)
    print(_row("全部(每步新增hints)", ratios_all))
    for cat in ("index", "join", "set_var"):
        for k in STATS_CONFIG[cat]:
            print(_row(k, ratios_by_sub[k]))
    print(sep)


# ===================================================================
# Section 4 — Index 探索分析 (candidate vs 实际尝试)
# ===================================================================

def _collect_explored_in_candidate(
    nodes: List[Dict[str, Any]], cand_set: Set[str],
) -> Set[str]:
    explored: Set[str] = set()
    for n in nodes:
        for h in n.get("executed_hints", []):
            if _is_index_hint(h) and h in cand_set:
                explored.add(h)
    return explored


def run_index_exploration(
    item_data_list: List[Tuple[Dict[str, Any], List[Dict[str, Any]]]],
) -> None:
    total_items = 0
    total_candidate = 0
    total_explored = 0
    items_with_candidate = 0

    bucket_data: Dict[str, List[Tuple[int, int]]] = defaultdict(list)

    for item_info, nodes in item_data_list:
        ch = item_info.get("candidate_hints", {})
        cand_index = ch.get("index", [])
        if not cand_index:
            continue

        cand_set = set(str(h) for h in cand_index)
        explored = _collect_explored_in_candidate(nodes, cand_set)

        items_with_candidate += 1
        total_items += 1
        nc = len(cand_index)
        ne = len(explored)
        total_candidate += nc
        total_explored += ne

        if nc <= 5:
            bucket_data["1-5"].append((nc, ne))
        elif nc <= 10:
            bucket_data["6-10"].append((nc, ne))
        elif nc <= 20:
            bucket_data["11-20"].append((nc, ne))
        else:
            bucket_data[">20"].append((nc, ne))

    print("\n" + "=" * 80)
    print("[Index 探索分析] candidate index hint 中有多少被实际尝试 (排除幻觉 hint)")
    print("-" * 80)
    if total_items == 0:
        print("无含 index candidate 的 item。")
        return

    print(f"  含 index candidate 的 item 数:   {items_with_candidate}")
    print(f"  candidate index hint 总数:        {total_candidate}")
    print(f"  被尝试的 candidate index (去重):   {total_explored}")
    avg_cand = total_candidate / items_with_candidate
    avg_expl = total_explored / items_with_candidate
    ratio = total_explored / total_candidate * 100 if total_candidate else 0
    print(f"  平均每 item candidate index 数:   {avg_cand:.2f}")
    print(f"  平均每 item 被尝试 index 数:      {avg_expl:.2f}")
    print(f"  被尝试 / candidate 比例:          {ratio:.2f}%")
    print("-" * 80)

    print(f"\n  {'candidate桶':<14} {'item数':<8} {'avg_candidate':<16} {'avg_explored':<16} {'探索比例'}")
    print("  " + "-" * 70)
    for bk in ["1-5", "6-10", "11-20", ">20"]:
        pairs = bucket_data.get(bk, [])
        if not pairs:
            continue
        sc = sum(p[0] for p in pairs)
        se = sum(p[1] for p in pairs)
        n = len(pairs)
        print(
            f"  {bk:<14} {n:<8} {sc / n:<16.2f} {se / n:<16.2f} "
            f"{se / sc * 100 if sc else 0:.2f}%"
        )
    print("-" * 80)


def _collect_explored_join_with_coverage(
    nodes: List[Dict[str, Any]], cand_join_list: List[str],
) -> Set[str]:
    executed_joins: Set[str] = set()
    for n in nodes:
        for h in n.get("executed_hints", []):
            if _is_join_hint(h):
                executed_joins.add(h)
    explored: Set[str] = set()
    for cand in cand_join_list:
        if _join_candidate_covered_by_executed(cand, executed_joins):
            explored.add(cand)
    return explored


def run_join_exploration(
    item_data_list: List[Tuple[Dict[str, Any], List[Dict[str, Any]]]],
) -> None:
    total_candidate = 0
    total_explored = 0
    items_with_join = 0

    for item_info, nodes in item_data_list:
        cand_join = item_info.get("candidate_hints", {}).get("join_order", [])
        if not cand_join:
            continue
        items_with_join += 1
        explored = _collect_explored_join_with_coverage(nodes, cand_join)
        total_candidate += len(cand_join)
        total_explored += len(explored)

    print("\n" + "=" * 80)
    print("[Join 探索分析] candidate join_order 中有多少被实际尝试 (覆盖: JOIN_PREFIX(a,b) 覆盖 JOIN_PREFIX(a))")
    print("-" * 80)
    if items_with_join == 0:
        print("无含 join candidate 的 item。")
        return
    print(f"  含 join candidate 的 item 数:   {items_with_join}")
    print(f"  candidate join 总数:             {total_candidate}")
    print(f"  被尝试的 candidate join (去重):  {total_explored}")
    ratio = total_explored / total_candidate * 100 if total_candidate else 0
    print(f"  被尝试 / candidate 比例:         {ratio:.2f}%")
    print("-" * 80)


def _collect_explored_set_var_with_coverage(
    nodes: List[Dict[str, Any]], cand_config_list: List[str],
) -> Set[str]:
    executed_configs: Set[str] = set()
    for n in nodes:
        for h in n.get("executed_hints", []):
            if _is_config_hint(h):
                executed_configs.add(h)
    explored: Set[str] = set()
    for cand in cand_config_list:
        if _set_var_candidate_covered_by_executed(cand, executed_configs):
            explored.add(cand)
    return explored


def run_set_var_exploration(
    item_data_list: List[Tuple[Dict[str, Any], List[Dict[str, Any]]]],
) -> None:
    total_candidate = 0
    total_explored = 0
    items_with_config = 0

    for item_info, nodes in item_data_list:
        cand_config = item_info.get("candidate_hints", {}).get("config", [])
        if not cand_config:
            continue
        items_with_config += 1
        explored = _collect_explored_set_var_with_coverage(nodes, cand_config)
        total_candidate += len(cand_config)
        total_explored += len(explored)

    print("\n" + "=" * 80)
    print("[Set_var 探索分析] candidate config 中有多少被实际尝试 (覆盖: 包含 key 即覆盖, 如 materialization)")
    print("-" * 80)
    if items_with_config == 0:
        print("无含 config candidate 的 item。")
        return
    print(f"  含 config candidate 的 item 数:  {items_with_config}")
    print(f"  candidate config 总数:          {total_candidate}")
    print(f"  被尝试的 candidate config (去重): {total_explored}")
    ratio = total_explored / total_candidate * 100 if total_candidate else 0
    print(f"  被尝试 / candidate 比例:        {ratio:.2f}%")
    print("-" * 80)


# ===================================================================
# Section 5 — 三种 hint candidate 比例
# ===================================================================

def run_candidate_type_ratio(
    item_data_list: List[Tuple[Dict[str, Any], List[Dict[str, Any]]]],
) -> None:
    total_index = 0
    total_join = 0
    total_config = 0
    n_items = 0

    for item_info, _nodes in item_data_list:
        ch = item_info.get("candidate_hints", {})
        ni = len(ch.get("index", []))
        nj = len(ch.get("join_order", []))
        nc = len(ch.get("config", []))
        if ni + nj + nc == 0:
            continue
        n_items += 1
        total_index += ni
        total_join += nj
        total_config += nc

    grand = total_index + total_join + total_config

    print("\n" + "=" * 60)
    print("[Candidate Hint 类型比例]")
    print("-" * 60)
    if grand == 0:
        print("无 candidate hints。")
        return

    print(f"  含 candidate hints 的 item 数: {n_items}")
    print(f"  candidate hints 总数:          {grand}")
    print(f"  ── index:      {total_index:>8} ({total_index / grand * 100:>6.2f}%)")
    print(f"  ── join_order: {total_join:>8} ({total_join / grand * 100:>6.2f}%)")
    print(f"  ── config:     {total_config:>8} ({total_config / grand * 100:>6.2f}%)")
    print(f"  平均每 item index:      {total_index / n_items:.2f}")
    print(f"  平均每 item join_order: {total_join / n_items:.2f}")
    print(f"  平均每 item config:     {total_config / n_items:.2f}")
    print("-" * 60)


# ===================================================================
# Section 6 — A5 / A6 动作分析
# ===================================================================

def _classify_a5_change(n: Dict[str, Any]) -> str:
    parent_set = set(n.get("parent_executed_hints", []))
    cur_set = set(n.get("executed_hints", []))
    has_del = bool(parent_set - cur_set)
    has_add = bool(cur_set - parent_set)
    if has_del and has_add:
        return "del_and_add"
    if has_del:
        return "only_del"
    if has_add:
        return "only_add"
    return "unchanged"


def _run_ax_analysis(nodes: List[Dict[str, Any]], all_nodes: List[Dict[str, Any]], action_label: str, title: str) -> None:
    """通用的 A5/A6 分析逻辑。"""
    ax = [n for n in nodes if n.get("action_type") == action_label]
    total = len(ax)

    CATEGORIES = ["only_del", "only_add", "del_and_add", "unchanged"]
    CAT_LABELS = {
        "only_del": "只删除 hints",
        "only_add": "只新增 hints",
        "del_and_add": "既删又加",
        "unchanged": "完全没改",
    }

    cat_nodes: Dict[str, List[Dict[str, Any]]] = {c: [] for c in CATEGORIES}
    cat_ratios: Dict[str, List[Optional[float]]] = {c: [] for c in CATEGORIES}
    all_ax_ratios: List[Optional[float]] = []

    del_index, del_join, del_config = 0, 0, 0
    add_index, add_join, add_config = 0, 0, 0
    ratios_del_index: List[Optional[float]] = []
    ratios_del_join: List[Optional[float]] = []
    ratios_del_config: List[Optional[float]] = []
    ratios_add_index: List[Optional[float]] = []
    ratios_add_join: List[Optional[float]] = []
    ratios_add_config: List[Optional[float]] = []

    for n in ax:
        cat = _classify_a5_change(n)
        cat_nodes[cat].append(n)
        r = _improvement_ratio(n)
        cat_ratios[cat].append(r)
        all_ax_ratios.append(r)

        parent_set = set(n.get("parent_executed_hints", []))
        cur_set = set(n.get("executed_hints", []))
        deleted = parent_set - cur_set
        added = cur_set - parent_set

        if any(_is_index_hint(h) for h in deleted):
            del_index += 1
            ratios_del_index.append(r)
        if any(_is_join_hint(h) for h in deleted):
            del_join += 1
            ratios_del_join.append(r)
        if any(_is_config_hint(h) for h in deleted):
            del_config += 1
            ratios_del_config.append(r)
        if any(_is_index_hint(h) for h in added):
            add_index += 1
            ratios_add_index.append(r)
        if any(_is_join_hint(h) for h in added):
            add_join += 1
            ratios_add_join.append(r)
        if any(_is_config_hint(h) for h in added):
            add_config += 1
            ratios_add_config.append(r)

    print("\n" + "=" * 110)
    print(f"[{action_label} 动作分析] {title} ({action_label} 总节点数: {total})")
    print("-" * 110)
    if total == 0:
        print(f"无 {action_label} 节点。")
        return

    pct = lambda c: f"{c:>6} ({c / total * 100:>6.2f}%)"

    print("  分布:")
    for cat in CATEGORIES:
        print(f"    {CAT_LABELS[cat]:<14}: {pct(len(cat_nodes[cat]))}")
    print()
    print("  删除 hint 细分:")
    print(f"    删除 index:  {pct(del_index)}    删除 join:  {pct(del_join)}    删除 config: {pct(del_config)}")
    print("  新增 hint 细分:")
    print(f"    新增 index:  {pct(add_index)}    新增 join:  {pct(add_join)}    新增 config: {pct(add_config)}")

    tiers = [(0.2, ">20%"), (1.0, ">100%"), (5.0, ">500%")]
    CW, CN, CA, CM, CP = 16, 8, 10, 10, 10

    print()
    print("  改进率对比 (ratio = parent_time/cur_time - 1, 相对上一步父节点):")
    header = (
        f"    {'类别':<{CW}}{'n':>{CN}}{'avg':>{CA}}{'median':>{CM}}{'P90':>{CP}}"
        + "".join(f"{t[1]:>{CP}}" for t in tiers)
    )
    sep = "    " + "-" * (len(header) - 4)
    print(sep)
    print(header)
    print(sep)

    def _row(label: str, rs: List[Optional[float]]) -> str:
        n = len(rs)
        if n == 0:
            return (
                f"    {label:<{CW}}{'0':>{CN}}{'-':>{CA}}{'-':>{CM}}{'-':>{CP}}"
                + "".join(f"{'-':>{CP}}" for _ in tiers)
            )
        avg, median, p90, _ = _stats_for_ratios(rs)
        parts = [f"    {label:<{CW}}", f"{n:>{CN}}", f"{avg:>{CA}.4f}", f"{median:>{CM}.4f}", f"{p90:>{CP}.4f}"]
        for thr, _ in tiers:
            c = sum(1 for x in rs if x is not None and x > thr)
            parts.append(f"{c / n * 100:>{CP}.2f}%")
        return "".join(parts)

    print(_row(f"{action_label} 全部", all_ax_ratios))
    for cat in CATEGORIES:
        print(_row(CAT_LABELS[cat], cat_ratios[cat]))
    print("    " + "-" * (len(header) - 4))
    print(_row("  删 index", ratios_del_index))
    print(_row("  删 join", ratios_del_join))
    print(_row("  删 config", ratios_del_config))
    print(_row("  加 index", ratios_add_index))
    print(_row("  加 join", ratios_add_join))
    print(_row("  加 config", ratios_add_config))

    non_ax = [n for n in all_nodes if n.get("action_type") and n.get("action_type") != action_label]
    non_ax_ratios = [_improvement_ratio(n) for n in non_ax]
    print(_row(f"非{action_label} (参考)", non_ax_ratios))
    print(sep)


def run_a5_analysis(nodes: List[Dict[str, Any]], title: str) -> None:
    _run_ax_analysis(nodes, nodes, "A5", title)


def run_a6_analysis(nodes: List[Dict[str, Any]], title: str) -> None:
    _run_ax_analysis(nodes, nodes, "A6", title)


# ===================================================================
# Section 7 — 幻觉 hint 分析
# ===================================================================

def _is_hint_phantom(
    h: str,
    cand_index_set: Set[str],
    cand_join_list: List[str],
    cand_config_list: List[str],
) -> bool:
    if _is_index_hint(h):
        return h not in cand_index_set
    if _is_join_hint(h):
        return not _join_executed_covered_by_candidates(h, cand_join_list)
    if _is_config_hint(h):
        return not _set_var_executed_covered_by_candidates(h, cand_config_list)
    return False


def _fmt_ratio(r: Optional[float]) -> str:
    return f"{r * 100:.2f}%" if r is not None else "N/A"


def _ratio_summary(rs: List[Optional[float]]) -> str:
    n = len(rs)
    if n == 0:
        return "n=0"
    avg, median, p90, _ = _stats_for_ratios(rs)
    gt20 = sum(1 for x in rs if x is not None and x > 0.2)
    gt100 = sum(1 for x in rs if x is not None and x > 1.0)
    gt500 = sum(1 for x in rs if x is not None and x > 5.0)
    return (
        f"n={n}  avg={avg:.4f}  median={median:.4f}  P90={p90:.4f}  "
        f">20%={gt20 / n * 100:.1f}%  >100%={gt100 / n * 100:.1f}%  >500%={gt500 / n * 100:.1f}%"
    )


def run_phantom_hints_analysis(
    item_data_list: List[Tuple[Dict[str, Any], List[Dict[str, Any]]]],
) -> None:
    total_nodes = 0
    nodes_with_phantom = 0
    total_hints_checked = 0
    total_phantom_hints = 0

    phantom_by_type = {"index": 0, "join": 0, "config": 0}
    nodes_phantom_by_type = {"index": 0, "join": 0, "config": 0}

    phantom_ratios: List[Optional[float]] = []
    normal_ratios: List[Optional[float]] = []
    all_ratios: List[Optional[float]] = []

    MAX_EXAMPLES = 30
    examples: List[str] = []

    for item_info, nodes in item_data_list:
        ch = item_info.get("candidate_hints", {})
        cand_index_set = set(str(x) for x in ch.get("index", []))
        cand_join_list = ch.get("join_order", [])
        cand_config_list = ch.get("config", [])

        has_any_candidate = cand_index_set or cand_join_list or cand_config_list
        if not has_any_candidate:
            continue

        for n in nodes:
            hints = n.get("executed_hints", [])
            if not hints:
                continue

            checked = [h for h in hints if _is_index_hint(h) or _is_join_hint(h) or _is_config_hint(h)]
            if not checked:
                continue

            total_nodes += 1
            total_hints_checked += len(checked)
            r = _improvement_ratio(n)
            all_ratios.append(r)

            phantoms = [
                h for h in checked
                if _is_hint_phantom(h, cand_index_set, cand_join_list, cand_config_list)
            ]
            if not phantoms:
                normal_ratios.append(r)
                continue

            nodes_with_phantom += 1
            total_phantom_hints += len(phantoms)
            phantom_ratios.append(r)

            has_idx = has_jn = has_cfg = False
            for p in phantoms:
                if _is_index_hint(p):
                    phantom_by_type["index"] += 1
                    has_idx = True
                elif _is_join_hint(p):
                    phantom_by_type["join"] += 1
                    has_jn = True
                elif _is_config_hint(p):
                    phantom_by_type["config"] += 1
                    has_cfg = True
            if has_idx:
                nodes_phantom_by_type["index"] += 1
            if has_jn:
                nodes_phantom_by_type["join"] += 1
            if has_cfg:
                nodes_phantom_by_type["config"] += 1

            if len(examples) < MAX_EXAMPLES:
                examples.append(
                    f"  file: {n.get('file_path')}\n"
                    f"    node={n.get('node_id')}  action={n.get('action_type')}  "
                    f"ratio={_fmt_ratio(r)}\n"
                    f"    candidate_index: {list(ch.get('index', []))}\n"
                    f"    candidate_join:  {cand_join_list}\n"
                    f"    candidate_config: {cand_config_list}\n"
                    f"    actual_hints:    {checked}\n"
                    f"    phantom_hints:   {phantoms}"
                )

    print("\n" + "=" * 110)
    print("[幻觉 Hint 分析] index/join/set_var，join 用覆盖逻辑 (JOIN_PREFIX(a,b)覆盖a,b)")
    print("-" * 100)
    if total_nodes == 0:
        print("无含 index/join/config hints 的节点。")
        return

    pct_node = nodes_with_phantom / total_nodes * 100
    pct_hint = total_phantom_hints / total_hints_checked * 100 if total_hints_checked else 0

    print(f"  检查的节点数(有 index/join/config hints):  {total_nodes}")
    print(f"  含幻觉 hint 的节点数:  {nodes_with_phantom} ({pct_node:.2f}%)")
    print(f"  检查的 index/join hint 条数: {total_hints_checked}")
    print(f"  幻觉 hint 条数:               {total_phantom_hints} ({pct_hint:.2f}%)")
    print()
    print("  按类型:")
    for tp in ("index", "join"):
        nc = nodes_phantom_by_type[tp]
        hc = phantom_by_type[tp]
        base = nodes_with_phantom if nodes_with_phantom else 1
        print(
            f"    {tp:<8}  节点数: {nc:>6} ({nc / base * 100:>6.2f}% of phantom nodes)"
            f"  hint条数: {hc:>6}"
        )

    print()
    print("  改进率对比 (ratio = parent_time/cur_time - 1):")
    print(f"    整体 (含 index/join hints):  {_ratio_summary(all_ratios)}")
    print(f"    含幻觉 hint 的节点:          {_ratio_summary(phantom_ratios)}")
    print(f"    无幻觉 hint 的节点:          {_ratio_summary(normal_ratios)}")

    if examples:
        print()
        print(f"  幻觉 hint 示例 (最多{MAX_EXAMPLES}条):")
        for ex in examples:
            print(ex)
    print("-" * 100)


# ===================================================================
# 文件处理与 main
# ===================================================================

def _process_file(
    path: str,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Tuple[Dict[str, Any], List[Dict[str, Any]]]], List[Tuple[float, List[Tuple[Optional[str], float]]]]]:
    """处理单个 .jsonl 或 .json 文件，返回 (raw_nodes, sft_nodes, item_data_list, round_data_list)。"""
    raw_nodes: List[Dict[str, Any]] = []
    sft_nodes: List[Dict[str, Any]] = []
    items: List[Tuple[Dict[str, Any], List[Dict[str, Any]]]] = []
    round_data_list: List[Tuple[float, List[Tuple[Optional[str], float]]]] = []

    def _handle_item(item: Dict[str, Any]) -> None:
        raw, sft, item_info = extract_from_item(item, path)
        raw_nodes.extend(raw)
        sft_nodes.extend(sft)
        items.append((item_info, raw))
        rd = extract_best_action_by_round(item)
        if rd is not None:
            round_data_list.append(rd)

    if path.endswith(".jsonl"):
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                item = json.loads(line)
                _handle_item(item)
    else:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, list):
            item_list = data
        else:
            item_list = [data]
        for item in item_list:
            _handle_item(item)
    return raw_nodes, sft_nodes, items, round_data_list


def main() -> None:
    parser = argparse.ArgumentParser(
        description="MCTS 节点详细分析工具 — 动作分布 / 改进比例 / Index 探索 / A5/A6 分析 / 幻觉 Hint"
    )
    parser.add_argument("--input-dir", required=True, help="MCTS JSON 文件目录")
    parser.add_argument(
        "--workers", type=int, default=8,
        help="并行进程数 (默认 8)",
    )
    args = parser.parse_args()

    file_list = iter_eval_files(args.input_dir)
    if not file_list:
        print(f"在 {args.input_dir} 中未找到任何 .jsonl/.json 文件。")
        return

    all_raw_nodes: List[Dict[str, Any]] = []
    all_sft_nodes: List[Dict[str, Any]] = []
    item_data_list: List[Tuple[Dict[str, Any], List[Dict[str, Any]]]] = []
    all_round_data: List[Tuple[float, List[Tuple[Optional[str], float]]]] = []

    workers = min(args.workers, len(file_list))

    print(f"处理 {len(file_list)} 个文件 (workers={workers})...")

    pbar = tqdm(total=len(file_list), desc="处理中", unit="文件") if HAS_TQDM else None

    with ProcessPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(_process_file, p): p for p in file_list}
        completed = 0
        for future in as_completed(futures):
            path = futures[future]
            try:
                raw, sft, items, round_data = future.result()
                all_raw_nodes.extend(raw)
                all_sft_nodes.extend(sft)
                item_data_list.extend(items)
                all_round_data.extend(round_data)
            except Exception as e:
                msg = f"  出错 {path}: {e}"
                if pbar:
                    tqdm.write(msg)
                else:
                    print(msg)
            completed += 1
            if pbar:
                pbar.update(1)
            elif completed % 10 == 0 or completed == len(file_list):
                print(f"  进度: {completed}/{len(file_list)}")

    if pbar:
        pbar.close()

    print(f"\n全量节点: {len(all_raw_nodes)}  |  SFT 筛选节点: {len(all_sft_nodes)}  |  item 数: {len(item_data_list)}  |  轮次样本: {len(all_round_data)}")

    # ---- Section 1: 动作分布 ----
    run_action_distribution(all_raw_nodes, "全量")
    run_action_distribution(all_sft_nodes, "SFT 筛选")
    run_action_child_count(all_raw_nodes, "全量")
    run_action_child_count(all_sft_nodes, "SFT 筛选")

    # ---- Section 2: 动作改进 ----
    run_action_improvement(all_raw_nodes, "全量")
    run_action_improvement(all_sft_nodes, "SFT 筛选")

    # ---- Section 2b: 按深度分层动作统计 ----
    run_action_by_depth(all_raw_nodes, "全量")
    run_action_by_depth(all_sft_nodes, "SFT 筛选")

    # ---- Section 2c: 按 1-9 轮最佳 hints 追溯动作比例 ----
    run_action_proportion_by_round(all_round_data)

    # ---- Section 3: Hint 分类统计 + 改进 ----
    raw_with_nh = [n for n in all_raw_nodes if n.get("new_hints")]
    sft_with_nh = [n for n in all_sft_nodes if n.get("new_hints")]
    run_hint_category_stats(raw_with_nh, "全量 — 仅含新增 hints 的节点")
    run_hint_category_stats(sft_with_nh, "SFT 筛选 — 仅含新增 hints 的节点")
    run_hint_improvement(raw_with_nh, "全量")
    run_hint_improvement(sft_with_nh, "SFT 筛选")

    # ---- Section 4: Index / Join / Set_var 探索分析 ----
    run_index_exploration(item_data_list)
    run_join_exploration(item_data_list)
    run_set_var_exploration(item_data_list)

    # ---- Section 5: Candidate Hint 类型比例 ----
    run_candidate_type_ratio(item_data_list)

    # ---- Section 6: A5 / A6 分析 ----
    run_a5_analysis(all_raw_nodes, "全量")
    run_a5_analysis(all_sft_nodes, "SFT 筛选")
    run_a6_analysis(all_raw_nodes, "全量")
    run_a6_analysis(all_sft_nodes, "SFT 筛选")

    # ---- Section 7: 幻觉 Hint 分析 ----
    run_phantom_hints_analysis(item_data_list)


if __name__ == "__main__":
    main()
