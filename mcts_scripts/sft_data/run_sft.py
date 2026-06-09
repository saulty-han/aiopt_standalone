#!/usr/bin/env python3
"""
SFT 数据生成脚本

从 MCTS JSON 结果目录提取节点，按动作（A1-A6）分桶，每个动作应用
独立的过滤/打分规则，排序后按配额选取；再补充约 10% 的「不调整」样本
（new_hints 与 deleted_hints 均为空），最终填充 prompt 生成 SFT 训练数据。

动作配额在 A1-A6 之间均匀分配（各占总样本数的 1/6）。

Prompt 模板直接复用 `mcts/utils/prompts.py`，保证训练与在线推理的 prompt
完全一致（单一 source-of-truth）。

Usage:
    python mcts_scripts/sft_data/run_sft.py \
        --input-dir /path/to/mcts/output \
        --output-dir /path/to/sft/output

    # 指定总样本上限
    python mcts_scripts/sft_data/run_sft.py \
        --input-dir /path/to/mcts/output \
        --output-dir /path/to/sft/output \
        --max-samples 30000

    # 调整 baseline 过滤阈值（秒）与不调整样本比例
    python mcts_scripts/sft_data/run_sft.py \
        --input-dir /path/to/mcts/output \
        --output-dir /path/to/sft/output \
        --min-baseline 0.1 \
        --no-change-ratio 0.10

    # 调整 improve 判定阈值：单步改进（相对父节点）需 >= 15% 才算 improve
    python mcts_scripts/sft_data/run_sft.py \
        --input-dir /path/to/mcts/output \
        --output-dir /path/to/sft/output \
        --min-step-improvement 0.15
"""
from __future__ import annotations

import argparse
import json
import math
import os
import re
import sys
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

# ---------------------------------------------------------------------------
# 让本脚本可以直接运行，同时复用 mcts.utils.prompts 作为唯一 prompt 源
# ---------------------------------------------------------------------------

_PROJECT_ROOT = os.path.abspath(
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..")
)
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from mcts.types import ActionType  # noqa: E402
from mcts.utils.prompts import (  # noqa: E402
    _ACTION_TEMPLATES,
    ACTION_HINT_CATEGORY,
    HintCategory,
    build_enhanced_candidate_hints,
    filter_candidate_hints,
    format_index_info,
)

try:
    from tqdm import tqdm
    HAS_TQDM = True
except ImportError:
    HAS_TQDM = False
    print("提示: 未安装 tqdm，将使用简单进度输出。可通过 pip install tqdm 安装。")


# ============================================================================
# 基础常量
# ============================================================================

ACTION_ORDER = ["A1", "A2", "A3", "A4", "A5", "A6"]


# ============================================================================
# 通用工具
# ============================================================================

def iter_json_files(input_dir: str) -> List[str]:
    """收集目录下所有 .json / .jsonl 文件路径。"""
    paths: List[str] = []
    for root, _, files in os.walk(input_dir):
        for name in files:
            if name.endswith(".json") or name.endswith(".jsonl"):
                paths.append(os.path.join(root, name))
    return sorted(paths)


def load_json_file(path: str) -> Optional[Any]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError):
        return None


def _progress(iterable, desc: str, unit: str = "条"):
    return tqdm(iterable, desc=desc, unit=unit) if HAS_TQDM else iterable


def get_parent_tag(tag: str) -> Optional[str]:
    if "." not in tag:
        return None
    return tag.rsplit(".", 1)[0]


def get_ancestor_tags(tag: str) -> List[str]:
    parts = tag.split(".")
    return [".".join(parts[:i]) for i in range(1, len(parts))]


def _maybe_parse_json(value: Any) -> Any:
    """尝试将字符串解析为 JSON，失败则原样返回。"""
    if not isinstance(value, str):
        return value
    stripped = value.strip()
    if not stripped or stripped[0] not in "[{":
        return value
    try:
        return json.loads(stripped)
    except json.JSONDecodeError:
        return value


# ============================================================================
# Hint 分类工具（用于幻觉检测）
# ============================================================================

_SET_VAR_KEYS = [
    "materialization=off", "derived_merge=off", "semijoin=off",
    "loosescan=off", "firstmatch=off", "duplicateweedout=off",
    "subquery_materialization_cost_based=off", "subquery_to_derived=on",
]


def _extract_optimizer_switch_keys(hint: str) -> Set[str]:
    """从 SET_VAR(optimizer_switch='...') 中提取所有 key 名（忽略 =on/off 的值部分）。

    例：SET_VAR(optimizer_switch='materialization=off, firstmatch=on')
    → {'materialization', 'firstmatch'}
    """
    m = re.search(r"optimizer_switch\s*=\s*['\"]([^'\"]+)['\"]", hint, re.IGNORECASE)
    if not m:
        return set()
    keys: Set[str] = set()
    for pair in m.group(1).split(","):
        pair = pair.strip()
        if "=" in pair:
            keys.add(pair.split("=")[0].strip().lower())
    return keys


def _build_candidate_config_keys(cand_config_list: List[str]) -> Set[str]:
    """从 candidate_hints.config 列表中提取所有合法的 optimizer_switch key 集合。"""
    allowed: Set[str] = set()
    for c in cand_config_list:
        allowed |= _extract_optimizer_switch_keys(c)
    return allowed


def _is_index_hint(h: str) -> bool:
    u = (h or "").upper().strip()
    return u.startswith("INDEX(") or u.startswith("NO_INDEX(")


def _is_join_hint(h: str) -> bool:
    u = (h or "").upper().strip()
    return u.startswith("JOIN_PREFIX(") or u.startswith("JOIN_SUFFIX(")


def _is_config_hint(h: str) -> bool:
    return (h or "").upper().strip().startswith("SET_VAR(")


def _parse_join_tables(h: str) -> Set[str]:
    m = re.match(r"JOIN_(PREFIX|SUFFIX)\s*\(\s*(.+?)\s*\)", (h or "").upper().strip())
    if not m:
        return set()
    inner = m.group(2).strip()
    return {t.strip() for t in re.split(r"[,&\s]+", inner) if t.strip()}


def _join_covered(executed: str, cand_join_list: List[str]) -> bool:
    ex_tables = _parse_join_tables(executed)
    if not ex_tables:
        return True
    ex_type = "PREFIX" if "PREFIX" in executed.upper() else "SUFFIX"
    cand_by_type = [c for c in cand_join_list if ex_type in str(c).upper() and _parse_join_tables(c)]
    for t in ex_tables:
        if not any(t in _parse_join_tables(c) for c in cand_by_type):
            return False
    return True


def _setvar_covered(executed: str, cand_config_list: List[str]) -> bool:
    """检查 SET_VAR(optimizer_switch=...) 是否合法。

    规则：
      1. SET_VAR 必须且只能包含 optimizer_switch 参数，否则视为幻觉。
      2. optimizer_switch 中的每个 key 必须都在 candidate_hints.config 中出现过。
    """
    ex_lower = (executed or "").lower()

    # 规则1：必须含 optimizer_switch
    if "optimizer_switch" not in ex_lower:
        return False

    # 规则1：不能含除 optimizer_switch 以外的其他 SET_VAR 参数
    # SET_VAR(...) 括号内去掉 optimizer_switch='...' 后不应还有其他 key=value
    inner_m = re.search(r"SET_VAR\s*\((.+)\)", executed, re.IGNORECASE | re.DOTALL)
    if inner_m:
        inner = inner_m.group(1)
        # 去掉 optimizer_switch='...' 整段后看剩余是否还有内容
        stripped = re.sub(r"optimizer_switch\s*=\s*'[^']*'", "", inner, flags=re.IGNORECASE)
        stripped = re.sub(r'optimizer_switch\s*=\s*"[^"]*"', "", stripped, flags=re.IGNORECASE)
        stripped = stripped.replace(",", "").strip()
        if stripped:
            return False

    # 规则2：每个 optimizer_switch key 必须在候选中出现过
    exec_keys = _extract_optimizer_switch_keys(executed)
    if not exec_keys:
        return False
    allowed_keys = _build_candidate_config_keys(cand_config_list)
    return exec_keys.issubset(allowed_keys)


def _is_phantom(h: str, cand_index: Set[str], cand_join: List[str], cand_config: List[str]) -> bool:
    """单个 hint 是否脱离候选范围（幻觉）。"""
    if _is_index_hint(h):
        return h not in cand_index
    if _is_join_hint(h):
        return not _join_covered(h, cand_join)
    if _is_config_hint(h):
        return not _setvar_covered(h, cand_config)
    # 未识别类型按幻觉处理（保守）
    return True


def _extract_candidate_sets(candidate_hints: Dict[str, Any]) -> Tuple[Set[str], List[str], List[str]]:
    idx_set = set(str(x) for x in candidate_hints.get("index", []) or [])
    join_list = [str(x) for x in candidate_hints.get("join_order", []) or []]
    cfg_list = [str(x) for x in candidate_hints.get("config", []) or []]
    return idx_set, join_list, cfg_list


# ============================================================================
# 节点记录
# ============================================================================

@dataclass
class NodeRecord:
    """单个候选 SFT 节点的全部信息。"""
    file_path: str
    index: Any
    query: str
    node_tag: str
    action_type: str
    cur_time: Optional[float]
    parent_time: Optional[float]
    baseline_time: Optional[float]
    plan_digest: Optional[str]
    parent_plan_digest: Optional[str]
    default_plan_digest: Optional[str]
    executed_hints: List[str]
    new_hints: List[str]
    deleted_hints: List[str]
    candidate_hints: Dict[str, Any]
    index_info: Any
    execution_info: Any
    ancestor_texts: List[str]
    answer: str
    # 额外字段：为评分提供的聚合指标
    subtree_max_improvement: float = 0.0  # 子树内相对 baseline 的最大改进比例（保留兼容）
    subtree_min_time: Optional[float] = None  # 子树内最小 execution_time（含自身），供 A4 单步口径使用
    query_best_plan_time: Optional[float] = None  # 整棵树内最短 execution_time
    # 在 dedupe 时用的 key
    def dedupe_key(self) -> Tuple[str, str, str]:
        return (
            self.parent_plan_digest or "",
            "|".join(self.executed_hints or []),
            self.plan_digest or "",
        )


# ============================================================================
# Step 1: 节点提取 —— 保留所有可能作为 SFT 的节点（improve + no_change）
# ============================================================================

def _normalize_list(value: Any) -> List[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(x) for x in value]
    return [str(value)]


def extract_nodes_from_item(item: Dict[str, Any], file_path: str) -> List[NodeRecord]:
    """从单条查询结果中提取所有有效的 SFT 候选节点。

    这里只做最基本的结构校验，不对 action_type 做过滤；具体的改进/幻觉
    判断交给后续 per-action 评分逻辑。
    """
    results: List[NodeRecord] = []
    baseline_time = item.get("baseline_time")
    baseline_digest = item.get("plan_digest")
    candidate_hints = item.get("candidate_hints") or {}
    if not isinstance(candidate_hints, dict):
        candidate_hints = {}

    tree_nodes = item.get("mcts_tree_nodes") or {}
    if not isinstance(tree_nodes, dict):
        return results

    node_map = {
        tag: node for tag, node in tree_nodes.items()
        if isinstance(node, dict) and "node_info" in node
    }
    if not node_map:
        return results

    # ---- 子树最大改进（仅 A4 需要，但统一计算便于后续扩展） ----
    # children_by_parent: parent_tag -> [child_tag, ...]
    children_by_parent: Dict[str, List[str]] = defaultdict(list)
    for tag in node_map:
        p = get_parent_tag(tag)
        if p and p in node_map:
            children_by_parent[p].append(tag)

    # 每个节点的「子树中相对 baseline 的最大改进」
    subtree_max_impr: Dict[str, float] = {}
    # 每个节点的「子树内（含自身）最小 execution_time」，None 表示无任何有效时间
    subtree_min_t: Dict[str, Optional[float]] = {}
    # 先收集每个节点自身改进
    def _self_impr(tag: str) -> float:
        t = node_map[tag].get("db_response", {}).get("execution_time_s")
        if baseline_time is None or t is None or baseline_time <= 0:
            return 0.0
        return max(0.0, (baseline_time - t) / baseline_time)

    # 按深度降序计算（叶子先算）
    def _depth(tag: str) -> int:
        return tag.count(".")

    for tag in sorted(node_map.keys(), key=lambda t: -_depth(t)):
        cur = _self_impr(tag)
        best = cur
        self_t = node_map[tag].get("db_response", {}).get("execution_time_s")
        min_t: Optional[float] = self_t if isinstance(self_t, (int, float)) else None
        for ch in children_by_parent.get(tag, []):
            if ch in subtree_max_impr:
                if subtree_max_impr[ch] > best:
                    best = subtree_max_impr[ch]
            ch_min = subtree_min_t.get(ch)
            if ch_min is not None and (min_t is None or ch_min < min_t):
                min_t = ch_min
        subtree_max_impr[tag] = best
        subtree_min_t[tag] = min_t

    # 整棵树最佳执行时间
    best_plan_time: Optional[float] = None
    for node in node_map.values():
        t = node.get("db_response", {}).get("execution_time_s")
        if t is None:
            continue
        if best_plan_time is None or t < best_plan_time:
            best_plan_time = t

    # ---- 提取节点 ----
    for tag, node in node_map.items():
        node_info = node.get("node_info", {}) or {}
        db_resp = node.get("db_response", {}) or {}
        llm_resp = node.get("llm_response", {}) or {}

        action_type = node_info.get("action_type")
        if action_type not in ACTION_ORDER:
            continue

        answer = llm_resp.get("response") or ""
        if not answer:
            continue

        parent_tag = get_parent_tag(tag)
        parent = node_map.get(parent_tag) if parent_tag else None

        parent_time = None
        parent_digest = None
        if parent:
            parent_time = parent.get("db_response", {}).get("execution_time_s")
            parent_digest = parent.get("db_response", {}).get("plan_digest")

        # 收集祖先 llm_response 作为 partial_solution
        ancestor_texts: List[str] = []
        for atag in get_ancestor_tags(tag):
            anc = node_map.get(atag)
            if not anc:
                continue
            txt = (anc.get("llm_response") or {}).get("response")
            if txt:
                ancestor_texts.append(txt)

        rec = NodeRecord(
            file_path=file_path,
            index=item.get("index"),
            query=item.get("query") or "",
            node_tag=tag,
            action_type=action_type,
            cur_time=db_resp.get("execution_time_s"),
            parent_time=parent_time,
            baseline_time=baseline_time,
            plan_digest=db_resp.get("plan_digest"),
            parent_plan_digest=parent_digest,
            default_plan_digest=baseline_digest,
            executed_hints=_normalize_list(node_info.get("executed_hints")),
            new_hints=_normalize_list(node_info.get("new_hints")),
            deleted_hints=_normalize_list(node_info.get("deleted_hints")),
            candidate_hints=candidate_hints,
            index_info=item.get("index_info"),
            execution_info=item.get("execution_info"),
            ancestor_texts=ancestor_texts,
            answer=answer,
            subtree_max_improvement=subtree_max_impr.get(tag, 0.0),
            subtree_min_time=subtree_min_t.get(tag),
            query_best_plan_time=best_plan_time,
        )
        results.append(rec)

    return results


def extract_all_nodes(input_dir: str) -> List[NodeRecord]:
    file_list = iter_json_files(input_dir)
    nodes: List[NodeRecord] = []
    for path in _progress(file_list, "提取节点", "文件"):
        data = load_json_file(path)
        if data is None:
            continue
        items = data if isinstance(data, list) else [data]
        for item in items:
            if not isinstance(item, dict):
                continue
            nodes.extend(extract_nodes_from_item(item, path))
    if not HAS_TQDM and file_list:
        print(f"  处理完成: {len(file_list)} 个文件, 提取 {len(nodes)} 个节点")
    return nodes


# ============================================================================
# Step 2: 去重 + baseline 过滤
# ============================================================================

def dedupe_nodes(nodes: List[NodeRecord]) -> List[NodeRecord]:
    """按 (parent_digest, executed_hints, cur_digest) 去重。"""
    seen: Set[Tuple[str, str, str]] = set()
    out: List[NodeRecord] = []
    for n in nodes:
        k = n.dedupe_key()
        if k in seen:
            continue
        seen.add(k)
        out.append(n)
    return out


def filter_by_baseline(nodes: List[NodeRecord], min_baseline: float) -> Tuple[List[NodeRecord], int]:
    kept: List[NodeRecord] = []
    skipped = 0
    for n in nodes:
        if n.baseline_time is not None and n.baseline_time > min_baseline:
            kept.append(n)
        else:
            skipped += 1
    return kept, skipped


# ============================================================================
# Step 3: 分类 —— improve vs no_change
# ============================================================================

def _is_no_change(n: NodeRecord) -> bool:
    return (not n.new_hints) and (not n.deleted_hints)


# ============================================================================
# Step 4: Per-action 过滤 + 评分
# ============================================================================

@dataclass
class ScoredNode:
    node: NodeRecord
    score: float


def _effective_parent_time(n: NodeRecord) -> Optional[float]:
    """相对上一步的 base time：有父节点用父节点时间，否则回退到 baseline_time。

    根节点（depth=0）没有 parent_time，但概念上"上一步"就是未加 hints 的 baseline。
    """
    if n.parent_time is not None and n.parent_time > 0:
        return n.parent_time
    if n.baseline_time is not None and n.baseline_time > 0:
        return n.baseline_time
    return None


def _relative_improvement(n: NodeRecord) -> Optional[float]:
    """相对上一步的改进比例 (base - cur) / base，根节点 base 取 baseline_time。"""
    base = _effective_parent_time(n)
    if base is None or n.cur_time is None or base <= 0:
        return None
    return (base - n.cur_time) / base


def _score_hint_action(
    nodes: List[NodeRecord],
    action: str,
    required_kind: str,  # "index" | "join_order" | "config" | "all"
    min_step_improvement: float,
) -> List[ScoredNode]:
    """A1/A2/A3 通用打分：去幻觉 + 按相对上一步的改进比例排序。

    过滤条件：
      - action_type 匹配
      - new_hints 非空（即必须是"调整"样本；no-change 由另一个池子负责）
      - 所有 new_hints 均属于 required_kind（A1→index, A2→join, A3→config），
        且都在对应候选列表中（非幻觉）
      - 相对上一步的单步改进比例 >= min_step_improvement，
        plan_digest 发生变化
      - 祖先链上不存在比当前节点更快的方案
    """
    scored: List[ScoredNode] = []
    c_total = c_no_change = c_no_hints = c_wrong_kind = c_phantom = 0
    c_low_improvement = c_same_digest = 0
    for n in nodes:
        if n.action_type != action:
            continue
        c_total += 1
        # 显式排除 no-change 样本（new_hints/deleted_hints 全空）
        if _is_no_change(n):
            c_no_change += 1
            continue
        if not n.new_hints:
            c_no_hints += 1
            continue

        # 类型 + 幻觉检查
        cand_index, cand_join, cand_cfg = _extract_candidate_sets(n.candidate_hints)

        def _matches_kind(h: str) -> bool:
            if required_kind == "index":
                return _is_index_hint(h)
            if required_kind == "join_order":
                return _is_join_hint(h)
            if required_kind == "config":
                return _is_config_hint(h)
            return True  # "all"

        if not all(_matches_kind(h) for h in n.new_hints):
            c_wrong_kind += 1
            continue
        if any(_is_phantom(h, cand_index, cand_join, cand_cfg) for h in n.new_hints):
            c_phantom += 1
            continue

        ratio = _relative_improvement(n)
        if ratio is None or ratio < min_step_improvement:
            c_low_improvement += 1
            continue
        if not n.plan_digest or n.plan_digest == n.parent_plan_digest:
            c_same_digest += 1
            continue

        scored.append(ScoredNode(node=n, score=ratio))
    scored.sort(key=lambda s: s.score, reverse=True)

    c_passed = len(scored)
    print(
        f"    [{action}] total={c_total}  "
        f"no_change={c_no_change}  no_hints={c_no_hints}  "
        f"wrong_kind={c_wrong_kind}  phantom={c_phantom}  "
        f"low_improvement={c_low_improvement}  same_digest={c_same_digest}  "
        f"→ passed={c_passed}"
    )
    return scored


def _score_a4(nodes: List[NodeRecord], min_step_improvement: float) -> List[ScoredNode]:
    """A4：过滤掉产生 new_hints 的节点，按"子树最优计划 vs 父节点"的单步改进排序。

    A4 节点自身不改动 hints（execution_time 等同于父节点），单步改进在节点自身
    层面恒为 0。A4 的真实价值体现在它分解出的子问题被解决之后：取子树内最小
    execution_time 相对 A4 的"上一步"（父节点，根 A4 回退到 baseline）的改进。
    """
    scored: List[ScoredNode] = []
    c_total = c_has_hints = c_no_base = c_no_subtree = c_low_improvement = 0
    for n in nodes:
        if n.action_type != "A4":
            continue
        c_total += 1
        # A4 不应该产生 new_hints（核心过滤规则）
        if n.new_hints:
            c_has_hints += 1
            continue
        base = _effective_parent_time(n)
        if base is None or base <= 0:
            c_no_base += 1
            continue
        if n.subtree_min_time is None:
            c_no_subtree += 1
            continue
        score = (base - n.subtree_min_time) / base
        if score < min_step_improvement:
            c_low_improvement += 1
            continue
        scored.append(ScoredNode(node=n, score=score))
    scored.sort(key=lambda s: s.score, reverse=True)

    c_passed = len(scored)
    print(
        f"    [A4] total={c_total}  "
        f"has_hints={c_has_hints}  no_base={c_no_base}  no_subtree={c_no_subtree}  "
        f"low_improvement={c_low_improvement}  "
        f"→ passed={c_passed}"
    )
    return scored


def _score_combination_action(
    nodes: List[NodeRecord], action: str, min_step_improvement: float,
) -> List[ScoredNode]:
    """A5/A6：去幻觉（所有 executed_hints 都要在候选列表中），按单步改进比例排序。

    A5 和 A6 的改进口径统一：都按"相对上一步（父节点）"的单步改进比例，
    阈值为 min_step_improvement。根节点的父节点时间回退到 baseline_time。

    A5 排除 no-change 样本（由 _score_no_change 负责）；
    A6 作为终态总结节点，new_hints 天然为空，不做 no-change 豁免——所有有效的
    A6 节点都归入 improve 池。
    """
    scored: List[ScoredNode] = []
    c_total = c_no_change = c_no_hints = c_phantom = c_no_time = c_low_improvement = 0
    for n in nodes:
        if n.action_type != action:
            continue
        c_total += 1
        # A5 排除 no_change；A6 不排除（见 docstring）
        if action == "A5" and _is_no_change(n):
            c_no_change += 1
            continue
        if not n.executed_hints:
            c_no_hints += 1
            continue

        cand_index, cand_join, cand_cfg = _extract_candidate_sets(n.candidate_hints)
        if any(_is_phantom(h, cand_index, cand_join, cand_cfg) for h in n.executed_hints):
            c_phantom += 1
            continue

        if n.cur_time is None:
            c_no_time += 1
            continue

        ratio = _relative_improvement(n)
        if ratio is None or ratio < min_step_improvement:
            c_low_improvement += 1
            continue

        scored.append(ScoredNode(node=n, score=ratio))
    scored.sort(key=lambda s: s.score, reverse=True)

    c_passed = len(scored)
    print(
        f"    [{action}] total={c_total}  "
        f"no_change={c_no_change}  no_hints={c_no_hints}  phantom={c_phantom}  "
        f"no_time={c_no_time}  low_improvement={c_low_improvement}  "
        f"→ passed={c_passed}"
    )
    return scored


ActionScorer = Callable[[List[NodeRecord]], List[ScoredNode]]


def _build_action_scorers(min_step_improvement: float) -> Dict[str, ActionScorer]:
    """构造按阈值参数化的 per-action 评分函数字典。

    所有动作（A1-A6）的 improve 判定统一采用"相对父节点的单步改进"口径；
    A4 由于自身不改动 hints，改用"子树最优计划 vs 父节点"的单步改进替代。
    """
    return {
        "A1": lambda ns: _score_hint_action(ns, "A1", "index", min_step_improvement),
        "A2": lambda ns: _score_hint_action(ns, "A2", "join_order", min_step_improvement),
        "A3": lambda ns: _score_hint_action(ns, "A3", "config", min_step_improvement),
        "A4": lambda ns: _score_a4(ns, min_step_improvement),
        "A5": lambda ns: _score_combination_action(ns, "A5", min_step_improvement),
        "A6": lambda ns: _score_combination_action(ns, "A6", min_step_improvement),
    }


# ============================================================================
# Step 5: No-change 样本打分
# ============================================================================

def _score_no_change(nodes: List[NodeRecord], action: str) -> List[ScoredNode]:
    """不调整样本打分：

    - 仅保留 action_type 匹配、new_hints/deleted_hints 均为空的节点。
    - 偏好「最优计划时间与 baseline 基本相同」的查询（即 baseline 本身就是最优）。
    - 分数越小越靠前（差距越小）：score = |best_plan_time - baseline| / baseline；
      对排序取负数，保证和 improve 分数方向一致（越大越好）。

    A4 按定义没有 new_hints，不在此池中参与（所有 A4 节点都归属 improve 池）。
    A6 作为终态总结节点，最终答案必然包含一组完整 hints，"不调整"无业务含义，
    因此也不参与此池。
    """
    if action in ("A4", "A6"):
        return []
    scored: List[ScoredNode] = []
    for n in nodes:
        if n.action_type != action:
            continue
        if not _is_no_change(n):
            continue
        if n.baseline_time is None or n.baseline_time <= 0:
            continue
        best = n.query_best_plan_time if n.query_best_plan_time is not None else n.baseline_time
        relative_diff = abs(best - n.baseline_time) / n.baseline_time
        scored.append(ScoredNode(node=n, score=-relative_diff))
    scored.sort(key=lambda s: s.score, reverse=True)
    return scored


# ============================================================================
# Step 6: 按配额抽取
# ============================================================================

@dataclass
class SelectionReport:
    action: str
    quota: int
    improve_kept: int
    improve_available: int
    no_change_kept: int
    no_change_available: int


def select_per_action(
    nodes: List[NodeRecord],
    max_samples: Optional[int],
    no_change_ratio: float,
    min_step_improvement: float,
) -> Tuple[List[NodeRecord], List[SelectionReport]]:
    """按动作配额选择样本。

    6 个动作（A1-A6）的配额比例固定为均匀 1/6。

    配额规则：
      - 指定 max_samples：每个动作配额 = max_samples // 6；动作内部按
        no_change_ratio 切片（默认 10% 给 no-change，90% 给 improve），
        两个桶各拿各的，任一桶不够都不从另一桶挪用。
      - 未指定（无限配额）：每个动作 improve 池全量采纳；no-change 样本数
        上限 = improve_kept * no_change_ratio（按实际收到的 improve 动态约束），
        确保 no-change 不超过 improve 的 no_change_ratio 倍。

    Returns:
        picked_nodes, per-action selection reports.
    """
    if max_samples is None or max_samples <= 0:
        action_quotas: Dict[str, Optional[int]] = {a: None for a in ACTION_ORDER}
    else:
        per_action = max_samples // len(ACTION_ORDER)
        action_quotas = {a: per_action for a in ACTION_ORDER}

    action_scorers = _build_action_scorers(min_step_improvement)

    picked: List[NodeRecord] = []
    reports: List[SelectionReport] = []

    print(f"\n── 节点过滤统计（min_step_improvement={min_step_improvement:.0%}）──")
    for action in ACTION_ORDER:
        improve_pool = action_scorers[action](nodes)
        nochange_pool = _score_no_change(nodes, action)

        quota = action_quotas[action]
        if quota is None:
            # 无上限：improve 全量采纳；no-change 上限按 improve_kept * ratio
            picked_improve = [s.node for s in improve_pool]
            nc_cap = int(math.floor(len(picked_improve) * no_change_ratio))
            picked_nochange = [s.node for s in nochange_pool[:nc_cap]]
        else:
            n_no_change = int(math.floor(quota * no_change_ratio))
            n_improve = quota - n_no_change

            # 严格固定配额：improve 和 no-change 各拿各的，
            # 任意一桶不够就照实收，不从另一桶挪用。
            picked_improve = [s.node for s in improve_pool[:n_improve]]
            picked_nochange = [s.node for s in nochange_pool[:n_no_change]]

        picked.extend(picked_improve)
        picked.extend(picked_nochange)

        reports.append(SelectionReport(
            action=action,
            quota=quota if quota is not None else -1,
            improve_kept=len(picked_improve),
            improve_available=len(improve_pool),
            no_change_kept=len(picked_nochange),
            no_change_available=len(nochange_pool),
        ))
    total_improve = sum(r.improve_kept for r in reports)
    total_nochange = sum(r.no_change_kept for r in reports)
    print(f"── 配额采样结果：improve={total_improve}  no_change={total_nochange}  total={total_improve+total_nochange} ──\n")
    return picked, reports


# ============================================================================
# Step 7: 输出校验 —— 信任新 prompt 下 LLM 的原文输出，仅做合格性过滤
# ============================================================================
#
# 新 prompt 已明确要求 LLM 输出：
#   - [动作] A* <名称>\n[步骤] {step_number} 作为头部；
#   - A1/A2/A3/A5/A6 以 <hints> /*+ ... */ </hints> 收尾；
#   - A4 以 <subproblem>...</subproblem> 收尾；
#   - 结束后不得再输出任何内容。
#
# 因此 SFT 目标直接使用 LLM 原文即可。这里只做两件事：
#   1. 去掉首尾多余空白；
#   2. 验证最后一个应有的结构化标签存在，否则丢弃这条样本（认为生成被截断或跑偏）。

_HAS_HINTS_CLOSE = re.compile(r"</\s*hints\s*>", re.IGNORECASE)
_HAS_SUBPROBLEM_CLOSE = re.compile(r"</\s*subproblem\s*>", re.IGNORECASE)


def _postprocess_answer(raw: str, action: str) -> Optional[str]:
    """Validate & trim the LLM response to serve as the SFT target.

    Returns the trimmed text when the response is well-formed under the current
    prompt convention; returns None (→ drop the sample) otherwise.
    """
    if not raw:
        return None
    text = raw.strip()
    if not text:
        return None

    if action == "A4":
        if not _HAS_SUBPROBLEM_CLOSE.search(text):
            return None
    else:
        if not _HAS_HINTS_CLOSE.search(text):
            return None
    return text


# ============================================================================
# Step 8: 构造 SFT 样本（使用 prompts.py 作为唯一模板源）
# ============================================================================

def _action_enum(action: str) -> ActionType:
    return ActionType(action)


def _format_execution_info(execution_info: Any) -> str:
    execution_info = _maybe_parse_json(execution_info)
    if isinstance(execution_info, (dict, list)):
        return json.dumps(execution_info, ensure_ascii=False, indent=2)
    if execution_info is None:
        return ""
    return str(execution_info)


def build_sft_sample(n: NodeRecord) -> Optional[Dict[str, Any]]:
    """用 prompts.py 的模板构造 SFT 样本。

    返回 LLaMA-Factory 风格字段：
      - system      : 模板 system
      - instruction : 模板 task.format(...)
      - input       : 空（所有可变内容已填入 instruction）
      - output      : 后处理后的答案
      - history / message : 保留原字段以兼容下游
    """
    try:
        action_enum = _action_enum(n.action_type)
    except ValueError:
        return None
    if action_enum not in _ACTION_TEMPLATES:
        return None

    template = _ACTION_TEMPLATES[action_enum]

    # 处理候选 hints：按动作类型筛选 + 附加 description
    raw_candidate_hints = _maybe_parse_json(n.candidate_hints)
    if not isinstance(raw_candidate_hints, dict):
        raw_candidate_hints = {}
    filtered = filter_candidate_hints(raw_candidate_hints, action_enum)
    enhanced = build_enhanced_candidate_hints(
        filtered, action_enum, n.index_info or {},
    )
    candidate_hints_text = json.dumps(enhanced, ensure_ascii=False)

    context = {
        "query": n.query,
        "execution_info": _format_execution_info(n.execution_info),
        "candidate_hints": candidate_hints_text,
        "partial_solution": "\n\n".join(t for t in (n.ancestor_texts or []) if t),
        "step_number": len(n.ancestor_texts or []) + 1,
    }

    try:
        instruction_text = template.task.format(**context)
    except KeyError:
        return None

    output = _postprocess_answer(raw=n.answer, action=n.action_type)
    if output is None:
        return None

    return {
        "system": template.system,
        "instruction": instruction_text,
        "input": "",
        "output": output,
        "history": [],
        "message": "",
    }


# ============================================================================
# 主流程
# ============================================================================

def main() -> None:
    parser = argparse.ArgumentParser(
        description="SFT 数据生成: 提取 → 去重 → 按动作配额评分 → 填充 prompt",
    )
    parser.add_argument("--input-dir", required=True, help="MCTS JSON 文件目录（可含子目录）")
    parser.add_argument("--output-dir", default="sft_output", help="SFT 输出目录 (默认: sft_output/)")
    parser.add_argument(
        "--min-baseline", type=float, default=0.1,
        help="过滤 baseline_time <= 此值(秒) 的节点（默认: 0.1）",
    )
    parser.add_argument(
        "--max-samples", type=int, default=-1,
        help="最大总样本数上限，-1 表示不限制（默认: -1）。"
             "动作配额在 A1-A6 之间均匀分配（各占 1/6）。",
    )
    parser.add_argument(
        "--no-change-ratio", type=float, default=0.10,
        help="每个动作配额中保留「不调整」样本的比例（默认: 0.10）",
    )
    parser.add_argument(
        "--min-step-improvement", type=float, default=0.20,
        help="improve 样本的最小单步改进阈值（相对父节点；根节点回退到 baseline）。"
             "默认 0.20 —— 即节点执行时间相比上一步至少缩短 20%% 才算 improve。"
             "适用于所有动作（A1-A6）；A4 由于自身不改 hints，采用"
             "「子树最优计划 vs 父节点」作为其单步改进替代。",
    )
    args = parser.parse_args()

    os.makedirs(args.output_dir, exist_ok=True)

    max_samples = args.max_samples if args.max_samples > 0 else None

    print(f"\n{'='*60}")
    print(f"输入目录:         {args.input_dir}")
    print(f"输出目录:         {args.output_dir}")
    print(f"最小baseline:     {args.min_baseline}s")
    print(f"最大样本数:       {'无限制' if max_samples is None else max_samples}")
    print(f"不调整样本比例:   {args.no_change_ratio:.2%}")
    print(f"单步改进阈值:     {args.min_step_improvement:.2%}（相对父节点）")
    print(f"动作配额:         A1-A6 均匀分配（各 1/6）")
    print(f"{'='*60}")

    # Step 1: 提取
    print("\n[1/5] 提取节点...")
    nodes = extract_all_nodes(args.input_dir)
    print(f"  原始节点数: {len(nodes)}")

    # Step 2: 去重
    print("\n[2/5] 去重...")
    deduped = dedupe_nodes(nodes)
    print(f"  去重后: {len(deduped)} (去除 {len(nodes) - len(deduped)})")

    # Step 3: baseline 过滤
    print(f"\n[3/5] 过滤 baseline <= {args.min_baseline}s ...")
    filtered, skipped_bl = filter_by_baseline(deduped, args.min_baseline)
    print(f"  过滤后: {len(filtered)} (跳过 {skipped_bl})")

    # Step 4: per-action 评分 + 配额抽取
    print("\n[4/5] 按动作配额评分并选择...")
    picked, reports = select_per_action(
        filtered, max_samples, args.no_change_ratio, args.min_step_improvement,
    )
    for r in reports:
        quota_str = str(r.quota) if r.quota >= 0 else "∞"
        print(f"  {r.action}: quota={quota_str}, improve {r.improve_kept}/{r.improve_available}, "
              f"no_change {r.no_change_kept}/{r.no_change_available}")
    print(f"  选中总数: {len(picked)}")

    # 保存中间节点（方便复查）
    steps_path = os.path.join(args.output_dir, "node_steps.json")
    with open(steps_path, "w", encoding="utf-8") as f:
        serializable = [
            {
                "file_path": n.file_path,
                "index": n.index,
                "node_tag": n.node_tag,
                "action_type": n.action_type,
                "cur_time": n.cur_time,
                "parent_time": n.parent_time,
                "baseline_time": n.baseline_time,
                "plan_digest": n.plan_digest,
                "parent_plan_digest": n.parent_plan_digest,
                "default_plan_digest": n.default_plan_digest,
                "executed_hints": n.executed_hints,
                "new_hints": n.new_hints,
                "deleted_hints": n.deleted_hints,
                "subtree_max_improvement": n.subtree_max_improvement,
                "query_best_plan_time": n.query_best_plan_time,
                "is_no_change": _is_no_change(n),
            }
            for n in picked
        ]
        json.dump(serializable, f, ensure_ascii=False, indent=2)
    print(f"  节点步骤保存至: {steps_path}")

    # Step 5: 填充 prompt
    print("\n[5/5] 生成 SFT 样本...")
    samples: List[Dict[str, Any]] = []
    for n in _progress(picked, "填充"):
        sample = build_sft_sample(n)
        if sample:
            samples.append(sample)

    sft_path = os.path.join(args.output_dir, "sft_samples.jsonl")
    with open(sft_path, "w", encoding="utf-8") as f:
        for s in samples:
            f.write(json.dumps(s, ensure_ascii=False) + "\n")

    size_mb = os.path.getsize(sft_path) / 1024 / 1024
    print(f"\n{'='*60}")
    print(f"完成!")
    print(f"  SFT 样本数: {len(samples)}")
    print(f"  文件大小:   {size_mb:.1f} MB")
    print(f"  输出:       {sft_path}")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
