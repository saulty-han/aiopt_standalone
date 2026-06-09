#!/usr/bin/env python3
"""
run_posterior_correction.py — 后验思维链订正：直接读取 tpcds_json，
对每个 MCTS 非根节点构建 [A]/[B]/[C] 三元组，通过裁判 LLM 生成修正后的 [B']，
输出 SFT JSONL + 对照表 JSONL。

【数据框架：状态→动作→反馈→修正】

  A  初始状态    当前节点被调用时的完整上下文（SQL、execution_info、candidate_hints、
                 partial_solution、step_number、action_type）。
  B  原始推理    基础模型在该节点的实际输出（llm_response.response）。
  C  物理反馈    该 Hint 在数据库上真实执行的 EXPLAIN ANALYZE + 计时信息
                 （来自 explain_analyze_info[plan_digest]）。
  B' 修正推理    裁判 LLM 依据 C，订正 B 的推理/分析文字（Hint 字面量绝对不变），
                 使其在不知晓 C 的前提下，通过扎实的代价分析合理推出 B 里那套 Hint。

SFT 样本 = (A, B')。

与现有脚本的区别
  - rebuild_sft_with_correction.py：两步骤（先生成 correction_prompt，再用
    run_corrections_llm.py 调 LLM）；裁判只返回纯文本 [B'] 或 [REJECT]。
  - 本脚本：单步骤，直接读 tpcds_json；裁判返回结构化 JSON，包含
    reasoning_score / correction_score / corrected_b / adopt / reject_reason，
    从而实现更精细的样本过滤与质量感知。

Usage
  # 全量处理 tpcds_json/
  python mcts_scripts/sft_data/run_posterior_correction.py \\
      --input-dir  mcts_scripts/tpcds_json \\
      --output-dir mcts_scripts/sft_data/posterior_corrected

  # 仅生成 prompt（不调 LLM），用于人工抽检
  python mcts_scripts/sft_data/run_posterior_correction.py \\
      --input-dir  mcts_scripts/tpcds_json \\
      --output-dir mcts_scripts/sft_data/posterior_corrected \\
      --prompts-only

  # 限制样本数、设置过滤阈值
  python mcts_scripts/sft_data/run_posterior_correction.py \\
      --input-dir  mcts_scripts/tpcds_json \\
      --output-dir mcts_scripts/sft_data/posterior_corrected \\
      --limit 20 \\
      --min-correction-score 6 \\
      --min-speedup-pct 5.0
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

_PROJECT_ROOT = os.path.abspath(
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..")
)
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from mcts.types import ActionType, LLMStatus, MCTSConfig  # noqa: E402
from mcts.utils.prompts import build_action_prompt, _ACTION_TEMPLATES  # noqa: E402

try:
    from tqdm import tqdm  # noqa: E402
    HAS_TQDM = True
except Exception:
    HAS_TQDM = False


# ============================================================================
# 裁判 LLM 提示模板（返回结构化 JSON）
# ============================================================================

CORRECTION_PROMPT_TEMPLATE = """你是顶尖数据库查询优化专家，精通代价估算（Cost Model）、物理算子实现及大模型辅助查询优化。

当前任务：对一份 MCTS 优化节点的 LLM 推理回答进行"后验错题订正"，通过注入底层代价感知，生成高质量的 SFT 训练样本。

【数据框架：状态 → 动作 → 物理反馈 → 修正】

① [初始状态 A]：MCTS 在该节点的完整上下文，包含：
   - 原始 SQL 及其当前执行计划（execution_info：包括算子、join order、代价等）
   - 可选候选 Hint 集合（candidate_hints）
   - 已走过的 MCTS 搜索路径（partial_solution）
   - 当前步骤的动作类型（如 A1 添加首个 hint、A2 叠加 hint 等）

② [原始推理 B]：基础模型针对上述状态给出的推理过程 + 最终 Hint 建议。
   基础模型往往缺乏对底层代价的敏锐感知，倾向于用表面逻辑（"加索引 = 快"）
   而非精确代价连锁反应（"加索引 → 改变基数估算 → Join Order 重排 → 消除排序"）。

③ [物理反馈 C]：将 [B] 中的 Hint 实际作用于数据库后，真实返回的：
   - 执行时间（vs 基准时间，speedup 百分比）
   - EXPLAIN ANALYZE 结果（join order、物理算子变化、实际行数、实际执行时间）

④ [修正目标 B']：**仅修改 [B] 的推理/分析文字**，保留其全部结构化 Hint 推荐，
   使 [B'] 成为：在不知晓 [C] 的前提下，基于扎实的代价模型分析，合理预测
   出 [B] 原本就给出的那套 Hint 会触发怎样的优化器行为，从而正当化这一选择。

【核心约束一：Hint 绝对不变】（违背则 adopt=false）
[B] 中的所有结构化 Hint 推荐必须在 [B'] 中原封不动保留，包括：
- <hints>/*+ ... */</hints> 块（字面量、顺序、参数全部一致）
- <subproblem>...</subproblem> 块（若存在）
- [选择候选 Hint]、[确认没有遗漏] 等段落中明确点名的 Hint 名称
只有自然语言分析/推理叙述部分（[分析...]、[推导...]、[预测...]）可以改写。

【核心约束二：因果隔离——订正错题，不是抄答案】（违背则 adopt=false）
[B'] 必须以"强烈的先验预判"和"严密的逻辑推导"口吻撰写，而非后验观察。
大模型的目标是学会"从代价分析出发，预测数据库会如何响应"，而非"观察到结果后再倒推"。

禁止使用后验词汇或泄露观察结果：
  ❌ "加上索引后，实际耗时从 10s 降到了 1s"
  ❌ "结果表明 Join Order 变成了 B-C-A"
  ❌ "实测执行计划发生了变化"

必须用推断/预测语气，且预测内容需与 [C] 实际观察吻合（这才是"错题订正"的核心）：
  ✅ "表 A 当前扫描代价极高。由于 A-B 关联列的 NDV 特性，强制索引不仅加速单表访问，
      还会打破代价平衡，大概率促使优化器将 Join Order 调整为 B-C-A，从而规避
      当前低效的 Nested Loop。因此选择 INDEX(A PRIMARY) 作为首个 hint..."

【评分维度】
- reasoning_score（0-10）：[B] 原始推理的质量
    0  = 推理完全错误、自相矛盾或毫无逻辑
    5  = 表面正确但缺乏底层代价感知
    8  = 逻辑清晰、提及了部分代价分析
    10 = 完整的、精准的代价连锁推理
- correction_score（0-10）：本次修正（从 [B] 到 [B']）对原有错误的实际纠正程度
    0  = [B'] 与 [B] 完全相同，或改动方向错误，未修正任何实质性问题
    3  = 仅做了措辞润色，核心逻辑错误（gap_analysis 中列举的问题）未被修正
    6  = 修正了主要逻辑问题，但仍有若干代价感知缺失
    8  = 绝大多数 gap_analysis 中列举的问题均已在 [B'] 中得到修正
    10 = [B'] 完整纠正了 [B] 的所有推理缺陷，代价连锁逻辑严密

【adopt 判断（综合评分 + REJECT 判据）】
满足以下任一条直接 adopt=false（REJECT，reject_reason 说明原因）：
  a. [C] 显示 speedup ≤ 2%（real_execution_time_s ≥ baseline_time × 0.98）
     （训练这类样本会让模型学会"把无效 Hint 包装成有洞察"）
  b. [C] 显示执行超时/错误（exec_time 接近 600s 或 Explain Analyze 为空/错误结构）
  c. [B] 的 Hint 明显超出 [A] 中 candidate_hints 的范围（选了不存在的索引或表名）
  d. [A] 的 execution_info 或 [C] 的 Explain Analyze 严重残缺，无法支撑任何有据代价分析

若无 REJECT 判据且 correction_score ≥ 6，建议 adopt=true。
若 speedup 显著（≥ 5%）但 reasoning_score 很低，仍然 adopt=true（好的执行结果值得学习）。

【输出格式】
请直接输出如下 JSON，不要加 ```json 等代码块标记，不要在 JSON 前后添加任何说明文字：
{{
  "reasoning_score": <0-10 整数>,
  "gap_analysis": "<列举 [B] 原始推理存在哪些具体错误或不足，对照 [C] 揭示的实际优化效果，说明原推理在哪些关键点上与实际结果脱节（如：误判 Join Order、遗漏基数变化、未预测物化收益等）；若 [B] 推理已很完善则填 '无明显缺陷'>",
  "correction_score": <0-10 整数>,
  "corrected_b": "<修正后的完整 [B'] 文本，必须保留 [B] 的全部 Hint 块，以推断口吻撰写推理过程>",
  "adopt": <true 或 false>,
  "reject_reason": "<若 adopt=false 填写拒绝原因；若 adopt=true 填空字符串>",
  "diff_summary": "<逐项说明 [B'] 相比 [B] 在文本内容上实际做出了哪些改动（若无任何改动则填 '无修改'）；不得凭空捏造未实际出现在 [B'] 中的改动>"
}}

---
【输入数据注入区】

=== [A] 初始状态（问题描述与当前执行计划）===
{Input_A}

=== [B] 原始推理（存在瑕疵的分析与 Hint 建议）===
{Original_B}

=== [C] 物理反馈（Hint 作用后数据库的真实 EXPLAIN ANALYZE 及计时）===
{Feedback_C}
---

请严格按照【输出格式】输出 JSON，现在开始："""


# ============================================================================
# MCTS 树结构工具（与 rebuild_sft_with_correction.py 保持一致）
# ============================================================================

ROOT_TAG = "0"


def _parent_tag(tag: str) -> Optional[str]:
    if "." not in tag:
        return None
    return tag.rsplit(".", 1)[0]


def _ancestor_chain(tree_nodes: Dict[str, Any], tag: str) -> List[Dict[str, Any]]:
    """Return [root, ..., node's parent] in depth order (excludes node itself)."""
    chain: List[Dict[str, Any]] = []
    t = _parent_tag(tag)
    while t is not None:
        node = tree_nodes.get(t)
        if node is None:
            break
        chain.append(node)
        t = _parent_tag(t)
    chain.reverse()
    return chain


def _rebuild_partial_solution(tree_nodes: Dict[str, Any], tag: str) -> str:
    """Reproduce collect_partial_solution_text for a non-root node."""
    pieces: List[str] = []
    for anc in _ancestor_chain(tree_nodes, tag):
        resp = (anc.get("llm_response") or {}).get("response") or ""
        if resp:
            pieces.append(resp)
    return "\n".join(pieces)


def _resolve_action(node_info: Dict[str, Any]) -> Optional[ActionType]:
    raw = node_info.get("action_type")
    if not raw:
        return None
    try:
        return ActionType(raw)
    except ValueError:
        pass
    try:
        return ActionType[raw]
    except KeyError:
        return None


# ============================================================================
# 样本数据类
# ============================================================================

@dataclass
class CorrectionSample:
    """One A/B/C triple ready to be sent to the judge LLM."""
    source_file: str
    entry_index: int
    node_tag: str
    action: str            # "A1".."A6"
    depth: int
    input_a: str           # full [A] prompt (SFT question)
    original_b: str        # [B] — base model's raw response
    feedback_c: str        # [C] — formatted physical feedback
    # Bookkeeping
    plan_digest: Optional[str]
    execution_time_s: Optional[float]
    step_improvement: Optional[float]
    baseline_time: Optional[float]
    new_plan_first_found: bool
    # Direct path flag — when True, original_b is used as-is (no judge LLM)
    direct: bool = False
    # For direct path: sub-category ("a4" or "no_change")
    direct_kind: str = ""

    @property
    def primary_key(self) -> str:
        return f"{self.source_file}#{self.entry_index}#{self.node_tag}"

    @property
    def speedup_pct(self) -> Optional[float]:
        if self.execution_time_s is not None and self.baseline_time and self.baseline_time > 0:
            return (self.baseline_time - self.execution_time_s) / self.baseline_time * 100.0
        return None


# ============================================================================
# [C] 格式化：计时信息 + EXPLAIN ANALYZE
# ============================================================================

def _parse_ea_payload(raw: Any) -> Any:
    """Parse explain_analyze_info value.

    The value may already be a parsed dict/list (MCTS serializer sometimes
    stores it as a JSON object directly), or it may be a string of the form
    '<JSON>\\nOutline Data:\\n-----\\n/*+ BEGIN_OUTLINE_DATA ... */'.

    Returns (json_obj_or_None, outline_text_str).
    """
    if raw is None:
        return None, ""

    # Already a Python object — no parsing needed.
    if isinstance(raw, (dict, list)):
        return raw, ""

    if not isinstance(raw, str):
        # Unexpected type; convert to string and fall through.
        raw = str(raw)

    raw = raw.strip()
    if not raw:
        return None, ""

    try:
        obj, end_idx = json.JSONDecoder().raw_decode(raw)
        outline_text = raw[end_idx:].strip()
        return obj, outline_text
    except json.JSONDecodeError:
        return None, raw


def _format_feedback_c(
    ea_raw: Optional[str],
    execution_time_s: Optional[float],
    baseline_time: Optional[float],
    step_improvement: Optional[float],
) -> str:
    """Render [C] for the judge LLM: timing summary + Explain Analyze."""
    lines: List[str] = []

    # --- Timing summary line ---
    if execution_time_s is not None:
        line = f"real_execution_time_s = {execution_time_s:.6f}s"
        if baseline_time is not None and baseline_time > 0:
            delta = baseline_time - execution_time_s
            pct = delta / baseline_time * 100.0
            line += (
                f"  (baseline = {baseline_time:.6f}s,"
                f"  delta = {delta:+.6f}s,  speedup = {pct:+.1f}%)"
            )
        lines.append(line)
    if step_improvement is not None:
        lines.append(f"step_improvement = {step_improvement}")
    lines.append("")

    # --- EXPLAIN ANALYZE ---
    lines.append("Explain Analyze（真实物理执行计划）:")
    if ea_raw:
        ea_obj, outline_text = _parse_ea_payload(ea_raw)
        if ea_obj is not None:
            lines.append(json.dumps(ea_obj, ensure_ascii=False, indent=2))
            if outline_text:
                lines.append("")
                lines.append("Outline Data:")
                lines.append(outline_text)
        else:
            lines.append(ea_raw)
    else:
        lines.append("<no Explain Analyze available>")

    return "\n".join(lines)


# ============================================================================
# run_sft.py 兼容过滤工具（与 run_sft.py 保持算法一致）
# ============================================================================

_SET_VAR_KEYS = [
    "materialization=off", "derived_merge=off", "semijoin=off",
    "loosescan=off", "firstmatch=off", "duplicateweedout=off",
    "subquery_materialization_cost_based=off", "subquery_to_derived=on",
]


def _normalize_list(value: Any) -> List[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(x) for x in value]
    return [str(value)]


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
    ex_lower = (executed or "").lower()
    for key in _SET_VAR_KEYS:
        key_part = key.split("=")[0].lower()
        if key_part in ex_lower:
            if not any(key_part in str(c).lower() for c in cand_config_list):
                return False
    return True


def _extract_candidate_sets(candidate_hints: Dict[str, Any]) -> Tuple[Set[str], List[str], List[str]]:
    idx_set = set(str(x) for x in candidate_hints.get("index", []) or [])
    join_list = [str(x) for x in candidate_hints.get("join_order", []) or []]
    cfg_list = [str(x) for x in candidate_hints.get("config", []) or []]
    return idx_set, join_list, cfg_list


def _is_phantom_hint(h: str, cand_index: Set[str], cand_join: List[str], cand_config: List[str]) -> bool:
    """单个 hint 是否脱离候选范围（幻觉）。"""
    if _is_index_hint(h):
        return h not in cand_index
    if _is_join_hint(h):
        return not _join_covered(h, cand_join)
    if _is_config_hint(h):
        return not _setvar_covered(h, cand_config)
    # 未识别类型按幻觉处理（保守）
    return True


def _passes_sft_node_filter(
    tag: str,
    node: Dict[str, Any],
    tree_nodes: Dict[str, Any],
    baseline_time: Optional[float],
    baseline_digest: Optional[str],
    candidate_hints: Dict[str, Any],
    filter_phantom: bool,
    stats: Dict[str, int],
) -> bool:
    """Apply run_sft.py quality gates to a single MCTS node.

    Gates (matching run_sft.py's _score_hint_action / _score_combination_action):
      1. cur_time < baseline_time      — node must beat the no-hint baseline
      2. cur_digest != baseline_digest  — plan must differ from default plan
      3. cur_time < parent_time        — node must improve over its parent
      4. cur_digest != parent_digest   — plan must change vs parent
      5. new_hints non-empty           — exclude no-change nodes (except A4/A6)
      6. no phantom hints              — all new/executed hints within candidate range
    """
    node_info = node.get("node_info") or {}
    db = node.get("db_response") or {}
    action_type = node_info.get("action_type")

    cur_time_raw = db.get("execution_time_s")
    try:
        cur_time_f: Optional[float] = float(cur_time_raw) if cur_time_raw is not None else None
    except (TypeError, ValueError):
        cur_time_f = None

    cur_digest = db.get("plan_digest")

    # Gate 1: cur_time must beat baseline
    if baseline_time is not None:
        if cur_time_f is None or cur_time_f >= baseline_time:
            stats["sft_filter_not_better_than_baseline"] = (
                stats.get("sft_filter_not_better_than_baseline", 0) + 1
            )
            return False

    # Gate 2: plan must differ from baseline plan
    if not cur_digest or cur_digest == baseline_digest:
        stats["sft_filter_same_digest_as_baseline"] = (
            stats.get("sft_filter_same_digest_as_baseline", 0) + 1
        )
        return False

    # Gate 3 & 4: parent improvement (both time and digest)
    parent_tag = _parent_tag(tag)
    parent_node = tree_nodes.get(parent_tag) if parent_tag else None
    if parent_node is not None:
        parent_db = (parent_node or {}).get("db_response") or {}
        parent_time_raw = parent_db.get("execution_time_s")
        try:
            parent_time_f: Optional[float] = (
                float(parent_time_raw) if parent_time_raw is not None else None
            )
        except (TypeError, ValueError):
            parent_time_f = None
        parent_digest = parent_db.get("plan_digest")

        if parent_time_f is not None and cur_time_f is not None and cur_time_f >= parent_time_f:
            stats["sft_filter_no_parent_time_improvement"] = (
                stats.get("sft_filter_no_parent_time_improvement", 0) + 1
            )
            return False
        if parent_digest and cur_digest == parent_digest:
            stats["sft_filter_same_digest_as_parent"] = (
                stats.get("sft_filter_same_digest_as_parent", 0) + 1
            )
            return False

    # Gate 5: new_hints non-empty (not applicable for A4/A6)
    if action_type not in ("A4", "A6"):
        new_hints = _normalize_list(node_info.get("new_hints"))
        if not new_hints:
            stats["sft_filter_no_new_hints"] = (
                stats.get("sft_filter_no_new_hints", 0) + 1
            )
            return False

    # Gate 6: phantom hints check
    if filter_phantom:
        if action_type == "A4":
            hints_to_check: List[str] = []
        elif action_type == "A6":
            hints_to_check = _normalize_list(node_info.get("executed_hints"))
        else:
            hints_to_check = _normalize_list(node_info.get("new_hints"))

        if hints_to_check:
            cand_index, cand_join, cand_cfg = _extract_candidate_sets(candidate_hints)
            if any(_is_phantom_hint(h, cand_index, cand_join, cand_cfg) for h in hints_to_check):
                stats["sft_filter_phantom_hint"] = (
                    stats.get("sft_filter_phantom_hint", 0) + 1
                )
                return False

    return True


# ============================================================================
# 直接路径工具（A4 + no-change，bypass judge LLM）
# ============================================================================

def _compute_subtree_min_times(tree_nodes: Dict[str, Any]) -> Dict[str, Optional[float]]:
    """Compute subtree_min_time for every node (mirroring run_sft.py).

    subtree_min_time[tag] = min execution_time among the node itself and all its
    descendants.  None if no valid time exists anywhere in the subtree.
    """
    # Build children map
    from collections import defaultdict as _defaultdict
    children_by_parent: Dict[str, List[str]] = _defaultdict(list)
    for tag in tree_nodes:
        p = _parent_tag(tag)
        if p and p in tree_nodes:
            children_by_parent[p].append(tag)

    def _depth(t: str) -> int:
        return t.count(".")

    result: Dict[str, Optional[float]] = {}
    for tag in sorted(tree_nodes.keys(), key=lambda t: -_depth(t)):
        node = tree_nodes[tag]
        if not isinstance(node, dict):
            result[tag] = None
            continue
        raw = (node.get("db_response") or {}).get("execution_time_s")
        try:
            self_t: Optional[float] = float(raw) if raw is not None else None
        except (TypeError, ValueError):
            self_t = None

        min_t: Optional[float] = self_t
        for ch in children_by_parent.get(tag, []):
            ch_min = result.get(ch)
            if ch_min is not None and (min_t is None or ch_min < min_t):
                min_t = ch_min
        result[tag] = min_t
    return result


def _is_no_change_node(node_info: Dict[str, Any]) -> bool:
    """True when the node added no hints and deleted no hints (no-change path)."""
    new_hints = _normalize_list(node_info.get("new_hints"))
    deleted_hints = _normalize_list(node_info.get("deleted_hints"))
    return (not new_hints) and (not deleted_hints)


def build_direct_samples_from_entry(
    source_file: str,
    entry_index: int,
    entry: Dict[str, Any],
    stats: Dict[str, int],
    min_baseline: float = 0.1,
    min_step_improvement: float = 0.0,
    no_change_ratio: float = 0.10,
    filter_phantom: bool = False,
) -> List[CorrectionSample]:
    """Extract A4 and no-change nodes that bypass the judge LLM.

    A4 nodes: value assessed via subtree_min_time (mirrors run_sft._score_a4).
    no-change nodes: nodes where new_hints and deleted_hints are both empty
        for A1/A2/A3/A5 (mirrors run_sft._score_no_change; A4/A6 excluded).
    Both categories use original_b as the SFT output directly.
    """
    tree_nodes: Dict[str, Any] = entry.get("mcts_tree_nodes") or {}
    query: str = entry.get("query") or ""
    execution_info: Any = entry.get("execution_info") or {}
    execution_info_json = (
        execution_info
        if isinstance(execution_info, str)
        else json.dumps(execution_info, ensure_ascii=False)
    )
    candidate_hints: Dict[str, List[str]] = entry.get("candidate_hints") or {}
    index_info: Dict[str, Any] = entry.get("index_info") or {}
    baseline_time = entry.get("baseline_time")
    try:
        baseline_time = float(baseline_time) if baseline_time is not None else None
    except (TypeError, ValueError):
        baseline_time = None

    # Entry-level baseline filter
    if baseline_time is None or baseline_time <= min_baseline:
        return []

    baseline_digest: Optional[str] = entry.get("plan_digest")

    # Compute subtree min times (needed for A4 scoring)
    subtree_min_times = _compute_subtree_min_times(tree_nodes)

    a4_samples: List[CorrectionSample] = []
    no_change_samples: List[CorrectionSample] = []

    for tag, node in tree_nodes.items():
        if not isinstance(node, dict):
            continue
        if tag == ROOT_TAG:
            continue

        node_info = node.get("node_info") or {}
        llm = node.get("llm_response") or {}
        db = node.get("db_response") or {}

        original_b = llm.get("response") or ""
        if not original_b:
            continue

        action = _resolve_action(node_info)
        if action is None:
            continue
        action_str = action.value

        # --- A4 path ---
        if action_str == "A4":
            new_hints = _normalize_list(node_info.get("new_hints"))
            if new_hints:
                # A4 with new_hints is not a valid A4 (should have none)
                continue

            # Parent time or baseline as effective parent time
            parent_tag = _parent_tag(tag)
            parent_time: Optional[float] = None
            if parent_tag and parent_tag in tree_nodes:
                raw = (tree_nodes[parent_tag].get("db_response") or {}).get("execution_time_s")
                try:
                    parent_time = float(raw) if raw is not None else None
                except (TypeError, ValueError):
                    parent_time = None
            effective_parent = parent_time if (parent_time is not None and parent_time > 0) else baseline_time

            if effective_parent is None or effective_parent <= 0:
                continue
            sub_min = subtree_min_times.get(tag)
            if sub_min is None:
                continue
            score = (effective_parent - sub_min) / effective_parent
            if score < min_step_improvement:
                stats["direct_a4_filter_low_subtree_improvement"] = (
                    stats.get("direct_a4_filter_low_subtree_improvement", 0) + 1
                )
                continue

            # Rebuild prompt
            try:
                partial = _rebuild_partial_solution(tree_nodes, tag)
                depth = int(node_info.get("depth", tag.count(".")))
                input_a = build_action_prompt(
                    action=action,
                    query=query,
                    execution_info=execution_info_json,
                    candidate_hints=candidate_hints,
                    index_info=index_info,
                    partial_solution=partial,
                    step_number=depth,
                )
            except Exception as e:
                stats["direct_reason_prompt_build_error"] = (
                    stats.get("direct_reason_prompt_build_error", 0) + 1
                )
                print(
                    f"  ⚠ direct A4 prompt 重建失败 {source_file}#entry{entry_index}@tag={tag}: "
                    f"{type(e).__name__}: {e}",
                    file=sys.stderr,
                )
                continue

            exec_time_raw = db.get("execution_time_s")
            try:
                exec_time_f: Optional[float] = float(exec_time_raw) if exec_time_raw is not None else None
            except (TypeError, ValueError):
                exec_time_f = None

            a4_samples.append(CorrectionSample(
                source_file=source_file,
                entry_index=entry_index,
                node_tag=tag,
                action=action_str,
                depth=int(node_info.get("depth", tag.count("."))),
                input_a=input_a,
                original_b=original_b,
                feedback_c="",
                plan_digest=db.get("plan_digest"),
                execution_time_s=exec_time_f,
                step_improvement=None,
                baseline_time=baseline_time,
                new_plan_first_found=bool(node_info.get("new_plan_first_found")),
                direct=True,
                direct_kind="a4",
            ))
            stats["direct_a4_kept"] = stats.get("direct_a4_kept", 0) + 1
            continue

        # --- no-change path: A1/A2/A3/A5 only (A4 handled above, A6 excluded) ---
        if action_str in ("A1", "A2", "A3", "A5") and _is_no_change_node(node_info):
            if baseline_time is None or baseline_time <= 0:
                continue

            # Optional: phantom check for no-change (executed_hints must be in range)
            if filter_phantom:
                executed = _normalize_list(node_info.get("executed_hints"))
                if executed:
                    cand_index, cand_join, cand_cfg = _extract_candidate_sets(candidate_hints)
                    if any(_is_phantom_hint(h, cand_index, cand_join, cand_cfg) for h in executed):
                        stats["direct_no_change_filter_phantom"] = (
                            stats.get("direct_no_change_filter_phantom", 0) + 1
                        )
                        continue

            # Rebuild prompt
            try:
                partial = _rebuild_partial_solution(tree_nodes, tag)
                depth = int(node_info.get("depth", tag.count(".")))
                input_a = build_action_prompt(
                    action=action,
                    query=query,
                    execution_info=execution_info_json,
                    candidate_hints=candidate_hints,
                    index_info=index_info,
                    partial_solution=partial,
                    step_number=depth,
                )
            except Exception as e:
                stats["direct_reason_prompt_build_error"] = (
                    stats.get("direct_reason_prompt_build_error", 0) + 1
                )
                print(
                    f"  ⚠ direct no-change prompt 重建失败 {source_file}#entry{entry_index}@tag={tag}: "
                    f"{type(e).__name__}: {e}",
                    file=sys.stderr,
                )
                continue

            exec_time_raw = db.get("execution_time_s")
            try:
                exec_time_f = float(exec_time_raw) if exec_time_raw is not None else None
            except (TypeError, ValueError):
                exec_time_f = None

            no_change_samples.append(CorrectionSample(
                source_file=source_file,
                entry_index=entry_index,
                node_tag=tag,
                action=action_str,
                depth=int(node_info.get("depth", tag.count("."))),
                input_a=input_a,
                original_b=original_b,
                feedback_c="",
                plan_digest=db.get("plan_digest"),
                execution_time_s=exec_time_f,
                step_improvement=None,
                baseline_time=baseline_time,
                new_plan_first_found=bool(node_info.get("new_plan_first_found")),
                direct=True,
                direct_kind="no_change",
            ))

    # Apply no-change ratio cap relative to a4 count (or 1 as floor) to avoid
    # flooding the direct output.  We cap at max(1, floor(len(a4) / (1 - ratio) * ratio))
    # when a4 is available; otherwise we keep a small absolute cap to avoid
    # emitting unbounded no-change samples for entries with no a4 data.
    if no_change_samples:
        a4_count = len(a4_samples)
        if a4_count > 0:
            # no_change : a4 ≈ no_change_ratio : (1 - no_change_ratio)
            import math as _math
            cap = max(1, int(_math.floor(a4_count * no_change_ratio / max(1.0 - no_change_ratio, 1e-9))))
        else:
            # No A4 to anchor against — keep at most 1 no-change per entry to
            # avoid injecting too many "nothing to change" samples with no
            # reference signal.
            cap = 1
        no_change_samples = no_change_samples[:cap]
        kept_nc = len(no_change_samples)
        stats["direct_no_change_kept"] = stats.get("direct_no_change_kept", 0) + kept_nc

    return a4_samples + no_change_samples


# ============================================================================
# 遍历输入目录 + 提取样本
# ============================================================================

def _iter_entries(input_dir: Path) -> List[Tuple[Path, int, Dict[str, Any]]]:
    out: List[Tuple[Path, int, Dict[str, Any]]] = []
    for fp in sorted(input_dir.glob("*.json")):
        try:
            data = json.load(open(fp, "r", encoding="utf-8"))
        except Exception as e:
            print(f"  ⚠ 跳过损坏文件 {fp.name}: {e}", file=sys.stderr)
            continue
        entries = data if isinstance(data, list) else [data]
        for i, ent in enumerate(entries):
            if isinstance(ent, dict):
                out.append((fp, i, ent))
    return out


def build_samples_from_entry(
    source_file: str,
    entry_index: int,
    entry: Dict[str, Any],
    stats: Dict[str, int],
    skip_no_ea: bool = True,
    only_new_plan: bool = False,
    min_step_improvement: Optional[float] = None,
    min_speedup_pct: Optional[float] = None,
    min_baseline: float = 0.1,
    filter_phantom: bool = False,
) -> List[CorrectionSample]:
    """Extract (A, B, C) triples for all qualifying non-root nodes."""
    tree_nodes: Dict[str, Any] = entry.get("mcts_tree_nodes") or {}
    ea_info: Dict[str, Any] = entry.get("explain_analyze_info") or {}
    query: str = entry.get("query") or ""
    execution_info: Any = entry.get("execution_info") or {}
    execution_info_json = (
        execution_info
        if isinstance(execution_info, str)
        else json.dumps(execution_info, ensure_ascii=False)
    )
    candidate_hints: Dict[str, List[str]] = entry.get("candidate_hints") or {}
    index_info: Dict[str, Any] = entry.get("index_info") or {}
    baseline_time = entry.get("baseline_time")
    try:
        baseline_time = float(baseline_time) if baseline_time is not None else None
    except (TypeError, ValueError):
        baseline_time = None

    # Entry-level baseline filter: skip entries with tiny baseline (not worth optimizing)
    if baseline_time is None or baseline_time <= min_baseline:
        stats["reason_entry_baseline_too_small"] = (
            stats.get("reason_entry_baseline_too_small", 0) + 1
        )
        return []

    # Baseline plan digest (the no-hint default plan)
    baseline_digest: Optional[str] = entry.get("plan_digest")

    samples: List[CorrectionSample] = []

    for tag, node in tree_nodes.items():
        if not isinstance(node, dict):
            continue
        if tag == ROOT_TAG:
            continue

        node_info = node.get("node_info") or {}
        llm = node.get("llm_response") or {}
        db = node.get("db_response") or {}
        original_b = llm.get("response") or ""
        if not original_b:
            stats["reason_empty_response"] = stats.get("reason_empty_response", 0) + 1
            continue

        action = _resolve_action(node_info)
        if action is None:
            stats["reason_unknown_action"] = stats.get("reason_unknown_action", 0) + 1
            continue

        # run_sft.py quality gates: only process nodes that would make it into SFT
        if not _passes_sft_node_filter(
            tag=tag,
            node=node,
            tree_nodes=tree_nodes,
            baseline_time=baseline_time,
            baseline_digest=baseline_digest,
            candidate_hints=candidate_hints,
            filter_phantom=filter_phantom,
            stats=stats,
        ):
            continue

        plan_digest = db.get("plan_digest")
        ea_raw: Optional[str] = ea_info.get(plan_digest) if plan_digest else None
        if skip_no_ea and ea_raw is None:
            stats["reason_no_ea"] = stats.get("reason_no_ea", 0) + 1
            continue

        if only_new_plan and not bool(node_info.get("new_plan_first_found")):
            stats["reason_not_new_plan"] = stats.get("reason_not_new_plan", 0) + 1
            continue

        exec_time_s = db.get("execution_time_s")
        try:
            exec_time_f = float(exec_time_s) if exec_time_s is not None else None
        except (TypeError, ValueError):
            exec_time_f = None

        step_imp = db.get("step_improvement")
        try:
            step_imp_f = float(step_imp) if step_imp is not None else None
        except (TypeError, ValueError):
            step_imp_f = None

        # Optional pre-filter: step_improvement
        if (
            min_step_improvement is not None
            and step_imp_f is not None
            and step_imp_f < min_step_improvement
        ):
            stats["reason_below_step_threshold"] = (
                stats.get("reason_below_step_threshold", 0) + 1
            )
            continue

        # Optional pre-filter: speedup_pct
        if min_speedup_pct is not None and exec_time_f is not None and baseline_time:
            speedup = (baseline_time - exec_time_f) / baseline_time * 100.0
            if speedup < min_speedup_pct:
                stats["reason_below_speedup_threshold"] = (
                    stats.get("reason_below_speedup_threshold", 0) + 1
                )
                continue

        # Reconstruct the exact prompt this node was called with.
        try:
            partial = _rebuild_partial_solution(tree_nodes, tag)
            depth = int(node_info.get("depth", tag.count(".")))
            input_a = build_action_prompt(
                action=action,
                query=query,
                execution_info=execution_info_json,
                candidate_hints=candidate_hints,
                index_info=index_info,
                partial_solution=partial,
                step_number=depth,
            )
        except Exception as e:
            stats["reason_prompt_build_error"] = (
                stats.get("reason_prompt_build_error", 0) + 1
            )
            print(
                f"  ⚠ prompt 重建失败 {source_file}#entry{entry_index}@tag={tag}: "
                f"{type(e).__name__}: {e}",
                file=sys.stderr,
            )
            continue

        feedback_c = _format_feedback_c(ea_raw, exec_time_f, baseline_time, step_imp_f)

        samples.append(
            CorrectionSample(
                source_file=source_file,
                entry_index=entry_index,
                node_tag=tag,
                action=action.value,
                depth=depth,
                input_a=input_a,
                original_b=original_b,
                feedback_c=feedback_c,
                plan_digest=plan_digest,
                execution_time_s=exec_time_f,
                step_improvement=step_imp_f,
                baseline_time=baseline_time,
                new_plan_first_found=bool(node_info.get("new_plan_first_found")),
            )
        )
        stats["samples_kept"] = stats.get("samples_kept", 0) + 1

    return samples


def build_correction_prompt(sample: CorrectionSample) -> str:
    return CORRECTION_PROMPT_TEMPLATE.format(
        Input_A=sample.input_a,
        Original_B=sample.original_b,
        Feedback_C=sample.feedback_c,
    )


# ============================================================================
# Hint 保持校验（与 run_corrections_llm.py 保持一致）
# ============================================================================

_HINTS_RE = re.compile(r"<hints>\s*/\*\+(.*?)\*/\s*</hints>", re.DOTALL)
_SUBPROBLEM_RE = re.compile(r"<subproblem>(.*?)</subproblem>", re.DOTALL)


def _extract_hint_blocks(text: str) -> List[str]:
    out: List[str] = []
    for m in _HINTS_RE.finditer(text or ""):
        payload = re.sub(r"\s+", " ", m.group(1).strip())
        out.append(payload)
    return out


def _extract_subproblem_blocks(text: str) -> List[str]:
    out: List[str] = []
    for m in _SUBPROBLEM_RE.finditer(text or ""):
        payload = re.sub(r"\s+", " ", m.group(1).strip())
        out.append(payload)
    return out


def validate_preservation(
    action: str, original_b: str, corrected_b: str
) -> Tuple[bool, str]:
    """Confirm [B'] preserves [B]'s final recommendation verbatim."""
    if action == "A4":
        orig = _extract_subproblem_blocks(original_b)
        new = _extract_subproblem_blocks(corrected_b)
        block = "subproblem"
    else:
        orig = _extract_hint_blocks(original_b)
        new = _extract_hint_blocks(corrected_b)
        block = "hints"

    if not orig:
        # [B] has no expected block — can't enforce; accept.
        return True, ""
    if orig != new:
        return False, (
            f"{block} block mismatch: original={orig!r}  corrected={new!r}"
        )
    return True, ""


# ============================================================================
# 裁判 JSON 响应解析
# ============================================================================

_JSON_BLOCK_RE = re.compile(r"```(?:json)?\s*([\s\S]*?)```", re.IGNORECASE)
_REQUIRED_KEYS = {"reasoning_score", "gap_analysis", "correction_score", "corrected_b", "adopt", "reject_reason", "diff_summary"}


def _parse_judge_json(text: str) -> Optional[Dict[str, Any]]:
    """Extract and validate the structured JSON from the judge's response.

    Tries, in order:
      1. Strip ```json ... ``` fences and parse the inner text.
      2. Use raw_decode on the text directly (handles leading whitespace / BOM).
      3. Find the first '{' and attempt a parse from there.

    Returns None if all attempts fail or required keys are missing.
    """
    candidates: List[str] = []

    # 1. Code block
    m = _JSON_BLOCK_RE.search(text)
    if m:
        candidates.append(m.group(1).strip())

    # 2. Raw text
    candidates.append(text.strip())

    # 3. From first '{'
    brace = text.find("{")
    if brace != -1:
        candidates.append(text[brace:])

    for candidate in candidates:
        if not candidate:
            continue
        try:
            obj, _ = json.JSONDecoder().raw_decode(candidate)
            if not isinstance(obj, dict):
                continue
            if not _REQUIRED_KEYS.issubset(obj.keys()):
                continue
            # Coerce types
            obj["reasoning_score"] = int(obj["reasoning_score"])
            obj["gap_analysis"] = str(obj.get("gap_analysis") or "")
            obj["correction_score"] = int(obj["correction_score"])
            obj["adopt"] = bool(obj["adopt"])
            obj["corrected_b"] = str(obj.get("corrected_b") or "")
            obj["reject_reason"] = str(obj.get("reject_reason") or "")
            obj["diff_summary"] = str(obj.get("diff_summary") or "")
            return obj
        except (json.JSONDecodeError, ValueError, TypeError):
            continue

    return None


# ============================================================================
# [A] 拆分为 system / instruction（与 run_corrections_llm.py 保持一致）
# ============================================================================

def _split_input_a(input_a: str, action: str) -> Tuple[str, str]:
    """Split a combined prompt back into (system, instruction) for SFT schema."""
    try:
        act_enum = ActionType(action)
        tpl = _ACTION_TEMPLATES.get(act_enum)
    except (ValueError, KeyError):
        tpl = None

    if tpl is not None:
        sys_prefix = tpl.system
        head = sys_prefix + "\n\n"
        if input_a.startswith(head):
            return sys_prefix, input_a[len(head):]
    return "", input_a


def build_sft_sample(sample: CorrectionSample, corrected_b: str) -> Dict[str, Any]:
    system, instruction = _split_input_a(sample.input_a, sample.action)
    return {
        "system": system,
        "instruction": instruction,
        "input": "",
        "output": corrected_b,
        "history": [],
        "message": "",
    }


def build_comparison_row(
    sample: CorrectionSample,
    judge_result: Dict[str, Any],
    meta: Dict[str, Any],
    drop_reason: str = "",
) -> Dict[str, Any]:
    row: Dict[str, Any] = {
        "input": sample.input_a,
        "original_response": sample.original_b,
        "feedback": sample.feedback_c,
        "corrected_response": judge_result.get("corrected_b", ""),
        "metadata": {
            "source_file": sample.source_file,
            "entry_index": sample.entry_index,
            "node_tag": sample.node_tag,
            "action": sample.action,
            "depth": sample.depth,
            "plan_digest": sample.plan_digest,
            "execution_time_s": sample.execution_time_s,
            "step_improvement": sample.step_improvement,
            "baseline_time": sample.baseline_time,
            "new_plan_first_found": sample.new_plan_first_found,
            "speedup_pct": sample.speedup_pct,
            # Judge scores
            "reasoning_score": judge_result.get("reasoning_score"),
            "gap_analysis": judge_result.get("gap_analysis", ""),
            "correction_score": judge_result.get("correction_score"),
            "adopt": judge_result.get("adopt"),
            "reject_reason": judge_result.get("reject_reason", ""),
            "diff_summary": judge_result.get("diff_summary", ""),
            "judge_llm": meta,
        },
    }
    if drop_reason:
        row["metadata"]["drop_reason"] = drop_reason
    return row


# ============================================================================
# Checkpoint/resume
# ============================================================================

def _already_done_keys(comparison_path: Path) -> Set[str]:
    if not comparison_path.exists():
        return set()
    keys: Set[str] = set()
    with open(comparison_path, "r", encoding="utf-8") as f:
        for line in f:
            try:
                obj = json.loads(line)
                m = obj.get("metadata") or {}
                pk = (
                    f"{m.get('source_file', '')}#"
                    f"{m.get('entry_index', 0)}#"
                    f"{m.get('node_tag', '')}"
                )
                if pk and pk != "##":
                    keys.add(pk)
            except Exception:
                continue
    return keys


# ============================================================================
# LLM 客户端构建
# ============================================================================

def _build_llm_client(llm_config_path: Optional[str]):
    """Construct an LLMClient using [mcts] llm_api_url_key from aiopt_conf.toml."""
    from mcts.config.config_loader import load_mcts_config
    from mcts.modules.llm_client import LLMClient

    config = load_mcts_config(custom_yaml_path=llm_config_path)
    if not config.llm_api_url_key:
        raise RuntimeError(
            "MCTSConfig.llm_api_url_key 为空：请在 etc/aiopt_conf.toml 的 "
            "[mcts] 段里填 llm_api_url_key = [[url, key, model], ...]"
        )
    return LLMClient(config), config


def _call_judge(client, prompt: str) -> Tuple[str, Dict[str, Any]]:
    """Send one prompt to the judge LLM, return (text, meta)."""
    completion = client.complete(prompt)
    status = (
        completion.status.value
        if isinstance(completion.status, LLMStatus)
        else str(completion.status)
    )
    return (completion.text or ""), {
        "status": status,
        "stop_reason": completion.stop_reason,
        "input_chars": completion.input_chars,
        "output_chars": completion.output_chars,
        "latency_seconds": round(completion.latency_seconds, 3),
    }


# ============================================================================
# 主处理循环
# ============================================================================

def run(args: argparse.Namespace) -> int:
    input_dir = Path(args.input_dir)
    if not input_dir.is_dir():
        print(f"错误: 目录不存在 {input_dir}", file=sys.stderr)
        return 1

    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    sft_path = out_dir / "sft_samples.jsonl"
    comparison_path = out_dir / "comparison.jsonl"
    review_path = out_dir / "review.jsonl"
    prompts_path = out_dir / "prompts_only.jsonl"

    prompts_only = args.prompts_only or args.dry_run

    # ---- Phase 1: 扫描并收集样本 ------------------------------------------
    stats: Dict[str, int] = {}
    entries = _iter_entries(input_dir)
    all_samples: List[CorrectionSample] = []      # judge-path samples
    direct_samples: List[CorrectionSample] = []   # A4 + no-change bypass samples

    no_change_ratio = args.no_change_ratio

    for fp, idx, ent in entries:
        # Judge-path samples (A1/A2/A3/A5/A6 with real speedup)
        ent_samples = build_samples_from_entry(
            source_file=fp.name,
            entry_index=idx,
            entry=ent,
            stats=stats,
            skip_no_ea=not args.include_no_ea,
            only_new_plan=args.only_new_plan,
            min_step_improvement=args.min_step_improvement,
            min_speedup_pct=args.min_speedup_pct,
            min_baseline=args.min_baseline,
            filter_phantom=args.filter_phantom,
        )
        if args.actions:
            action_filter = {a.strip() for a in args.actions.split(",") if a.strip()}
            before = len(ent_samples)
            ent_samples = [s for s in ent_samples if s.action in action_filter]
            stats["reason_action_filter"] = (
                stats.get("reason_action_filter", 0) + (before - len(ent_samples))
            )
        all_samples.extend(ent_samples)

        # Direct-path samples (A4 + no-change — no judge LLM, no action filter)
        ent_direct = build_direct_samples_from_entry(
            source_file=fp.name,
            entry_index=idx,
            entry=ent,
            stats=stats,
            min_baseline=args.min_baseline,
            min_step_improvement=args.min_step_improvement or 0.0,
            no_change_ratio=no_change_ratio,
            filter_phantom=args.filter_phantom,
        )
        direct_samples.extend(ent_direct)

        if args.limit and len(all_samples) >= args.limit:
            all_samples = all_samples[: args.limit]
            break

    print(f"[posterior_correction] 扫描文件 = {len(set(e[0].name for e in entries))}")
    print(f"[posterior_correction] 扫描条目 = {len(entries)}")
    print(f"[posterior_correction] 候选样本(judge路径) = {len(all_samples)}")
    print(f"[posterior_correction] 候选样本(直接路径)  = {len(direct_samples)}"
          f"  (A4={stats.get('direct_a4_kept', 0)}"
          f", no_change={stats.get('direct_no_change_kept', 0)})")
    if stats:
        for k in sorted(stats):
            print(f"  {k:<40} = {stats[k]}")

    if not all_samples and not direct_samples:
        print("没有可用样本，退出。", file=sys.stderr)
        return 2

    # ---- Phase 2: prompts-only 模式 ----------------------------------------
    if prompts_only:
        prompts_path.parent.mkdir(parents=True, exist_ok=True)
        with open(prompts_path, "w", encoding="utf-8") as f:
            for s in all_samples:
                f.write(json.dumps({
                    "primary_key": s.primary_key,
                    "source_file": s.source_file,
                    "entry_index": s.entry_index,
                    "node_tag": s.node_tag,
                    "action": s.action,
                    "depth": s.depth,
                    "plan_digest": s.plan_digest,
                    "execution_time_s": s.execution_time_s,
                    "baseline_time": s.baseline_time,
                    "step_improvement": s.step_improvement,
                    "speedup_pct": s.speedup_pct,
                    "new_plan_first_found": s.new_plan_first_found,
                    "correction_prompt": build_correction_prompt(s),
                    "input_a": s.input_a,
                    "original_b": s.original_b,
                    "feedback_c": s.feedback_c,
                }, ensure_ascii=False) + "\n")
            # Also emit direct samples (no correction prompt, use original_b directly)
            for s in direct_samples:
                f.write(json.dumps({
                    "primary_key": s.primary_key,
                    "source_file": s.source_file,
                    "entry_index": s.entry_index,
                    "node_tag": s.node_tag,
                    "action": s.action,
                    "depth": s.depth,
                    "plan_digest": s.plan_digest,
                    "execution_time_s": s.execution_time_s,
                    "baseline_time": s.baseline_time,
                    "step_improvement": s.step_improvement,
                    "speedup_pct": s.speedup_pct,
                    "new_plan_first_found": s.new_plan_first_found,
                    "direct": True,
                    "direct_kind": s.direct_kind,
                    "input_a": s.input_a,
                    "original_b": s.original_b,
                }, ensure_ascii=False) + "\n")
        total_prompts = len(all_samples) + len(direct_samples)
        print(
            f"[posterior_correction] --prompts-only: {total_prompts} 条样本"
            f"（judge路径={len(all_samples)}, 直接路径={len(direct_samples)}）"
            f" 已写入 {prompts_path}（未调用 LLM）"
        )
        return 0

    # ---- Phase 3: Resume — 读取已完成的 primary_key -------------------------
    done_keys: Set[str] = set()
    if args.resume and comparison_path.exists():
        done_keys = _already_done_keys(comparison_path)
        print(f"[resume] 已完成样本 = {len(done_keys)}（从 {comparison_path.name} 恢复）")

    pending = [s for s in all_samples if s.primary_key not in done_keys]
    pending_direct = [s for s in direct_samples if s.primary_key not in done_keys]
    print(
        f"[posterior_correction] 待处理样本(judge路径) = {len(pending)}"
        f"  待处理样本(直接路径) = {len(pending_direct)}"
        f"（已跳过 {len(done_keys)} 条）"
    )
    if not pending and not pending_direct:
        print("没有可处理样本，退出。")
        return 0

    # ---- Phase 4: 构建 LLM 客户端（仅 judge 路径需要）------------------------
    client = None
    cfg = None
    if pending:
        try:
            client, cfg = _build_llm_client(args.llm_config or None)
        except Exception as e:
            print(f"错误: 构造 LLMClient 失败: {e}", file=sys.stderr)
            return 3
        workers = args.workers or max(1, len(cfg.llm_api_url_key or []))
        print(f"[posterior_correction] workers = {workers}")
    else:
        workers = 1

    # ---- Phase 5: 写入文件句柄 + 计数器 ------------------------------------
    sft_fp = open(sft_path, "a", encoding="utf-8")
    cmp_fp = open(comparison_path, "a", encoding="utf-8")
    rev_fp = open(review_path, "a", encoding="utf-8")
    writer_lock = threading.Lock()

    stat_counters = {
        "ok": 0,
        "direct_a4": 0,
        "direct_no_change": 0,
        "judge_reject": 0,        # adopt=false（REJECT 判据命中）
        "hint_mismatch": 0,       # B' 改了 hint 块，强制丢弃
        "low_score": 0,           # correction_score < --min-correction-score
        "json_parse_fail": 0,     # 裁判输出不是合法 JSON
        "llm_err": 0,             # LLM 调用失败
        "other": 0,
    }
    stats_lock = threading.Lock()
    t0 = time.time()

    def _make_review_record(
        sample: CorrectionSample,
        judge: Optional[Dict[str, Any]],
        drop_reason: str = "",
    ) -> Dict[str, Any]:
        rec: Dict[str, Any] = {
            "primary_key": sample.primary_key,
            "source_file": sample.source_file,
            "node_tag": sample.node_tag,
            "action": sample.action,
            "depth": sample.depth,
            "speedup_pct": round(sample.speedup_pct, 2) if sample.speedup_pct is not None else None,
            "execution_time_s": sample.execution_time_s,
            "baseline_time": sample.baseline_time,
            "step_improvement": sample.step_improvement,
        }
        if sample.direct:
            rec["direct"] = True
            rec["direct_kind"] = sample.direct_kind
            rec["judge"] = None
            rec["adopt"] = True
            rec["reject_reason"] = ""
            rec["diff_summary"] = "direct path — original_b used as-is"
            rec["original_b"] = sample.original_b
            rec["corrected_b"] = sample.original_b
        elif judge is not None:
            rec["reasoning_score"] = judge.get("reasoning_score")
            rec["gap_analysis"] = judge.get("gap_analysis", "")
            rec["correction_score"] = judge.get("correction_score")
            rec["adopt"] = judge.get("adopt")
            rec["reject_reason"] = judge.get("reject_reason", "")
            rec["diff_summary"] = judge.get("diff_summary", "")
            rec["original_b"] = sample.original_b
            rec["corrected_b"] = judge.get("corrected_b", "")
        else:
            rec["reasoning_score"] = None
            rec["gap_analysis"] = ""
            rec["correction_score"] = None
            rec["adopt"] = None
            rec["reject_reason"] = ""
            rec["diff_summary"] = ""
            rec["original_b"] = sample.original_b
            rec["corrected_b"] = ""
        if drop_reason:
            rec["drop_reason"] = drop_reason
        return rec

    def _write_review(rec: Dict[str, Any]) -> None:
        """Write one review record as pretty-printed JSON followed by a blank line."""
        rev_fp.write(json.dumps(rec, ensure_ascii=False, indent=2))
        rev_fp.write("\n\n")
        rev_fp.flush()

    def _emit_direct(sample: CorrectionSample) -> None:
        """Write a direct-path sample straight to sft_samples without calling judge."""
        sft_row = build_sft_sample(sample, sample.original_b)
        # Minimal comparison row (no judge fields)
        cmp_row: Dict[str, Any] = {
            "primary_key": sample.primary_key,
            "metadata": {
                "source_file": sample.source_file,
                "entry_index": sample.entry_index,
                "node_tag": sample.node_tag,
                "action": sample.action,
                "depth": sample.depth,
                "plan_digest": sample.plan_digest,
                "execution_time_s": sample.execution_time_s,
                "step_improvement": sample.step_improvement,
                "baseline_time": sample.baseline_time,
                "new_plan_first_found": sample.new_plan_first_found,
                "speedup_pct": sample.speedup_pct,
                "direct": True,
                "direct_kind": sample.direct_kind,
            },
        }
        rev = _make_review_record(sample, None)
        with writer_lock:
            sft_fp.write(json.dumps(sft_row, ensure_ascii=False) + "\n")
            sft_fp.flush()
            cmp_fp.write(json.dumps(cmp_row, ensure_ascii=False) + "\n")
            cmp_fp.flush()
            _write_review(rev)
        with stats_lock:
            if sample.direct_kind == "a4":
                stat_counters["direct_a4"] += 1
            else:
                stat_counters["direct_no_change"] += 1

    def _handle(sample: CorrectionSample) -> None:
        correction_prompt = build_correction_prompt(sample)
        text, meta = _call_judge(client, correction_prompt)
        status = meta.get("status")

        if status != "ok":
            with stats_lock:
                stat_counters["llm_err"] += 1
            rev = _make_review_record(sample, None, drop_reason="llm_err")
            with writer_lock:
                _write_review(rev)
            return

        # Parse structured JSON
        judge = _parse_judge_json(text)
        if judge is None:
            with stats_lock:
                stat_counters["json_parse_fail"] += 1
            # Log the raw response so we can debug the parse failure
            cmp_row = {
                "primary_key": sample.primary_key,
                "metadata": {
                    "source_file": sample.source_file,
                    "entry_index": sample.entry_index,
                    "node_tag": sample.node_tag,
                    "action": sample.action,
                    "depth": sample.depth,
                    "drop_reason": "json_parse_fail",
                    "raw_response": text[:2000],
                    "judge_llm": meta,
                },
            }
            rev = _make_review_record(sample, None, drop_reason="json_parse_fail")
            with writer_lock:
                cmp_fp.write(json.dumps(cmp_row, ensure_ascii=False) + "\n")
                cmp_fp.flush()
                _write_review(rev)
            return

        corrected_b = judge.get("corrected_b", "").strip()
        adopt = judge.get("adopt", False)
        correction = judge.get("correction_score", 0)
        reject_reason = judge.get("reject_reason", "")

        # Gate 1: adopt=false from judge
        if not adopt:
            with stats_lock:
                stat_counters["judge_reject"] += 1
            cmp_row = build_comparison_row(
                sample, judge, meta, drop_reason=f"judge_adopt_false: {reject_reason}"
            )
            rev = _make_review_record(sample, judge, drop_reason=f"judge_adopt_false: {reject_reason}")
            with writer_lock:
                cmp_fp.write(json.dumps(cmp_row, ensure_ascii=False) + "\n")
                cmp_fp.flush()
                _write_review(rev)
            return

        # Gate 2: correction score threshold
        if correction < args.min_fixability_score:
            with stats_lock:
                stat_counters["low_score"] += 1
            cmp_row = build_comparison_row(
                sample, judge, meta,
                drop_reason=f"correction_score={correction} < {args.min_fixability_score}"
            )
            rev = _make_review_record(
                sample, judge,
                drop_reason=f"correction_score={correction} < {args.min_fixability_score}"
            )
            with writer_lock:
                cmp_fp.write(json.dumps(cmp_row, ensure_ascii=False) + "\n")
                cmp_fp.flush()
                _write_review(rev)
            return

        # Gate 3: Hint preservation (even if judge said it's fine, we verify)
        ok_preserve, preserve_reason = validate_preservation(
            sample.action, sample.original_b, corrected_b
        )
        if not ok_preserve:
            with stats_lock:
                stat_counters["hint_mismatch"] += 1
            cmp_row = build_comparison_row(
                sample, judge, meta, drop_reason="hint_block_changed"
            )
            cmp_row["metadata"]["preservation_detail"] = preserve_reason
            rev = _make_review_record(sample, judge, drop_reason="hint_block_changed")
            with writer_lock:
                cmp_fp.write(json.dumps(cmp_row, ensure_ascii=False) + "\n")
                cmp_fp.flush()
                _write_review(rev)
            return

        # All gates passed — emit SFT sample
        sft_row = build_sft_sample(sample, corrected_b)
        cmp_row = build_comparison_row(sample, judge, meta)
        rev = _make_review_record(sample, judge)
        with writer_lock:
            sft_fp.write(json.dumps(sft_row, ensure_ascii=False) + "\n")
            sft_fp.flush()
            cmp_fp.write(json.dumps(cmp_row, ensure_ascii=False) + "\n")
            cmp_fp.flush()
            _write_review(rev)
        with stats_lock:
            stat_counters["ok"] += 1

    # --- Emit direct-path samples first (no LLM needed) ---
    for s in pending_direct:
        try:
            _emit_direct(s)
        except Exception as e:
            with stats_lock:
                stat_counters["other"] += 1
            print(f"  ⚠ direct 样本写入异常: {type(e).__name__}: {e}", file=sys.stderr)

    # --- Judge-path samples via ThreadPool ---
    try:
        if pending and client is not None:
            with ThreadPoolExecutor(max_workers=workers) as ex:
                futures = [ex.submit(_handle, s) for s in pending]
                completed_iter = as_completed(futures)
                if args.progress and HAS_TQDM:
                    completed_iter = tqdm(completed_iter, total=len(futures), desc="judge LLM")
                for fut in completed_iter:
                    try:
                        fut.result()
                    except Exception as e:
                        with stats_lock:
                            stat_counters["other"] += 1
                        print(f"  ⚠ worker 异常: {type(e).__name__}: {e}", file=sys.stderr)
    finally:
        sft_fp.close()
        cmp_fp.close()
        rev_fp.close()

    dt = time.time() - t0
    print()
    print(f"[done] 耗时 {dt:.1f}s")
    print(f"  ok               = {stat_counters['ok']}  (judge路径，已订正)")
    print(f"  direct_a4        = {stat_counters['direct_a4']}  (直接路径，A4)")
    print(f"  direct_no_change = {stat_counters['direct_no_change']}  (直接路径，无改进节点)")
    print(f"  judge_reject     = {stat_counters['judge_reject']}  (adopt=false)")
    print(f"  hint_mismatch    = {stat_counters['hint_mismatch']}  (hint 块被改写，已丢弃)")
    print(f"  low_score        = {stat_counters['low_score']}  (correction_score 不足)")
    print(f"  json_parse_fail  = {stat_counters['json_parse_fail']}  (裁判输出非法 JSON)")
    print(f"  llm_err          = {stat_counters['llm_err']}")
    print(f"  other            = {stat_counters['other']}")
    print(f"  sft:        {sft_path}")
    print(f"  comparison: {comparison_path}")
    print(f"  review:     {review_path}")
    return 0


# ============================================================================
# 入口
# ============================================================================

def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "后验思维链订正：直接读取 tpcds_json 目录，"
            "构建 [A]/[B]/[C] 三元组，通过裁判 LLM 生成修正后的 [B']，"
            "输出 SFT JSONL + 对照表 JSONL。"
        ),
    )
    parser.add_argument(
        "--input-dir",
        required=True,
        help="MCTS 结果 JSON 目录（tpcds_json/ 等）",
    )
    parser.add_argument(
        "--output-dir",
        required=True,
        help="输出目录；生成 sft_samples.jsonl 和 comparison.jsonl",
    )
    parser.add_argument(
        "--prompts-only",
        action="store_true",
        help="只生成 correction_prompt，不调用 LLM，写到 output_dir/prompts_only.jsonl",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="--prompts-only 的历史别名（保留兼容）",
    )
    parser.add_argument(
        "--llm-config",
        default="",
        help="可选：自定义 YAML（覆盖 mcts_defaults.yaml 里的 LLM 字段）",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=0,
        help="并发 worker 数（0 = API 池大小）",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="仅处理前 N 条（0 = 全量），用于小样本抽测",
    )
    parser.add_argument(
        "--min-correction-score",
        type=int,
        default=6,
        dest="min_fixability_score",
        help="过滤裁判评分 correction_score < 此值的样本（默认 6）",
    )
    parser.add_argument(
        "--min-adopt-score",
        type=int,
        default=0,
        dest="min_adopt_score",
        help="（预留）综合评分阈值，当前由 adopt 布尔值控制，可传 0",
    )
    parser.add_argument(
        "--min-speedup-pct",
        type=float,
        default=None,
        dest="min_speedup_pct",
        help="预过滤：仅保留 speedup >= 此百分比的节点（默认不过滤，裁判会处理 REJECT）",
    )
    parser.add_argument(
        "--min-step-improvement",
        type=float,
        default=None,
        dest="min_step_improvement",
        help="预过滤：仅保留 step_improvement >= 此值的节点（默认不过滤）",
    )
    parser.add_argument(
        "--include-no-ea",
        action="store_true",
        dest="include_no_ea",
        help="包含 explain_analyze_info 里没有记录的节点（默认跳过）",
    )
    parser.add_argument(
        "--only-new-plan",
        action="store_true",
        dest="only_new_plan",
        help="只保留 new_plan_first_found=True 的节点",
    )
    parser.add_argument(
        "--min-baseline",
        type=float,
        default=0.1,
        dest="min_baseline",
        help="entry 级别过滤：baseline_time <= 此值（秒）的 entry 整体跳过（默认 0.1s）",
    )
    parser.add_argument(
        "--filter-phantom",
        action="store_true",
        dest="filter_phantom",
        help="启用幻觉 hint 过滤：new_hints/executed_hints 里超出 candidate_hints 范围的节点会被跳过",
    )
    parser.add_argument(
        "--no-change-ratio",
        type=float,
        default=0.10,
        dest="no_change_ratio",
        help="direct 路径中无改进节点（A1/A2/A3/A5）的最大占比，相对于 A4 节点数（默认 0.10 = 10%%）",
    )
    parser.add_argument(
        "--actions",
        default="",
        help="逗号分隔的 action 过滤列表（默认全收，例如 'A1,A5,A6'）",
    )
    parser.add_argument(
        "--no-resume",
        dest="resume",
        action="store_false",
        help="不恢复：无视已有 comparison.jsonl，重跑全部（默认会恢复）",
    )
    parser.set_defaults(resume=True)
    parser.add_argument(
        "--progress",
        action="store_true",
        help="启用 tqdm 进度条（需安装 tqdm）",
    )

    args = parser.parse_args()
    sys.exit(run(args))


if __name__ == "__main__":
    main()
