"""
mcts.utils.prompts - Prompt templates for each action type.

All prompts are defined inline (no external JSON files) to keep
the mcts module fully self-contained.

This file is the single source of truth for prompt templates.
Both the runtime MCTS search (`build_action_prompt`) and the offline
SFT data generation script (`mcts_scripts/sft_data/run_sft.py`) import
`_ACTION_TEMPLATES` from here. Keep them in sync by editing only this file.

Output format convention (all actions):
- Every action's output MUST begin with two header lines that identify the
  action and the step number:
    ``[动作] A<k> <动作名称>``
    ``[步骤] {step_number}``
- For every action except A4, the model then outputs the chosen hints wrapped
  in a pair of ``<hints>`` ... ``</hints>`` tags containing a ``/*+ ... */``
  block, e.g. ``<hints> /*+ INDEX(t idx_a) */ </hints>``.
- For A4 (subproblem), the model outputs a concise subproblem description
  wrapped in ``<subproblem>`` ... ``</subproblem>`` and MUST NOT emit any hints.
- No ``</think>`` or ``</step>`` tags are required anywhere.
"""
from __future__ import annotations

import json
import re
from enum import Enum
from typing import Any, Dict, List, Set, Tuple

from pydantic import BaseModel, Field

from mcts.types import ActionType


# ---------------------------------------------------------------------------
# Hint category enum (replaces string-based category mapping)
# ---------------------------------------------------------------------------

class HintCategory(str, Enum):
    """Categories of MySQL optimizer hints."""
    INDEX = "index"
    JOIN_ORDER = "join_order"
    CONFIG = "config"
    NONE = "none"       # A4 (subproblem): no hints
    ALL = "all"         # A5/A6: all categories


# ---------------------------------------------------------------------------
# Hint category descriptions (constant)
# ---------------------------------------------------------------------------

HINT_DESC: Dict[HintCategory, str] = {
    HintCategory.INDEX: (
        "索引优化Hint说明：\n"
        "- INDEX(table_name index_name): 针对某个表强制使用某个索引进行扫描或者查找。"
        "括号内第一个是表名(或表别名)，第二个是索引名，两者都不可省略。\n"
        "- NO_INDEX(table_name): 禁用该表上所有可用索引，强制进行全表扫描。"
        "括号内只能写表名(或表别名)，严禁在其后跟任何索引名；"
        "如需禁用具体某个索引，请直接改用其他 INDEX(...) 选项，而不是 NO_INDEX(table idx)。\n"
        "- 索引Hint主要用于优化查询中的表的访问方式，提高查询性能。"
    ),
    HintCategory.JOIN_ORDER: (
        "连接顺序优化Hint说明：\n"
        "- JOIN_PREFIX(table1): 指定某表在进行连接操作时，作为连接的第一个表。\n"
        "- JOIN_SUFFIX(table1): 指定某表在进行连接操作时，作为连接的最后一个表。\n"
        "- JOIN_PREFIX(table1, table2, ...): 指定连接顺序，table1在table2之前连接。\n"
        "- 连接顺序Hint用于优化多表连接时的执行顺序，影响查询性能。\n"
        "- !!! 表名匹配规则：如果 SQL 查询中为某个表定义了别名（例如 "
        "`FROM orders o JOIN customer c ...`），则 JOIN_PREFIX / JOIN_SUFFIX 中"
        "必须使用表别名（`o`、`c`），不得使用原表名；如果该表没有定义别名，"
        "才直接使用原表名。"
    ),
    HintCategory.CONFIG: (
        "配置优化Hint说明：\n"
        "- 配置Hint用于调整MySQL优化器的行为参数，影响查询优化策略。"
        "SET_VAR()用于设置会话级别的优化器变量。具体的配置语义如下：\n"
        "- Semijoin执行策略：MySQL 在执行 IN/EXISTS 子查询时支持四种 semijoin 策略："
        "Duplicate Weedout（join 后用临时表去重）、FirstMatch（找到首行匹配即停止）、"
        "Materialization（把子查询结果物化成临时表再参与 join）、"
        "LooseScan（利用索引有序性做松散扫描快速判存）；"
        "也可以通过 semijoin=off 完全关闭 semijoin 优化。\n"
        "- 子查询转换策略：子查询有两种主要转换方式：INTOEXISTS（将 IN (subquery) 转换为 EXISTS 半连接）、"
        "MATERIALIZATION（将子查询物化成临时表并加入外层查询处理）。\n"
        "- subquery_materialization_cost_based：ON（默认）基于成本进行智能选择，"
        "OFF 优先使用 MATERIALIZATION。\n"
        "- derived_merge：决定优化器是否尝试将派生表、视图、CTE 合并回外层查询。\n"
        "- subquery_to_derived：subquery_to_derived=on 允许优化器将标量子查询重写为派生表 + LEFT JOIN。"
    ),
}


# ---------------------------------------------------------------------------
# Action → hint category mapping
# ---------------------------------------------------------------------------

ACTION_HINT_CATEGORY: Dict[ActionType, HintCategory] = {
    ActionType.A1_INDEX: HintCategory.INDEX,
    ActionType.A2_JOIN: HintCategory.JOIN_ORDER,
    ActionType.A3_CONFIG: HintCategory.CONFIG,
    ActionType.A4_SUBPROBLEM: HintCategory.NONE,
    ActionType.A5_RETHINK: HintCategory.ALL,
    ActionType.A6_ANSWER: HintCategory.ALL,
}


# ---------------------------------------------------------------------------
# Per-action prompt template (typed)
#
# Tag convention inside the output format section:
#   [短标签] 简短引导这一段该关注什么；模型自行决定深度与长度。
# e.g. [概括已有步骤] 复盘每一步 Hint 及其 execution_time 变化，判断是否生效。
# ---------------------------------------------------------------------------

class ActionPromptTemplate(BaseModel):
    """Typed prompt template for a single action."""
    system: str
    task: str


_ACTION_TEMPLATES: Dict[ActionType, ActionPromptTemplate] = {
    ActionType.A1_INDEX: ActionPromptTemplate(
        system=(
            "你是一个专业的SQL优化专家，精通数据库查询优化。一步一步进行优化。"
            "你需要针对执行计划中的单表访问路径问题进行优化。\n\n"
            "<动作>：A1 单步思考-索引选择修正\n\n"
            "<输出约定>：最终推荐的Hint必须用 <hints> /*+ ... */ </hints> 包裹；"
            "若认为没有继续优化空间，则输出 <hints> /*+ */ </hints>。"
        ),
        task=(
            "现在进行 A1 动作：单步思考-索引选择修正\n\n"
            "<查询>: {query}\n\n"
            "<执行计划信息>: {execution_info}\n\n"
            "<候选Hints>: {candidate_hints}\n\n"
            "<已有推理步骤>: {partial_solution}\n\n"
            "<当前步骤编号>: {step_number}\n\n"
            "<任务>：定位执行计划中访问路径不合理的表，从候选中选出一个 INDEX / NO_INDEX。"
            "所选 Hint 影响的表应与已生效 Hint 的表不同。\n\n"
            "<输出格式（必须严格遵守）>:\n"
            "[动作] A1 单步思考-索引选择修正\n"
            "[步骤] {step_number}\n"
            "[概括已有步骤] 复盘每一步 Hint 及其 execution_time 变化，判断是否生效、是否存在问题。\n"
            "[分析尚未解决的问题] 指出执行时间占比大或过滤性差的表。\n"
            "[语义分析] 结合过滤条件/连接键语义，比较候选 INDEX/NO_INDEX 的效果。\n"
            "[选择候选 Hint] 选一个并说明理由。\n"
            "<hints> /*+ 推荐的一个Hint */ </hints>\n\n"
            "!!! 关键要求：\n"
            "- 只能从候选Hints中选择一个Hint放入 <hints>，不得创建索引。\n"
            "- 所选Hint影响的表不得与已有推理步骤中生效Hint的表重复。\n"
            "- 所有输出的Hint必须出现在<候选Hints>中。\n"
            "- 输出 </hints> 之后立即结束回答。\n"
        ),
    ),
    ActionType.A2_JOIN: ActionPromptTemplate(
        system=(
            "你是一个专业的SQL优化专家，精通数据库查询优化。一步一步进行优化。"
            "你需要针对连接顺序问题进行单步思考。\n\n"
            "<动作>：A2 单步思考-修改连接顺序\n\n"
            "<输出约定>：最终推荐的Hint必须用 <hints> /*+ ... */ </hints> 包裹；"
            "若认为没有继续优化空间，则输出 <hints> /*+ */ </hints>。"
        ),
        task=(
            "现在进行 A2 动作：单步思考-修改连接顺序\n\n"
            "<查询>: {query}\n\n"
            "<执行信息>: {execution_info}\n\n"
            "<候选Hints>: {candidate_hints}\n\n"
            "<已有推理步骤>: {partial_solution}\n\n"
            "<当前步骤编号>: {step_number}\n\n"
            "<任务>：从候选中选出一个 JOIN_PREFIX 或 JOIN_SUFFIX。"
            "JOIN_PREFIX(t1, t2, ...) 可以一次指定多个表的连续连接顺序，JOIN_SUFFIX 同理。\n\n"
            "<输出格式（必须严格遵守）>:\n"
            "[动作] A2 单步思考-修改连接顺序\n"
            "[步骤] {step_number}\n"
            "[概括已有步骤] 复盘每一步 Hint 及其 execution_time 变化，判断是否生效。\n"
            "[分析尚未解决的问题] 指出中间结果集过大或 loop*actual time 占比高的连接。\n"
            "[语义分析] 结合连接键语义，判断哪些表应提前/推后，以及多表之间的相对顺序。\n"
            "[选择候选 Hint] 选一个 JOIN_PREFIX/JOIN_SUFFIX 并说明理由。\n"
            "<hints> /*+ 推荐的一个Hint */ </hints>\n\n"
            "!!! 关键要求：\n"
            "- 本步只输出一个 JOIN_PREFIX 或一个 JOIN_SUFFIX。\n"
            "- 所有输出的Hint必须完整出现在<候选Hints>中（不要自行拼装表组合）。\n"
            "- 输出 </hints> 之后立即结束回答。\n"
        ),
    ),
    ActionType.A3_CONFIG: ActionPromptTemplate(
        system=(
            "你是一个专业的SQL优化专家，精通数据库查询优化。一步一步进行优化。"
            "你需要针对 Semijoin / 子查询相关的优化器配置问题进行单步思考。\n\n"
            "<动作>：A3 单步思考-修改配置\n\n"
            "<输出约定>：最终推荐的Hint必须用 <hints> /*+ ... */ </hints> 包裹；"
            "若认为没有继续优化空间，则输出 <hints> /*+ */ </hints>。"
        ),
        task=(
            "现在进行 A3 动作：单步思考-修改配置\n\n"
            "<查询>: {query}\n\n"
            "<执行信息>: {execution_info}\n\n"
            "<候选Hints>: {candidate_hints}\n\n"
            "<已有推理步骤>: {partial_solution}\n\n"
            "<当前步骤编号>: {step_number}\n\n"
            "<任务>：从候选中选出一个 SET_VAR(...) 配置 Hint，不应与已生效的 Hint 重复。\n\n"
            "<输出格式（必须严格遵守）>:\n"
            "[动作] A3 单步思考-修改配置\n"
            "[步骤] {step_number}\n"
            "[概括已有步骤] 复盘每一步配置 Hint 及其 execution_time 变化，判断是否生效。\n"
            "[分析配置问题] 指出 semijoin / 子查询 / 派生表路径中 actual time 偏高的点。\n"
            "[选择候选 Hint] 选一个配置 Hint 并说明理由。\n"
            "<hints> /*+ 推荐的一个Hint */ </hints>\n\n"
            "!!! 关键要求：\n"
            "- 只能从候选Hints中选择一个配置 Hint 放入 <hints>。\n"
            "- 所有输出的Hint必须出现在<候选Hints>中。\n"
            "- 输出 </hints> 之后立即结束回答。\n"
        ),
    ),
    ActionType.A4_SUBPROBLEM: ActionPromptTemplate(
        system=(
            "你是一个专业的SQL优化专家，精通数据库查询优化。一步一步进行优化。"
            "你负责对当前查询的优化过程进行查漏补缺，提出新的值得关注的子问题。\n\n"
            "<动作>：A4 提出新的子问题\n\n"
            "<输出约定>：最终的新子问题必须用 <subproblem>...</subproblem> 包裹；"
            "本动作不产生任何 Hint，不得输出 <hints> 标签。"
        ),
        task=(
            "现在进行 A4 动作：提出新的子问题\n\n"
            "<查询>: {query}\n\n"
            "<执行信息>: {execution_info}\n\n"
            "<候选Hints>: {candidate_hints}\n\n"
            "<已有推理步骤>: {partial_solution}\n\n"
            "<当前步骤编号>: {step_number}\n\n"
            "<任务>：查漏补缺，找出尚未充分探索的优化方向，提出一个具体的新子问题。\n\n"
            "<输出格式（必须严格遵守）>:\n"
            "[动作] A4 提出新的子问题\n"
            "[步骤] {step_number}\n"
            "[概括已有步骤] 复盘已尝试的方向（索引 / 连接顺序 / 配置），判断哪些已经收敛、哪些只是浅尝辄止。\n"
            "[识别尚未覆盖的方向] 指出还可能提升的问题点，可以列多个候选方向。\n"
            "[选择最有价值的子问题] 挑一个最值得深入的方向，说明理由。\n"
            "<subproblem>用一句话明确、简练地概括这个子问题。</subproblem>\n\n"
            "!!! 关键要求：\n"
            "- <subproblem>...</subproblem> 内部必须是一句话，便于后续动作承接。\n"
            "- 本动作不得输出任何 Hint，也不得包含 <hints> 标签或 /*+ ... */ 块。\n"
            "- 输出 </subproblem> 之后立即结束回答。\n"
        ),
    ),
    ActionType.A5_RETHINK: ActionPromptTemplate(
        system=(
            "你是一个专业的SQL优化专家，精通数据库查询优化。一步一步进行优化。"
            "你需要回顾整条推理路径，发现错误或不足并给出修正后的 Hint 组合。\n\n"
            "<动作>：A5 重新思考\n\n"
            "<输出约定>：最终推荐的Hint必须用 <hints> /*+ ... */ </hints> 包裹；"
            "若认为没有继续优化空间，则输出 <hints> /*+ */ </hints>。"
        ),
        task=(
            "现在进行 A5 动作：重新思考\n\n"
            "<查询>: {query}\n\n"
            "<执行信息>: {execution_info}\n\n"
            "<候选Hints>: {candidate_hints}\n\n"
            "<已有推理步骤>: {partial_solution}\n\n"
            "<当前步骤编号>: {step_number}\n\n"
            "<任务>：回顾推理路径，找出可能有误或无效的步骤，提出修正方案，"
            "并与已生效的 Hint 汇总成完整组合。\n\n"
            "<输出格式（必须严格遵守）>:\n"
            "[动作] A5 重新思考\n"
            "[步骤] {step_number}\n"
            "[概括已有步骤] 复盘每一步 Hint 及其 execution_time 变化，判断是否真的生效或推理有误。\n"
            "[识别推理中的问题] 指出哪些步骤的分析或选择可能错了，错在哪里。\n"
            "[重新思考] 针对问题给出修正思路；若有多种方案可以简要对比后收敛。\n"
            "[整合修正方案] 列出最终要保留的 Hint 组合。\n"
            "<hints> /*+ 整合后的完整Hint组合 */ </hints>\n\n"
            "!!! 关键要求：\n"
            "- 所有输出的Hint必须出现在<候选Hints>中，不得创建索引。\n"
            "- Hint组合中最多只有一个 JOIN_PREFIX 和一个 JOIN_SUFFIX。\n"
            "- 输出 </hints> 之后立即结束回答。\n"
        ),
    ),
    ActionType.A6_ANSWER: ActionPromptTemplate(
        system=(
            "你是一个专业的SQL优化专家，精通数据库查询优化。一步一步进行优化。"
            "你需要停止思考，对前面的推理步骤进行归纳总结并给出最终 Hint 组合。\n\n"
            "<动作>：A6 总结回答\n\n"
            "<输出约定>：最终推荐的Hint必须用 <hints> /*+ ... */ </hints> 包裹。"
        ),
        task=(
            "现在进行 A6 动作：总结回答\n\n"
            "<查询>: {query}\n\n"
            "<执行信息>: {execution_info}\n\n"
            "<候选Hints>: {candidate_hints}\n\n"
            "<之前推理步骤>: {partial_solution}\n\n"
            "<当前步骤编号>: {step_number}\n\n"
            "<任务>：总结之前推理步骤中生效的 Hint，给出最终答案。\n\n"
            "<输出格式（必须严格遵守）>:\n"
            "[动作] A6 总结回答\n"
            "[步骤] {step_number}\n"
            "[概括已有步骤] 复盘每一步 Hint 及其 execution_time 变化，确认哪些真正生效、最终保留哪些。\n"
            "[确认没有遗漏] 简述是否还有值得继续优化的方向以及停止的原因。\n"
            "<hints> /*+ 整合后的完整Hint组合 */ </hints>\n\n"
            "!!! 关键要求：\n"
            "- 所有输出的 Hint 必须出现在 <候选Hints> 中（不要创建索引）。\n"
            "- Hint 组合中最多只有一个 JOIN_PREFIX 和一个 JOIN_SUFFIX。\n"
            "- 输出 </hints> 之后立即结束回答。\n"
        ),
    ),
}


# ---------------------------------------------------------------------------
# Index info model (replaces untyped Dict[str, Any])
# ---------------------------------------------------------------------------

class IndexDetail(BaseModel):
    """Metadata for a single index."""
    columns: List[str] = Field(default_factory=list)
    unique: bool = False


class TableIndexInfo(BaseModel):
    """Index information for a single table."""
    indexes: Dict[str, IndexDetail] = Field(default_factory=dict)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def filter_candidate_hints(
    all_hints: Dict[str, List[str]],
    action: ActionType,
) -> Dict[str, List[str]]:
    """Return only the hint categories relevant to *action*."""
    category = ACTION_HINT_CATEGORY[action]
    if category == HintCategory.ALL:
        return all_hints
    if category == HintCategory.NONE:
        return {}
    if category.value in all_hints:
        return {category.value: all_hints[category.value]}
    return {}


def format_index_info(index_info: Dict[str, Any]) -> str:
    """Format index_info dict into a human-readable string for prompts.

    Accepts Dict[str, Dict[str, Dict]] with keys: columns, unique, expressions.
    """
    if not index_info:
        return ""
    lines: List[str] = []
    for table_name, indexes in index_info.items():
        for index_name, info in indexes.items():
            unique_text = "是unique的" if info.get("unique") else "不是unique的"
            parts: List[str] = [f"INDEX({table_name} {index_name})"]
            columns = info.get("columns") or []
            if columns:
                parts.append(f"设置的列为{'，'.join(columns)}")
            expressions = info.get("expressions") or []
            if expressions:
                parts.append(f"设置的表达式为{'，'.join(expressions)}")
            parts.append(f"其{unique_text}")
            lines.append("，".join(parts) + ";")
    return "\n".join(lines)


# Match `INDEX(table_name index_name)` — only positive INDEX hints contribute
# to the allow-list. NO_INDEX hints refer to tables, not named indexes, so
# they are not used to gate which entries in index_info survive.
_INDEX_HINT_PATTERN = re.compile(
    r"\bINDEX\s*\(\s*([^\s,)]+)\s+([^\s,)]+)\s*\)",
    re.IGNORECASE,
)


def _collect_index_allowlist(
    candidate_index_hints: List[str],
) -> Set[Tuple[str, str]]:
    """Return the set of (table, index_name) pairs referenced by candidate INDEX hints.

    Names are lower-cased so comparison against index_info keys is case-insensitive.
    Backticks / single / double quotes around identifiers are stripped.
    """
    allow: Set[Tuple[str, str]] = set()
    if not candidate_index_hints:
        return allow
    for hint in candidate_index_hints:
        if not isinstance(hint, str):
            continue
        for m in _INDEX_HINT_PATTERN.finditer(hint):
            table, idx = m.group(1), m.group(2)
            allow.add((_normalize_ident(table), _normalize_ident(idx)))
    return allow


def _normalize_ident(name: str) -> str:
    s = (name or "").strip()
    if len(s) >= 2 and s[0] in ("`", "'", '"') and s[-1] == s[0]:
        s = s[1:-1]
    return s.lower()


def filter_index_info_by_candidates(
    index_info: Dict[str, Any],
    candidate_index_hints: List[str],
) -> Dict[str, Any]:
    """Return a copy of ``index_info`` containing only indexes that appear as
    ``INDEX(table idx_name)`` in the candidate hint list.

    If ``candidate_index_hints`` is empty, the function returns an empty dict
    (no index should be advertised to the model when nothing is recommendable).
    Tables that lose all of their indexes after filtering are removed.
    """
    if not index_info:
        return {}
    allow = _collect_index_allowlist(candidate_index_hints or [])
    if not allow:
        return {}
    filtered: Dict[str, Any] = {}
    for table, indexes in index_info.items():
        if not isinstance(indexes, dict):
            continue
        table_key = _normalize_ident(table)
        kept: Dict[str, Any] = {}
        for idx_name, detail in indexes.items():
            if (table_key, _normalize_ident(idx_name)) in allow:
                kept[idx_name] = detail
        if kept:
            filtered[table] = kept
    return filtered


def build_enhanced_candidate_hints(
    filtered_hints: Dict[str, List[str]],
    action: ActionType,
    index_info: Dict[str, Any],
) -> Dict[str, Any]:
    """Build candidate hints with descriptions for prompt inclusion.

    ``index_info`` is filtered down to only the indexes that also appear in
    ``filtered_hints["index"]`` as ``INDEX(table idx_name)``. Indexes that are
    not candidates are dropped so the model does not see irrelevant options.
    """
    category = ACTION_HINT_CATEGORY[action]
    enhanced: Dict[str, Any] = {}

    if category == HintCategory.ALL:
        for cat_str, hints in filtered_hints.items():
            cat_enum = HintCategory(cat_str) if cat_str in HintCategory._value2member_map_ else None
            desc = HINT_DESC.get(cat_enum, "") if cat_enum else ""
            enhanced[cat_str] = {"hints": hints, "description": desc}
    else:
        cat_str = category.value
        if cat_str in filtered_hints:
            enhanced[cat_str] = {
                "hints": filtered_hints[cat_str],
                "description": HINT_DESC.get(category, ""),
            }

    if category in (HintCategory.INDEX, HintCategory.ALL):
        candidate_index_hints = list(filtered_hints.get("index") or [])
        filtered_index_info = filter_index_info_by_candidates(
            index_info or {}, candidate_index_hints,
        )
        formatted = format_index_info(filtered_index_info)
        enhanced.setdefault("index", {})
        enhanced["index"]["index_info"] = formatted

    return enhanced


def build_action_prompt(
    action: ActionType,
    query: str,
    execution_info: str,
    candidate_hints: Dict[str, List[str]],
    index_info: Dict[str, Any],
    partial_solution: str,
    step_number: int,
    rag_refs: Any = None,
) -> str:
    """Build the complete prompt for a single action step.

    Args:
        rag_refs: optional RagContext (mcts.rag.types.RagContext) with retrieved
            historical references. When None / empty, the prompt is identical to
            the pre-RAG prompt — so RAG-off behaviour is unchanged.

    Returns:
        The full prompt string ready to send to the LLM.
    """
    template = _ACTION_TEMPLATES[action]

    filtered = filter_candidate_hints(candidate_hints, action)
    enhanced = build_enhanced_candidate_hints(filtered, action, index_info)

    prompt = template.system + "\n\n" + template.task.format(
        query=query,
        execution_info=execution_info,
        candidate_hints=json.dumps(enhanced, ensure_ascii=False),
        partial_solution=partial_solution,
        step_number=step_number,
    )

    rag_section = _format_rag_refs(rag_refs, action)
    if rag_section:
        prompt = prompt + "\n\n" + rag_section
    return prompt


# Hint-type prefixes relevant to each action, so the historical-reference block
# only surfaces hints the current action can actually use.
_ACTION_HINT_PREFIXES: Dict[ActionType, Tuple[str, ...]] = {
    ActionType.A1_INDEX: ("INDEX(", "NO_INDEX("),
    ActionType.A2_JOIN: ("JOIN_PREFIX(", "JOIN_SUFFIX("),
    ActionType.A3_CONFIG: ("SET_VAR(",),
}


def _format_rag_refs(rag_refs: Any, action: ActionType) -> str:
    """Render retrieved historical references as a prompt section.

    Returns an empty string when there is nothing to inject (rag_refs is None /
    empty, or no hint in the references is relevant to this action), so the
    prompt is byte-for-byte unchanged whenever RAG yields nothing.

    The block is explicitly framed as advisory (参考、非强制) to avoid the model
    blindly copying historical hints that may not fit the current plan.
    """
    refs = getattr(rag_refs, "refs", None)
    if not refs:
        return ""

    # For A1-A3, only show hints of the matching type; A4-A6 may see all.
    prefixes = _ACTION_HINT_PREFIXES.get(action)

    lines: List[str] = []
    for ref in refs:
        qc = getattr(ref, "qconfig", None)
        if qc is None:
            continue
        hints = list(getattr(qc, "executed_hints", []) or [])
        if prefixes:
            hints = [h for h in hints if h.strip().upper().startswith(prefixes)]
        if not hints:
            continue
        sim = getattr(ref, "similarity", 0.0) or 0.0
        impr = getattr(qc, "improvement_ratio", None)
        impr_txt = f"，历史加速约{impr:.1f}x" if isinstance(impr, (int, float)) and impr > 0 else ""
        lines.append(f"- [相似度{sim:.2f}{impr_txt}] {' '.join(hints)}")

    if not lines:
        return ""

    return (
        "<历史参考>\n"
        "以下是与当前查询结构相似的历史查询中，曾真实带来执行时间下降的 Hint 组合，"
        "仅供参考。请结合当前执行计划判断是否适用，不要盲目照搬；"
        "最终仍须从 <候选Hints> 中选择。\n"
        + "\n".join(lines)
        + "\n</历史参考>"
    )
