"""
mcts.tree - MCTS tree node and tree operations.

The tree is a simple N-ary tree where each node holds a ``NodeState``.
UCT selection, expansion, backpropagation, and solution collection are
implemented as free functions operating on the tree.
"""
from __future__ import annotations

import json
import math
import random
from typing import List, Optional, Dict, Any

from mcts.types import (
    ActionType,
    NodeStatus,
    NodeState,
    MCTSSolution,
    TR_FINAL_ANSWER,
)
from mcts.utils.utils import compute_reward, round6


# ---------------------------------------------------------------------------
# Tree Node
# ---------------------------------------------------------------------------

class TreeNode:
    """A single node in the MCTS search tree.

    Attributes are directly accessible — no dict-based state bag.
    """
    __slots__ = (
        "parent", "children", "depth", "tag",
        "action_type", "status", "terminal_reason",
        "state",
        "reward", "rollout_index",
        # MCTS statistics
        "_visit_count", "_value_sum", "_c_puct",
    )

    def __init__(
        self,
        parent: Optional[TreeNode] = None,
        c_puct: float = 2.0,
    ) -> None:
        self.parent = parent
        self.children: List[TreeNode] = []
        self.depth: int = parent.depth + 1 if parent else 0
        self.tag: str = "0"
        self.action_type: Optional[ActionType] = None
        self.status: NodeStatus = NodeStatus.PENDING
        self.terminal_reason: Optional[str] = None
        self.state: NodeState = NodeState()
        self.reward: Optional[float] = None
        self.rollout_index: int = 0

        # MCTS statistics
        self._visit_count: int = 0
        self._value_sum: float = 0.0
        self._c_puct: float = c_puct

    # -- Properties --

    @property
    def is_leaf(self) -> bool:
        return len(self.children) == 0

    @property
    def is_root(self) -> bool:
        return self.parent is None

    @property
    def is_terminal(self) -> bool:
        return self.status == NodeStatus.TERMINAL

    @property
    def visit_count(self) -> int:
        return self._visit_count

    @property
    def q_value(self) -> float:
        if self._visit_count == 0:
            return 0.0
        return self._value_sum / self._visit_count

    def puct_score(self) -> float:
        """Compute the PUCT (UCB) score for node selection."""
        if self.parent is None:
            return 0.0
        q = self.q_value if self._visit_count > 0 else 0.0
        exploration = self._c_puct * math.sqrt(
            2.0 * math.log(self.parent._visit_count + 1) / (self._visit_count + 1e-6)
        )
        return q + exploration

    def update(self, value: float) -> None:
        """Update this node's statistics with a new value."""
        self._visit_count += 1
        self._value_sum += value

    def backpropagate(self, value: float) -> None:
        """Recursively update from this node to root."""
        node: Optional[TreeNode] = self
        while node is not None:
            node.update(value)
            node = node.parent


# ---------------------------------------------------------------------------
# Action rules (which actions are allowed given the current node)
# ---------------------------------------------------------------------------

def get_allowed_actions(node: TreeNode) -> List[ActionType]:
    """Determine allowed follow-up actions based on the node's context.

    A6 (answer) is always terminal — it never appears as a parent node.

    Rules:
      1. Root node → A1, A2, A3, A4, A6 (no A5)
      2. After A4 → A1, A2, A3, A6
      3. Previous step was A4-A5, OR A1-A3 produced new hints → A1-A6
      4. Previous step was A1-A3 and no new hints → same action + A4, A5, A6
    """
    if node.is_root:
        return [ActionType.A1_INDEX, ActionType.A2_JOIN, ActionType.A3_CONFIG,
                ActionType.A4_SUBPROBLEM, ActionType.A6_ANSWER]

    # Rule 2: After A4
    if node.action_type == ActionType.A4_SUBPROBLEM:
        return [ActionType.A1_INDEX, ActionType.A2_JOIN, ActionType.A3_CONFIG,
                ActionType.A6_ANSWER]

    # Check if this node changed hints (added or removed)
    has_hint_changes = len(node.state.new_hints) > 0 or len(node.state.deleted_hints) > 0

    # Rule 3: A4-A5, or A1-A3 with hint changes → allow all
    if node.action_type in (ActionType.A4_SUBPROBLEM, ActionType.A5_RETHINK):
        return list(ActionType)

    if has_hint_changes:
        return list(ActionType)

    # Rule 4: A1-A3 without hint changes → same action + A4, A5, A6
    allowed = {node.action_type, ActionType.A4_SUBPROBLEM, ActionType.A5_RETHINK, ActionType.A6_ANSWER}
    all_actions = list(ActionType)
    return [a for a in all_actions if a in allowed]


# ---------------------------------------------------------------------------
# Tree operations
# ---------------------------------------------------------------------------

def create_child_nodes(
    parent: TreeNode,
    actions: List[ActionType],
    rollout_index: int,
    c_puct: float,
) -> List[TreeNode]:
    """Create one child node per action type and attach to parent."""
    new_children: List[TreeNode] = []
    for action in actions:
        child = TreeNode(parent=parent, c_puct=c_puct)
        child.tag = f"{parent.tag}.{len(parent.children) + 1}"
        child.action_type = action
        child.rollout_index = rollout_index
        parent.children.append(child)
        new_children.append(child)
    return new_children


def select_leaf(root: TreeNode) -> Optional[TreeNode]:
    """MCTS Selection: traverse from root to a leaf using PUCT.

    Skips terminal nodes. Returns None if no expandable leaf exists.
    """
    node = root
    while not node.is_leaf:
        best_score = -float("inf")
        best_children: List[TreeNode] = []

        for child in node.children:
            if child.is_terminal:
                continue
            score = child.puct_score()
            if score > best_score:
                best_score = score
                best_children = [child]
            elif score == best_score:
                best_children.append(child)

        if not best_children:
            # All children are terminal — this node is effectively terminal
            return None

        node = random.choice(best_children)

    if node.is_terminal:
        return None
    return node


def collect_partial_solution_text(node: TreeNode) -> str:
    """Build a text representation of the reasoning path from root to node."""
    path: List[str] = []
    current: Optional[TreeNode] = node
    while current is not None:
        if current.state.llm_response_text:
            path.append(current.state.llm_response_text)
        current = current.parent
    path.reverse()
    return "\n".join(path)


def collect_ancestor_hints(node: TreeNode) -> List[str]:
    """Collect all executed hints from root to node's parent.

    Returns hints in root→parent order so that, under "last wins" dedup
    semantics, deeper ancestors override shallower ones. Each level's
    ``deleted_hints`` are removed from the accumulator so that explicit
    overrides propagate.
    """
    # Walk root→parent.
    chain: List[TreeNode] = []
    current: Optional[TreeNode] = node.parent
    while current is not None:
        chain.append(current)
        current = current.parent
    chain.reverse()

    hints: List[str] = []
    for anc in chain:
        deleted = getattr(anc.state, "deleted_hints", None) or []
        if deleted:
            deleted_set = set(deleted)
            hints = [h for h in hints if h not in deleted_set]
        for h in anc.state.new_hints:
            if h not in hints:
                hints.append(h)
    return hints


def _collect_trajectory(node: TreeNode) -> List[str]:
    """Collect the trajectory (root→node) as an ordered list of node tags."""
    tags: List[str] = []
    current: Optional[TreeNode] = node
    while current is not None:
        if not current.is_root:
            tags.append(current.tag)
        current = current.parent
    tags.reverse()
    return tags


def collect_solutions(root: TreeNode, baseline_time: Optional[float]) -> List[MCTSSolution]:
    """Walk the tree and collect all nodes that found a new execution plan.

    Collects both terminal nodes and nodes with new_plan_first_found=True.
    Solutions are sorted by reward (descending).
    """
    solutions: List[MCTSSolution] = []
    stack = [root]

    while stack:
        node = stack.pop()

        # Collect if: terminal final_answer, OR first found a new plan
        is_solution = (
            (node.is_terminal and node.terminal_reason == TR_FINAL_ANSWER)
            or node.state.new_plan_first_found
        )

        if is_solution and node.state.executed_hints:
            reward = node.reward
            if reward is None and baseline_time is not None and node.state.execution_time_seconds is not None:
                reward = compute_reward(baseline_time, node.state.execution_time_seconds)

            solutions.append(MCTSSolution(
                executed_hints=list(node.state.executed_hints),
                execution_time_seconds=node.state.execution_time_seconds,
                plan_digest=node.state.plan_digest,
                reward=reward,
                q_value=node.q_value,
                action_type=node.action_type.value,
                node_tag=node.tag,
                rollout_index=node.rollout_index,
                depth=node.depth,
            ))

        for child in node.children:
            stack.append(child)

    solutions.sort(key=lambda s: s.reward if s.reward is not None else -999, reverse=True)
    return solutions


def _compute_step_improvement(baseline: Optional[float], current: Optional[float]) -> Optional[float]:
    """Compute (baseline / current) - 1. Returns 0 if difference is negligible."""
    if not baseline or not current:
        return None
    if abs(baseline - current) / max(baseline, current) < 1e-4:
        return 0.0
    return round6(baseline / current - 1)


def _explain_analyze_for_dump(node_state: NodeState) -> Any:
    """Extract the EXPLAIN ANALYZE JSON from a node's db_result.

    The DB executor captures the raw server string into
    ``db_result.explain_analyze_json``. We try to parse it so the downstream
    payload stays structured JSON (easier to diff / inspect). If the string
    is not valid JSON we fall back to emitting it verbatim.

    Returns ``None`` when no EXPLAIN ANALYZE was captured for this node (e.g.
    plan-digest-only probes that reused a cached execution time).
    """
    db = node_state.db_result
    if db is None:
        return None
    raw = db.explain_analyze_json
    if not raw:
        return None
    if isinstance(raw, (dict, list)):
        return raw
    if isinstance(raw, str):
        s = raw.strip()
        if not s:
            return None
        if s[0] in "[{":
            try:
                return json.loads(s)
            except ValueError:
                pass
        return raw
    return raw


def build_explain_analyze_info(root: TreeNode) -> Dict[str, Any]:
    """Aggregate EXPLAIN ANALYZE payloads indexed by ``plan_digest``.

    Walks the full tree. For each ``plan_digest`` we keep the first non-empty
    EXPLAIN ANALYZE we encounter (BFS by depth-then-tag). Rationale: once a
    plan has been explained, later nodes that land on the same plan usually
    hit the plan-digest cache and leave ``explain_analyze_json`` empty — but
    the cached node still carries a meaningful plan_digest. Keeping a
    single-source-of-truth map lets every node reference its plan's EA via
    the plan_digest, independent of whether the node itself hit the cache.

    Returns a dict like::

        {
          "0x00...a": { "query_block": { ... } },
          "0x00...b": "plain-text fallback",
          ...
        }

    Entries with empty / missing EA are skipped.
    """
    nodes: List[TreeNode] = []
    stack = [root]
    while stack:
        node = stack.pop()
        nodes.append(node)
        for ch in node.children:
            stack.append(ch)

    def _tag_sort_key(n: TreeNode):
        parts = n.tag.split(".")
        return (n.depth, [int(p) for p in parts])

    nodes.sort(key=_tag_sort_key)

    info: Dict[str, Any] = {}
    for node in nodes:
        pd = node.state.plan_digest
        if not pd or pd in info:
            continue
        ea = _explain_analyze_for_dump(node.state)
        if ea is None:
            continue
        info[pd] = ea
    return info


def dump_tree(root: TreeNode, baseline_time: Optional[float] = None) -> Dict[str, Any]:
    """Create an ordered flat dict dump of the tree, sorted by depth then tag (BFS order).

    Each entry includes LLM prompt/response for full traceability.
    """
    nodes: List[TreeNode] = []
    stack = [root]

    while stack:
        node = stack.pop()
        nodes.append(node)
        for child in node.children:
            stack.append(child)

    # Sort by depth first, then tag (lexicographic on dot-separated integers)
    def _tag_sort_key(n: TreeNode):
        parts = n.tag.split(".")
        return (n.depth, [int(p) for p in parts])

    nodes.sort(key=_tag_sort_key)

    result: Dict[str, Any] = {}
    for node in nodes:
        entry: Dict[str, Any] = {
            "node_info": {
                "depth": node.depth,
                "action_type": node.action_type.value if node.action_type else None,
                "status": node.status.value,
                "terminal_reason": node.terminal_reason,
                "reward": round6(node.reward),
                "q_value": round6(node.q_value),
                "visit_count": node._visit_count,
                "rollout_index": node.rollout_index,
                "executed_hints": node.state.executed_hints,
                "new_hints": node.state.new_hints,
                "deleted_hints": node.state.deleted_hints,
                "new_plan_first_found": node.state.new_plan_first_found,
            },
            "llm_response": {
                "response": node.state.llm_response_text,
                "input_length": node.state.llm_input_chars,
                "output_length": node.state.llm_output_chars,
                "response_time": round6(node.state.llm_latency_seconds),
            },
            "db_response": {
                "execution_time_s": round6(node.state.execution_time_seconds),
                "step_improvement": _compute_step_improvement(baseline_time, node.state.execution_time_seconds),
                "plan_digest": node.state.plan_digest,
            },
        }
        result[node.tag] = entry

    return result
