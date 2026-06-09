#!/usr/bin/env python3
"""
Unit tests for mcts core logic (no database required).

Tests:
  - types: reward computation, model construction
  - hint_utils: parsing, deduplication, SQL building
  - prompts: prompt building
  - tree: node creation, selection, backpropagation, action rules
  - search: output parsing
"""
import sys
import os
import math

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))


class TestResult:
    def __init__(self):
        self.passed = 0
        self.failed = 0
        self.errors = []

    def check(self, condition: bool, description: str, detail: str = ""):
        if condition:
            self.passed += 1
        else:
            self.failed += 1
            msg = f"FAIL: {description}"
            if detail:
                msg += f" ({detail})"
            print(f"  ✗ {msg}")
            self.errors.append(description)

    def summary(self) -> bool:
        total = self.passed + self.failed
        print(f"\n{'='*60}")
        print(f"[Summary] {self.passed}/{total} checks passed")
        if self.errors:
            for e in self.errors:
                print(f"  - {e}")
        return self.failed == 0


def test_types(t: TestResult):
    print("\n--- test_types ---")
    from mcts.types import (
        ActionType, NodeStatus,
        MCTSConfig, MCTSInputData, MCTSRunMetrics,
        MCTSSolution, MCTSSearchResult,
        NodeState, DBExecutionResult, ParsedLLMOutput,
        TR_FINAL_ANSWER,
    )
    from mcts.utils.utils import compute_reward

    # compute_reward
    t.check(compute_reward(None, 1.0) is None, "reward: None baseline")
    t.check(compute_reward(1.0, None) is None, "reward: None current")
    t.check(compute_reward(0, 1.0) is None, "reward: zero baseline")
    t.check(compute_reward(1.0, 0) is None, "reward: zero current")

    r = compute_reward(2.0, 1.0)
    t.check(r is not None, "reward: valid inputs")
    t.check(abs(r - math.tanh(math.log(2.0))) < 1e-6, "reward: correct value")

    r2 = compute_reward(1.0, 2.0)
    t.check(r2 is not None and r2 < 0, "reward: negative when slower")

    # ActionType enum
    t.check(ActionType.A1_INDEX.value == "A1", "ActionType.A1_INDEX")
    t.check(len(list(ActionType)) == 6, "6 action types")

    # NodeStatus
    t.check(NodeStatus.PENDING.value == "pending", "NodeStatus.PENDING")

    # MCTSConfig defaults
    cfg = MCTSConfig()
    t.check(cfg.max_depth == 3, "default max_depth=3")
    t.check(cfg.c_puct == 2.5, "default c_puct=2.5")

    # MCTSRunMetrics
    m = MCTSRunMetrics()
    m.record_llm_call(100, 200, 1.5)
    t.check(m.llm_call_count == 1, "metrics: call count")
    t.check(m.llm_input_chars == 100, "metrics: input chars")
    m.record_db_execute(0.5)
    t.check(m.db_execute_count == 1, "metrics: db exec count")
    m.finalize()
    t.check(m.llm_chars_per_second > 0, "metrics: chars/s after finalize")

    # DBExecutionResult
    ok = DBExecutionResult(plan_digest="abc", execution_time_seconds=0.5)
    t.check(ok.is_success, "DBResult: success")
    err = DBExecutionResult(error="timeout")
    t.check(not err.is_success, "DBResult: error")


def test_hint_utils(t: TestResult):
    print("\n--- test_hint_utils ---")
    from mcts.utils.hint_utils import (
        extract_hints_from_text,
        extract_final_answer,
        dedupe_join_hints,
        dedupe_index_hints_by_table,
        merge_set_var_hints,
        deduplicate_hints,
        build_sql_with_hints,
    )

    # extract_hints_from_text
    text1 = "Some text /*+ INDEX(t1 idx1) JOIN_PREFIX(t2) */ more text"
    hints = extract_hints_from_text(text1)
    t.check(len(hints) == 2, f"extract hints: got {len(hints)}")
    t.check("INDEX(t1 idx1)" in hints, "extract: INDEX found")
    t.check("JOIN_PREFIX(t2)" in hints, "extract: JOIN_PREFIX found")

    t.check(extract_hints_from_text("") == [], "extract: empty")
    t.check(extract_hints_from_text("no hints here") == [], "extract: no hints")

    # extract_final_answer
    t.check(extract_final_answer("<answer>/*+ INDEX(t1 idx1) */</answer>") is not None, "final answer: found")
    t.check(extract_final_answer("no answer") is None, "final answer: not found")

    # dedupe_join_hints — last wins
    h = ["JOIN_PREFIX(t1)", "INDEX(t2 idx)", "JOIN_PREFIX(t3)", "JOIN_SUFFIX(t4)", "JOIN_SUFFIX(t5)"]
    d = dedupe_join_hints(h)
    prefix_count = sum(1 for x in d if x.upper().startswith("JOIN_PREFIX"))
    suffix_count = sum(1 for x in d if x.upper().startswith("JOIN_SUFFIX"))
    t.check(prefix_count == 1, f"dedupe join: prefix count={prefix_count}")
    t.check(suffix_count == 1, f"dedupe join: suffix count={suffix_count}")
    t.check("JOIN_PREFIX(t3)" in d and "JOIN_PREFIX(t1)" not in d, "dedupe join: last JOIN_PREFIX wins")
    t.check("JOIN_SUFFIX(t5)" in d and "JOIN_SUFFIX(t4)" not in d, "dedupe join: last JOIN_SUFFIX wins")
    # Slot is preserved at first occurrence: t3 should be before INDEX(t2 idx)
    t.check(d.index("JOIN_PREFIX(t3)") < d.index("INDEX(t2 idx)"), "dedupe join: slot preserved")

    # dedupe_index_hints_by_table — last wins for same table
    h2 = ["INDEX(t1 idx1)", "NO_INDEX(t1)", "INDEX(t2 idx2)"]
    d2 = dedupe_index_hints_by_table(h2)
    t.check(len(d2) == 2, f"dedupe index: {len(d2)} hints")
    t.check("NO_INDEX(t1)" in d2 and "INDEX(t1 idx1)" not in d2, "dedupe index: last wins for t1")

    # merge_set_var_hints
    h3 = [
        "SET_VAR(optimizer_switch='semijoin=off')",
        "INDEX(t1 idx1)",
        "SET_VAR(optimizer_switch='derived_merge=off')",
    ]
    m = merge_set_var_hints(h3)
    setvar_count = sum(1 for x in m if x.upper().startswith("SET_VAR"))
    t.check(setvar_count == 1, f"merge set_var: count={setvar_count}")
    t.check("semijoin=off" in m[0], "merge: contains semijoin")
    t.check("derived_merge=off" in m[0], "merge: contains derived_merge")

    # merge_set_var_hints — last value wins for same key
    h3b = [
        "SET_VAR(optimizer_switch='semijoin=off')",
        "SET_VAR(optimizer_switch='semijoin=on')",
    ]
    mb = merge_set_var_hints(h3b)
    setvar_b = [x for x in mb if x.upper().startswith("SET_VAR")]
    t.check(len(setvar_b) == 1, f"merge set_var last-wins: count={len(setvar_b)}")
    t.check("semijoin=on" in setvar_b[0], f"merge set_var last-wins: got {setvar_b[0]!r}")
    t.check("semijoin=off" not in setvar_b[0], "merge set_var last-wins: old value dropped")

    # build_sql_with_hints
    sql = "SELECT * FROM t1 WHERE id = 1"
    built = build_sql_with_hints(sql, ["INDEX(t1 idx1)"])
    t.check("/*+" in built, "build sql: has hint block")
    t.check("INDEX(t1 idx1)" in built, "build sql: has hint")

    # Case insensitive
    sql2 = "select * from t1"
    built2 = build_sql_with_hints(sql2, ["INDEX(t1 idx1)"])
    t.check("/*+" in built2, "build sql: case insensitive")


def test_prompts(t: TestResult):
    print("\n--- test_prompts ---")
    from mcts.utils.prompts import build_action_prompt, filter_candidate_hints
    from mcts.types import ActionType

    hints = {"index": ["INDEX(t1 idx1)"], "join_order": ["JOIN_PREFIX(t2)"], "config": ["SET_VAR(optimizer_switch='semijoin=off')"]}

    # filter
    f1 = filter_candidate_hints(hints, ActionType.A1_INDEX)
    t.check("index" in f1, "filter A1: has index")
    t.check("join_order" not in f1, "filter A1: no join_order")

    f5 = filter_candidate_hints(hints, ActionType.A5_RETHINK)
    t.check(len(f5) == 3, f"filter A5: all categories ({len(f5)})")

    f4 = filter_candidate_hints(hints, ActionType.A4_SUBPROBLEM)
    t.check(len(f4) == 0, "filter A4: none")

    # build prompt
    prompt = build_action_prompt(
        action=ActionType.A1_INDEX,
        query="SELECT * FROM t1",
        execution_info="{}",
        candidate_hints=hints,
        index_info={},
        partial_solution="",
        step_number=1,
    )
    t.check(len(prompt) > 100, "prompt: non-trivial length")
    t.check("A1" in prompt, "prompt: contains A1")
    t.check("SELECT * FROM t1" in prompt, "prompt: contains query")


def test_tree(t: TestResult):
    print("\n--- test_tree ---")
    from mcts.tree import (
        TreeNode, get_allowed_actions, create_child_nodes,
        select_leaf, collect_solutions, dump_tree,
    )
    from mcts.types import ActionType, NodeStatus, TR_FINAL_ANSWER

    # Basic node
    root = TreeNode(c_puct=2.0)
    t.check(root.is_root, "root: is_root")
    t.check(root.is_leaf, "root: is_leaf initially")
    t.check(root.depth == 0, "root: depth=0")

    # Action rules for root
    actions = get_allowed_actions(root)
    t.check(ActionType.A5_RETHINK not in actions, "root: no A5")
    t.check(ActionType.A1_INDEX in actions, "root: has A1")
    t.check(ActionType.A6_ANSWER in actions, "root: has A6")

    # Create children
    root.status = NodeStatus.EXPANDED
    children = create_child_nodes(root, actions, rollout_index=0, c_puct=2.0)
    t.check(len(children) == len(actions), f"children count matches actions ({len(children)})")
    t.check(not root.is_leaf, "root: not leaf after expansion")
    t.check(children[0].depth == 1, "child depth=1")
    t.check(children[0].parent is root, "child parent is root")

    # Action rules after A4
    a4_child = None
    for c in children:
        if c.action_type == ActionType.A4_SUBPROBLEM:
            a4_child = c
            break
    if a4_child:
        a4_child.status = NodeStatus.EXPANDED
        a4_actions = get_allowed_actions(a4_child)
        t.check(ActionType.A5_RETHINK not in a4_actions, "after A4: no A5")
        t.check(ActionType.A1_INDEX in a4_actions, "after A4: has A1")

    # PUCT selection
    for c in children:
        c.status = NodeStatus.EXPANDED  # make them selectable
    leaf = select_leaf(root)
    t.check(leaf is not None, "select_leaf: found a leaf")

    # Backpropagation
    children[0].backpropagate(0.5)
    t.check(children[0].visit_count == 1, "backprop: child visited")
    t.check(root.visit_count == 1, "backprop: root visited")
    t.check(abs(children[0].q_value - 0.5) < 1e-6, "backprop: q_value correct")

    # Solutions collection
    children[0].status = NodeStatus.TERMINAL
    children[0].terminal_reason = TR_FINAL_ANSWER
    children[0].reward = 0.5
    children[0].state.executed_hints = ["INDEX(t1 idx1)"]
    children[0].state.execution_time_seconds = 0.1

    solutions = collect_solutions(root, baseline_time=1.0)
    t.check(len(solutions) >= 1, f"solutions: {len(solutions)} found")
    if solutions:
        t.check(solutions[0].executed_hints == ["INDEX(t1 idx1)"], "solution: hints match")

    # Tree dump
    td = dump_tree(root)
    t.check("0" in td, "dump: root node exists")
    t.check(len(td) > 1, f"dump: has {len(td)} entries")


def test_search_parser(t: TestResult):
    print("\n--- test_search_parser ---")
    from mcts.search import parse_llm_output

    # Continue thinking (no hints)
    p1 = parse_llm_output("This is analysis without any hints </step>")
    t.check(p1.is_continue_thinking, "parser: continue thinking")

    # With hints
    p2 = parse_llm_output("Analysis... /*+ INDEX(t1 idx1) */ </step>")
    t.check(not p2.is_continue_thinking, "parser: not continue")
    t.check(len(p2.hints) == 1, f"parser: {len(p2.hints)} hints")
    t.check(p2.hints[0] == "INDEX(t1 idx1)", "parser: correct hint")

    # Answer with hints
    p3 = parse_llm_output("Summary <answer>/*+ JOIN_PREFIX(t1) INDEX(t2 idx2) */</answer>")
    t.check(len(p3.hints) == 2, f"parser: {len(p3.hints)} hints in answer")

    # Empty
    p4 = parse_llm_output("")
    t.check(p4.is_continue_thinking, "parser: empty = continue")


def main():
    t = TestResult()

    test_types(t)
    test_hint_utils(t)
    test_prompts(t)
    test_tree(t)
    test_search_parser(t)

    all_passed = t.summary()
    if all_passed:
        print("\n✓ ALL UNIT TESTS PASSED")
        sys.exit(0)
    else:
        print(f"\n✗ {t.failed} TEST(S) FAILED")
        sys.exit(1)


if __name__ == "__main__":
    main()
