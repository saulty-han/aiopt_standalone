"""
mcts.modules.memory_plan_cache - In-memory plan digest cache.

Tracks execution plan digests discovered during a single MCTS search, including
execution times, rollout history, and root-child visit statistics. Lives only
in memory for the duration of one search — distinct from the optional,
persisted RemotePlanCache.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

from mcts.utils.utils import round6

from mcts.tree import TreeNode


class MemoryPlanCache:
    """Manages plan digest → execution info mappings during MCTS search.

    Responsibilities:
      - Register baseline (default) plan digest
      - Register newly discovered plan digests
      - Track repeated rollouts (deduplicated)
      - Compute root_children_stats after backpropagation
    """

    def __init__(self) -> None:
        self._cache: Dict[str, Dict[str, Any]] = {}
        # Nodes pending stats update (digest → node), cleared after finalize
        self._pending: Dict[str, TreeNode] = {}

    def init_baseline(
        self,
        plan_digest: Optional[str],
        baseline_time: Optional[float],
    ) -> None:
        """Pre-populate the cache with the baseline (default) plan."""
        if plan_digest and baseline_time:
            self._cache[plan_digest] = {
                "execution_time_s": round6(baseline_time),
                "first_rollout": -1,
                "repeated_rollouts": [],
                "root_child_tag": None,
                "root_children_stats": [],
            }

    def lookup(self, plan_digest: Optional[str]) -> Optional[Dict[str, Any]]:
        """Return cached entry for a plan digest, or None."""
        if not plan_digest:
            return None
        return self._cache.get(plan_digest)

    def record_hit(
        self,
        plan_digest: str,
        rollout_index: int,
    ) -> float:
        """Record a cache hit: append rollout if not already present.

        Returns the cached execution time.
        """
        entry = self._cache[plan_digest]
        # Deduplicate: skip if this rollout is the first_rollout or already recorded
        if (rollout_index != entry["first_rollout"]
                and rollout_index not in entry["repeated_rollouts"]):
            entry["repeated_rollouts"].append(rollout_index)
        return entry["execution_time_s"]

    def register_new(
        self,
        plan_digest: str,
        execution_time: float,
        rollout_index: int,
        root_child_tag: Optional[str],
        node: TreeNode,
    ) -> None:
        """Register a newly discovered plan digest."""
        self._cache[plan_digest] = {
            "execution_time_s": round6(execution_time),
            "first_rollout": rollout_index,
            "repeated_rollouts": [],
            "root_child_tag": root_child_tag,
            "root_children_stats": [],
        }
        # Track pending node for stats finalization
        self._pending[plan_digest] = node

    def finalize_stats(self, root: TreeNode) -> None:
        """Compute root_children_stats for all pending entries after backpropagation.

        Captures a one-time snapshot of root children visit distribution at the
        moment each plan digest is first discovered. This is NOT updated later.
        """
        if not self._pending:
            return

        stats = self._compute_root_children_stats(root)
        if stats is None:
            self._pending.clear()
            return

        for digest in self._pending:
            if digest in self._cache:
                self._cache[digest]["root_children_stats"] = stats

        self._pending.clear()

    def _compute_root_children_stats(self, root: TreeNode):
        """Compute visit distribution across root's direct children.

        visit_ratio = child.visit_count / sum(all children visit_count).
        Returns list of stats dicts, or None if root has no children.
        """
        root_children = root.children
        if not root_children:
            return None

        total_visits = sum(child.visit_count for child in root_children)
        stats = []
        for child in root_children:
            ratio = child.visit_count / total_visits if total_visits > 0 else 0.0
            stats.append({
                "tag": child.tag,
                "visit_count": child.visit_count,
                "visit_ratio": round(ratio, 4),
            })
        return stats

    def to_dict(self) -> Dict[str, Dict[str, Any]]:
        """Export the cache as plan_digest → {execution_time_s} only."""
        return {
            digest: {"execution_time_s": entry["execution_time_s"]}
            for digest, entry in self._cache.items()
        }

    def to_early_stopping_metrics(self) -> Dict[str, Dict[str, Any]]:
        """Export full cache details for early stopping analysis."""
        return dict(self._cache)
