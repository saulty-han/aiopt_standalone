"""
mcts.types - All data types for the MCTS module.

Every structured piece of data flowing through the MCTS pipeline has an explicit
Pydantic model. No raw dicts or string-keyed bags cross function boundaries.
"""
from __future__ import annotations

from enum import Enum
from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field


# ---------------------------------------------------------------------------
# Action types
# ---------------------------------------------------------------------------

class ActionType(str, Enum):
    """The six optimization actions available in the MCTS search."""
    A1_INDEX = "A1"
    A2_JOIN = "A2"
    A3_CONFIG = "A3"
    A4_SUBPROBLEM = "A4"
    A5_RETHINK = "A5"
    A6_ANSWER = "A6"


# ---------------------------------------------------------------------------
# Node status
# ---------------------------------------------------------------------------

class NodeStatus(str, Enum):
    """Mutually exclusive lifecycle status of a search node.

    Only 3 states. Use ``terminal_reason`` on the tree node for detail.
    """
    PENDING = "pending"          # Created but not yet expanded (no children)
    EXPANDED = "expanded"        # Selected and expanded (children created)
    TERMINAL = "terminal"        # Cannot be expanded further (see terminal_reason)


# ---------------------------------------------------------------------------
# terminal_reason constants for TERMINAL nodes
# ---------------------------------------------------------------------------

TR_FINAL_ANSWER = "final_answer"       # A6 (answer) action — always terminal
TR_DEPTH_EXCEEDED = "depth_exceeded"   # Max depth reached, no valid execution
TR_INEFFECTIVE_HINTS = "ineffective_hints"  # Hints had no effect on plan
TR_LLM_ERROR = "llm_error"             # LLM call failed (rate limit exhausted, HTTP error)
TR_EXECUTION_TIMEOUT = "db_execution_timeout"   # EXPLAIN ANALYZE hit max_execution_time
TR_EXECUTION_ERROR = "db_execution_error"          # DB EXPLAIN or EXPLAIN ANALYZE failed (non-timeout)


# ---------------------------------------------------------------------------
# LLM interaction types
# ---------------------------------------------------------------------------

class LLMStatus(str, Enum):
    """Status of a single LLM completion call."""
    OK = "ok"                          # Successful completion
    UNAVAILABLE = "unavailable"        # Network / timeout error — retriable, endpoint rotated
    HTTP_ERROR = "http_error"          # HTTP 4xx/5xx — non-retriable, return immediately
    RATE_LIMIT_EXCEEDED = "rate_limit_exceeded"  # All endpoints exhausted after max retries


class LLMCompletion(BaseModel):
    """A single completion returned by the LLM."""
    text: str = ""
    status: LLMStatus = LLMStatus.OK
    stop_reason: Optional[str] = None
    input_chars: int = 0
    output_chars: int = 0
    latency_seconds: float = 0.0


class LLMRequest(BaseModel):
    """A prompt sent to the LLM together with its eventual response."""
    prompt: str
    completions: List[LLMCompletion] = Field(default_factory=list)


# ---------------------------------------------------------------------------
# Parsed LLM output
# ---------------------------------------------------------------------------

class ParsedLLMOutput(BaseModel):
    """Result of parsing the raw LLM text for a single action step."""
    raw_text: str
    hints: List[str] = Field(default_factory=list)
    is_continue_thinking: bool = False  # no new hints produced


# ---------------------------------------------------------------------------
# Database execution result
# ---------------------------------------------------------------------------

class DBExecutionResult(BaseModel):
    """Result of executing (or explaining) a SQL statement against the database."""
    plan_digest: Optional[str] = None
    execution_time_seconds: Optional[float] = None
    explain_analyze_json: Optional[str] = None
    error: Optional[str] = None
    is_timeout: bool = False  # True when execution hit max_execution_time

    @property
    def is_success(self) -> bool:
        return self.error is None


# ---------------------------------------------------------------------------
# Node state — the immutable snapshot attached to each tree node
# ---------------------------------------------------------------------------

class NodeState(BaseModel):
    """All information gathered during a single MCTS step.

    Once a node is filled, its state should not be mutated from outside.
    """
    action_type: Optional[ActionType] = None
    llm_request_text: Optional[str] = None    # Input prompt sent to LLM
    llm_response_text: Optional[str] = None   # Raw LLM output for this step
    llm_input_chars: int = 0
    llm_output_chars: int = 0
    llm_latency_seconds: float = 0.0
    parsed_output: Optional[ParsedLLMOutput] = None

    # Accumulated hints along the path root -> this node
    executed_hints: List[str] = Field(default_factory=list)
    # Only the new hints introduced at this step
    new_hints: List[str] = Field(default_factory=list)
    # Ancestor hints removed at this step (A5/A6 override may drop some)
    deleted_hints: List[str] = Field(default_factory=list)

    # Database results
    db_result: Optional[DBExecutionResult] = None

    # Reward fields
    execution_time_seconds: Optional[float] = None
    plan_digest: Optional[str] = None

    # Whether this node was the first to discover a new (different) execution plan
    new_plan_first_found: bool = False


# ---------------------------------------------------------------------------
# Solution — a leaf collected at the end of MCTS
# ---------------------------------------------------------------------------

class MCTSSolution(BaseModel):
    """A single solution extracted from the MCTS tree."""
    executed_hints: List[str]
    execution_time_seconds: Optional[float] = None
    plan_digest: Optional[str] = None
    reward: Optional[float] = None
    q_value: float = 0.0
    action_type: Optional[str] = None
    node_tag: str = ""
    rollout_index: int = 0
    depth: int = 0


# ---------------------------------------------------------------------------
# Metrics
# ---------------------------------------------------------------------------

class MCTSRunMetrics(BaseModel):
    """Performance metrics for a single MCTS run."""
    llm_call_count: int = 0
    llm_input_chars: int = 0
    llm_output_chars: int = 0
    llm_output_seconds: float = 0.0
    llm_chars_per_second: float = 0.0

    db_explain_count: int = 0
    db_execute_count: int = 0
    db_execute_seconds: float = 0.0

    # Cache hit counters
    memory_cache_hit_count: int = 0   # in-memory MemoryPlanCache hits (search.py)
    remote_cache_hit_count: int = 0   # remote query_cache hits (db_executor.py)

    mcts_e2e_seconds: float = 0.0

    early_stop_reason: Optional[str] = None
    early_stop_rollout: Optional[int] = None
    early_stop_detail: Optional[str] = None

    def record_llm_call(self, input_chars: int, output_chars: int, latency_seconds: float) -> None:
        self.llm_call_count += 1
        self.llm_input_chars += input_chars
        self.llm_output_chars += output_chars
        if latency_seconds > 0:
            self.llm_output_seconds += latency_seconds

    def record_db_explain(self) -> None:
        self.db_explain_count += 1

    def record_db_execute(self, execution_seconds: float) -> None:
        self.db_execute_count += 1
        if execution_seconds > 0:
            self.db_execute_seconds += execution_seconds

    def record_memory_cache_hit(self) -> None:
        self.memory_cache_hit_count += 1

    def record_remote_cache_hit(self) -> None:
        self.remote_cache_hit_count += 1

    def finalize(self) -> None:
        """Compute derived metrics."""
        if self.llm_output_seconds > 0:
            self.llm_chars_per_second = self.llm_output_chars / self.llm_output_seconds


# ---------------------------------------------------------------------------
# MCTS output — the complete result of a search
# ---------------------------------------------------------------------------

class MCTSSearchResult(BaseModel):
    """Complete output of one MCTS search for a single query."""
    query: str
    query_digest: Optional[str] = None
    baseline_time_seconds: Optional[float] = None
    default_plan_digest: Optional[str] = None
    solutions: List[MCTSSolution] = Field(default_factory=list)
    metrics: MCTSRunMetrics = Field(default_factory=MCTSRunMetrics)
    # Tree dump for debugging (node_tag -> node summary)
    tree_dump: Dict[str, Any] = Field(default_factory=dict)
    plan_digest_cache: Dict[str, Any] = Field(default_factory=dict)
    early_stopping_metrics: Dict[str, Any] = Field(default_factory=dict)
    # plan_digest -> EXPLAIN ANALYZE payload (first non-empty EA we captured
    # for that plan). Decoupled from individual nodes so cache-hit nodes
    # can still reference their plan's EA via plan_digest.
    explain_analyze_info: Dict[str, Any] = Field(default_factory=dict)


# ---------------------------------------------------------------------------
# Input data format for one query
# ---------------------------------------------------------------------------

class MCTSInputData(BaseModel):
    """Input data for one query to be optimized."""
    query: str
    baseline_time_seconds: float
    execution_info_json: str  # Serialized execution plan info
    candidate_hints: Dict[str, List[str]]  # {"index": [...], "join_order": [...], "config": [...]}
    default_plan_digest: Optional[str] = None
    index_info: Dict[str, Any] = Field(default_factory=dict)


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

class MCTSConfig(BaseModel):
    """Configuration for the MCTS search."""
    # LLM
    llm_api_url_key: List[List[str]] = Field(default_factory=list)  # [[url, key, model], ...]
    wsid: Optional[str] = None
    temperature: float = 0.8
    top_p: float = 0.8
    top_k: int = 40
    max_tokens: int = 1048576
    repetition_penalty: int = 1
    seed: Optional[int] = None
    stop_tokens: Optional[List[str]] = Field(default=None)
    # Extra kwargs passed through to the chat template. Forwarded to the LLM
    # request body as-is (key: "chat_template_kwargs"). For Qwen/Deepseek
    # "thinking" models, set {"enable_thinking": false} to disable CoT scratch.
    chat_template_kwargs: Dict[str, Any] = Field(default_factory=dict)

    # Search
    max_depth: int = 3
    # If True (default), max_depth bounds the GLOBAL tree depth — any node at
    # depth >= max_depth is marked TERMINAL/DEPTH_EXCEEDED and no further
    # expansion is possible under it. If False, max_depth only bounds the
    # number of steps taken within a single rollout; the tree as a whole may
    # grow deeper as subsequent rollouts explore further down a chain.
    limit_global_depth: bool = True
    iterations: int = 1  # number of rollouts
    c_puct: float = 2.5
    negative_reward: float = -1.0
    timeout_amplifier: float = 1.0
    # Early stopping
    plan_time_threshold_seconds: float = 0.1  # <=0 disables
    estimated_tokens_budget: int = 0           # 0 = unlimited

    # API retry
    tpm_rate_limit_max_retries: int = 100
    api_cooldown_seconds: float = 30.0
    api_all_failed_wait_seconds: float = 60.0

    # Max chars for execution_info before skipping
    max_execution_info_chars: int = 128 * 1024

    # Output-payload toggles: control which optional blocks appear in the
    # MCTS result dict written to disk / returned to the optimizer.
    include_early_stopping_metrics: bool = True
    include_explain_analyze_info: bool = False

    # Remote cache toggle: when False, DBExecutor will not read from or write
    # to the remote {db}_cache.query_cache table. The in-memory MemoryPlanCache
    # is unaffected and continues to deduplicate plan executions within a run.
    remote_cache_enabled: bool = True

    # Upper bound (seconds) that the DB executor uses both as its own timeout
    # when the caller didn't pass one and as the cache entry's ``timeout_time``
    # column in the remote query_cache. Decoupled from ``timeout_amplifier``:
    # that one scales the rollout timeout against the probed baseline; this
    # one is the absolute wall-clock cap for any single EXPLAIN ANALYZE.
    remote_cache_timeout_seconds: int = 600

    # When True, the effective cache timeout is capped at
    # min(remote_cache_timeout_seconds, probed_baseline_time_seconds) after the
    # baseline probe completes. This prevents the cache from storing entries
    # whose timeout is longer than the baseline itself — any plan that already
    # exceeds the baseline is uninteresting and need not be waited out fully.
    # Default False (legacy behaviour: always use remote_cache_timeout_seconds).
    cap_cache_timeout_by_baseline: bool = False

    # Fallback per-query timeout (seconds) used only when no baseline is known
    # and the remote cache is disabled — i.e. the first time a query's baseline
    # is measured. Mirrors training.default_plan_timeout_seconds.
    default_plan_timeout_seconds: float = 60.0

    # Wall-clock timeout (seconds) for the cheap EXPLAIN calls used to obtain a
    # plan digest (never EXPLAIN ANALYZE). Default 30.
    explain_timeout_seconds: float = 30.0