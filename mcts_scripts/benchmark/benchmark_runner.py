from __future__ import annotations

"""
用法:
    # 默认: 每实例 4 线程并行优化 + 优化后单线程验证(重跑每个 rollout 的 best hint) + CSV
    python mcts_scripts/benchmark/benchmark_runner.py \
        --bench-json mcts_scripts/benchmark/core_set_1000_cdb.json

    # 只优化, 跳过验证
    python mcts_scripts/benchmark/benchmark_runner.py \
        --bench-json .../core_set_1000_cdb.json --no-validate

    # 只验证不优化: 复用 [mcts].output_dir 落盘的 MCTS JSON, 重跑 best hints 出 CSV
    python mcts_scripts/benchmark/benchmark_runner.py \
        --bench-json .../core_set_1000_cdb.json \
        --validate-only --input-dir mcts/eval_data

    # 调整每实例并行度 / 验证阶段额外重测 baseline / 验证预热
    python mcts_scripts/benchmark/benchmark_runner.py \
        --bench-json .../core_set_1000_cdb.json \
        --per-instance-workers 2 --validate-baseline --validate-warmup 1

配置:
    默认读 etc/aiopt_conf.toml。可用 etc/aiopt_conf.benchmark.toml.tpl 作为模版
    (default_plan_timeout_seconds=300, cache 开, cap 开); 复制为 aiopt_conf.toml
    并填好 [mcts].llm_api_url_key 即可。
"""

import argparse
import hashlib
import json
import multiprocessing as mp
import signal
import sys
import time
import traceback
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from queue import Empty
from typing import Any, Optional, Sequence

SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent.parent
DEFAULT_BENCH_JSON_PATH = SCRIPT_DIR / "core_set_1000_cdb.json"
DEFAULT_INSTANCE_LOOKUP_PATH = SCRIPT_DIR / "instance_lookup_table.txt"
DEFAULT_DB_USER = "tencentroot"
DEFAULT_FALLBACK_HOST = "127.0.0.1"
DEFAULT_FALLBACK_PORT = 13000
MIN_SUPPORTED_PYTHON = (3, 10)

sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(REPO_ROOT / "mcts_scripts"))


@dataclass
class BenchmarkQuery:
    idx: int
    original_idx: int
    benchmark_id: str
    instance_id: str
    query_digest: str
    query_text: str
    baseline_time: float
    difficulty_level: str
    difficulty_score: float
    pattern_label: str
    num_qb: int
    num_joins: int
    num_hints: int
    num_subqueries: int
    db_name: Optional[str] = None


@dataclass(frozen=True)
class ConnectionConfig:
    host: str
    port: int
    user: str
    password: str
    default_db: Optional[str] = None


@dataclass
class QueryResult:
    idx: int
    benchmark_id: str
    instance_id: str
    query_digest: str
    sql: str
    baseline_time: float          # 来自 bench JSON 的历史 baseline（仅作参考/兜底）
    difficulty_level: str
    pattern_label: str
    success: bool
    elapsed: float = 0.0
    n_candidates: int = 0
    runtime_baseline: Optional[float] = None  # 本次实际探测到的 baseline（优先用它）
    mcts_results: Optional[list[dict[str, Any]]] = None
    debug_trace: Optional[dict[str, Any]] = None
    error: Optional[str] = None

    @property
    def effective_baseline(self) -> float:
        """优先用本次运行实测的 baseline，没有时回退到 JSON 里的历史值。"""
        if self.runtime_baseline is not None and self.runtime_baseline > 0:
            return self.runtime_baseline
        return self.baseline_time


@dataclass
class MctsSummary:
    best_time: Optional[float] = None
    best_hints: Optional[list[Any]] = None
    speedup: Optional[float] = None
    solution_count: int = 0
    llm_calls: int = 0
    db_executes: int = 0


class ResultCollectionInterrupted(Exception):
    def __init__(self, message: str, results: list[QueryResult]):
        super().__init__(message)
        self.results = results


def coerce_text(value: Any) -> str:
    if value is None:
        return ""
    return value if isinstance(value, str) else str(value)


def coerce_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def coerce_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


DIFFICULTY_FILTER_MAP: dict[str, str] = {
    "1": "L1-Easy",
    "2": "L2-Medium",
    "3": "L3-Hard",
    "4": "L4-Expert"
}

PATTERN_FILTER_MAP: dict[str, str] = {
    "agg": "aggregation",
    "filter": "filter",
    "order": "order-sensitive",
    "nested": "nested/set-op",
}

DIFFICULTY_FILTER_HELP = (
    "按难度过滤，支持原值或别名: "
    "1 -> L1-Easy, "
    "2 -> L2-Medium, "
    "3 -> L3-Hard, "
    "4 -> L4-Expert"
)

PATTERN_FILTER_HELP = (
    "按模式过滤，支持原值或别名: "
    "agg -> aggregation, "
    "filter -> filter, "
    "order -> order-sensitive, "
    "nested -> nested/set-op"
)


def build_filter_value_parser(
    alias_map: dict[str, str], arg_name: str
):
    def parse_value(value: str) -> str:
        normalized = value.strip().lower()
        if not normalized:
            raise argparse.ArgumentTypeError(f"{arg_name} 不能为空")

        canonical = alias_map.get(normalized)
        if canonical is None:
            raise argparse.ArgumentTypeError(
                f"{arg_name} 不支持 {value!r}; 可选值/别名见 --help"
            )
        return canonical

    return parse_value


def dedupe_preserving_order(values: Optional[Sequence[str]]) -> Optional[list[str]]:
    if not values:
        return None
    return list(dict.fromkeys(values))


parse_difficulty_filter = build_filter_value_parser(
    DIFFICULTY_FILTER_MAP, "--difficulty"
)
parse_pattern_filter = build_filter_value_parser(PATTERN_FILTER_MAP, "--pattern")


def ensure_runtime_compatibility():
    if tuple(sys.version_info[:2]) >= MIN_SUPPORTED_PYTHON:
        return

    current = ".".join(str(part) for part in sys.version_info[:3])
    required = ".".join(str(part) for part in MIN_SUPPORTED_PYTHON)
    raise SystemExit(
        "错误: benchmark_runner 需要 Python "
        f"{required}+ 运行，当前解释器是 Python {current}。\n"
        "原因: 依赖模块使用了 `X | None` 等 Python 3.10+ 语法，"
        "否则会在 worker 进程启动后才报导入错误。"
    )


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Core-Set Benchmark MCTS 多进程并行优化测试"
    )

    parser.add_argument(
        "--bench-json",
        default=str(DEFAULT_BENCH_JSON_PATH),
        help=f"Benchmark JSON 文件路径 (默认: {DEFAULT_BENCH_JSON_PATH.name})",
    )

    conn_group = parser.add_argument_group("连接参数")
    conn_group.add_argument(
        "--host",
        default=DEFAULT_FALLBACK_HOST,
        help=(
            "数据库 IP 占位值 "
            f"(默认: {DEFAULT_FALLBACK_HOST}; 查表命中后会被覆盖)"
        ),
    )
    conn_group.add_argument(
        "--port",
        type=int,
        default=DEFAULT_FALLBACK_PORT,
        help=(
            "数据库端口占位值 "
            f"(默认: {DEFAULT_FALLBACK_PORT}; 查表命中后会被覆盖)"
        ),
    )
    conn_group.add_argument("--user", default=DEFAULT_DB_USER, help="数据库用户名")
    conn_group.add_argument("--password", default="", help="数据库密码")
    conn_group.add_argument(
        "--db",
        default=None,
        help="查询未带 schema 时使用的兜底数据库",
    )

    filter_group = parser.add_argument_group("过滤参数")
    filter_group.add_argument(
        "--difficulty",
        nargs="+",
        type=parse_difficulty_filter,
        default=None,
        help=DIFFICULTY_FILTER_HELP,
    )
    filter_group.add_argument(
        "--pattern",
        nargs="+",
        type=parse_pattern_filter,
        default=None,
        help=PATTERN_FILTER_HELP,
    )
    filter_group.add_argument(
        "--instance",
        nargs="+",
        default=None,
        help="按 instance_id 过滤 (可指定多个)",
    )
    filter_group.add_argument(
        "--limit",
        type=int,
        default=0,
        help="只优化前 N 条 (0=全部)",
    )

    parser.add_argument(
        "--workers",
        type=int,
        default=0,
        help="并行 worker 进程数上限 (默认 0=每个实例一个 worker)",
    )
    parser.add_argument(
        "--per-instance-workers",
        type=int,
        default=4,
        help=(
            "单个实例内并行的 worker 进程数 (默认 4)。"
            "总进程数 = 实例数 × 该值；同一实例的查询会被这些 worker 并行消费"
        ),
    )

    # ---- 优化后单线程验证阶段 ----
    validate_group = parser.add_argument_group("验证阶段参数")
    validate_group.add_argument(
        "--no-validate",
        action="store_true",
        help="优化后不做验证 (默认开启验证: 单线程重跑每个 rollout 的 best hint 并出 CSV)",
    )
    validate_group.add_argument(
        "--validate-only",
        action="store_true",
        help="只验证不优化: 复用 [mcts].output_dir 落盘的 MCTS JSON, 重跑 best hints 出 CSV",
    )
    validate_group.add_argument(
        "--input-dir",
        default="",
        help="--validate-only 模式下读取的 MCTS 输出 JSON 目录",
    )
    validate_group.add_argument(
        "--output-csv",
        default="",
        help="验证 CSV 输出路径 (默认 mcts_scripts/benchmark/results/ 下自动命名)",
    )
    validate_group.add_argument(
        "--validate-baseline",
        action="store_true",
        help="验证阶段额外重测 baseline(无 hints)",
    )
    validate_group.add_argument(
        "--validate-timeout",
        type=float,
        default=0.0,
        help="验证单次 EXPLAIN ANALYZE 超时(秒); 0=自动(baseline×1.1)",
    )
    validate_group.add_argument(
        "--validate-warmup",
        type=int,
        default=1,
        help="验证时每条 SQL 正式计时前的预热轮数 (默认 1)",
    )
    validate_group.add_argument(
        "--validate-workers",
        type=int,
        default=0,
        help="(已弃用) 验证阶段为每个实例起一个进程, 并在每个 rollout 处 barrier 对齐, "
             "因此所有实例必须同时运行才能跨实例汇总; 此参数当前不再限制并发数。",
    )
    raw_argv = list(sys.argv[1:] if argv is None else argv)
    args = parser.parse_args(raw_argv)
    if args.workers < 0:
        parser.error("--workers 必须 >= 0")
    if args.per_instance_workers < 1:
        parser.error("--per-instance-workers 必须 >= 1")
    if args.limit < 0:
        parser.error("--limit 必须 >= 0")
    args.difficulty = dedupe_preserving_order(args.difficulty)
    args.pattern = dedupe_preserving_order(args.pattern)
    args.host_fallback_provided = any(
        token == "--host"
        or token.startswith("--host=")
        or token == "--port"
        or token.startswith("--port=")
        for token in raw_argv
    )
    return args


def load_benchmark_records(path: str) -> list[dict[str, Any]]:
    with Path(path).open("r", encoding="utf-8") as handle:
        records = json.load(handle)
    if not isinstance(records, list):
        raise ValueError(f"期望 JSON 数组，实际类型: {type(records).__name__}")
    return records


def extract_db_from_sql(sql: str) -> Optional[str]:
    import re

    match = re.search(
        r"(?:from|join)\s+`?([A-Za-z0-9_]+)`?\s*\.\s*`?([A-Za-z0-9_]+)`?",
        sql,
        re.IGNORECASE,
    )
    if match:
        return match.group(1)
    return None


def resolve_record_db_name(record: dict[str, Any], query_text: str) -> Optional[str]:
    # Benchmark JSON uses `db`; keep `db_name` as a legacy fallback.
    db_name = coerce_text(record.get("db")).strip()
    if db_name:
        return db_name
    db_name = coerce_text(record.get("db_name")).strip()
    if db_name:
        return db_name
    return extract_db_from_sql(query_text)


def build_queries(
    records: list[dict[str, Any]],
    difficulty_filter: Optional[list[str]] = None,
    pattern_filter: Optional[list[str]] = None,
    instance_filter: Optional[list[str]] = None,
    limit: int = 0,
) -> list[BenchmarkQuery]:
    pattern_filter_lower = {item.lower() for item in pattern_filter or []}
    instance_filter_set = set(instance_filter or [])
    difficulty_filter_set = set(difficulty_filter or [])

    queries: list[BenchmarkQuery] = []
    for original_idx, record in enumerate(records):
        difficulty_level = coerce_text(record.get("difficulty_level"))
        if difficulty_filter_set and difficulty_level not in difficulty_filter_set:
            continue

        pattern_label = coerce_text(record.get("pattern_label"))
        if pattern_filter_lower and pattern_label.lower() not in pattern_filter_lower:
            continue

        instance_id = coerce_text(record.get("instance_id"))
        if instance_filter_set and instance_id not in instance_filter_set:
            continue

        query_text = coerce_text(record.get("query_text"))
        queries.append(
            BenchmarkQuery(
                idx=len(queries),
                original_idx=original_idx,
                benchmark_id=coerce_text(record.get("benchmark_id")) or f"BM-{original_idx}",
                instance_id=instance_id,
                query_digest=coerce_text(record.get("query_digest")),
                query_text=query_text,
                baseline_time=coerce_float(record.get("baseline_time")),
                difficulty_level=difficulty_level,
                difficulty_score=coerce_float(record.get("difficulty_score")),
                pattern_label=pattern_label,
                num_qb=coerce_int(record.get("num_qb")),
                num_joins=coerce_int(record.get("num_joins")),
                num_hints=coerce_int(record.get("num_hints")),
                num_subqueries=coerce_int(record.get("num_subqueries")),
                db_name=resolve_record_db_name(record, query_text),
            )
        )

        if limit > 0 and len(queries) >= limit:
            break

    return queries


def generate_digest(sql: str) -> str:
    return hashlib.md5(sql.encode()).hexdigest()[:16]


def load_default_lookup():
    if not DEFAULT_INSTANCE_LOOKUP_PATH.is_file():
        return None
    try:
        from instance_lookup import get_lookup
    except ModuleNotFoundError:
        from mcts_scripts.benchmark.instance_lookup import get_lookup
    return get_lookup()


def build_connection_map(
    args: argparse.Namespace,
    queries: list[BenchmarkQuery],
) -> dict[str, ConnectionConfig]:
    lookup = load_default_lookup()
    if lookup is None and not args.host_fallback_provided:
        print(
            "错误: 默认 Instance Lookup Table 不存在，且未显式传入 --host/--port: "
            f"{DEFAULT_INSTANCE_LOOKUP_PATH}"
        )
        sys.exit(1)

    connection_map: dict[str, ConnectionConfig] = {}
    route_stats = Counter()

    for instance_id in sorted({query.instance_id for query in queries}):
        if lookup is not None:
            info = lookup.get(instance_id)
            if info is not None:
                connection_map[instance_id] = ConnectionConfig(
                    host=info.ip,
                    port=int(info.port),
                    user=args.user,
                    password=args.password,
                    default_db=args.db,
                )
                route_stats["default_lookup"] += 1
                continue

        if args.host_fallback_provided:
            if lookup is not None:
                print(
                    f"  警告: instance_id={instance_id} 不在默认 Instance Lookup Table 中，"
                    "将使用 --host/--port 兜底"
                )
            else:
                print(
                    f"  警告: 默认 Instance Lookup Table 不存在，"
                    f"instance_id={instance_id} 将使用 --host/--port"
                )
            connection_map[instance_id] = ConnectionConfig(
                host=args.host,
                port=args.port,
                user=args.user,
                password=args.password,
                default_db=args.db,
            )
            route_stats["host_fallback"] += 1
            continue

        if lookup is None:
            print(
                "  错误: 默认 Instance Lookup Table 不存在，"
                f"且未显式传入 --host/--port ({DEFAULT_INSTANCE_LOOKUP_PATH})"
            )
        else:
            print(
                f"  错误: instance_id={instance_id} 不在默认 Instance Lookup Table 中，"
                "且未显式传入 --host/--port"
            )
        sys.exit(1)

    print(
        "  路由汇总: "
        f"default_lookup={route_stats['default_lookup']}, "
        f"host_fallback={route_stats['host_fallback']}"
    )
    return connection_map


def resolve_query_db(
    query: BenchmarkQuery,
    connection: ConnectionConfig,
) -> Optional[str]:
    return query.db_name or connection.default_db


def extract_runtime_baseline(
    mcts_results: Optional[list[dict[str, Any]]],
) -> Optional[float]:
    """从 MCTS 结果里取本次实际探测到的 baseline_time。

    每个 run 里的 ``baseline_time`` 是 DBExecutor 实测的默认计划耗时（见
    optimizer/llm_optimizer.py）。取所有 run 里第一个有效值即可（一条模板
    通常只有一个 sample）。
    """
    if not mcts_results:
        return None
    for run in mcts_results:
        value = run.get("baseline_time")
        try:
            value = float(value)
        except (TypeError, ValueError):
            continue
        if value > 0:
            return value
    return None


def summarize_mcts_results(
    mcts_results: Optional[list[dict[str, Any]]],
    baseline_time: float,
) -> MctsSummary:
    summary = MctsSummary()
    if not mcts_results:
        return summary

    for run in mcts_results:
        metrics = run.get("performance_metrics", {})
        summary.llm_calls += int(metrics.get("llm_call_count", 0) or 0)
        summary.db_executes += int(metrics.get("db_execute_count", 0) or 0)

        for solution in run.get("solutions", []):
            summary.solution_count += 1
            execution_time = solution.get("execution_time_s")
            if execution_time is None:
                continue
            if summary.best_time is None or execution_time < summary.best_time:
                summary.best_time = execution_time
                summary.best_hints = solution.get("executed_hints", [])

    if baseline_time and summary.best_time and summary.best_time > 0:
        summary.speedup = baseline_time / summary.best_time

    return summary


def short_exc(exc: BaseException) -> str:
    return f"{type(exc).__name__}: {exc}"


def calc_execution_info_char_len(optimizer: Any, execution_info: Any) -> int:
    helper = getattr(optimizer, "_mcts_qdf_execution_info_char_len", None)
    if callable(helper):
        try:
            return int(helper(execution_info))
        except Exception:
            pass

    if execution_info is None:
        return 0
    if isinstance(execution_info, str):
        return len(execution_info)
    return len(json.dumps(execution_info))


def make_debug_trace(
    *,
    benchmark_id: str,
    instance_id: str,
    db_name: str,
    digest: str,
) -> dict[str, Any]:
    return {
        "benchmark_id": benchmark_id,
        "instance_id": instance_id,
        "db": db_name,
        "digest": digest,
        "prepare_calls": 0,
        "prepare_returned_rows": None,
        "prepare_errors": [],
        "execution_info_chars": [],
        "execution_info_baselines": [],
        "candidate_hint_counts": [],
        "run_mcts_calls": 0,
        "run_mcts_input_rows": None,
        "run_mcts_returned_rows": None,
        "execution_info_char_limit": None,
        "oversized_execution_info": [],
        "run_result_solution_counts": [],
        "run_result_errors": [],
    }


def clone_debug_trace(trace: Optional[dict[str, Any]]) -> Optional[dict[str, Any]]:
    if trace is None:
        return None
    return json.loads(json.dumps(trace, ensure_ascii=False, default=str))


def infer_debug_reason(
    mcts_results: Optional[list[dict[str, Any]]],
    debug_trace: Optional[dict[str, Any]],
) -> Optional[str]:
    if not debug_trace:
        return None

    prepare_errors = debug_trace.get("prepare_errors") or []
    if prepare_errors:
        return "prepare_step_failed"

    prepare_rows = debug_trace.get("prepare_returned_rows")
    if prepare_rows == 0:
        return "prepare_returned_no_qdf_rows"

    run_rows = debug_trace.get("run_mcts_returned_rows")
    input_rows = debug_trace.get("run_mcts_input_rows")
    oversized = debug_trace.get("oversized_execution_info") or []
    if run_rows == 0 and input_rows and len(oversized) >= input_rows:
        return "all_qdf_rows_skipped_by_execution_info_limit"
    if run_rows == 0 and input_rows:
        return "run_mcts_returned_no_rows"

    if mcts_results and all(not run.get("solutions") for run in mcts_results):
        return "mcts_ran_but_solutions_empty"

    return None


def format_debug_trace_lines(
    debug_trace: Optional[dict[str, Any]],
) -> list[str]:
    if not debug_trace:
        return []

    lines: list[str] = []

    prepare_bits: list[str] = []
    if debug_trace.get("prepare_calls") is not None:
        prepare_bits.append(f"prepare_calls={debug_trace.get('prepare_calls')}")
    if debug_trace.get("prepare_returned_rows") is not None:
        prepare_bits.append(f"prepare_rows={debug_trace.get('prepare_returned_rows')}")
    if debug_trace.get("execution_info_chars"):
        prepare_bits.append(f"exec_info_chars={debug_trace.get('execution_info_chars')}")
    if debug_trace.get("execution_info_baselines"):
        prepare_bits.append(f"baselines={debug_trace.get('execution_info_baselines')}")
    if debug_trace.get("candidate_hint_counts"):
        hint_parts = []
        for idx, item in enumerate(debug_trace["candidate_hint_counts"][:3]):
            hint_parts.append(
                f"#{idx}:idx={item.get('index', 0)}/join={item.get('join_order', 0)}"
                f"/cfg={item.get('config', 0)}/tables={item.get('tables', 0)}"
            )
        prepare_bits.append(f"hint_counts=[{', '.join(hint_parts)}]")
    if prepare_bits:
        lines.append("prepare: " + ", ".join(prepare_bits))

    prepare_errors = debug_trace.get("prepare_errors") or []
    if prepare_errors:
        lines.append("prepare_errors: " + "; ".join(prepare_errors[:3]))

    run_bits: list[str] = []
    if debug_trace.get("run_mcts_calls") is not None:
        run_bits.append(f"run_calls={debug_trace.get('run_mcts_calls')}")
    if debug_trace.get("run_mcts_input_rows") is not None:
        run_bits.append(f"run_input_rows={debug_trace.get('run_mcts_input_rows')}")
    if debug_trace.get("run_mcts_returned_rows") is not None:
        run_bits.append(f"run_rows={debug_trace.get('run_mcts_returned_rows')}")
    if debug_trace.get("execution_info_char_limit") is not None:
        run_bits.append(f"char_limit={debug_trace.get('execution_info_char_limit')}")
    oversized = debug_trace.get("oversized_execution_info") or []
    if oversized:
        oversized_text = ", ".join(
            f"{item.get('idx')}:{item.get('chars')}" for item in oversized[:5]
        )
        run_bits.append(f"oversized=[{oversized_text}]")
    if debug_trace.get("run_result_solution_counts"):
        run_bits.append(
            f"solutions={debug_trace.get('run_result_solution_counts')}"
        )
    if run_bits:
        lines.append("run: " + ", ".join(run_bits))

    run_errors = debug_trace.get("run_result_errors") or []
    if run_errors:
        lines.append("run_errors: " + "; ".join(run_errors[:3]))

    top_level_error = debug_trace.get("top_level_error")
    if top_level_error:
        lines.append(f"top_level_error: {top_level_error}")

    return lines


def install_optimizer_debug_hooks(optimizer: Any) -> None:
    if getattr(optimizer, "_benchmark_debug_hooks_installed", False):
        return

    optimizer._benchmark_debug_hooks_installed = True
    optimizer._benchmark_debug_trace = make_debug_trace(
        benchmark_id="",
        instance_id="",
        db_name="",
        digest="",
    )

    # 兼容性说明：LLMOptimizer 经过重构后，原先独立的
    # _get_execution_info_mcts / _get_candidate_hints_mcts / _prepare_mcts_data
    # 已被合并进 _run_mcts（baseline 探测 + candidate hints + MCTS 都在其内部完成），
    # 且 _run_mcts 的签名由 (qdf_data, db, log_prefix) 改为 (sql_samples, db, log_prefix)。
    # 这里只 hook 仍然存在的方法，避免 AttributeError；缺失的细粒度 trace 字段
    # 保持默认值，format_debug_trace_lines / infer_debug_reason 均能安全降级。

    original_get_execution_info = getattr(optimizer, "_get_execution_info_mcts", None)
    original_get_candidate_hints = getattr(optimizer, "_get_candidate_hints_mcts", None)
    original_prepare = getattr(optimizer, "_prepare_mcts_data", None)
    original_run_mcts = getattr(optimizer, "_run_mcts", None)

    def wrapped_get_execution_info(controller, sql):
        trace = optimizer._benchmark_debug_trace
        sample_idx = len(trace["execution_info_chars"])
        try:
            baseline_time, execution_info = original_get_execution_info(controller, sql)
            trace["execution_info_chars"].append(
                calc_execution_info_char_len(optimizer, execution_info)
            )
            trace["execution_info_baselines"].append(round(float(baseline_time or 0.0), 6))
            return baseline_time, execution_info
        except Exception as exc:
            trace["prepare_errors"].append(
                f"execution_info[{sample_idx}]={short_exc(exc)}"
            )
            raise

    def wrapped_get_candidate_hints(controller, db, sql):
        candidate_hints, table_names = original_get_candidate_hints(controller, db, sql)
        trace = optimizer._benchmark_debug_trace
        trace["candidate_hint_counts"].append({
            "index": len((candidate_hints or {}).get("index", []) or []),
            "join_order": len((candidate_hints or {}).get("join_order", []) or []),
            "config": len((candidate_hints or {}).get("config", []) or []),
            "tables": len(table_names or []),
        })
        return candidate_hints, table_names

    def wrapped_prepare(db, digest, sql_samples, log_prefix):
        trace = optimizer._benchmark_debug_trace
        trace["prepare_calls"] += 1
        try:
            qdf_data = original_prepare(db, digest, sql_samples, log_prefix)
            trace["prepare_returned_rows"] = len(qdf_data or [])
            return qdf_data
        except Exception as exc:
            trace["prepare_errors"].append(f"prepare={short_exc(exc)}")
            raise

    def wrapped_run_mcts(sql_samples, db, log_prefix):
        trace = optimizer._benchmark_debug_trace
        trace["run_mcts_calls"] += 1
        trace["run_mcts_input_rows"] = len(sql_samples or [])

        try:
            from mcts.config.config_loader import load_mcts_config

            config = load_mcts_config(
                custom_yaml_path=optimizer.mcts_custom_cfg,
                toml_overrides={
                    "llm_api_url_key": optimizer.mcts_llm_api_url_key or [],
                    "iterations": optimizer.mcts_iterations,
                    "plan_time_threshold_seconds": optimizer.mcts_stop_mcts_search_plan_time_threshold_seconds,
                    "estimated_tokens_budget": optimizer.mcts_stop_mcts_search_estimated_tokens_budget,
                },
            )
            trace["execution_info_char_limit"] = int(config.max_execution_info_chars)
        except Exception as exc:
            trace["prepare_errors"].append(f"load_mcts_config={short_exc(exc)}")

        try:
            results = original_run_mcts(sql_samples, db, log_prefix)
            trace["run_mcts_returned_rows"] = len(results or [])
            if results:
                trace["run_result_solution_counts"] = [
                    len(result.get("solutions", []) or [])
                    for result in results[:10]
                ]
                trace["run_result_errors"] = [
                    metrics.get("error")
                    for metrics in (
                        result.get("performance_metrics", {}) for result in results[:10]
                    )
                    if metrics.get("error")
                ]
                # execution_info 现在在 _run_mcts 内部计算, 这里事后从结果回填字符数,
                # 以及超限统计 (与旧版 oversized 检查等价的近似)。
                limit = trace.get("execution_info_char_limit")
                oversized = []
                for idx, result in enumerate(results):
                    ei = result.get("execution_info")
                    if ei is None:
                        continue
                    ei_len = calc_execution_info_char_len(optimizer, ei)
                    trace["execution_info_chars"].append(ei_len)
                    bt = result.get("baseline_time")
                    trace["execution_info_baselines"].append(round(float(bt or 0.0), 6))
                    if limit is not None and ei_len > int(limit):
                        oversized.append({"idx": idx, "chars": ei_len})
                if oversized:
                    trace["oversized_execution_info"] = oversized
            return results
        except Exception as exc:
            trace["run_result_errors"].append(short_exc(exc))
            raise

    if callable(original_get_execution_info):
        optimizer._get_execution_info_mcts = wrapped_get_execution_info
    if callable(original_get_candidate_hints):
        optimizer._get_candidate_hints_mcts = wrapped_get_candidate_hints
    if callable(original_prepare):
        optimizer._prepare_mcts_data = wrapped_prepare
    if callable(original_run_mcts):
        optimizer._run_mcts = wrapped_run_mcts


def worker_process(
    worker_id: int,
    instance_index: int,
    task_queue: mp.Queue,
    result_queue: mp.Queue,
    total_queries: int,
    connection_map: dict[str, ConnectionConfig],
    shutdown_event: mp.Event,
):
    """Worker 进程：从自己的专属 task_queue 取任务，同实例查询在此串行执行。"""
    signal.signal(signal.SIGINT, signal.SIG_IGN)

    from db_controller import DBController
    from data_models import (
        InstanceConfig,
        InstanceInfo,
        OutlineType,
        ProductType,
        Region,
        TrainingEnvType,
        WorkloadSource,
    )
    from feature_detector import detect_features
    from optimizer.basic_optimizer import OptimizationContext
    from optimizer.llm_optimizer import LLMOptimizer

    optimizer_cache: dict[str, LLMOptimizer] = {}

    def get_or_create_optimizer(instance_id: str) -> LLMOptimizer:
        cached = optimizer_cache.get(instance_id)
        if cached is not None:
            return cached

        connection = connection_map[instance_id]
        runtime_instance_id = (
            f"bench_{instance_id[:8]}_{connection.host}_{connection.port}_w{worker_id}"
        )
        task_id = f"bench_{int(time.time())}_w{worker_id}"

        env_config = InstanceConfig(
            instance_id=runtime_instance_id,
            ip=connection.host,
            port=connection.port,
            user=connection.user,
            password=connection.password,
            read_only=False,
            with_ai_marker=True,
            allow_reconnect=True,
        )

        temp_controller = DBController(env_config, db=None)
        try:
            feature_flags = detect_features(temp_controller)
        finally:
            temp_controller.close()

        training_controller = DBController(
            env_config,
            db=None,
            is_training_env=True,
            feature_flags=feature_flags,
        )

        instance_info = InstanceInfo(
            cluster_id=1,
            product_type=ProductType.CDB,
            instance_id=runtime_instance_id,
            node_uuid=f"bench_node_w{worker_id}",
            workload_source=WorkloadSource.SLOW_LOG,
            outline_type=OutlineType.STATEMENT_OUTLINE,
            region=Region.test,
            comments=f"Benchmark MCTS Worker-{worker_id} inst={instance_id[:8]}",
        )

        context = OptimizationContext(
            task_id=task_id,
            instance_id=runtime_instance_id,
            outline_type=instance_info.outline_type,
            training_controller=training_controller,
            env_type=TrainingEnvType.CLONE,
            feature_flags=feature_flags,
            instance_info=instance_info,
        )

        optimizer = LLMOptimizer(context)
        install_optimizer_debug_hooks(optimizer)
        optimizer_cache[instance_id] = optimizer
        return optimizer

    try:
        while not shutdown_event.is_set():
            try:
                query = task_queue.get(timeout=0.5)
            except Empty:
                continue

            if query is None:
                break

            if not isinstance(query, BenchmarkQuery):
                raise TypeError(f"Unexpected task type: {type(query).__name__}")

            query_prefix = (
                f"[I{instance_index} W{worker_id}] [{query.idx + 1}/{total_queries}] "
                f"{query.benchmark_id} ({query.difficulty_level})"
            )
            sql_preview = (
                query.query_text[:70] + "..."
                if len(query.query_text) > 70
                else query.query_text
            )
            print(f"\n{query_prefix} {sql_preview}", flush=True)

            # 查询标记 qXXXX（与 tpcds runner 一致），并入 digest 以便落到 JSON 文件名里
            query_label = f"q{query.idx + 1:04d}"
            base_digest = query.query_digest[:16] or generate_digest(query.query_text)
            digest = f"{query_label}_{base_digest}"

            try:
                connection = connection_map[query.instance_id]
                db_name = resolve_query_db(query, connection)
                if not db_name:
                    raise ValueError(
                        "查询未解析出 db，且未通过 --db 提供兜底数据库"
                    )

                optimizer = get_or_create_optimizer(query.instance_id)
                optimizer._benchmark_debug_trace = make_debug_trace(
                    benchmark_id=query.benchmark_id,
                    instance_id=query.instance_id,
                    db_name=db_name,
                    digest=digest,
                )
                optimizer.mcts_results = None

                start_time = time.time()
                candidates = optimizer._collect_additional_candidates(
                    db=db_name,
                    digest=digest,
                    sql_samples=[query.query_text],
                )
                elapsed = time.time() - start_time

                mcts_results = optimizer.mcts_results
                debug_trace = clone_debug_trace(
                    getattr(optimizer, "_benchmark_debug_trace", None)
                )
                runtime_baseline = extract_runtime_baseline(mcts_results)
                effective_baseline = (
                    runtime_baseline
                    if runtime_baseline is not None and runtime_baseline > 0
                    else query.baseline_time
                )
                mcts_summary = summarize_mcts_results(mcts_results, effective_baseline)
                debug_reason = infer_debug_reason(mcts_results, debug_trace)

                baseline_text = (
                    f"{effective_baseline:.3f}s"
                    if effective_baseline else "N/A"
                )
                print(
                    f"    {query_prefix} digest={digest} {elapsed:.1f}s, "
                    f"solutions={mcts_summary.solution_count}, "
                    f"candidates={len(candidates)}, "
                    f"llm_calls={mcts_summary.llm_calls}, "
                    f"db_executes={mcts_summary.db_executes}, "
                    f"baseline={baseline_text}",
                    flush=True,
                )
                if mcts_summary.best_time is not None:
                    speedup_text = (
                        f"{mcts_summary.speedup:.2f}x"
                        if mcts_summary.speedup is not None
                        else "N/A"
                    )
                    print(
                        f"    {query_prefix} Best: time={mcts_summary.best_time}s, "
                        f"speedup={speedup_text}, "
                        f"baseline={baseline_text}, "
                        f"hints={mcts_summary.best_hints or []}",
                        flush=True,
                    )
                if debug_reason:
                    print(f"  {query_prefix} DebugReason: {debug_reason}", flush=True)
                if not mcts_results:
                    for line in format_debug_trace_lines(debug_trace):
                        print(f"  {query_prefix} Debug: {line}", flush=True)

                result_queue.put(
                    QueryResult(
                        idx=query.idx,
                        benchmark_id=query.benchmark_id,
                        instance_id=query.instance_id,
                        query_digest=query.query_digest,
                        sql=query.query_text,
                        baseline_time=query.baseline_time,
                        difficulty_level=query.difficulty_level,
                        pattern_label=query.pattern_label,
                        success=True,
                        elapsed=elapsed,
                        n_candidates=len(candidates),
                        runtime_baseline=runtime_baseline,
                        mcts_results=mcts_results,
                        debug_trace=debug_trace,
                    )
                )

            except Exception as exc:
                current_optimizer = locals().get("optimizer")
                debug_trace = clone_debug_trace(
                    getattr(current_optimizer, "_benchmark_debug_trace", None)
                )
                if debug_trace is not None:
                    debug_trace["top_level_error"] = short_exc(exc)
                print(f"  {query_prefix} ERROR: {exc}", flush=True)
                traceback.print_exc()
                if debug_trace:
                    for line in format_debug_trace_lines(debug_trace):
                        print(f"  {query_prefix} Debug: {line}", flush=True)
                result_queue.put(
                    QueryResult(
                        idx=query.idx,
                        benchmark_id=query.benchmark_id,
                        instance_id=query.instance_id,
                        query_digest=query.query_digest,
                        sql=query.query_text,
                        baseline_time=query.baseline_time,
                        difficulty_level=query.difficulty_level,
                        pattern_label=query.pattern_label,
                        success=False,
                        debug_trace=debug_trace,
                        error=str(exc),
                    )
                )
    finally:
        for optimizer in optimizer_cache.values():
            try:
                optimizer.context.training_controller.close()
            except Exception:
                pass


def count_by(queries: list[BenchmarkQuery], field: str) -> dict[str, int]:
    counts = Counter(getattr(query, field) for query in queries)
    return dict(sorted(counts.items()))


def collect_key_config():
    """读取关键配置(MCTS 超时 / remote cache 相关), 用于 runner 开头/结尾打印。

    返回 (label, value) 列表; 读取失败的项以占位字符串代替, 不影响主流程。
    """
    def _toml(section, key, default="N/A"):
        try:
            from config.toml_config import TomlConfig
            return TomlConfig.get_instance().get(section, key)
        except Exception as e:
            return f"{default} ({type(e).__name__})"

    return [
        ("explain 超时 (mcts.explain_timeout_seconds)",
         _toml("mcts", "explain_timeout_seconds")),
        ("默认 plan 超时 (training.default_plan_timeout_seconds)",
         _toml("training", "default_plan_timeout_seconds")),
        ("remote cache (mcts.remote_cache_enabled)",
         _toml("mcts", "remote_cache_enabled")),
        ("remote cache 超时 (mcts.remote_cache_timeout_seconds)",
         _toml("mcts", "remote_cache_timeout_seconds")),
        ("cache 收紧到 baseline (mcts.cap_cache_timeout_by_baseline)",
         _toml("mcts", "cap_cache_timeout_by_baseline")),
    ]


def print_key_config(workers=None):
    """打印并行度 + 关键 MCTS/cache 配置。"""
    if workers is not None:
        print(f"  并行度 (workers): {workers}")
    for label, value in collect_key_config():
        print(f"  {label}: {value}")


def print_run_header(
    args: argparse.Namespace,
    queries: list[BenchmarkQuery],
    record_count: int,
):
    baseline_times = [query.baseline_time for query in queries]

    print("=" * 80)
    print("  Core-Set Benchmark MCTS 多进程并行优化测试")
    print("-" * 80)
    print(
        f"  连接:      {DEFAULT_INSTANCE_LOOKUP_PATH} "
        f"[默认 Instance Lookup Table, user={args.user}]"
    )
    if args.host_fallback_provided:
        print(f"  兜底连接:  {args.host}:{args.port}")
    if args.db:
        print(f"  兜底 DB:   {args.db}")
    print(f"  Bench JSON: {args.bench_json}")
    print(f"  Queries:    {len(queries)} 条 (原始 {record_count} 条)")
    num_instances = len({query.instance_id for query in queries})
    per_instance = max(1, getattr(args, "per_instance_workers", 1))
    total_workers = num_instances * per_instance
    print(
        f"  Workers:    {total_workers} "
        f"({num_instances} 实例 × {per_instance} 并行/实例)"
    )
    print(f"  验证阶段:   {'关闭' if getattr(args, 'no_validate', False) else '开启 (优化后单线程重跑 best hint + CSV)'}")
    if args.difficulty:
        print(f"  难度过滤:  {args.difficulty}")
    if args.pattern:
        print(f"  模式过滤:  {args.pattern}")
    if args.instance:
        print(f"  实例过滤:  {args.instance}")
    print(f"  难度分布:  {count_by(queries, 'difficulty_level')}")
    print(f"  模式分布:  {count_by(queries, 'pattern_label')}")
    print(f"  实例数:    {len({query.instance_id for query in queries})}")
    print(
        "  Baseline(JSON,仅参考):  "
        f"min={min(baseline_times):.3f}s, "
        f"max={max(baseline_times):.3f}s, "
        f"avg={sum(baseline_times) / len(baseline_times):.3f}s, "
        f"sum={sum(baseline_times):.1f}s"
    )
    print("  注: 运行/统计的 baseline 用实际探测值，JSON 值仅作兜底")
    print_key_config()
    print("=" * 80)


def group_queries_by_instance(
    queries: list[BenchmarkQuery],
    per_instance_workers: int = 1,
) -> dict[str, list[BenchmarkQuery]]:
    """按 instance_id 分组。每个实例由 per_instance_workers 个 worker 并行消费其查询队列。"""
    from collections import OrderedDict
    groups: OrderedDict[str, list[BenchmarkQuery]] = OrderedDict()
    for q in queries:
        groups.setdefault(q.instance_id, []).append(q)

    for instance_id, instance_queries in groups.items():
        print(f"  Instance {instance_id}: {len(instance_queries)} queries")
    total_workers = len(groups) * max(1, per_instance_workers)
    mode = "串行执行" if per_instance_workers <= 1 else f"每实例 {per_instance_workers} 并行"
    print(
        f"  调度: {len(groups)} 个实例 → {total_workers} 个 worker（{mode}）"
    )
    return dict(groups)


def start_instance_workers(
    instance_groups: dict[str, list[BenchmarkQuery]],
    result_queue: mp.Queue,
    total_queries: int,
    connection_map: dict[str, ConnectionConfig],
    shutdown_event: mp.Event,
    per_instance_workers: int = 1,
) -> tuple[list[mp.Process], list[mp.Queue]]:
    """为每个 instance 启动 per_instance_workers 个 worker 进程，共享该实例的任务队列。

    总进程数 = 实例数 × per_instance_workers；同一实例的查询由这些 worker 并行消费。

    返回 (workers, task_queues)；task_queues 需在收尾时关闭，否则解释器退出时
    会卡在 feeder 线程的 join 上。
    """
    workers: list[mp.Process] = []
    task_queues: list[mp.Queue] = []
    worker_id = 0
    for instance_idx, (instance_id, instance_queries) in enumerate(instance_groups.items()):
        task_q: mp.Queue = mp.Queue()
        task_queues.append(task_q)
        for q in instance_queries:
            task_q.put(q)
        for _ in range(per_instance_workers):
            task_q.put(None)  # 每个 worker 一个 sentinel

        for _ in range(per_instance_workers):
            process = mp.Process(
                target=worker_process,
                args=(
                    worker_id,
                    instance_idx,
                    task_q,
                    result_queue,
                    total_queries,
                    connection_map,
                    shutdown_event,
                ),
                name=f"bench-worker-{worker_id}-i{instance_idx}-{instance_id[:8]}",
                daemon=True,
            )
            process.start()
            workers.append(process)
            worker_id += 1
    return workers, task_queues


def collect_results(
    result_queue: mp.Queue,
    workers: list[mp.Process],
    query_count: int,
    total_start: float,
    instance_groups: Optional[dict[str, list[BenchmarkQuery]]] = None,
) -> list[QueryResult]:
    results: list[QueryResult] = []

    # 为实例结束提示准备：每个实例的预期查询数、序号、已完成计数。
    instance_expected: dict[str, int] = {}
    instance_index: dict[str, int] = {}
    if instance_groups:
        for idx, (iid, iqueries) in enumerate(instance_groups.items()):
            instance_expected[iid] = len(iqueries)
            instance_index[iid] = idx
    instance_done: Counter = Counter()
    instance_finished: set[str] = set()

    try:
        while len(results) < query_count:
            try:
                result = result_queue.get(timeout=1.0)
            except Empty:
                if all(not worker.is_alive() for worker in workers):
                    raise ResultCollectionInterrupted("所有 worker 进程已退出", results)
                continue

            results.append(result)

            # 实例结束提示：某实例所有查询都已回收时打印一次。
            iid = result.instance_id
            if iid in instance_expected:
                instance_done[iid] += 1
                if (
                    instance_done[iid] >= instance_expected[iid]
                    and iid not in instance_finished
                ):
                    instance_finished.add(iid)
                    inst_results = [r for r in results if r.instance_id == iid]
                    succ = sum(1 for r in inst_results if r.success)
                    elapsed = time.time() - total_start
                    print(
                        f"\n  ★ Instance I{instance_index[iid]} ({iid}) 已结束："
                        f"{instance_done[iid]} 条查询全部完成，成功 {succ}，"
                        f"已完成实例 {len(instance_finished)}/{len(instance_expected)}，"
                        f"elapsed={elapsed:.0f}s",
                        flush=True,
                    )

            if len(results) % 50 == 0:
                elapsed = time.time() - total_start
                rate = len(results) / elapsed if elapsed > 0 else 0
                eta = (query_count - len(results)) / rate if rate > 0 else 0
                print(
                    f"\n  === Progress: {len(results)}/{query_count} "
                    f"({len(results) / query_count * 100:.0f}%) | "
                    f"elapsed={elapsed:.0f}s | "
                    f"rate={rate:.1f} q/s | "
                    f"ETA={eta:.0f}s ===",
                    flush=True,
                )
    except KeyboardInterrupt as exc:
        raise ResultCollectionInterrupted("Ctrl+C received", results) from exc

    return results


def shutdown_workers(
    workers: list[mp.Process],
    shutdown_event: mp.Event,
    queues: Optional[list[mp.Queue]] = None,
):
    shutdown_event.set()

    deadline = time.time() + 5
    for worker in workers:
        remaining = max(0.0, deadline - time.time())
        worker.join(timeout=remaining)

    for worker in workers:
        if worker.is_alive():
            print(f"  Terminating {worker.name} (pid={worker.pid})", flush=True)
            worker.terminate()

    for worker in workers:
        worker.join(timeout=3)
        if worker.is_alive():
            worker.kill()
            worker.join(timeout=1)

    # 关闭队列并放弃 feeder 线程 join：worker 被 terminate/kill 后队列里可能仍有
    # 未 flush 的数据，否则解释器退出时会卡在 join feeder 线程上，导致跑完后进程
    # 不自动结束（需要 Ctrl+C）。
    for q in (queues or []):
        try:
            q.cancel_join_thread()
            q.close()
        except Exception:
            pass


def print_group_summary(
    title: str,
    groups: dict[str, list[QueryResult]],
    include_avg_elapsed: bool,
    width: int,
):
    divider = "=" * 80 if title == "Statistics by Difficulty" else "-" * 80
    print(f"\n{divider}")
    print(f"  {title}")
    print("-" * 80)

    for label in sorted(groups):
        group = groups[label]
        success_count = sum(1 for result in group if result.success)
        if include_avg_elapsed:
            avg_elapsed = (
                sum(result.elapsed for result in group if result.success) / success_count
                if success_count
                else 0.0
            )
            print(
                f"  {label:{width}s}: {len(group):4d} queries, "
                f"{success_count:4d} success, avg_elapsed={avg_elapsed:.1f}s"
            )
        else:
            print(
                f"  {label:{width}s}: {len(group):4d} queries, "
                f"{success_count:4d} success"
            )


def print_summary(
    results: list[QueryResult],
    total_queries: int,
    total_time: float,
    interrupted: bool,
):
    results_sorted = sorted(results, key=lambda result: result.idx)
    success_count = sum(1 for result in results_sorted if result.success)
    total_candidates = sum(result.n_candidates for result in results_sorted)

    print("\n" + "=" * 80)
    if interrupted:
        print(f"  INTERRUPTED! Completed {len(results_sorted)}/{total_queries} queries")
    else:
        print(f"  All {total_queries} queries completed")
    print("-" * 80)

    improved_count = 0
    improved_speedups: list[float] = []
    baseline_sum = 0.0
    min_time_sum = 0.0
    runtime_baseline_count = 0
    missing_baseline_count = 0

    for result in results_sorted:
        tag = f"[{result.idx + 1}/{total_queries}] {result.benchmark_id} ({result.difficulty_level})"
        # 统计口径：只认本次 MCTS 实际探测到的 baseline，不再读 bench JSON 的历史值。
        rt_baseline = (
            result.runtime_baseline
            if result.runtime_baseline is not None and result.runtime_baseline > 0
            else None
        )
        has_rt = rt_baseline is not None
        if has_rt:
            runtime_baseline_count += 1
            baseline_sum += rt_baseline
        else:
            missing_baseline_count += 1

        if not result.success:
            # 无实测 baseline 的失败查询不计入 min_time_sum（无可比基准）。
            if has_rt:
                min_time_sum += rt_baseline
            print(f"  {tag} | ERROR: {result.error}")
            if result.debug_trace:
                for line in format_debug_trace_lines(result.debug_trace):
                    print(f"    Debug: {line}")
            continue

        summary = summarize_mcts_results(result.mcts_results, rt_baseline or 0.0)
        best_time_text = "N/A"
        speedup_text = "N/A"

        if summary.best_time is not None:
            if has_rt:
                min_time_sum += min(rt_baseline, summary.best_time)
            best_time_text = f"{summary.best_time:.4f}s"
            if summary.speedup is not None:
                speedup_text = f"{summary.speedup:.2f}x"
                if summary.speedup > 1.01:
                    improved_count += 1
                    improved_speedups.append(summary.speedup)
        elif has_rt:
            min_time_sum += rt_baseline

        baseline_text = f"{rt_baseline:.3f}s(实测)" if has_rt else "无实测baseline"
        print(
            f"  {tag} | {result.elapsed:.1f}s | "
            f"baseline={baseline_text} | "
            f"best={best_time_text} | speedup={speedup_text} | "
            f"solutions={summary.solution_count} | "
            f"llm={summary.llm_calls} | "
            f"db={summary.db_executes}"
        )
        debug_reason = infer_debug_reason(result.mcts_results, result.debug_trace)
        if debug_reason:
            print(f"    DebugReason: {debug_reason}")
        if result.debug_trace and not result.mcts_results:
            for line in format_debug_trace_lines(result.debug_trace):
                print(f"    Debug: {line}")

    difficulty_groups: dict[str, list[QueryResult]] = {}
    pattern_groups: dict[str, list[QueryResult]] = {}
    for result in results_sorted:
        difficulty_groups.setdefault(result.difficulty_level or "Unknown", []).append(result)
        pattern_groups.setdefault(result.pattern_label or "Unknown", []).append(result)

    print_group_summary(
        title="Statistics by Difficulty",
        groups=difficulty_groups,
        include_avg_elapsed=True,
        width=12,
    )
    print_group_summary(
        title="Statistics by Pattern",
        groups=pattern_groups,
        include_avg_elapsed=False,
        width=20,
    )

    avg_speedup = (
        sum(improved_speedups) / len(improved_speedups)
        if improved_speedups
        else 0.0
    )
    overall_speedup = baseline_sum / min_time_sum if min_time_sum > 0 else 0.0

    print("\n" + "=" * 80)
    print(f"  Total:            {len(results_sorted)}/{total_queries} queries")
    print(f"  Success:          {success_count}")
    print(f"  Improved (>1.01x):{improved_count}")
    print(f"  Total candidates: {total_candidates}")
    print(f"  Avg speedup:      {avg_speedup:.2f}x (improved queries only)")
    print(
        f"  Baseline 来源:    实测 {runtime_baseline_count} / "
        f"无实测 {missing_baseline_count} (仅统计实测值, 不读 bench JSON)"
    )
    print(f"  Baseline sum:     {baseline_sum:.2f}s (仅实测查询)")
    print(f"  Min-time sum:     {min_time_sum:.2f}s (仅实测查询)")
    print(f"  Overall speedup:  {overall_speedup:.2f}x (baseline_sum / min_sum)")
    print(f"  Wall-clock time:  {total_time:.1f}s")
    try:
        from ai_config import MCTSConfig
        if MCTSConfig.output_dir:
            print(f"  MCTS 结果目录:    {MCTSConfig.output_dir}")
    except Exception:
        pass
    print_key_config()
    print("=" * 80)


def sanitize_filename_part(text: str, max_len: int = 48) -> str:
    cleaned = "".join(
        ch if ch.isalnum() or ch in ("-", "_", ".") else "-"
        for ch in text.strip()
    )
    cleaned = cleaned.strip("-_.")
    if not cleaned:
        cleaned = "na"
    return cleaned[:max_len]


def build_results_output_path(
    args: argparse.Namespace,
    queries: list[BenchmarkQuery],
) -> Path:
    results_dir = SCRIPT_DIR / "results"
    results_dir.mkdir(parents=True, exist_ok=True)

    parts = [sanitize_filename_part(Path(args.bench_json).stem, max_len=36)]

    scope_parts: list[str] = []
    if args.difficulty:
        scope_parts.append("diff-" + "-".join(args.difficulty))
    if args.pattern:
        scope_parts.append("pattern-" + "-".join(args.pattern))
    if args.instance:
        scope_parts.append(f"inst-{len(args.instance)}")
    if not scope_parts:
        scope_parts.append("all")

    parts.append(sanitize_filename_part("__".join(scope_parts), max_len=48))
    parts.append(f"{len(queries)}q")
    num_instances = len({q.instance_id for q in queries})
    total_workers = num_instances * max(1, getattr(args, "per_instance_workers", 1))
    parts.append(f"w{total_workers}")
    parts.append(time.strftime("%Y%m%d_%H%M%S"))

    return results_dir / ("__".join(parts) + ".json")


def save_results_report(
    args: argparse.Namespace,
    queries: list[BenchmarkQuery],
    results: list[QueryResult],
    total_time: float,
    interrupted: bool,
) -> Path:
    output_path = build_results_output_path(args, queries)
    results_sorted = sorted(results, key=lambda result: result.idx)

    serialized_results: list[dict[str, Any]] = []
    for result in results_sorted:
        eff_baseline = result.effective_baseline
        summary = summarize_mcts_results(result.mcts_results, eff_baseline)
        serialized_results.append({
            "idx": result.idx,
            "benchmark_id": result.benchmark_id,
            "instance_id": result.instance_id,
            "query_digest": result.query_digest,
            "sql": result.sql,
            "baseline_time": eff_baseline,
            "baseline_time_runtime": result.runtime_baseline,
            "baseline_time_json": result.baseline_time,
            "baseline_source": (
                "runtime"
                if result.runtime_baseline is not None and result.runtime_baseline > 0
                else "json_fallback"
            ),
            "difficulty_level": result.difficulty_level,
            "pattern_label": result.pattern_label,
            "success": result.success,
            "elapsed": result.elapsed,
            "n_candidates": result.n_candidates,
            "best_time": summary.best_time,
            "speedup": summary.speedup,
            "solution_count": summary.solution_count,
            "llm_calls": summary.llm_calls,
            "db_executes": summary.db_executes,
            "debug_reason": infer_debug_reason(result.mcts_results, result.debug_trace),
            "debug_trace": result.debug_trace,
            "mcts_results": result.mcts_results,
            "error": result.error,
        })

    payload = {
        "meta": {
            "created_at": time.strftime("%Y-%m-%d %H:%M:%S"),
            "bench_json": str(Path(args.bench_json).resolve()),
            "query_count": len(queries),
            "workers": len({q.instance_id for q in queries}) * max(1, getattr(args, "per_instance_workers", 1)),
            "per_instance_workers": getattr(args, "per_instance_workers", 1),
            "difficulty_filter": args.difficulty,
            "pattern_filter": args.pattern,
            "instance_filter": args.instance,
            "fallback_host": args.host if args.host_fallback_provided else None,
            "fallback_port": args.port if args.host_fallback_provided else None,
            "fallback_db": args.db,
            "interrupted": interrupted,
            "wall_clock_time_s": total_time,
        },
        "results": serialized_results,
    }

    with output_path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)

    return output_path


# ---------------------------------------------------------------------------
# 优化后单线程验证阶段
# ---------------------------------------------------------------------------

def default_validation_csv_path(args: argparse.Namespace) -> str:
    if args.output_csv:
        return args.output_csv
    results_dir = SCRIPT_DIR / "results"
    results_dir.mkdir(parents=True, exist_ok=True)
    stem = sanitize_filename_part(Path(args.bench_json).stem, max_len=36)
    ts = time.strftime("%Y%m%d_%H%M%S")
    return str(results_dir / f"{stem}__validation_{ts}.csv")


def analyze_optimization(results: list["QueryResult"], args: argparse.Namespace):
    """优化阶段结果分析: 每条 query 关键指标 + 平均值, 写 CSV。"""
    from opt_result_analysis import extract_query_metrics, write_optimization_csv

    rows = []
    for r in results:
        if not r.success or not r.mcts_results:
            continue
        key = f"{r.benchmark_id}_{(r.query_digest or '')[:8]}"
        rows.extend(extract_query_metrics(
            r.mcts_results, key=key, instance_id=r.instance_id,
        ))
    if not rows:
        print("  [analyze] 无可分析的优化结果, 跳过。")
        return
    results_dir = SCRIPT_DIR / "results"
    results_dir.mkdir(parents=True, exist_ok=True)
    stem = sanitize_filename_part(Path(args.bench_json).stem, max_len=36)
    ts = time.strftime("%Y%m%d_%H%M%S")
    csv_path = str(results_dir / f"{stem}__opt_metrics_{ts}.csv")
    write_optimization_csv(rows, csv_path)


def collect_validation_entries_by_instance_from_dir(
    input_dir: str,
    records: list[dict[str, Any]],
):
    """--validate-only: 从落盘 MCTS JSON 目录读取 entries, 并通过 query_digest /
    query_text 在 bench 记录里反查每条 query 归属的 instance_id, 按实例分组。

    返回 (by_instance, unrouted) ——
      by_instance: {instance_id: [ValidationEntry, ...]}
      unrouted:    无法在 bench 记录中匹配到实例的 entry 列表 (digest/sql 对不上)。

    落盘 MCTS JSON 不含 instance_id (设计上不改 JSON), 因此必须靠 bench JSON 路由:
      1. 优先用 query_digest (JSON 文件名里嵌的是 digest[:16], bench 记录有完整
         query_digest);
      2. 兜底用 query_text 精确匹配 (JSON 的 `query` 字段 == bench 的 query_text)。
    """
    from rollout_validation import entries_from_mcts_results

    p = Path(input_dir)
    if not p.is_dir():
        print(f"错误: --input-dir 目录不存在: {input_dir}")
        sys.exit(1)

    # 建立反查索引: 完整 digest / digest[:16] / 规范化 query_text -> instance_id
    digest_to_inst: dict[str, str] = {}
    digest16_to_inst: dict[str, str] = {}
    sql_to_inst: dict[str, str] = {}
    for rec in records:
        inst = coerce_text(rec.get("instance_id")).strip()
        if not inst:
            continue
        dg = coerce_text(rec.get("query_digest")).strip()
        if dg:
            digest_to_inst.setdefault(dg, inst)
            digest16_to_inst.setdefault(dg[:16], inst)
        qt = coerce_text(rec.get("query_text")).strip()
        if qt:
            sql_to_inst.setdefault(qt, inst)

    def _route(mr: dict[str, Any], fp_stem: str) -> Optional[str]:
        # 1) 文件名里嵌的 digest[:16]: 形如 {db}_q0001_{digest16}_{ts}
        parts = fp_stem.split("_")
        for token in parts:
            if len(token) == 16 and token in digest16_to_inst:
                return digest16_to_inst[token]
        # 2) run 内若带 query_digest (一般没有), 尝试完整/前缀匹配
        dg = coerce_text(mr.get("query_digest")).strip()
        if dg:
            if dg in digest_to_inst:
                return digest_to_inst[dg]
            if dg[:16] in digest16_to_inst:
                return digest16_to_inst[dg[:16]]
        # 3) 兜底: query 原文精确匹配 bench query_text
        q = coerce_text(mr.get("query")).strip()
        if q and q in sql_to_inst:
            return sql_to_inst[q]
        return None

    by_instance: dict[str, list] = {}
    unrouted: list = []
    for fp in sorted(p.glob("*.json")):
        try:
            with fp.open("r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception as exc:
            print(f"  ⚠ 跳过损坏文件 {fp.name}: {exc}")
            continue
        mcts_results = data if isinstance(data, list) else [data]
        for mr in mcts_results:
            if not isinstance(mr, dict):
                continue
            inst = _route(mr, fp.stem)
            ents = entries_from_mcts_results(
                [mr], key=fp.stem, db=None, instance_id=inst,
            )
            if not ents:
                continue
            if inst is None:
                unrouted.extend(ents)
            else:
                by_instance.setdefault(inst, []).extend(ents)
    return by_instance, unrouted


def build_validation_controller(args: argparse.Namespace, connection: "ConnectionConfig"):
    from data_models import InstanceConfig
    from db_controller import DBController

    cfg = InstanceConfig(
        instance_id=f"validate_bench_{connection.host}_{connection.port}",
        ip=connection.host,
        port=connection.port,
        user=connection.user,
        password=connection.password or "",
        read_only=False,
        with_ai_marker=True,
        allow_reconnect=True,
    )
    return DBController(cfg, db=connection.default_db)


def _validate_instance_worker(
    instance_id: str,
    entries: list,
    connection: "ConnectionConfig",
    per_inst_csv: str,
    args: argparse.Namespace,
    cmd_queue: "mp.Queue",
    result_queue: "mp.Queue",
):
    """单个实例的验证 worker (独立进程), 由协调器按 rollout 单步驱动。

    协议 (每条消息经 cmd_queue 下发, 结果经 result_queue 回传, 均带 instance_id):
      * 初始化完成后回传 {"phase": "ready", "max_rollout": N, "baseline_sum": ...,
        "entries": M};
      * 收到 {"cmd": "run", "rollout": r} → 跑第 r 轮, 回传
        {"phase": "rollout", "rollout": r, ...该轮计数/累计和...};
      * 收到 {"cmd": "finalize"} → 写 CSV 分片并读回, 回传
        {"phase": "done", "rows": [...], "stats": {...}, "error": ...}。

    实例内部仍是单线程串行 (InstanceRolloutValidator 顺序执行), 保证同一实例同时
    只有一条查询在跑; 跨实例则由协调器在每个 rollout 处 barrier 对齐。
    """
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    from rollout_validation import InstanceRolloutValidator

    tag = f"[I-{instance_id[:8]}]"

    def _log(msg: str) -> None:
        print(f"{tag} {msg}", flush=True)

    controller = None
    error: Optional[str] = None
    try:
        controller = build_validation_controller(args, connection)
        validator = InstanceRolloutValidator(
            entries, controller,
            timeout_seconds=args.validate_timeout,
            warmup_runs=args.validate_warmup,
            validate_baseline=args.validate_baseline,
            log=_log,
        )
        result_queue.put({
            "instance_id": instance_id,
            "phase": "ready",
            "max_rollout": validator.max_rollout,
            "baseline_sum": validator.total_baseline_s,
            "entries": len(validator.entries),
        })

        while True:
            cmd = cmd_queue.get()
            if not isinstance(cmd, dict):
                break
            action = cmd.get("cmd")
            if action == "run":
                r = int(cmd.get("rollout", 0))
                s = validator.run_rollout(r)
                s.update({"instance_id": instance_id, "phase": "rollout", "rollout": r})
                result_queue.put(s)
            elif action == "finalize":
                stats = validator.finalize(per_inst_csv)
                import csv as _csv
                rows: list[list[str]] = []
                try:
                    with open(per_inst_csv, "r", encoding="utf-8") as pf:
                        rows = list(_csv.reader(pf))
                    try:
                        Path(per_inst_csv).unlink()
                    except Exception:
                        pass
                except Exception:
                    rows = []
                result_queue.put({
                    "instance_id": instance_id,
                    "phase": "done",
                    "rows": rows,
                    "error": error,
                    "stats": {
                        "best_executed": stats.best_executed,
                        "best_from_cache": stats.best_from_cache,
                        "best_skipped_as_baseline": stats.best_skipped_as_baseline,
                        "best_skipped_worse_than_running_best": stats.best_skipped_worse_than_running_best,
                        "best_errors": stats.best_errors,
                        "external_exec_peak": stats.external_exec_peak,
                        "total_baseline_s": stats.total_baseline_s,
                        "total_best_s": stats.total_best_s,
                    },
                })
                break
            else:
                break
    except Exception as exc:
        error = f"{type(exc).__name__}: {exc}"
        _log(f"[validate] 实例验证异常: {error}")
        result_queue.put({
            "instance_id": instance_id, "phase": "done",
            "rows": [], "error": error, "stats": None,
        })
    finally:
        if controller is not None:
            try:
                controller.close()
            except Exception:
                pass


def _run_validation_by_instance(
    args: argparse.Namespace,
    by_instance: dict[str, list],
    connection_map: dict[str, "ConnectionConfig"],
    csv_path: str,
):
    """按实例并行 + 每轮 barrier 对齐的验证核心。

    每个实例一个独立进程 (实例内单线程串行, 同一实例同时只跑一条 query); 但跨实例
    在每个 rollout 处对齐: 协调器给所有实例下发"跑第 r 轮", 等**所有**实例的第 r 轮
    都结束后, 把各实例的累计 Sum 加总 (原结果Sum / 单线程Sum / BaselineSum), 打印
    一条全局结果, 再放行第 r+1 轮。这样跨实例的时间是合在一起、与 baseline 对比看的。

    被 run_validation_phase 和 --validate-only 复用。
    """
    if not by_instance:
        print("  [validate] 无可验证 entry, 跳过验证阶段。")
        return

    merged_path = Path(csv_path)
    merged_path.parent.mkdir(parents=True, exist_ok=True)

    # 跳过无连接信息的实例。
    runnable: list[tuple[str, list]] = []
    for instance_id, entries in by_instance.items():
        if connection_map.get(instance_id) is None:
            print(f"  [validate] 实例 {instance_id} 无连接信息, 跳过其 {len(entries)} 条")
            continue
        runnable.append((instance_id, entries))

    if not runnable:
        print("  [validate] 没有可验证的实例 (均无连接信息)。")
        return

    num_instances = len(runnable)
    total_entries = sum(len(ents) for _, ents in runnable)
    print("\n" + "#" * 80)
    print(f"  验证阶段 (按实例并行 + 每轮 barrier 对齐): {total_entries} entries, "
          f"{num_instances} 个实例 (每实例一进程/内部单线程, 跨实例每轮汇总对比 baseline)")
    print("#" * 80)

    result_queue: mp.Queue = mp.Queue()
    cmd_queues: dict[str, mp.Queue] = {}
    procs: dict[str, mp.Process] = {}

    t0 = time.time()
    for instance_id, entries in runnable:
        connection = connection_map[instance_id]
        per_inst_csv = str(merged_path.with_name(
            merged_path.stem + f"__inst_{instance_id[:8]}" + merged_path.suffix
        ))
        cq: mp.Queue = mp.Queue()
        cmd_queues[instance_id] = cq
        p = mp.Process(
            target=_validate_instance_worker,
            args=(instance_id, entries, connection, per_inst_csv, args, cq, result_queue),
            name=f"validate-{instance_id[:8]}",
            daemon=True,
        )
        p.start()
        procs[instance_id] = p

    all_queues = [result_queue, *cmd_queues.values()]
    collected: list[dict] = []

    def _drain_ready(expected: int) -> dict[str, dict]:
        """等 expected 个实例回传 ready/done。done(带 error) 视作该实例完成。"""
        got: dict[str, dict] = {}
        while len(got) < expected:
            msg = result_queue.get()
            iid = msg["instance_id"]
            if msg.get("phase") in ("ready", "done"):
                got[iid] = msg
                if msg.get("phase") == "done":
                    collected.append(msg)
        return got

    try:
        # 1) 等所有实例就绪, 求全局 max_rollout / baseline 总和。
        ready = _drain_ready(len(procs))
        max_rollout = max((m.get("max_rollout", -1) for m in ready.values()), default=-1)
        baseline_total = sum(m.get("baseline_sum", 0.0) for m in ready.values())
        alive = {iid for iid, m in ready.items() if m.get("phase") == "ready"}
        print(f"  [validate] 全局 max_rollout={max_rollout}, "
              f"BaselineSum={baseline_total:.3f}s ({len(alive)} 个实例就绪)")

        # 2) 逐 rollout: 下发 → barrier 等齐 → 汇总打印。
        #    某实例本轮异常退出 (done) 即从 alive 移除, 之后不再计入汇总。
        for r in range(max_rollout + 1):
            if not alive:
                break
            for iid in list(alive):
                cmd_queues[iid].put({"cmd": "run", "rollout": r})
            done_this_r: dict[str, dict] = {}
            failed_this_r: set[str] = set()
            while len(done_this_r) + len(failed_this_r) < len(alive):
                msg = result_queue.get()
                iid = msg["instance_id"]
                if msg.get("phase") == "rollout" and msg.get("rollout") == r:
                    done_this_r[iid] = msg
                elif msg.get("phase") == "done":
                    collected.append(msg)
                    failed_this_r.add(iid)
            alive -= failed_this_r

            # 跨实例汇总 (只统计仍存活实例本轮的累计和)。
            rec_sum = sum(m["recorded_sum_cum"] for m in done_this_r.values())
            best_sum = sum(m["best_sum_cum"] for m in done_this_r.values())
            measured = sum(m["measured"] for m in done_this_r.values())
            cached = sum(m["cached"] for m in done_this_r.values())
            skip_base = sum(m["skip_base"] for m in done_this_r.values())
            skip_worse = sum(m["skip_worse"] for m in done_this_r.values())
            timeout = sum(m["timeout"] for m in done_this_r.values())
            ext_peak_r = max((m["ext_peak_r"] for m in done_this_r.values()), default=0)
            ext_txt = f"  ⚠ 外部并发峰值={ext_peak_r}" if ext_peak_r > 0 else ""
            ovr = (baseline_total / best_sum) if best_sum > 0 else None
            ovr_txt = f", Overall≈{ovr:.3f}x" if ovr else ""
            print(f"  [validate][R{r + 1}] (全实例汇总) measured={measured} cached={cached} "
                  f"skip_base={skip_base} skip_worse={skip_worse} timeout={timeout}  "
                  f"原结果Sum {rec_sum:.3f}s -> 单线程Sum {best_sum:.3f}s "
                  f"(BaselineSum={baseline_total:.3f}s{ovr_txt}){ext_txt}", flush=True)

        # 3) 收尾: 让所有存活实例写 CSV 分片并回传 done。
        n_alive = len(alive)
        for iid in list(alive):
            cmd_queues[iid].put({"cmd": "finalize"})
        got = 0
        while got < n_alive:
            msg = result_queue.get()
            if msg.get("phase") == "done":
                collected.append(msg)
                got += 1
    finally:
        for iid, p in procs.items():
            p.join(timeout=5)
            if p.is_alive():
                p.terminate()
        for q in all_queues:
            try:
                q.cancel_join_thread()
                q.close()
            except Exception:
                pass

    # 合并各实例 CSV 分片 (主进程串行写, 表头只写一次)。
    header_written = False
    with merged_path.open("w", encoding="utf-8", newline="") as merged_fh:
        for msg in collected:
            rows = msg.get("rows") or []
            if not rows:
                continue
            if not header_written:
                merged_fh.write(",".join(_csv_quote(c) for c in rows[0]) + "\n")
                header_written = True
            for row in rows[1:]:
                merged_fh.write(",".join(_csv_quote(c) for c in row) + "\n")

    # 全局统计汇总。
    agg = {
        "best_executed": 0, "best_from_cache": 0, "best_skipped_as_baseline": 0,
        "best_skipped_worse_than_running_best": 0, "best_errors": 0,
        "external_exec_peak": 0, "total_baseline_s": 0.0, "total_best_s": 0.0,
    }
    for msg in collected:
        st = msg.get("stats")
        if not st:
            continue
        for k in ("best_executed", "best_from_cache", "best_skipped_as_baseline",
                  "best_skipped_worse_than_running_best", "best_errors",
                  "total_baseline_s", "total_best_s"):
            agg[k] += st.get(k, 0) or 0
        agg["external_exec_peak"] = max(agg["external_exec_peak"], st.get("external_exec_peak", 0) or 0)

    elapsed = time.time() - t0
    print("-" * 80)
    print(f"  [validate] 全部实例完成 耗时 {elapsed:.1f}s | "
          f"实测 {agg['best_executed']} 缓存 {agg['best_from_cache']} "
          f"跳过(≥base) {agg['best_skipped_as_baseline']} "
          f"跳过(worse) {agg['best_skipped_worse_than_running_best']} "
          f"错误 {agg['best_errors']}")
    if agg["external_exec_peak"] > 0:
        print(f"  [validate] ⚠ 外部并发执行峰值={agg['external_exec_peak']} "
              f"(测量时检测到外部执行, 计时可能受干扰)")
    else:
        print("  [validate] 外部并发执行峰值=0 (测量期间无外部查询干扰)")
    ov = (agg["total_baseline_s"] / agg["total_best_s"]) if agg["total_best_s"] > 0 else None
    line = (f"  [validate] (全实例) BaselineSum={agg['total_baseline_s']:.2f}s "
            f"BestSum={agg['total_best_s']:.2f}s")
    if ov:
        line += f" Overall≈{ov:.3f}x"
    print(line)

    errored = [m["instance_id"][:8] for m in collected if m.get("error")]
    if errored:
        print(f"  [validate] 以下实例验证过程中报错: {', '.join(errored)}")
    print(f"  [validate] 合并 CSV 输出: {merged_path}")


def run_validation_phase(
    args: argparse.Namespace,
    results: list[QueryResult],
    connection_map: dict[str, "ConnectionConfig"],
):
    """验证阶段。按实例分组, 每个实例一个独立进程并行验证 (实例内单线程串行重跑
    best-per-rollout hints); 各实例写独立 CSV 分片, 回传主进程合并成一份 CSV。"""
    from rollout_validation import entries_from_mcts_results

    csv_path = default_validation_csv_path(args)

    # 按 instance 分组 entry: 不同实例连接不同, 必须用各自 controller 重跑。
    by_instance: dict[str, list] = {}
    for r in results:
        if not r.success or not r.mcts_results:
            continue
        ents = entries_from_mcts_results(
            r.mcts_results,
            key=f"{r.benchmark_id}_{(r.query_digest or '')[:8]}",
            db=None,
            instance_id=r.instance_id,
            benchmark_id=r.benchmark_id,
        )
        by_instance.setdefault(r.instance_id, []).extend(ents)

    if not by_instance:
        print("  [validate] 无可验证 entry (没有成功的 mcts_results), 跳过验证阶段。")
        return

    _run_validation_by_instance(args, by_instance, connection_map, csv_path)


def _csv_quote(value: str) -> str:
    """最小 CSV 转义 (合并各实例分片时使用)。"""
    if value is None:
        value = ""
    if any(ch in value for ch in [",", '"', "\n", "\r"]):
        return '"' + value.replace('"', '""') + '"'
    return value



def main(argv: Optional[Sequence[str]] = None):
    args = parse_args(argv)
    ensure_runtime_compatibility()
    e2e_t0 = time.time()

    # ---- 只验证不优化: 从落盘 MCTS JSON 目录读取, 单线程重跑 ----
    if args.validate_only:
        if not args.input_dir:
            print("错误: --validate-only 需要 --input-dir 指向 MCTS 输出 JSON 目录")
            sys.exit(1)
        records = load_benchmark_records(args.bench_json)
        # 用全量记录 (不按 difficulty/pattern/instance 过滤) 构建连接表与路由索引,
        # 保证 JSON 里任意 query 都能找到其归属实例的连接。
        all_queries = build_queries(records)
        connection_map = build_connection_map(args, all_queries) if all_queries else {}

        by_instance, unrouted = collect_validation_entries_by_instance_from_dir(
            args.input_dir, records,
        )
        total_routed = sum(len(v) for v in by_instance.values())
        if total_routed == 0 and not unrouted:
            print("  [validate] --input-dir 下没有可验证 entry, 退出。")
            return
        print(
            f"  [validate] 路由结果: {total_routed} 条 -> {len(by_instance)} 个实例; "
            f"未匹配实例 {len(unrouted)} 条"
        )
        if unrouted:
            print(
                f"  [validate] ⚠ {len(unrouted)} 条 query 无法在 bench JSON 中按 "
                f"digest/SQL 匹配到实例 (跳过, 不会硬塞到错误实例)。"
            )

        csv_path = default_validation_csv_path(args)
        _run_validation_by_instance(args, by_instance, connection_map, csv_path)

        print("\n" + "=" * 80)
        print(f"  测试 E2E 总耗时: {time.time() - e2e_t0:.1f}s (validate-only)")
        print("=" * 80)
        return

    records = load_benchmark_records(args.bench_json)
    print(f"  加载 {len(records)} 条原始查询记录")

    queries = build_queries(
        records,
        difficulty_filter=args.difficulty,
        pattern_filter=args.pattern,
        instance_filter=args.instance,
        limit=args.limit,
    )
    if not queries:
        print("错误: 过滤后没有查询, 请检查过滤条件")
        sys.exit(1)

    print_run_header(args, queries, len(records))
    connection_map = build_connection_map(args, queries)

    result_queue: mp.Queue = mp.Queue()
    shutdown_event = mp.Event()

    # 每个 instance 由 per_instance_workers 个 worker 并行消费其查询队列
    instance_groups = group_queries_by_instance(queries, args.per_instance_workers)

    total_start = time.time()
    workers, task_queues = start_instance_workers(
        instance_groups=instance_groups,
        result_queue=result_queue,
        total_queries=len(queries),
        connection_map=connection_map,
        shutdown_event=shutdown_event,
        per_instance_workers=args.per_instance_workers,
    )

    results: list[QueryResult] = []
    interrupted = False
    try:
        results = collect_results(
            result_queue=result_queue,
            workers=workers,
            query_count=len(queries),
            total_start=total_start,
            instance_groups=instance_groups,
        )
    except ResultCollectionInterrupted as exc:
        interrupted = True
        results = exc.results
        if str(exc) == "Ctrl+C received":
            print("\n\n  Ctrl+C received, shutting down workers...", flush=True)
        else:
            print(f"\n\n  {exc}, shutting down...", flush=True)
    finally:
        shutdown_workers(workers, shutdown_event, [result_queue, *task_queues])

    total_time = time.time() - total_start
    print_summary(results, len(queries), total_time, interrupted)
    output_path = save_results_report(args, queries, results, total_time, interrupted)
    print(f"  Results JSON:   {output_path}")

    # ---- 优化结果分析 -> CSV (每条 query 指标 + 平均值) ----
    analyze_optimization(results, args)

    # ---- 优化后单线程验证阶段 (默认开启) ----
    validate_time = None
    if not args.no_validate and not interrupted:
        v0 = time.time()
        run_validation_phase(args, results, connection_map)
        validate_time = time.time() - v0
    elif args.no_validate:
        print("  [validate] --no-validate 已设置, 跳过验证阶段。")
    elif interrupted:
        print("  [validate] 优化被中断, 跳过验证阶段。")

    print("\n" + "=" * 80)
    print(f"  测试 E2E 总耗时: {time.time() - e2e_t0:.1f}s")
    print(f"    优化阶段: {total_time:.1f}s", flush=True)
    if validate_time is not None:
        print(f"    验证阶段: {validate_time:.1f}s", flush=True)
    print("=" * 80)


if __name__ == "__main__":
    # mp.set_start_method("spawn", force=True)
    mp.set_start_method("fork", force=True)
    main()
