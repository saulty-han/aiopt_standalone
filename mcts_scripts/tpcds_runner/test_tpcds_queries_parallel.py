from __future__ import annotations

"""
TPC-DS Queries MCTS 多进程优化 + 单线程验证

阶段一(优化): 默认 8 个 worker 进程并行, 每个进程独立 LLMOptimizer/DBController,
              共享一个任务队列, 逐条调用 LLMOptimizer._collect_additional_candidates。
阶段二(验证): 优化结束后, 主进程单线程把每个 rollout 的 best hint 重跑一遍
              (EXPLAIN ANALYZE), 生成宽表 CSV 报告 (参考 mcts/validate_mcts_results.py)。
              默认开启, 可关闭, 也可"只验证不优化"。

默认参数已可直接跑(无需额外指定 --workers / --validate):
    python mcts_scripts/tpcds_runner/test_tpcds_queries_parallel.py \
        --host 127.0.0.1 --port 13000 --user root --db tpcds \
        --queries mcts_scripts/tpcds_runner/queries_tpcds.txt

常用命令:
    # 1) 默认: 8 进程优化 + 单线程验证 + CSV
    python .../test_tpcds_queries_parallel.py --host H --port P --db tpcds \
        --queries Q.txt

    # 2) 只优化, 跳过验证阶段
    python .../test_tpcds_queries_parallel.py --host H --port P --db tpcds \
        --queries Q.txt --no-validate

    # 3) 只验证不优化: 复用已落盘的 MCTS 输出 JSON (来自 [mcts].output_dir),
    #    重跑 best hints 并出 CSV; 不再调用 LLM/不再做 MCTS 搜索
    python .../test_tpcds_queries_parallel.py --host H --port P --db tpcds \
        --mode validate-only --input-dir mcts/eval_data

    # 4) 调整并行度 / 验证时预热
    python .../test_tpcds_queries_parallel.py --host H --port P --db tpcds \
        --queries Q.txt --workers 4 --validate-warmup 1

    # 5) 验证阶段额外重测 baseline(无 hints)
    python .../test_tpcds_queries_parallel.py --host H --port P --db tpcds \
        --queries Q.txt --validate-baseline

配置:
    默认读 etc/aiopt_conf.toml。可用 etc/aiopt_conf.tpcds.toml.tpl 作为模版
    (default_plan_timeout_seconds=60, cache 开, cap 开); 复制为 aiopt_conf.toml
    并填好 [mcts].llm_api_url_key 即可。
"""

import argparse
import dataclasses
import hashlib
import multiprocessing as mp
import os
import signal
import sys
import time
from pathlib import Path
from queue import Empty
from typing import Any, List, Optional

SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent.parent
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(REPO_ROOT / "mcts_scripts"))

DEFAULT_WORKERS = 8


def collect_key_config():
    """读取关键配置(MCTS 超时 / remote cache 相关), 用于 runner 开头/结尾打印。

    返回 (label, value) 列表; 读取失败的项以 'N/A (err)' 占位, 不影响主流程。
    """
    items = []

    def _toml(section, key, default="N/A"):
        try:
            from config.toml_config import TomlConfig
            return TomlConfig.get_instance().get(section, key)
        except Exception as e:
            return f"{default} ({type(e).__name__})"

    items.append(("explain 超时 (mcts.explain_timeout_seconds)",
                  _toml("mcts", "explain_timeout_seconds")))
    items.append(("默认 plan 超时 (training.default_plan_timeout_seconds)",
                  _toml("training", "default_plan_timeout_seconds")))
    items.append(("remote cache (mcts.remote_cache_enabled)",
                  _toml("mcts", "remote_cache_enabled")))
    items.append(("remote cache 超时 (mcts.remote_cache_timeout_seconds)",
                  _toml("mcts", "remote_cache_timeout_seconds")))
    items.append(("cache 收紧到 baseline (mcts.cap_cache_timeout_by_baseline)",
                  _toml("mcts", "cap_cache_timeout_by_baseline")))
    return items


def print_key_config(workers=None):
    """打印并行度 + 关键 MCTS/cache 配置。"""
    if workers is not None:
        print(f"  并行度 (workers): {workers}")
    for label, value in collect_key_config():
        print(f"  {label}: {value}")


@dataclasses.dataclass
class QueryTask:
    idx: int
    db: str
    digest: str
    sql: str


@dataclasses.dataclass
class TaskResult:
    idx: int
    db: str
    digest: str
    sql: str
    n_candidates: int
    mcts_results: Optional[list]
    elapsed_seconds: float
    error: Optional[str] = None

    @property
    def success(self) -> bool:
        return self.error is None


def load_queries_from_file(filepath: str) -> List[str]:
    queries = []
    with open(filepath, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("--"):
                continue
            queries.append(line)
    return queries


def generate_digest(controller, sql: str) -> str:
    """Compute the query identifier as the first 16 chars of the MySQL
    statement_digest (same digest algorithm used by the MCTS logger and the
    mcts_json ``query_digest`` field). Falls back to md5 if the DB call fails."""
    import db_utils
    try:
        digest = db_utils.compute_statement_digest(controller, sql)
        if digest:
            return digest[:16]
    except Exception as e:
        print(f"⚠ statement_digest failed, falling back to md5: {e}", flush=True)
    return hashlib.md5(sql.encode()).hexdigest()[:16]


# ===========================================================================
# 阶段一: 多进程优化
# ===========================================================================

def worker_process(
    worker_id: int,
    task_queue: "mp.Queue",
    result_queue: "mp.Queue",
    total_queries: int,
    host: str,
    port: int,
    user: str,
    password: str,
    db: str,
    shutdown_event,
) -> None:
    """优化 worker: 从共享队列取 query, 跑 MCTS, 把结果(含 mcts_results)回传。"""
    signal.signal(signal.SIGINT, signal.SIG_IGN)

    from db_controller import DBController
    from data_models import (
        OutlineType, InstanceInfo, ProductType, Region, WorkloadSource,
        InstanceConfig, TrainingEnvType,
    )
    from feature_detector import detect_features
    from optimizer.basic_optimizer import OptimizationContext
    from optimizer.llm_optimizer import LLMOptimizer

    instance_id = f"test_tpcds_{host}_{port}_w{worker_id}"
    task_id = f"test_tpcds_{int(time.time())}_w{worker_id}"

    env_config = InstanceConfig(
        instance_id=instance_id,
        ip=host,
        port=port,
        user=user,
        password=password,
        read_only=False,
        with_ai_marker=True,
        allow_reconnect=True,
    )

    temp_controller = DBController(env_config, db=db)
    try:
        feature_flags = detect_features(temp_controller)
    finally:
        temp_controller.close()

    training_controller = DBController(
        env_config, db=db, is_training_env=True, feature_flags=feature_flags,
    )

    instance_info = InstanceInfo(
        cluster_id=1,
        product_type=ProductType.CDB,
        instance_id=instance_id,
        node_uuid=f"test_tpcds_node_w{worker_id}",
        workload_source=WorkloadSource.SLOW_LOG,
        outline_type=OutlineType.STATEMENT_OUTLINE,
        region=Region.test,
        comments=f"TPC-DS MCTS Worker-{worker_id}",
    )
    context = OptimizationContext(
        task_id=task_id,
        instance_id=instance_id,
        outline_type=instance_info.outline_type,
        training_controller=training_controller,
        env_type=TrainingEnvType.CLONE,
        feature_flags=feature_flags,
        instance_info=instance_info,
    )
    optimizer = LLMOptimizer(context)

    try:
        while not shutdown_event.is_set():
            try:
                task = task_queue.get(timeout=0.5)
            except Empty:
                continue
            if task is None:
                break
            if not isinstance(task, QueryTask):
                raise TypeError(f"Unexpected task type: {type(task).__name__}")

            prefix = f"[W{worker_id}] [{task.idx + 1}/{total_queries}]"
            preview = task.sql[:70] + "..." if len(task.sql) > 70 else task.sql
            print(f"\n{prefix} digest={task.digest} {preview}", flush=True)

            optimizer.mcts_results = None
            t0 = time.time()
            try:
                cands = optimizer._collect_additional_candidates(
                    db=task.db, digest=task.digest, sql_samples=[task.sql],
                )
                mcts_results = optimizer.mcts_results
                elapsed = time.time() - t0
                # 进度行
                if mcts_results:
                    mr = mcts_results[0]
                    solutions = mr.get("solutions", [])
                    metrics = mr.get("performance_metrics", {})
                    baseline = mr.get("baseline_time")
                    baseline_text = f"{baseline:.3f}s" if baseline else "N/A"
                    print(
                        f"  {prefix} {elapsed:.1f}s, "
                        f"solutions={len(solutions)}, "
                        f"candidates={len(cands)}, "
                        f"llm_calls={metrics.get('llm_call_count', 0)}, "
                        f"db_executes={metrics.get('db_execute_count', 0)}",
                        flush=True,
                    )
                    if solutions:
                        best = solutions[0]
                        et = best.get("execution_time_s")
                        sp = baseline / et if baseline and et else 0
                        print(
                            f"  {prefix} Best: time={et}s, "
                            f"speedup={sp:.2f}x, "
                            f"baseline={baseline_text}, "
                            f"hints={best.get('executed_hints', [])}",
                            flush=True,
                        )
                else:
                    print(
                        f"  {prefix} {elapsed:.1f}s, no mcts_results",
                        flush=True,
                    )
                result_queue.put(TaskResult(
                    idx=task.idx, db=task.db, digest=task.digest, sql=task.sql,
                    n_candidates=len(cands), mcts_results=mcts_results,
                    elapsed_seconds=elapsed,
                ))
            except Exception as e:
                elapsed = time.time() - t0
                print(f"  {prefix} ERROR: {type(e).__name__}: {e}", flush=True)
                result_queue.put(TaskResult(
                    idx=task.idx, db=task.db, digest=task.digest, sql=task.sql,
                    n_candidates=0, mcts_results=getattr(optimizer, "mcts_results", None),
                    elapsed_seconds=elapsed, error=f"{type(e).__name__}: {e}",
                ))
    finally:
        try:
            training_controller.close()
        except Exception:
            pass


def run_optimization(
    queries: List[str], args: argparse.Namespace,
) -> List[TaskResult]:
    """启动 worker 池并行优化, 收集结果。"""
    # Compute query digests up front via a short-lived controller, so the
    # identifier matches the MCTS logger / mcts_json query_digest (statement_digest).
    digest_controller = build_validation_controller(args)
    try:
        tasks = [
            QueryTask(
                idx=i, db=args.db,
                digest=f"q{i + 1:04d}_{generate_digest(digest_controller, sql)}",
                sql=sql,
            )
            for i, sql in enumerate(queries)
        ]
    finally:
        try:
            digest_controller.close()
        except Exception:
            pass

    n = len(tasks)
    workers_n = max(1, min(args.workers, n))

    task_queue: mp.Queue = mp.Queue()
    result_queue: mp.Queue = mp.Queue()
    shutdown_event = mp.Event()
    for t in tasks:
        task_queue.put(t)
    for _ in range(workers_n):
        task_queue.put(None)

    procs: List[mp.Process] = []
    for wid in range(workers_n):
        p = mp.Process(
            target=worker_process,
            args=(wid, task_queue, result_queue, n,
                  args.host, args.port, args.user, args.password, args.db,
                  shutdown_event),
            name=f"tpcds-worker-{wid}",
            daemon=True,
        )
        p.start()
        procs.append(p)

    results: List[TaskResult] = []
    interrupted = False
    try:
        while len(results) < n:
            try:
                results.append(result_queue.get(timeout=1.0))
            except Empty:
                if all(not p.is_alive() for p in procs):
                    print("  ⚠ 所有 worker 已退出, 提前结束收集", flush=True)
                    break
                continue
    except KeyboardInterrupt:
        interrupted = True
        print("\n\n⚠ 收到 Ctrl+C, 正在关闭 worker...", flush=True)
    finally:
        shutdown_event.set()
        deadline = time.time() + 5
        for p in procs:
            p.join(timeout=max(0.0, deadline - time.time()))
        for p in procs:
            if p.is_alive():
                p.terminate()
        for p in procs:
            p.join(timeout=3)
            if p.is_alive():
                p.kill()
        # 关闭队列并放弃 feeder 线程的 join：worker 被 terminate/kill 后队列里
        # 可能仍有未 flush 的数据，否则解释器退出时会卡在 join feeder 线程上，
        # 导致跑完后进程不自动结束（需要 Ctrl+C）。
        for q in (task_queue, result_queue):
            try:
                q.cancel_join_thread()
                q.close()
            except Exception:
                pass

    results.sort(key=lambda r: r.idx)
    if interrupted:
        print(f"⚠ 被中断, 已完成 {len(results)}/{n} 条", flush=True)
    return results


def print_optimization_summary(results: List[TaskResult], total: int, total_time: float):
    success = sum(1 for r in results if r.success)
    cands = sum(r.n_candidates for r in results)
    print("\n" + "=" * 70)
    print("优化阶段汇总 (按原始顺序):")
    print("-" * 70)
    for r in results:
        summary = r.sql[:80] + "..." if len(r.sql) > 80 else r.sql
        tag = f"[{r.idx + 1}/{total}]"
        if not r.success:
            print(f"{tag} {summary}\n  ✗ 错误: {r.error}")
            continue
        if r.mcts_results:
            mr = r.mcts_results[0]
            solutions = mr.get("solutions", [])
            metrics = mr.get("performance_metrics", {})
            print(f"{tag} {summary}\n  {r.elapsed_seconds:.1f}s, solutions={len(solutions)}, "
                  f"candidates={r.n_candidates}, llm={metrics.get('llm_call_count', 0)}, "
                  f"db={metrics.get('db_execute_count', 0)}")
            if solutions:
                best = solutions[0]
                baseline = mr.get("baseline_time")
                et = best.get("execution_time_s")
                sp = baseline / et if baseline and et else 0
                print(f"  Best: time={et}s, speedup={sp:.2f}x, hints={best.get('executed_hints', [])}")
        else:
            print(f"{tag} {summary}\n  {r.elapsed_seconds:.1f}s, no mcts_results")
    print("\n" + "=" * 70)
    print(f"优化完成: {success}/{total} 条, {cands} 个 candidates, 耗时 {total_time:.1f}s")
    try:
        from ai_config import MCTSConfig
        if MCTSConfig.output_dir:
            print(f"  MCTS 结果目录: {MCTSConfig.output_dir}")
    except Exception:
        pass
    print_key_config()
    print("=" * 70)


# ===========================================================================
# 阶段二: 单线程验证
# ===========================================================================

def build_validation_controller(args: argparse.Namespace):
    from data_models import InstanceConfig
    from db_controller import DBController
    cfg = InstanceConfig(
        instance_id=f"validate_tpcds_{args.host}_{args.port}",
        ip=args.host, port=args.port, user=args.user, password=args.password or "",
        read_only=False, with_ai_marker=True, allow_reconnect=True,
    )
    return DBController(cfg, db=args.db)


def collect_validation_entries(results: List[TaskResult], db: str):
    """优化结果(内存 mcts_results) → ValidationEntry 列表。"""
    from rollout_validation import entries_from_mcts_results
    entries = []
    for r in results:
        if not r.success or not r.mcts_results:
            continue
        entries.extend(entries_from_mcts_results(
            r.mcts_results, key=r.digest, db=db,
        ))
    return entries


def collect_validation_entries_from_dir(input_dir: str, db_fallback: str):
    """validate-only 模式: 从落盘的 MCTS JSON 目录读取 entries。"""
    import json
    from rollout_validation import entries_from_mcts_results
    p = Path(input_dir)
    if not p.is_dir():
        print(f"错误: --input-dir 目录不存在: {input_dir}", file=sys.stderr)
        sys.exit(1)
    entries = []
    for fp in sorted(p.glob("*.json")):
        try:
            data = json.load(open(fp, "r", encoding="utf-8"))
        except Exception as e:
            print(f"  ⚠ 跳过损坏文件 {fp.name}: {e}", file=sys.stderr)
            continue
        mcts_results = data if isinstance(data, list) else [data]
        entries.extend(entries_from_mcts_results(
            mcts_results, key=fp.stem, db=db_fallback,
        ))
    return entries


def run_validation(entries, args: argparse.Namespace, csv_path: str):
    from rollout_validation import validate_results
    if not entries:
        print("  [validate] 无可验证 entry, 跳过。")
        return
    controller = build_validation_controller(args)
    try:
        validate_results(
            entries=entries,
            controller=controller,
            csv_path=csv_path,
            timeout_seconds=args.validate_timeout,
            warmup_runs=args.validate_warmup,
            validate_baseline=args.validate_baseline,
        )
    finally:
        try:
            controller.close()
        except Exception:
            pass


# ===========================================================================
# 入口
# ===========================================================================

def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="TPC-DS MCTS 多进程优化 + 单线程验证")
    parser.add_argument("--host", required=True, help="数据库 IP")
    parser.add_argument("--port", type=int, required=True, help="数据库端口")
    parser.add_argument("--user", default="root", help="数据库用户名")
    parser.add_argument("--password", default="", help="数据库密码")
    parser.add_argument("--db", required=True, help="目标数据库")
    parser.add_argument("--queries", default="", help="Queries 文件路径 (optimize/both 模式必填)")
    parser.add_argument("--limit", type=int, default=0, help="只优化前 N 条 (0=全部)")
    parser.add_argument("--workers", type=int, default=DEFAULT_WORKERS,
                        help=f"优化阶段并行 worker 进程数 (默认 {DEFAULT_WORKERS})")

    parser.add_argument(
        "--mode", choices=["both", "optimize", "validate-only"], default="both",
        help="both=优化+验证(默认); optimize=只优化; validate-only=只验证(读 --input-dir)",
    )
    parser.add_argument("--no-validate", action="store_true",
                        help="等价 --mode optimize: 优化后不做验证")
    parser.add_argument("--input-dir", default="",
                        help="validate-only 模式下读取的 MCTS 输出 JSON 目录")
    parser.add_argument("--output-csv", default="",
                        help="验证 CSV 输出路径 (默认 mcts_scripts/tpcds_runner/results/ 下自动命名)")
    parser.add_argument("--validate-baseline", action="store_true",
                        help="验证阶段额外重测 baseline(无 hints)")
    parser.add_argument("--validate-timeout", type=float, default=0.0,
                        help="验证单次 EXPLAIN ANALYZE 超时(秒); 0=自动(baseline×1.1)")
    parser.add_argument("--validate-warmup", type=int, default=1,
                        help="验证时每条 SQL 正式计时前的预热轮数 (默认 1)")

    args = parser.parse_args(argv)
    if args.no_validate and args.mode == "both":
        args.mode = "optimize"
    return args


def default_csv_path(args: argparse.Namespace) -> str:
    if args.output_csv:
        return args.output_csv
    results_dir = SCRIPT_DIR / "results"
    results_dir.mkdir(parents=True, exist_ok=True)
    ts = time.strftime("%Y%m%d_%H%M%S")
    return str(results_dir / f"tpcds_validation_{args.db}_{ts}.csv")


def analyze_optimization(results: List[TaskResult], args: argparse.Namespace):
    """优化阶段结果分析: 每条 query 关键指标 + 平均值, 写 CSV。"""
    from opt_result_analysis import extract_query_metrics, write_optimization_csv

    rows = []
    for r in results:
        if not r.success or not r.mcts_results:
            continue
        rows.extend(extract_query_metrics(
            r.mcts_results, key=r.digest, instance_id=None,
        ))
    if not rows:
        print("  [analyze] 无可分析的优化结果, 跳过。")
        return
    results_dir = SCRIPT_DIR / "results"
    results_dir.mkdir(parents=True, exist_ok=True)
    ts = time.strftime("%Y%m%d_%H%M%S")
    csv_path = str(results_dir / f"tpcds_opt_metrics_{args.db}_{ts}.csv")
    write_optimization_csv(rows, csv_path)



def main(argv=None):
    args = parse_args(argv)

    csv_path = default_csv_path(args)
    e2e_t0 = time.time()

    if args.mode == "validate-only":
        if not args.input_dir:
            print("错误: --mode validate-only 需要 --input-dir 指向 MCTS 输出 JSON 目录")
            sys.exit(1)
        print("=" * 70)
        print(f"[validate-only] 从 {args.input_dir} 读取 MCTS 结果并验证")
        print("=" * 70)
        entries = collect_validation_entries_from_dir(args.input_dir, args.db)
        run_validation(entries, args, csv_path)
        print("\n" + "=" * 70)
        print(f"测试 E2E 总耗时: {time.time() - e2e_t0:.1f}s (validate-only)")
        print("=" * 70)
        return

    # optimize / both 都需要 queries
    if not args.queries or not os.path.isfile(args.queries):
        print(f"错误: queries 文件不存在: {args.queries}")
        sys.exit(1)
    queries = load_queries_from_file(args.queries)
    if args.limit and args.limit > 0:
        queries = queries[: args.limit]
    if not queries:
        print("错误: 无可优化 query")
        sys.exit(1)

    print("=" * 70)
    print("TPC-DS MCTS 多进程优化")
    print(f"  连接:    {args.host}:{args.port} (user={args.user})")
    print(f"  数据库:  {args.db}")
    print(f"  Queries: {len(queries)} 条")
    print(f"  Workers: {min(args.workers, len(queries))} (进程)")
    print(f"  验证:    {'开启 (优化后单线程重跑)' if args.mode == 'both' else '关闭'}")
    print_key_config()
    print("=" * 70)

    t0 = time.time()
    results = run_optimization(queries, args)
    optimize_time = time.time() - t0
    print_optimization_summary(results, len(queries), optimize_time)

    # 优化阶段结果分析 -> CSV (每条 query 指标 + 平均值)
    analyze_optimization(results, args)

    validate_time = None
    if args.mode == "both":
        entries = collect_validation_entries(results, args.db)
        v0 = time.time()
        run_validation(entries, args, csv_path)
        validate_time = time.time() - v0

    print("\n" + "=" * 70)
    print(f"测试 E2E 总耗时: {time.time() - e2e_t0:.1f}s")
    print(f"  优化阶段: {optimize_time:.1f}s", flush=True)
    if validate_time is not None:
        print(f"  验证阶段: {validate_time:.1f}s", flush=True)
    print("=" * 70)


if __name__ == "__main__":
    mp.set_start_method("fork", force=True)
    main()
