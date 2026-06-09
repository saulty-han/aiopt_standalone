"""
SQL Queries MCTS 多进程并行优化测试

每个 worker 进程独立创建 DBController / LLMOptimizer，
通过共享 Queue 逐条取出查询（而非提前分配），保证负载均匀。

Usage:
    python mcts_parallel_runner.py \
        --host 127.0.0.1 --port 13000 --user root \
        --queries queries.txt --db your_db --workers 4
"""
import sys
import os
import time
import signal
import json
import hashlib
import argparse
import multiprocessing as mp
from pathlib import Path
from typing import List, Dict, Any, Optional
from dataclasses import dataclass

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))


# ============================================================================
# Queries 文件加载
# ============================================================================

def load_queries_from_file(filepath: str) -> List[str]:
    """从文件加载查询，每行一条 SQL。忽略空行和 -- 注释。"""
    queries = []
    with open(filepath, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('--'):
                continue
            queries.append(line)
    return queries


def generate_digest(sql: str) -> str:
    return hashlib.md5(sql.encode()).hexdigest()[:16]


# ============================================================================
# Worker 进程结果
# ============================================================================

@dataclass
class QueryResult:
    """单条查询的优化结果，用于跨进程传递。"""
    idx: int                           # 查询原始索引 (0-based)
    sql: str
    success: bool
    elapsed: float = 0.0
    n_candidates: int = 0
    mcts_results: Optional[List[Dict]] = None
    error: Optional[str] = None


# ============================================================================
# Worker 进程入口
# ============================================================================

def worker_process(
    worker_id: int,
    task_queue: mp.Queue,
    result_queue: mp.Queue,
    total_queries: int,
    args_dict: Dict[str, Any],
    shutdown_event: mp.Event,
):
    """
    Worker 进程逻辑：保持与原脚本一致。
    """
    # Worker 进程忽略 SIGINT，由主进程统一管理关闭
    signal.signal(signal.SIGINT, signal.SIG_IGN)

    # --- 延迟导入 ---
    from db_controller import DBController
    from data_models import (
        TrainingEnvType, OutlineType, InstanceInfo, ProductType,
        Region, WorkloadSource, InstanceConfig,
    )
    from optimizer.llm_optimizer import LLMOptimizer
    from optimizer.basic_optimizer import OptimizationContext
    from feature_detector import detect_features

    host = args_dict["host"]
    port = args_dict["port"]
    user = args_dict["user"]
    password = args_dict["password"]
    db = args_dict["db"]

    # 将前缀改为通用的 opt_
    instance_id = f"opt_{host}_{port}_w{worker_id}"
    task_id = f"task_{int(time.time())}_w{worker_id}"

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
    feature_flags = detect_features(temp_controller)

    training_controller = DBController(
        env_config,
        db=db,
        is_training_env=True,
        feature_flags=feature_flags,
    )

    instance_info = InstanceInfo(
        cluster_id=1,
        product_type=ProductType.CDB,
        instance_id=instance_id,
        node_uuid=f"node_w{worker_id}",
        workload_source=WorkloadSource.SLOW_LOG,
        outline_type=OutlineType.STATEMENT_OUTLINE,
        region=Region.test,
        comments=f"MCTS Parallel Optimizer Worker-{worker_id}",
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

    while not shutdown_event.is_set():
        try:
            item = task_queue.get(timeout=0.5)
        except Exception:
            continue
        if item is None:
            break

        idx, sql = item
        query_prefix = f"[Worker-{worker_id}] [{idx + 1}/{total_queries}]"
        summary = sql[:80] + "..." if len(sql) > 80 else sql

        print(f"\n{query_prefix} {summary}", flush=True)
        digest = f"q{idx + 1}_{generate_digest(sql)}"

        try:
            t0 = time.time()
            candidates = optimizer._collect_additional_candidates(
                db=db,
                digest=digest,
                sql_samples=[sql],
            )
            elapsed = time.time() - t0

            n_candidates = len(candidates)
            mcts_results = optimizer.mcts_results

            if mcts_results:
                for r in mcts_results:
                    solutions = r.get("solutions", [])
                    metrics = r.get("performance_metrics", {})
                    print(
                        f"  {query_prefix} {elapsed:.1f}s, "
                        f"solutions={len(solutions)}, "
                        f"candidates={n_candidates}, "
                        f"llm_calls={metrics.get('llm_call_count', 0)}, "
                        f"db_executes={metrics.get('db_execute_count', 0)}",
                        flush=True,
                    )
                    if solutions:
                        best = solutions[0]
                        baseline = r.get("baseline_time")
                        exec_time = best.get("execution_time_s")
                        speedup = baseline / exec_time if baseline and exec_time else 0
                        print(
                            f"  {query_prefix} Best: reward={best.get('reward', 'N/A')}, "
                            f"time={exec_time}s, "
                            f"speedup={speedup:.2f}x, "
                            f"hints={best.get('executed_hints', [])}",
                            flush=True,
                        )
            else:
                print(f"  {query_prefix} {elapsed:.1f}s, no mcts_results", flush=True)

            result_queue.put(QueryResult(
                idx=idx, sql=sql, success=True, elapsed=elapsed,
                n_candidates=n_candidates, mcts_results=mcts_results,
            ))

        except Exception as e:
            import traceback
            print(f"  {query_prefix} ✗ 错误: {e}", flush=True)
            traceback.print_exc()
            result_queue.put(QueryResult(idx=idx, sql=sql, success=False, error=str(e)))


# ============================================================================
# 主流程
# ============================================================================

def main():
    parser = argparse.ArgumentParser(description="SQL Queries MCTS 多进程并行优化测试")
    parser.add_argument("--host", required=True)
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--user", default="root")
    parser.add_argument("--password", default="")
    parser.add_argument("--db", required=True)
    parser.add_argument("--queries", required=True)
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--workers", type=int, default=4)

    args = parser.parse_args()

    if not os.path.isfile(args.queries):
        print(f"错误: queries 文件不存在: {args.queries}")
        sys.exit(1)

    queries = load_queries_from_file(args.queries)
    if args.limit and args.limit > 0:
        queries = queries[:args.limit]

    num_workers = min(args.workers, len(queries))

    print("=" * 70)
    print(f"MCTS 多进程并行优化测试启动")
    print(f"  连接:    {args.host}:{args.port}")
    print(f"  数据库:  {args.db}")
    print(f"  Queries: {len(queries)} 条")
    print(f"  Workers: {num_workers} 个进程")
    print("=" * 70)

    task_queue = mp.Queue()
    result_queue = mp.Queue()
    shutdown_event = mp.Event()

    for idx, sql in enumerate(queries):
        task_queue.put((idx, sql))

    for _ in range(num_workers):
        task_queue.put(None)

    args_dict = vars(args)
    total_start = time.time()

    workers = []
    for wid in range(num_workers):
        p = mp.Process(
            target=worker_process,
            args=(wid, task_queue, result_queue, len(queries), args_dict, shutdown_event),
            name=f"mcts-worker-{wid}",
            daemon=True,
        )
        p.start()
        workers.append(p)

    results: List[QueryResult] = []
    interrupted = False

    try:
        for _ in range(len(queries)):
            while True:
                try:
                    result = result_queue.get(timeout=1.0)
                    break
                except Exception:
                    if all(not p.is_alive() for p in workers):
                        raise RuntimeError("所有 worker 进程已退出")
                    continue
            results.append(result)
    except (KeyboardInterrupt, RuntimeError) as e:
        interrupted = True
        print(f"\n\n⚠ 终止中: {e}", flush=True)

    shutdown_event.set()
    for p in workers:
        p.join(timeout=2)
        if p.is_alive():
            p.terminate()

    results.sort(key=lambda r: r.idx)
    success_count = sum(1 for r in results if r.success)

    print("\n" + "=" * 70)
    print("各查询结果汇总 (按原始顺序):")
    print("-" * 70)
    for r in results:
        query_prefix = f"[{r.idx + 1}/{len(queries)}]"
        summary = r.sql[:80] + "..." if len(r.sql) > 80 else r.sql
        if r.success:
            print(f"{query_prefix} {summary}\n  {r.elapsed:.1f}s, OK")
        else:
            print(f"{query_prefix} {summary}\n  ✗ 错误: {r.error}")

    print("\n" + "=" * 70)
    print(f"完成: {success_count}/{len(queries)} | 总耗时: {time.time() - total_start:.1f}s")
    print("=" * 70)


if __name__ == "__main__":
    mp.set_start_method("spawn", force=True)
    main()