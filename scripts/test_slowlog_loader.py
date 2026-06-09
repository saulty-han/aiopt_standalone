#!/usr/bin/env python3
"""
slowlog_loader 模块验证脚本

通过命令行参数调用 load_workload_from_slowlog，
验证 PREWHERE 基线 + OOM 降级重试逻辑是否正常工作。

用法:
    # CDB 实例
    python scripts/test_slowlog_loader.py \
        --node_uuid e16d610b-0366-11f0-920b-b8cef65bf162 \
        --region gz --product_type cdb

    # NCDB 实例
    python scripts/test_slowlog_loader.py \
        --node_uuid <uuid> --region bj --product_type ncdb

    # 自定义参数
    python scripts/test_slowlog_loader.py \
        --node_uuid <uuid> --region gz --product_type cdb \
        --window_days 7 --min_query_time 0.5
"""

import argparse
import sys
import os
import time

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from data_models import ProductType
from workload.slowlog_loader import load_workload_from_slowlog


def run_load(args, product_type):
    print(f"\n{'='*60}")
    print("  load_workload_from_slowlog")
    print(f"{'='*60}")
    t0 = time.time()
    try:
        rows = load_workload_from_slowlog(
            instance_id=args.node_uuid,
            region=args.region,
            product_type=product_type,
            node_uuid=args.node_uuid,
            min_query_time=args.min_query_time,
            window_days=args.window_days,
        )
        elapsed = time.time() - t0
    except Exception as e:
        elapsed = time.time() - t0
        print(f"  FAILED ({elapsed:.2f}s): {e}")
        return False

    print(f"  rows    = {len(rows)}")
    print(f"  elapsed = {elapsed:.2f}s")

    if not rows:
        print("  (empty result)")
        return True

    # 按 db 汇总
    db_stats = {}
    for r in rows:
        s = db_stats.setdefault(r.db, {"count": 0, "total_exec": 0})
        s["count"] += 1
        s["total_exec"] += r.count_star

    print(f"\n  {'db':<30s} {'templates':>10s} {'total_exec':>12s}")
    print(f"  {'-'*30} {'-'*10} {'-'*12}")
    for db, s in sorted(db_stats.items(), key=lambda x: -x[1]["total_exec"]):
        print(f"  {db:<30s} {s['count']:>10d} {s['total_exec']:>12d}")

    # 打印前 3 条样例
    print(f"\n  Top 3 by avg elapsed time:")
    top3 = sorted(rows, key=lambda r: -r.elapsed_time_avg)[:3]
    for i, r in enumerate(top3, 1):
        sql_preview = r.sql_text[:80].replace('\n', ' ')
        print(f"  {i}. [{r.db}] avg={r.elapsed_time_avg:.3f}s  count={r.count_star}")
        print(f"     {sql_preview}...")

    return True


def main():
    parser = argparse.ArgumentParser(
        description="slowlog_loader 模块验证脚本",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--node_uuid", required=True, help="CDB instid 或 NCDB node_uuid")
    parser.add_argument("--region", required=True, help="区域: bj/sh/gz/sg/test")
    parser.add_argument("--product_type", required=True, choices=["cdb", "ncdb"])
    parser.add_argument("--window_days", type=int, default=30, help="时间窗口天数 (default: 30)")
    parser.add_argument("--min_query_time", type=float, default=0.1, help="最小查询时间秒 (default: 0.1)")
    args = parser.parse_args()

    product_type = ProductType(args.product_type)

    print(f"node_uuid      = {args.node_uuid}")
    print(f"region         = {args.region}")
    print(f"product_type   = {args.product_type}")
    print(f"window_days    = {args.window_days}")
    print(f"min_query_time = {args.min_query_time}")

    if not run_load(args, product_type):
        sys.exit(1)


if __name__ == "__main__":
    main()
