#!/usr/bin/env python3
"""
批量校验 core_set_1000_cdb.json 涉及的所有 cache 库，
将 digest 不一致的结果统一输出到一个 CSV 文件。

逻辑：
  1. 解析 core_set_1000_cdb.json，按 instance_id 分组，收集每个实例的所有 {db}_cache 库名。
  2. 通过 instance_lookup_table.txt 将 instance_id（benchmark_id）映射到克隆实例的 IP:Port。
  3. 对每个 (instance_id, cache_db) 组合，直接连接数据库执行 digest 校验。
  4. 将所有 digest 不一致 (FAIL) 的条目汇总写入一个 CSV 文件。

用法:
    python mcts_scripts/benchmark/validate_all_caches.py

    # 自定义输出
    python mcts_scripts/benchmark/validate_all_caches.py -o digest_failures.csv

    # 只校验指定实例
    python mcts_scripts/benchmark/validate_all_caches.py \
        --instance-id cfad69b1-1c47-11f1-a476-506b4b430194

    # 只校验指定 cache 库
    python mcts_scripts/benchmark/validate_all_caches.py --cache-db neocrmbi_cache
"""

import argparse
import csv
import json
import os
import sys
import traceback
import time
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_BENCH_JSON = SCRIPT_DIR/"mcts_scripts"/"benchmark" / "core_set_1000_cdb.json"
DEFAULT_LOOKUP_TABLE = SCRIPT_DIR /"mcts_scripts"/"benchmark"/ "instance_lookup_table.txt"
DEFAULT_USER = "tencentroot"
DEFAULT_LIMIT = 99999999

from data_models import InstanceConfig  # noqa: E402
from db_controller import DBController  # noqa: E402
from feature_detector import detect_features  # noqa: E402
from validate_cache_entries import (  # noqa: E402
    _fetch_rows,
    _digest_check_row,
    _preview,
    ValidationResult,
)


# ---------------------------------------------------------------------------
# lookup table 解析
# ---------------------------------------------------------------------------

def parse_lookup_table(path: Path) -> dict:
    """解析 instance_lookup_table.txt，返回 {benchmark_id: (ip, port)} 映射。"""
    import re
    line_re = re.compile(
        r"^\s*(?P<benchmark>[0-9a-f\-]+)\s*---\s*"
        r"(?P<original>[0-9a-f\-]+)\s*---\s*"
        r"(?P<clone>[0-9a-f\-]+)\s+"
        r"(?P<ip>[\d.]+):(?P<port>\d+)\s*$"
    )
    mapping = {}
    with open(path) as f:
        for lineno, line in enumerate(f, 1):
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            m = line_re.match(line)
            if not m:
                print(f"[WARN] lookup table 第 {lineno} 行格式不匹配，跳过: {line!r}")
                continue
            mapping[m.group("benchmark").strip()] = (
                m.group("ip"),
                int(m.group("port")),
            )
    return mapping


# ---------------------------------------------------------------------------
# benchmark JSON 解析
# ---------------------------------------------------------------------------

def collect_cache_dbs(bench_json_path: Path) -> dict:
    """
    返回: {instance_id: sorted list of cache_db names}
    """
    with open(bench_json_path) as f:
        records = json.load(f)

    inst_dbs = defaultdict(set)
    for record in records:
        instance_id = record.get("instance_id", "")
        db = record.get("db", "").strip()
        if instance_id and db:
            inst_dbs[instance_id].add(f"{db}_cache")

    return {iid: sorted(dbs) for iid, dbs in inst_dbs.items()}


# ---------------------------------------------------------------------------
# 单个 cache 库校验
# ---------------------------------------------------------------------------

def validate_one_cache(
    ip: str,
    port: int,
    user: str,
    password: str,
    instance_id: str,
    cache_db: str,
    limit: int,
) -> tuple[list[dict], int, int]:
    """
    对一个 cache 库执行 digest 校验。

    返回: (fail_rows, total_rows, timeout_count)
      - fail_rows: 所有 digest FAIL 记录的 dict 列表
      - total_rows: 该 cache 库的总行数
      - timeout_count: is_timeout=1 的行数
    """
    cfg = InstanceConfig(
        instance_id=f"validate_{instance_id[:8]}",
        ip=ip,
        port=port,
        user=user,
        password=password,
        read_only=False,
        with_ai_marker=False,
        allow_reconnect=True,
    )

    probe = DBController(cfg)
    try:
        feature_flags = detect_features(probe)
    finally:
        probe.close()

    controller = DBController(cfg, is_training_env=True, feature_flags=feature_flags)
    fail_rows: list[dict] = []
    total_rows = 0
    timeout_count = 0

    try:
        rows = _fetch_rows(
            controller=controller,
            cache_db=cache_db,
            db_name=None,
            query_digest=None,
            plan_digest=None,
            limit=limit,
        )
        if not rows:
            print(f"    No rows in `{cache_db}`.`query_cache`")
            return fail_rows, 0, 0

        total_rows = len(rows)
        # is_timeout 在 _fetch_rows 返回的第 7 列 (索引 7)
        timeout_count = sum(1 for row in rows if bool(row[7]))

        print(f"    Loaded {total_rows} rows from `{cache_db}`.`query_cache`"
              f"  (timeout: {timeout_count}, {timeout_count/total_rows:.1%})")

        for idx, row in enumerate(rows, 1):
            cache_id, db_name = row[0], row[1]
            try:
                results = _digest_check_row(controller, row)
            except Exception as e:
                fail_rows.append({
                    "instance_id": instance_id,
                    "db_name": str(db_name),
                    "ip": ip,
                    "port": port,
                    "cache_db": cache_db,
                    "cache_id": int(cache_id),
                    "query_digest_expected": str(row[3]),
                    "query_digest_actual": "",
                    "plan_digest_expected": str(row[4]),
                    "plan_digest_actual": "",
                    "digest_reason": f"exception: {type(e).__name__}: {e}",
                    "hint_set": str(row[6] or ""),
                    "query_text": str(row[2] or ""),
                    "sql_with_hints": "",
                })
                continue

            for r in results:
                if r.digest_status != "FAIL":
                    continue
                fail_rows.append({
                    "instance_id": instance_id,
                    "db_name": r.db_name,
                    "ip": ip,
                    "port": port,
                    "cache_db": cache_db,
                    "cache_id": r.cache_id,
                    "query_digest_expected": r.query_digest_expected,
                    "query_digest_actual": r.query_digest_actual or "",
                    "plan_digest_expected": r.plan_digest_expected,
                    "plan_digest_actual": r.plan_digest_actual or "",
                    "digest_reason": r.digest_reason,
                    "hint_set": r.hint_preview,
                    "query_text": r._query_text,
                    "sql_with_hints": r.sql_with_hints or "",
                })

        print(f"    Digest FAIL: {len(fail_rows)}, total rows: {total_rows}, timeout: {timeout_count}")

    finally:
        controller.close()

    return fail_rows, total_rows, timeout_count


# ---------------------------------------------------------------------------
# CSV 输出
# ---------------------------------------------------------------------------

CSV_COLUMNS = [
    "instance_id",
    "db_name",
    "ip",
    "port",
    "cache_db",
    "cache_id",
    "query_digest_expected",
    "query_digest_actual",
    "plan_digest_expected",
    "plan_digest_actual",
    "digest_reason",
    "hint_set",
    "query_text",
    "sql_with_hints",
]


def write_fail_csv(fail_rows: list[dict], output_path: str) -> None:
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
        writer.writeheader()
        for row in fail_rows:
            writer.writerow(row)
    print(f"\nCSV saved: {output_path} ({len(fail_rows)} rows)")


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="批量校验 core_set_1000_cdb.json 下所有 cache 库，"
                    "将 digest 不一致结果输出为统一 CSV"
    )
    parser.add_argument(
        "--bench-json", type=str, default=str(DEFAULT_BENCH_JSON),
        help="benchmark JSON 文件路径"
    )
    parser.add_argument(
        "--lookup-table", type=str, default=str(DEFAULT_LOOKUP_TABLE),
        help="instance_lookup_table.txt 文件路径"
    )
    parser.add_argument("--user", default=DEFAULT_USER, help="MySQL 用户名")
    parser.add_argument("--password", default="", help="MySQL 密码")
    parser.add_argument("--limit", type=int, default=DEFAULT_LIMIT,
                        help="每个 cache 库的最大校验行数")
    parser.add_argument(
        "--instance-id", type=str, default=None,
        help="只校验指定 instance_id（逗号分隔）"
    )
    parser.add_argument(
        "--cache-db", type=str, default=None,
        help="只校验指定的 cache 库名（逗号分隔）"
    )
    parser.add_argument(
        "-o", "--output", type=str, default=None,
        help="输出 CSV 路径（默认: digest_failures_<timestamp>.csv）"
    )

    args = parser.parse_args()

    # ── 1. 加载 lookup table ──
    lookup = parse_lookup_table(Path(args.lookup_table))
    print(f"Lookup table loaded: {len(lookup)} entries")

    # ── 2. 收集 cache 库列表 ──
    inst_cache_dbs = collect_cache_dbs(Path(args.bench_json))
    print(f"Benchmark JSON loaded: {len(inst_cache_dbs)} instances")

    # ── 3. 过滤 ──
    filter_iids = None
    if args.instance_id:
        filter_iids = set(s.strip() for s in args.instance_id.split(",") if s.strip())

    filter_cdbs = None
    if args.cache_db:
        filter_cdbs = set(s.strip() for s in args.cache_db.split(",") if s.strip())

    # ── 4. 构建任务列表 ──
    tasks = []
    for instance_id, cache_dbs in sorted(inst_cache_dbs.items()):
        if filter_iids and instance_id not in filter_iids:
            continue
        if instance_id not in lookup:
            print(f"[WARN] instance_id={instance_id} 不在 lookup table 中，跳过")
            continue
        ip, port = lookup[instance_id]
        for cache_db in cache_dbs:
            if filter_cdbs and cache_db not in filter_cdbs:
                continue
            tasks.append((instance_id, ip, port, cache_db))

    if not tasks:
        print("No validation tasks to run.")
        return

    # ── 5. 打印计划 ──
    print(f"\n{'=' * 90}")
    print(f"Validation Plan: {len(tasks)} cache databases "
          f"across {len(set(t[0] for t in tasks))} instances")
    print(f"{'=' * 90}")
    for i, (iid, ip, port, cdb) in enumerate(tasks, 1):
        print(f"  [{i:3d}] {iid}  ->  {ip}:{port}  cache_db={cdb}")
    print(f"{'=' * 90}\n")

    # ── 6. 逐个校验，收集所有 FAIL 结果 ──
    all_fail_rows: list[dict] = []
    summary = []
    total = len(tasks)

    for idx, (instance_id, ip, port, cache_db) in enumerate(tasks, 1):
        print(f"\n{'#' * 80}")
        print(f"# [{idx}/{total}] instance={instance_id}")
        print(f"#          host={ip}:{port}  cache_db={cache_db}")
        print(f"{'#' * 80}")

        start = time.time()
        try:
            fails, total_rows, timeout_count = validate_one_cache(
                ip=ip, port=port,
                user=args.user, password=args.password,
                instance_id=instance_id,
                cache_db=cache_db,
                limit=args.limit,
            )
            all_fail_rows.extend(fails)
            status = "OK"
            fail_count = len(fails)
        except Exception as e:
            print(f"    [ERROR] {type(e).__name__}: {e}")
            traceback.print_exc()
            status = f"ERROR"
            fail_count = -1
            total_rows = 0
            timeout_count = 0

        elapsed = time.time() - start
        summary.append((instance_id, ip, port, cache_db, status, fail_count, total_rows, timeout_count, elapsed))
        timeout_pct = f"{timeout_count/total_rows:.1%}" if total_rows > 0 else "N/A"
        print(f"  -> {status}, digest_fail={fail_count}, total={total_rows}, "
              f"timeout={timeout_count}({timeout_pct}) ({elapsed:.1f}s)")

    # ── 7. 输出 CSV ──
    csv_path = args.output or f"digest_failures_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    write_fail_csv(all_fail_rows, csv_path)

    # ── 8. 总汇总 ──
    print(f"\n{'=' * 130}")
    print(f"{'FINAL SUMMARY':^130}")
    print(f"{'=' * 130}")
    print(f"{'#':>4}  {'instance_id':40}  {'host':>22}  {'cache_db':30}  "
          f"{'total':>6}  {'timeout':>7}  {'t/o%':>6}  {'fail':>6}  {'status':>7}  {'time':>7}")
    print(f"{'-' * 130}")

    total_fail = 0
    grand_total_rows = 0
    grand_timeout = 0
    for i, (iid, ip, port, cdb, status, fc, tr, tc, elapsed) in enumerate(summary, 1):
        fc_str = str(fc) if fc >= 0 else "ERR"
        to_pct = f"{tc/tr:.1%}" if tr > 0 else "N/A"
        print(f"{i:4d}  {iid:40}  {ip}:{port:>5}  {cdb:30}  "
              f"{tr:>6}  {tc:>7}  {to_pct:>6}  {fc_str:>6}  {status:>7}  {elapsed:6.1f}s")
        if fc > 0:
            total_fail += fc
        grand_total_rows += tr
        grand_timeout += tc

    print(f"{'-' * 130}")
    grand_to_pct = f"{grand_timeout/grand_total_rows:.1%}" if grand_total_rows > 0 else "N/A"
    print(f"Total cache rows: {grand_total_rows}, "
          f"timeout: {grand_timeout} ({grand_to_pct}), "
          f"digest failures: {total_fail}")
    print(f"CSV output: {csv_path}")
    print(f"{'=' * 130}")


if __name__ == "__main__":
    main()
