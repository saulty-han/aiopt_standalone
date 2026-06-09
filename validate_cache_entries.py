#!/usr/bin/env python3
"""
验证 cache 库中的 query_cache 记录是否可复现。

分两轮验证：
  第一轮（Digest 校验）：对所有 cache 行的每个 hint 组合，校验 query_digest 与 plan_digest 是否一致。
  第二轮（执行时间校验）：对第一轮 PASS 且 cache.execution_time >= 100ms 的条目，
      用 warmup_runs=2 执行（2 次预热 + 1 次热执行），比对热执行耗时与 cache 值的偏差（默认 5%）。
"""
#  python validate_cache_entries.py  --host 28.67.117.189 --port 20355 --user tencentroot --cache-db jiyun_v2_cache --digest-only
import argparse
import csv
import json
import os
import sys
import traceback
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, List, Optional, Set, Tuple
import re
from sqlalchemy import text

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from data_models import InstanceConfig, FeatureFlags  # noqa: E402
from db_controller import DBController  # noqa: E402
from feature_detector import detect_features  # noqa: E402
import db_utils  # noqa: E402
import hints_generator  # noqa: E402
from mcts.utils.hint_utils import build_sql_with_hints, deduplicate_hints  # noqa: E402


def _build_sql_with_hints(query: str, hints: List[str]) -> str:
    if not hints:
        return query
    # deduped = deduplicate_hints(hints)
    return build_sql_with_hints(query, hints)
SKIP_TIME_THRESHOLD_S = 0.1  # 100ms


@dataclass
class ValidationResult:
    cache_id: int
    db_name: str
    hint_index: int
    hint_total: int
    hint_preview: str
    query_digest_expected: str
    plan_digest_expected: str
    cache_execution_time_s: Optional[float]
    # digest 阶段
    digest_status: str = ""          # PASS / FAIL / SKIP
    digest_reason: str = ""
    query_digest_actual: Optional[str] = None
    plan_digest_actual: Optional[str] = None
    # 执行时间阶段
    time_status: str = ""            # PASS / FAIL / SKIP / (空=未进入)
    time_reason: str = ""
    hot_execution_time_s: Optional[float] = None
    relative_diff: Optional[float] = None
    # 附属：用于第二轮快速构建 SQL
    _query_text: str = field(default="", repr=False)
    _hints: list = field(default_factory=list, repr=False)
    sql_with_hints: str = field(default="", repr=False)


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _preview(hints: list[str], max_len: int = 1000000) -> str:
    if not hints:
        return "(no hint)"
    s = " | ".join(hints)
    return s if len(s) <= max_len else s[:max_len] + "..."


def _parse_hint_sets(hint_set_raw: Optional[str]) -> list[list[str]]:
    """返回所有 hint 组合；至少返回一组（无 hint 时为 [[]]）。"""
    if not hint_set_raw:
        return [[]]

    def norm(xs: list[str]) -> list[str]:
        return [x.strip() for x in xs if isinstance(x, str) and x.strip()]

    try:
        parsed = json.loads(hint_set_raw)
    except Exception:
        s = hint_set_raw.strip()
        return [[s]] if s else [[]]

    if isinstance(parsed, str):
        s = parsed.strip()
        return [[s]] if s else [[]]

    if not isinstance(parsed, list) or not parsed:
        return [[]]

    if all(isinstance(x, str) for x in parsed):
        one = norm(parsed)
        return [one] if one else [[]]

    out: list[list[str]] = []
    for item in parsed:
        if isinstance(item, list):
            h = norm(item)
            if h:
                out.append(h)
        elif isinstance(item, str):
            s = item.strip()
            if s:
                out.append([s])

    seen: set[tuple[str, ...]] = set()
    dedup: list[list[str]] = []
    for hs in out:
        k = tuple(hs)
        if k not in seen:
            seen.add(k)
            dedup.append(hs)

    return dedup if dedup else [[]]



def _fetch_rows(
    controller: DBController,
    cache_db: str,
    db_name: Optional[str],
    query_digest: Optional[str],
    plan_digest: Optional[str],
    limit: int,
) -> list[Any]:
    where = ["1=1"]
    params: dict[str, Any] = {"limit": limit}
    if db_name:
        where.append("db_name = :db_name")
        params["db_name"] = db_name
    if query_digest:
        where.append("query_digest = :query_digest")
        params["query_digest"] = query_digest
    if plan_digest:
        where.append("plan_digest = :plan_digest")
        params["plan_digest"] = plan_digest

    sql = f"""
    SELECT id, db_name, query_text, query_digest, plan_digest,
           execution_time, hint_set, is_timeout, created_at
    FROM `{cache_db}`.`query_cache`
    WHERE {' AND '.join(where)}
    LIMIT :limit
    """
    return controller.execute(text(sql), params).fetchall()


def _indent(s: str, prefix: str = "  ") -> str:
    return "\n".join(prefix + line for line in s.rstrip("\n").split("\n"))


# ---------------------------------------------------------------------------
# 第一轮：digest 校验
# ---------------------------------------------------------------------------

def _digest_check_row(
    controller: DBController,
    row: Any,
) -> list[ValidationResult]:
    """对一行 cache 的所有 hint 组合做 digest 校验，返回结果列表。"""
    cache_id = int(row[0])
    db_name = row[1]
    query_text = row[2]
    query_digest_expected = row[3]
    plan_digest_expected = row[4]
    cache_execution_time_s = float(row[5]) if row[5] is not None else None
    hint_set_raw = row[6]
    is_timeout = bool(row[7])

    def _make(hint_idx, hint_total, hints, status, reason, qd_act=None, pd_act=None, sql_with_hints=None):
        return ValidationResult(
            cache_id=cache_id,
            db_name=db_name,
            hint_index=hint_idx,
            hint_total=hint_total,
            hint_preview=_preview(hints),
            query_digest_expected=query_digest_expected,
            plan_digest_expected=plan_digest_expected,
            cache_execution_time_s=cache_execution_time_s,
            digest_status=status,
            digest_reason=reason,
            query_digest_actual=qd_act,
            plan_digest_actual=pd_act,
            _query_text=query_text if isinstance(query_text, str) else "",
            sql_with_hints=sql_with_hints,
            _hints=hints,
        )

    if not query_text or not isinstance(query_text, str):
        return [_make(1, 1, [], "FAIL", "cache query_text 为空或非法")]

    # if is_timeout:
    #     return [_make(1, 1, [], "SKIP", "cache 记录为 timeout，跳过")]

    all_hint_sets = _parse_hint_sets(hint_set_raw)
    results: list[ValidationResult] = []

    controller.use_db(db_name)
    query_digest_actual = db_utils.compute_statement_digest(controller, query_text)

    for i, hints in enumerate(all_hint_sets, start=1):
        if query_digest_actual != query_digest_expected:
            results.append(_make(i, len(all_hint_sets), hints, "FAIL", "query_digest 不一致",
                                 qd_act=query_digest_actual))
            continue

        sql_with_hints = _build_sql_with_hints(query_text, hints)
        try:
            plan_digest_actual = db_utils.get_plan_id_only(controller, sql_with_hints, explain_timeout_seconds=30)
        except Exception as e:
            results.append(_make(i, len(all_hint_sets), hints, "FAIL",
                                 f"get_plan_id_only 异常: {type(e).__name__}: {e}",
                                 qd_act=query_digest_actual))
            continue

        if plan_digest_actual != plan_digest_expected:
            results.append(_make(i, len(all_hint_sets), hints, "FAIL", "plan_digest 不一致",
                                 qd_act=query_digest_actual, pd_act=plan_digest_actual, sql_with_hints=sql_with_hints))
            continue

        r = _make(i, len(all_hint_sets), hints, "PASS", "digest OK",
                  qd_act=query_digest_actual, pd_act=plan_digest_actual)
        results.append(r)

    return results


# ---------------------------------------------------------------------------
# 第二轮：执行时间校验
# ---------------------------------------------------------------------------

def _time_check(
    controller: DBController,
    r: ValidationResult,
    tolerance: float,
    timeout_seconds: Optional[float],
) -> None:
    """对单条 digest PASS 的结果做执行时间校验，原地更新 r 的 time_* 字段。"""
    cache_time = r.cache_execution_time_s

    if cache_time is not None and cache_time < SKIP_TIME_THRESHOLD_S:
        r.time_status = "SKIP"
        r.time_reason = f"cache execution_time={cache_time:.6f}s < {SKIP_TIME_THRESHOLD_S:.1f}s(100ms)"
        return

    if cache_time is None or cache_time <= 0:
        r.time_status = "FAIL"
        r.time_reason = "cache execution_time 非法（<=0 或 NULL）"
        return

    controller.use_db(r.db_name)
    sql_with_hints = _build_sql_with_hints(r._query_text, r._hints)

    # 与 MCTS 保持一致：先设置 JSON v2 格式，再执行 EXPLAIN ANALYZE FORMAT=JSON
    db_utils.set_explain_json_format_v2(controller)
    explain_sql = f"EXPLAIN ANALYZE FORMAT=JSON {sql_with_hints}"

    # warmup_runs=2: DBController 内部先跑 2 次预热，再跑 1 次计时
    hot_time, _ = controller.evaluate_elapsed_time_with_result(
        text(explain_sql),
        warmup_runs=2,
        timeout_seconds=timeout_seconds,
    )
    r.hot_execution_time_s = hot_time

    relative_diff = abs(hot_time - cache_time) / cache_time
    r.relative_diff = relative_diff
    r.sql_with_hints = sql_with_hints
    if relative_diff <= tolerance:
        r.time_status = "PASS"
        r.time_reason = "OK"
    else:
        r.time_status = "FAIL"
        r.time_reason = f"热执行耗时偏差超限: {relative_diff:.2%} > {tolerance:.2%}"


# ---------------------------------------------------------------------------
# 汇总
# ---------------------------------------------------------------------------

def _print_round_summary(title: str, results: list[ValidationResult], field: str) -> None:
    total = len(results)
    passed = sum(1 for r in results if getattr(r, field) == "PASS")
    failed = sum(1 for r in results if getattr(r, field) == "FAIL")
    skipped = sum(1 for r in results if getattr(r, field) == "SKIP")
    reason_field = field.replace("_status", "_reason")

    print(f"\n{'=' * 96}")
    print(f"{title}")
    print(f"{'=' * 96}")
    print(f"total={total}, pass={passed}, fail={failed}, skip={skipped}")

    if failed:
        print(f"\nFailed Details:")
        for r in results:
            if getattr(r, field) != "FAIL":
                continue
            reason = getattr(r, reason_field)
            extra = ""
            if field == "time_status":
                extra = (f", cache={r.cache_execution_time_s}, hot={r.hot_execution_time_s}, "
                         f"diff={f'{r.relative_diff:.2%}' if r.relative_diff is not None else 'N/A'}")
            else:
                extra = (f", qd(exp/act)={r.query_digest_expected}/{r.query_digest_actual}, "
                         f"pd(exp/act)={r.plan_digest_expected}/{r.plan_digest_actual}, sql={r.sql_with_hints}")
            print(f"  - id={r.cache_id}, hint={r.hint_index}/{r.hint_total}, "
                  f"reason={reason}{extra} | {r.hint_preview}")


def _write_csv(results: list[ValidationResult], output_path: str, args: argparse.Namespace) -> None:
    """将所有验证结果写入 CSV，包含足够信息以便复现。"""
    columns = [
        "cache_id",
        "db_name",
        "hint_index",
        "hint_total",
        "hints",
        "query_digest_expected",
        "query_digest_actual",
        "query_digest_match",
        "plan_digest_expected",
        "plan_digest_actual",
        "plan_digest_match",
        "digest_status",
        "digest_reason",
        "cache_execution_time_s",
        "hot_execution_time_s",
        "absolute_diff_s",
        "relative_diff_pct",
        "time_status",
        "time_reason",
        "query_text",
        "sql_with_hints",
    ]

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(columns)
        for r in results:
            qd_match = "YES" if (r.query_digest_actual and r.query_digest_actual == r.query_digest_expected) else "NO"
            pd_match = "YES" if (r.plan_digest_actual and r.plan_digest_actual == r.plan_digest_expected) else "NO"

            cache_t = r.cache_execution_time_s
            hot_t = r.hot_execution_time_s
            abs_diff = abs(hot_t - cache_t) if (hot_t is not None and cache_t is not None) else None
            rel_pct = f"{r.relative_diff * 100:.4f}" if r.relative_diff is not None else ""

            writer.writerow([
                r.cache_id,
                r.db_name,
                r.hint_index,
                r.hint_total,
                r.hint_preview,
                r.query_digest_expected,
                r.query_digest_actual or "",
                qd_match,
                r.plan_digest_expected,
                r.plan_digest_actual or "",
                pd_match,
                r.digest_status,
                r.digest_reason,
                f"{cache_t:.6f}" if cache_t is not None else "",
                f"{hot_t:.6f}" if hot_t is not None else "",
                f"{abs_diff:.6f}" if abs_diff is not None else "",
                rel_pct,
                r.time_status,
                r.time_reason,
                r._query_text,
                r.sql_with_hints or "",
            ])

    print(f"\nCSV saved: {output_path} ({len(results)} rows)")


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Validate query_cache by replaying hinted SQL")
    parser.add_argument("--host", required=True)
    parser.add_argument("--port", type=int, default=3306)
    parser.add_argument("--user", required=True)
    parser.add_argument("--password", default="")
    parser.add_argument("--instance-id", default="cache-validation")

    parser.add_argument("--cache-db", required=True)
    parser.add_argument("--db-name", default=None)
    parser.add_argument("--query-digest", default=None)
    parser.add_argument("--plan-digest", default=None)
    parser.add_argument("--limit", type=int, default=50)

    parser.add_argument("--digest-only", action="store_true", help="只做 digest 校验，跳过第二轮执行时间验证")
    parser.add_argument("--tolerance", type=float, default=0.05)
    parser.add_argument("--timeout-seconds", type=float, default=None)
    parser.add_argument("--output", "-o", default=None,
                        help="CSV 输出路径（默认自动生成: validation_<cache-db>_<timestamp>.csv）")

    args = parser.parse_args()

    cfg = InstanceConfig(
        instance_id=args.instance_id,
        ip=args.host,
        port=args.port,
        user=args.user,
        password=args.password,
        read_only=False,
        with_ai_marker=False,
        allow_reconnect=True,
    )

    # 先用普通连接检测 feature flags，再以 training 模式重建 controller
    # 与 MCTS 训练环境保持一致：禁用 SPM / Statement Outline，避免 plan 被干预
    probe = DBController(cfg)
    try:
        feature_flags = detect_features(probe)
        print(f"Detected feature_flags: {feature_flags}")
    finally:
        probe.close()

    controller = DBController(cfg, is_training_env=True, feature_flags=feature_flags)

    try:
        rows = _fetch_rows(
            controller=controller,
            cache_db=args.cache_db,
            db_name=args.db_name,
            query_digest=args.query_digest,
            plan_digest=args.plan_digest,
            limit=args.limit,
        )
        if not rows:
            print("No cache rows found with current filters.")
            return

        print(f"Loaded {len(rows)} rows from `{args.cache_db}`.`query_cache`.\n")

        # =====================================================================
        # 第一轮：Digest 校验
        # =====================================================================
        print("=" * 96)
        print("Round 1: Digest Validation")
        print("=" * 96)

        all_results: list[ValidationResult] = []
        for idx, row in enumerate(rows, start=1):
            cache_id, db_name, _, qd, pd = row[0], row[1], row[2], row[3], row[4]
            print(f"\n[{idx}/{len(rows)}] id={cache_id}, db={db_name}, qd={qd}, pd={pd}")
            try:
                row_results = _digest_check_row(controller, row)
                all_results.extend(row_results)
                for r in row_results:
                    print(f"  -> [{r.digest_status}] hint {r.hint_index}/{r.hint_total}: "
                          f"{r.digest_reason} | {r.hint_preview}")
            except Exception as e:
                print(f"  -> [FAIL] exception: {type(e).__name__}: {e}")
                print(_indent(traceback.format_exc(), prefix="    "))
                all_results.append(ValidationResult(
                    cache_id=int(cache_id), db_name=str(db_name),
                    hint_index=1, hint_total=1, hint_preview="(exception)",
                    query_digest_expected=str(qd), plan_digest_expected=str(pd),
                    cache_execution_time_s=float(row[5]) if row[5] is not None else None,
                    digest_status="FAIL",
                    digest_reason=f"exception: {type(e).__name__}: {e}",
                ))

        _print_round_summary("Round 1 Summary: Digest Validation", all_results, "digest_status")

        if args.digest_only:
            print("\n--digest-only: 跳过第二轮执行时间验证。")
            csv_path = args.output or f"validation_{args.cache_db}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            _write_csv(all_results, csv_path, args)
            return

        # =====================================================================
        # 第二轮：执行时间校验（仅 digest PASS 的条目）
        # =====================================================================
        candidates = [r for r in all_results if r.digest_status == "PASS"]
        if not candidates:
            print("\nNo digest-PASS entries, skipping Round 2.")
            csv_path = args.output or f"validation_{args.cache_db}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            _write_csv(all_results, csv_path, args)
            return

        print(f"\n\n{'=' * 96}")
        print(f"Round 2: Execution Time Validation  (warmup_runs=2, tolerance={args.tolerance:.0%})")
        print(f"{'=' * 96}")
        print(f"Candidates: {len(candidates)} (digest PASS)")
        print(f"Skip rule: cache execution_time < {SKIP_TIME_THRESHOLD_S * 1000:.0f}ms\n")

        for idx, r in enumerate(candidates, start=1):
            print(f"[{idx}/{len(candidates)}] id={r.cache_id}, db={r.db_name}, "
                  f"hint {r.hint_index}/{r.hint_total}, cache_time={r.cache_execution_time_s}")
            try:
                _time_check(controller, r, tolerance=args.tolerance, timeout_seconds=args.timeout_seconds)
                print(f"  -> [{r.time_status}] {r.time_reason}"
                      + (f" (hot={r.hot_execution_time_s:.6f}s)" if r.hot_execution_time_s is not None else ""))
            except Exception as e:
                r.time_status = "FAIL"
                r.time_reason = f"exception: {type(e).__name__}: {e}"
                print(f"  -> [FAIL] {r.time_reason}")
                print(_indent(traceback.format_exc(), prefix="    "))

        _print_round_summary("Round 2 Summary: Execution Time Validation", candidates, "time_status")

        # 输出 CSV（包含所有记录，不只是 candidates）
        csv_path = args.output or f"validation_{args.cache_db}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        _write_csv(all_results, csv_path, args)

    finally:
        controller.close()


if __name__ == "__main__":
    main()