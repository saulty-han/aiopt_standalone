#!/usr/bin/env python3
"""
Benchmark Core Set 采样脚本 (Instance-Aware)

三阶段策略：
  Phase 1: 贪心集合覆盖 — 用最少 instance 覆盖全部 bucket
  Phase 2: 稀疏补充 — 为极稀疏 bucket 追加 instance
  Phase 3: 分层配额采样 — 从选定 instance 池中采出 1000 条

用法:
  python3 benchmark_sampler.py \
    --input  数据/benchmark_basic.json \
    --output benchmark_v2/core_set_1000.json \
    --target 1000 \
    --min-per-bucket 3 \
    --min-digest-per-bucket 5 \
    --seed 42 \
    --source CDB          # 可选，限制只从指定 source 中选取
"""

import argparse
import json
import math
import random
from collections import Counter, defaultdict
from pathlib import Path


# ──────────────────────────────────────────────
# Difficulty scoring
# ──────────────────────────────────────────────
def difficulty_score(r):
    """D = 1.0 * qb_bucket + 1.5 * join_bucket + 1.0 * hint_bucket"""
    return 1.0 * r["qb_bucket"] + 1.5 * r["join_bucket"] + 1.0 * r["hint_bucket"]


def difficulty_level(d):
    if d <= 5.0:
        return "L1-Easy"
    elif d <= 8.0:
        return "L2-Medium"
    elif d <= 11.0:
        return "L3-Hard"
    else:
        return "L4-Expert"


# ──────────────────────────────────────────────
# Phase 1: 贪心集合覆盖
# ──────────────────────────────────────────────
def greedy_set_cover(inst_buckets, inst_digests, all_buckets):
    """返回覆盖全部 bucket 所需的最少 instance 列表"""
    remaining = set(all_buckets)
    chosen = []
    available = set(inst_buckets.keys())

    while remaining and available:
        # 选覆盖最多未覆盖 bucket 的 instance，平局选 digest 多的
        best = max(
            available,
            key=lambda x: (
                len(inst_buckets[x] & remaining),
                len(inst_digests[x]),
            ),
        )
        new_covered = inst_buckets[best] & remaining
        if not new_covered:
            break
        chosen.append(
            {
                "instance_id": best,
                "phase": 1,
                "new_buckets_covered": len(new_covered),
            }
        )
        remaining -= new_covered
        available.remove(best)

    if remaining:
        print(f"⚠️  Warning: {len(remaining)} buckets not covered: {remaining}")

    return chosen


# ──────────────────────────────────────────────
# Phase 2: 稀疏 bucket 补充
# ──────────────────────────────────────────────
def supplement_sparse_buckets(
    chosen_ids, inst_records, inst_buckets, inst_digests, all_buckets,
    min_digest_per_bucket=5,
):
    """为 digest 不足的 bucket 追加 instance"""
    chosen_set = set(chosen_ids)

    # 计算当前每个 bucket 的 digest 池
    bucket_pool = defaultdict(set)
    for iid in chosen_set:
        for r in inst_records[iid]:
            bucket_pool[r["bucket_id"]].add(r["query_digest"])

    supplements = []
    available = set(inst_records.keys()) - chosen_set

    for b in sorted(all_buckets):
        current = len(bucket_pool.get(b, set()))
        if current >= min_digest_per_bucket:
            continue

        # 找能为此 bucket 贡献最多记录的 instance
        candidates = [
            (iid, len([r for r in inst_records[iid] if r["bucket_id"] == b]))
            for iid in available
            if b in inst_buckets[iid]
        ]
        if not candidates:
            continue

        candidates.sort(key=lambda x: -x[1])
        best_iid, cnt = candidates[0]

        chosen_set.add(best_iid)
        available.remove(best_iid)
        supplements.append(
            {
                "instance_id": best_iid,
                "phase": 2,
                "target_bucket": b,
                "records_added": cnt,
            }
        )
        # 更新 bucket pool
        for r in inst_records[best_iid]:
            bucket_pool[r["bucket_id"]].add(r["query_digest"])

    return supplements, bucket_pool


# ──────────────────────────────────────────────
# Phase 2.5: 池扩展 — 确保采样比例健康
# ──────────────────────────────────────────────
def expand_pool_for_target(
    chosen_ids, inst_records, inst_buckets, inst_digests, bucket_pool,
    target, min_per_bucket, max_exhaustion_rate=0.20,
):
    """
    当 target 较大时，自动追加 instance 使得被 100% 耗尽的 bucket 比例
    不超过 max_exhaustion_rate（默认 20%，即极稀疏的 bucket 除外）。

    返回 (expansions_list, updated_bucket_pool)
    """
    chosen_set = set(chosen_ids)
    available = set(inst_records.keys()) - chosen_set
    chosen_digests = set()
    for iid in chosen_set:
        chosen_digests |= inst_digests.get(iid, set())

    all_buckets = set(bucket_pool.keys())
    # 先算一下有多少 bucket 是"天然稀疏"的（全量 digest < min_per_bucket*2）
    # 这些 bucket 怎么加 instance 也填不满，不计入 exhaustion 判断
    global_bucket_digest = defaultdict(set)
    for iid in inst_records:
        for r in inst_records[iid]:
            global_bucket_digest[r["bucket_id"]].add(r["query_digest"])
    naturally_sparse = {b for b in all_buckets if len(global_bucket_digest[b]) < min_per_bucket * 2}

    def count_exhausted(bp):
        import math
        sqrt_sizes = {b: math.sqrt(len(d)) for b, d in bp.items()}
        total_sqrt = sum(sqrt_sizes.values())
        reserved = min_per_bucket * len(bp)
        rem = target - reserved
        exhausted = 0
        for b, digests in bp.items():
            if b in naturally_sparse:
                continue
            pool = len(digests)
            prop = int(rem * sqrt_sizes[b] / total_sqrt)
            ideal = min_per_bucket + prop
            if min(ideal, pool) >= pool:
                exhausted += 1
        non_sparse = len(bp) - len(naturally_sparse)
        return exhausted, non_sparse

    exhausted, non_sparse = count_exhausted(bucket_pool)
    current_rate = exhausted / non_sparse if non_sparse > 0 else 0

    if current_rate <= max_exhaustion_rate:
        return [], bucket_pool

    expansions = []
    max_rounds = 200  # safety limit

    for _ in range(max_rounds):
        if not available:
            break
        exhausted, non_sparse = count_exhausted(bucket_pool)
        current_rate = exhausted / non_sparse if non_sparse > 0 else 0
        if current_rate <= max_exhaustion_rate:
            break

        # 选贡献最多新 digest 的 instance
        best = max(available, key=lambda x: len(inst_digests[x] - chosen_digests))
        new_d = len(inst_digests[best] - chosen_digests)
        if new_d == 0:
            break

        chosen_set.add(best)
        available.remove(best)
        chosen_digests |= inst_digests[best]
        expansions.append(
            {
                "instance_id": best,
                "phase": 2.5,
                "new_digests": new_d,
            }
        )
        for r in inst_records[best]:
            bucket_pool[r["bucket_id"]].add(r["query_digest"])

    return expansions, bucket_pool


# ──────────────────────────────────────────────
# Phase 3: 分层配额采样
# ──────────────────────────────────────────────
def compute_quotas(bucket_pool, target, min_per_bucket):
    """平方根折中配额分配"""
    sqrt_sizes = {}
    for b, digests in bucket_pool.items():
        sqrt_sizes[b] = math.sqrt(len(digests))

    total_sqrt = sum(sqrt_sizes.values())
    quotas = {}

    # 先分配保底
    reserved = min_per_bucket * len(bucket_pool)
    remaining_target = target - reserved

    for b in bucket_pool:
        pool_size = len(bucket_pool[b])
        proportional = int(remaining_target * sqrt_sizes[b] / total_sqrt)
        quota = min_per_bucket + proportional
        # 不能超过池子大小
        quota = min(quota, pool_size)
        # 至少保底
        quota = max(quota, min(min_per_bucket, pool_size))
        quotas[b] = quota

    # 如果总量偏离 target，调整
    total = sum(quotas.values())
    if total < target:
        # 从大 bucket 中增加
        for b in sorted(bucket_pool.keys(), key=lambda x: len(bucket_pool[x]), reverse=True):
            if total >= target:
                break
            can_add = len(bucket_pool[b]) - quotas[b]
            add = min(can_add, target - total)
            quotas[b] += add
            total += add
    elif total > target:
        # 从大 bucket 中减少
        for b in sorted(bucket_pool.keys(), key=lambda x: quotas[x], reverse=True):
            if total <= target:
                break
            can_remove = quotas[b] - min(min_per_bucket, len(bucket_pool[b]))
            remove = min(can_remove, total - target)
            quotas[b] -= remove
            total -= remove

    return quotas


def sample_from_bucket(records, quota, rng):
    """
    从一个 bucket 的记录中采样 quota 条
    规则: digest 去重 → 优先覆盖不同 pattern_label → 随机
    """
    # 按 digest 去重，保留 baseline_time 最大的（更有优化价值）
    digest_best = {}
    for r in records:
        d = r["query_digest"]
        if d not in digest_best or r["baseline_time"] > digest_best[d]["baseline_time"]:
            digest_best[d] = r

    pool = list(digest_best.values())
    if len(pool) <= quota:
        return pool

    # 分 pattern_label 组
    pattern_groups = defaultdict(list)
    for r in pool:
        pattern_groups[r.get("pattern_label", "unknown")].append(r)

    selected = []
    selected_digests = set()

    # 第一轮: 每个 pattern 至少选 1 条
    for pattern, group in pattern_groups.items():
        if len(selected) >= quota:
            break
        pick = rng.choice(group)
        selected.append(pick)
        selected_digests.add(pick["query_digest"])

    # 第二轮: 轮转各 pattern 补充
    remaining_quota = quota - len(selected)
    if remaining_quota > 0:
        remaining_pool = [r for r in pool if r["query_digest"] not in selected_digests]
        rng.shuffle(remaining_pool)
        selected.extend(remaining_pool[:remaining_quota])

    return selected[:quota]


# ──────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="Benchmark Core Set Sampler")
    parser.add_argument("--input", required=True, help="Path to benchmark_basic.json")
    parser.add_argument("--output", required=True, help="Output path for sampled benchmark")
    parser.add_argument("--target", type=int, default=1000)
    parser.add_argument("--min-per-bucket", type=int, default=3)
    parser.add_argument("--min-digest-per-bucket", type=int, default=5)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument(
        "--source", type=str, default=None,
        help="Filter by source, e.g. 'CDB' or 'NCDB'. If not set, use all sources.",
    )
    args = parser.parse_args()

    rng = random.Random(args.seed)

    # ── Load data ──
    print(f"Loading {args.input} ...")
    with open(args.input) as f:
        data = json.load(f)
    print(f"  Total records: {len(data)}")

    # ── Source filter ──
    if args.source:
        before = len(data)
        data = [r for r in data if r.get("source") == args.source]
        print(f"  Filtered by source='{args.source}': {before} → {len(data)} records")
        if not data:
            print(f"❌ No records with source='{args.source}', exiting.")
            return

    # ── Build indexes ──
    inst_records = defaultdict(list)
    inst_buckets = defaultdict(set)
    inst_digests = defaultdict(set)
    all_buckets = set()

    for r in data:
        iid = r["instance_id"]
        bid = r["bucket_id"]
        inst_records[iid].append(r)
        inst_buckets[iid].add(bid)
        inst_digests[iid].add(r["query_digest"])
        all_buckets.add(bid)

    print(f"  Instances: {len(inst_records)}")
    print(f"  Buckets: {len(all_buckets)}")
    print(f"  Unique digests: {len(set(r['query_digest'] for r in data))}")

    # ── Phase 1: Greedy Set Cover ──
    print("\n═══ Phase 1: Greedy Set Cover ═══")
    phase1 = greedy_set_cover(inst_buckets, inst_digests, all_buckets)
    chosen_ids = [e["instance_id"] for e in phase1]
    print(f"  Selected {len(phase1)} instances to cover all {len(all_buckets)} buckets:")
    for e in phase1:
        print(f"    {e['instance_id'][:20]}... +{e['new_buckets_covered']} buckets")

    # ── Phase 2: Supplement Sparse Buckets ──
    print(f"\n═══ Phase 2: Supplement Sparse Buckets (min_digest={args.min_digest_per_bucket}) ═══")
    phase2, bucket_pool = supplement_sparse_buckets(
        chosen_ids, inst_records, inst_buckets, inst_digests, all_buckets,
        min_digest_per_bucket=args.min_digest_per_bucket,
    )
    chosen_ids.extend(e["instance_id"] for e in phase2)
    print(f"  Added {len(phase2)} instances:")
    for e in phase2:
        print(f"    {e['instance_id'][:20]}... for bucket {e['target_bucket']} (+{e['records_added']} records)")

    total_pool = sum(len(d) for d in bucket_pool.values())
    print(f"\n  Instance count after Phase 2: {len(chosen_ids)}")
    print(f"  Total digest pool: {total_pool}")

    # ── Phase 2.5: Pool Expansion (if target is large) ──
    print(f"\n═══ Phase 2.5: Pool Expansion (target={args.target}) ═══")
    phase2_5, bucket_pool = expand_pool_for_target(
        chosen_ids, inst_records, inst_buckets, inst_digests, bucket_pool,
        target=args.target, min_per_bucket=args.min_per_bucket,
        max_exhaustion_rate=0.20,
    )
    chosen_ids.extend(e["instance_id"] for e in phase2_5)
    if phase2_5:
        total_pool = sum(len(d) for d in bucket_pool.values())
        print(f"  Expanded by {len(phase2_5)} instances")
        print(f"  Final instance count: {len(chosen_ids)}")
        print(f"  Total digest pool: {total_pool}")
        for e in phase2_5[:10]:
            print(f"    {e['instance_id'][:20]}... +{e['new_digests']} new digests")
        if len(phase2_5) > 10:
            print(f"    ... and {len(phase2_5) - 10} more")
    else:
        print(f"  No expansion needed — pool is sufficient for target={args.target}")

    # ── Phase 3: Quota Sampling ──
    print(f"\n═══ Phase 3: Quota Sampling (target={args.target}) ═══")
    quotas = compute_quotas(bucket_pool, args.target, args.min_per_bucket)

    # 汇总每个 bucket 在选定 instance 中的记录
    chosen_set = set(chosen_ids)
    bucket_records = defaultdict(list)
    for iid in chosen_set:
        for r in inst_records[iid]:
            bucket_records[r["bucket_id"]].append(r)

    # 采样
    sampled = []
    for b in sorted(all_buckets):
        quota = quotas.get(b, 0)
        records = bucket_records.get(b, [])
        picked = sample_from_bucket(records, quota, rng)
        sampled.extend(picked)
        pool_size = len(bucket_pool.get(b, set()))
        print(f"  {b}: pool={pool_size:>5}, quota={quota:>4}, sampled={len(picked):>4}")

    # 全局 digest 去重
    seen_digests = set()
    deduped = []
    for r in sampled:
        if r["query_digest"] not in seen_digests:
            seen_digests.add(r["query_digest"])
            deduped.append(r)
    print(f"\n  Before global dedup: {len(sampled)}")
    print(f"  After global dedup:  {len(deduped)}")

    # 添加 benchmark 元数据
    for i, r in enumerate(deduped):
        d = difficulty_score(r)
        r["benchmark_id"] = f"BM-v2-{i+1:04d}"
        r["difficulty_score"] = d
        r["difficulty_level"] = difficulty_level(d)

    # ── 统计报告 ──
    print(f"\n═══ Final Benchmark Summary ═══")
    print(f"  Total queries: {len(deduped)}")
    print(f"  Instances used: {len(chosen_set)}")

    level_counts = Counter(r["difficulty_level"] for r in deduped)
    for level in ["L1-Easy", "L2-Medium", "L3-Hard", "L4-Expert"]:
        cnt = level_counts.get(level, 0)
        print(f"  {level}: {cnt} ({cnt/len(deduped)*100:.1f}%)")

    pattern_counts = Counter(r.get("pattern_label", "unknown") for r in deduped)
    print(f"\n  Pattern distribution:")
    for p, cnt in pattern_counts.most_common():
        print(f"    {p}: {cnt}")

    instance_counts = Counter(r["instance_id"] for r in deduped)
    print(f"\n  Queries per instance (top 10):")
    for iid, cnt in instance_counts.most_common(10):
        print(f"    {iid[:20]}...: {cnt}")

    # ── Write output ──
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w") as f:
        json.dump(deduped, f, ensure_ascii=False, indent=2)
    print(f"\n✅ Benchmark written to {output_path} ({len(deduped)} queries)")

    # Write instance manifest (filename derived from output)
    stem = output_path.stem  # e.g. "core_set_3000_cdb"
    manifest_path = output_path.parent / f"instance_manifest_{stem}.json"
    manifest = {
        "version": "v2.0",
        "source_filter": args.source,
        "seed": args.seed,
        "total_instances": len(chosen_ids),
        "instances": [],
    }
    for e in phase1 + phase2 + phase2_5:
        iid = e["instance_id"]
        queries_sampled = instance_counts.get(iid, 0)
        manifest["instances"].append(
            {
                "instance_id": iid,
                "phase": e["phase"],
                "queries_sampled": queries_sampled,
                "total_records_in_source": len(inst_records[iid]),
                "buckets_covered": sorted(inst_buckets[iid]),
            }
        )
    with open(manifest_path, "w") as f:
        json.dump(manifest, f, ensure_ascii=False, indent=2)
    print(f"✅ Instance manifest written to {manifest_path}")

    # Write sampling metadata
    meta_path = output_path.parent / f"sampling_metadata_{stem}.json"
    meta = {
        "version": "v2.0",
        "source_filter": args.source,
        "target": args.target,
        "actual": len(deduped),
        "seed": args.seed,
        "min_per_bucket": args.min_per_bucket,
        "min_digest_per_bucket": args.min_digest_per_bucket,
        "total_source_records": len(data),
        "total_instances_used": len(chosen_ids),
        "phase1_instances": len(phase1),
        "phase2_instances": len(phase2),
        "phase2_5_instances": len(phase2_5),
        "quotas": {b: quotas.get(b, 0) for b in sorted(all_buckets)},
        "bucket_pool_sizes": {b: len(bucket_pool.get(b, set())) for b in sorted(all_buckets)},
        "difficulty_distribution": dict(level_counts),
        "pattern_distribution": dict(pattern_counts),
    }
    with open(meta_path, "w") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)
    print(f"✅ Sampling metadata written to {meta_path}")


if __name__ == "__main__":
    main()
