"""
统一 E2E Workload 定义

合并所有 E2E 测试使用的 workload，作为基准测试集。
本地测试统一使用 awr_table 数据源（由 instances_config 配置）。
"""

# ============================================================================
# 统一 Workload 列表
# ============================================================================
#
# 选择标准：
# 1. OPTIMIZE 场景：能稳定找到 common better plan
# 2. RESET 场景：无更优 plan 或 candidate=default
# 3. ERROR 场景：覆盖 Outline Data 解析失败、表不存在等异常
#
# 以下 workloads 同时适用于 SPM 和 Statement Outline 测试
#

UNIFIED_WORKLOADS = [
    # ============ 正常路径: OPTIMIZE ============
    # 1. [OPTIMIZE ✓] TPC-H Q14 - 2表 JOIN, 稳定优化
    {
        "db": "tpch",
        "digest": "4bbbfdeedb09cef1cb34645654bfdee9ee0b70c1b564e82fe4583ee973089554",
        "note": "TPC-H Q14 (promo_revenue): OPTIMIZE 稳定"
    },
    # 2. [OPTIMIZE/RESET] TPC-H Q18 - 3表 JOIN + IN 子查询
    {
        "db": "tpch",
        "digest": "5f9a6a79b03e97f494dd3383a07a7f049d7001c832eb59ff24b6b82d01c8ca6b",
        "note": "TPC-H Q18 (large volume customer): OPTIMIZE/RESET 波动"
    },
    # 3. [OPTIMIZE ✓] TPC-H Q16 - NOT IN 子查询
    {
        "db": "tpch",
        "digest": "a82f6491d50efb74de7c43b025281fd70234e1964875d5e0a1cc66f10941e188",
        "note": "TPC-H Q16 (part supplier): OPTIMIZE 稳定"
    },
    
    # ============ 正常路径: RESET ============
    # 4. [RESET] TPC-H Q22 - 子查询 + EXISTS
    {
        "db": "tpch",
        "digest": "11702e7a690befeea6c92689719676fd6d044ad6343cdcd9fab1c3c8a90498fc",
        "note": "TPC-H Q22: RESET (candidate = default)"
    },
    # 5. [RESET/ERROR] TPC-H Q1 变体 - Outline 不稳定
    {
        "db": "tpch",
        "digest": "00be081c3b40ef6573fc131d47f0e7e498367f37af565d3f4b8ba1d6e77851fd",
        "note": "TPC-H Q1 变体: RESET/ERROR (Unstable Outline 不稳定)"
    },
    
    # ============ 异常路径: ERROR ============
    # 6. [ERROR] INFORMATION_SCHEMA 查询
    {
        "db": "tpch",
        "digest": "a9971bbbfe401fb5c32b3ec7e966a3160fbf00717bbb24365224a858d422c7bb",
        "note": "ERROR: Outline Data not found (INFORMATION_SCHEMA)"
    },
    # 7. [ERROR] TPC-H Q15 - 临时视图 revenue0
    {
        "db": "tpch",
        "digest": "a2572bff60af956f6d960263440d5ed752f1f38b6a10bebe6c9eb43870f57562",
        "note": "ERROR: Table doesn't exist (Q15 临时视图)"
    },
    
    # ============ Statement Outline 专用 ============
    # 8. SQL - 单候选 BETTER, 性能显著提升
    {
        "db": "tpch",
        "digest": "c460b79b15e76383d90f6ff5a1f809b0a8181d341c50c4d086ffe244c60e4ae9",
        "note": "OPTIMIZE: 单候选 3x 性能提升"
    },
]


def get_workload_tuples() -> list[tuple[str, str]]:
    """返回 (db, digest) 元组列表，用于增量训练"""
    return [(w["db"], w["digest"]) for w in UNIFIED_WORKLOADS]
