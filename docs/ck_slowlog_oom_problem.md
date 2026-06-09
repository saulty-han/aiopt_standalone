# ClickHouse Slowlog 查询 OOM 问题定义

本文档仅记录观测到的现象，不做根因推测或解决方案建议。

---

## 1. 环境信息

### 表结构

```
表名:             cdblog.slowlog (Distributed)
底层物理表:       slowlog_physical (ReplicatedMergeTree)
PARTITION BY:     toDate(log_timestamp)
ORDER BY:         (instid, insttype, log_timestamp)
TTL:              log_timestamp + 31 天
index_granularity: 8192
集群:             3 shard × 2 replica (default_cluster)
CK 版本:          22.3.10, revision 54455
```

### 列大小（单节点）

| 指标 | 值 |
|---|---|
| `sql_raw_text` 压缩大小 | 2,253 GB |
| `sql_raw_text` 未压缩大小 | 8,274 GB |
| `sql_raw_text` 平均长度 | ~660 字节/行 |
| 本地表总行数 | ~5,734,888,033 (57 亿) |
| 本地表总 parts | 290 |
| 本地表总 granules | 1,023,935 |

### 各 shard 全表行数（非 tencentroot, 30 天窗口）

| Shard | 行数 |
|---|---|
| shard 1 | 964,653,786 (~9.6 亿) |
| shard 2 | 916,982,402 (~9.2 亿) |
| shard 3 | 1,175,013,945 (~11.8 亿) |

### 查询内存限制

```
单查询最大内存: 9.31 GiB (max_memory_usage)
```

---

## 2. 业务查询 SQL

所有测试使用同一条业务 SQL 模板（仅 instid 不同）：

```sql
SELECT
    database,
    argMin(sql_raw_text, query_time) AS sql_text,
    argMin(sql_raw_text, query_time) AS sql_text_min,
    argMax(sql_raw_text, query_time) AS sql_text_max,
    COUNT(*) AS count_star,
    AVG(query_time) AS elapsed_time_avg,
    MIN(query_time) AS elapsed_time_min,
    MAX(query_time) AS elapsed_time_max,
    MAX(start_time) AS last_start_time,
    md5
FROM cdblog.slowlog
WHERE user_name != 'tencentroot'
  AND instid = '<INST_ID>'
  AND insttype = 'master'
  AND timestamp >= toDate(now()) - 30
  AND query_time >= 0.1
  AND lower(sql_raw_text) LIKE '%select%'
  AND length(sql_raw_text) < 10176
GROUP BY database, md5
```

PREWHERE 变体将 `instid` 和 `insttype` 条件从 WHERE 移到显式 PREWHERE：

```sql
SELECT ...
FROM cdblog.slowlog
PREWHERE instid = '<INST_ID>' AND insttype = 'master'
WHERE user_name != 'tencentroot'
  AND timestamp >= toDate(now()) - 30
  AND query_time >= 0.1
  AND lower(sql_raw_text) LIKE '%select%'
  AND length(sql_raw_text) < 10176
GROUP BY database, md5
```

---

## 3. 三个实例的测试结果

### 实例 A: `e16d610b-0366-11f0-920b-b8cef65bf162`（小实例）

**实例特征:**

| 指标 | 值 |
|---|---|
| shard 1 行数 | 9,554 |
| shard 2 行数 | 6,731 |
| shard 3 行数 | 8,268 |
| (database, md5) 组数 | 18 |
| 主键裁剪后 granules | 261 |

**测试结果:**

| 查询方式 | 结果 |
|---|---|
| 原始 SQL（无 PREWHERE） | **OOM** |
| 显式 PREWHERE instid + insttype | **OK**, 15 行 |
| 显式 PREWHERE instid（单字段） | **OK**, 15 行 |
| 显式 PREWHERE instid（本地表） | **OK**, 18 行 |

**OOM 错误信息:**
```
Code: 241. DB::Exception: Memory limit (for query) exceeded:
would use 9.43 GiB (attempt to allocate chunk of 536870912 bytes),
maximum: 9.31 GiB:
(while reading column sql_raw_text):
(while reading from part .../20260227_0_50396_116/ from mark 22471
with max_rows_to_read = 4031):
While executing MergeTreeThread. (MEMORY_LIMIT_EXCEEDED)
```

---

### 实例 B: `82225293-c674-11ef-8d21-b8cef6b7828c`（小实例）

**测试结果:**

| 查询方式 | 结果 |
|---|---|
| 原始 SQL（无 PREWHERE） | **OK**（未触发 OOM） |

此实例数据量较小，原始 SQL 即可正常执行。

---

### 实例 C: `84a62014-358e-11ee-9824-6c0b84d53ec0`（大实例）

**测试结果:**

| 查询方式 | 结果 | Processed rows | Processed bytes | Peak memory |
|---|---|---|---|---|
| 原始 SQL（无 PREWHERE） | **OOM** | 1.20M | 6.91 GB | 9.64 GiB |
| 显式 PREWHERE（第一次） | **OOM** | 1.32M | 6.31 GB | 9.74 GiB |
| 显式 PREWHERE（第二次） | **OOM** | 1.55M | 9.08 GB | 9.43 GiB |

**无 PREWHERE 错误信息** (Query id: `0f7c3584-49d2-4132-9577-b01acc72eac4`):
```
Elapsed: 1.694 sec. Processed 1.20 million rows, 6.91 GB
(705.31 thousand rows/s., 4.08 GB/s.)

Code: 241. DB::Exception: Received from 11.149.254.38:9000.
DB::Exception: Memory limit (for query) exceeded:
would use 9.64 GiB (attempt to allocate chunk of 268435456 bytes),
maximum: 9.31 GiB:
(while reading column sql_raw_text):
(while reading from part .../20260309_0_29521_42/ from mark 7966
with max_rows_to_read = 7362):
While executing MergeTreeThread. (MEMORY_LIMIT_EXCEEDED)
```

**有 PREWHERE 错误信息** (Query id: `d44f25cd-e826-4f88-8888-7f8a83193f7e`):
```
Elapsed: 4.172 sec. Processed 1.55 million rows, 9.08 GB
(371.53 thousand rows/s., 2.18 GB/s.)

Code: 241. DB::Exception: Received from 11.149.254.38:9000.
DB::Exception: Memory limit (for query) exceeded:
would use 9.43 GiB (attempt to allocate chunk of 268435456 bytes),
maximum: 9.31 GiB:
(while reading column sql_raw_text):
(while reading from part .../20260308_0_47506_24/ from mark 12297
with max_rows_to_read = 6879):
While executing MergeTreeThread. (MEMORY_LIMIT_EXCEEDED)
```

---

## 4. 现象总结

| 实例 | 量级 | 无 PREWHERE | 有 PREWHERE |
|---|---|---|---|
| `e16d610b` | ~7K 行 | OOM | OK |
| `82225293` | 小 | OK | — |
| `84a62014` | ~1.5M 行 | OOM | OOM |

所有 OOM 均发生在 `while reading column sql_raw_text` 阶段，错误码均为 `241 (MEMORY_LIMIT_EXCEEDED)`，峰值内存在 9.4–9.7 GiB 之间，超过 9.31 GiB 限制。

---

## 5. 可直接复现的 SQL

连接目标: `11.149.254.38:9000`，用户 `default`。

### 实例 A — 小实例 OOM（PREWHERE 可解）

```sql
SELECT
    database,
    argMin(sql_raw_text, query_time) AS sql_text,
    argMin(sql_raw_text, query_time) AS sql_text_min,
    argMax(sql_raw_text, query_time) AS sql_text_max,
    COUNT(*) AS count_star,
    AVG(query_time) AS elapsed_time_avg,
    MIN(query_time) AS elapsed_time_min,
    MAX(query_time) AS elapsed_time_max,
    MAX(start_time) AS last_start_time,
    md5
FROM cdblog.slowlog
WHERE user_name != 'tencentroot'
  AND instid = 'e16d610b-0366-11f0-920b-b8cef65bf162'
  AND insttype = 'master'
  AND timestamp >= toDate(now()) - 30
  AND query_time >= 0.1
  AND lower(sql_raw_text) LIKE '%select%'
  AND length(sql_raw_text) < 10176
GROUP BY database, md5;
```

### 实例 C — 大实例 OOM（PREWHERE 仍然 OOM）

```sql
SELECT
    database,
    argMin(sql_raw_text, query_time) AS sql_text,
    argMin(sql_raw_text, query_time) AS sql_text_min,
    argMax(sql_raw_text, query_time) AS sql_text_max,
    COUNT(*) AS count_star,
    AVG(query_time) AS elapsed_time_avg,
    MIN(query_time) AS elapsed_time_min,
    MAX(query_time) AS elapsed_time_max,
    MAX(start_time) AS last_start_time,
    md5
FROM cdblog.slowlog
WHERE user_name != 'tencentroot'
  AND instid = '84a62014-358e-11ee-9824-6c0b84d53ec0'
  AND insttype = 'master'
  AND timestamp >= toDate(now()) - 30
  AND query_time >= 0.1
  AND lower(sql_raw_text) LIKE '%select%'
  AND length(sql_raw_text) < 10176
GROUP BY database, md5;
```

---

## 6. 方案对比实验

验证脚本: `scripts/test_ck_oom_solutions.py`

### 测试方案说明

| # | 方案 | SQL 改动 |
|---|---|---|
| 0 | 基线 | 完全不读 `sql_raw_text`，只聚合数值列 |
| 1 | 原始 SQL | 对照组，与业务 SQL 一致 |
| 2 | `any()` 无 PREWHERE | 用 `any(sql_raw_text)` 替代 `argMin/argMax`，去掉 `sql_raw_text` 过滤条件，不加 PREWHERE |
| 3 | `any()` + PREWHERE | 同上 + 显式 `PREWHERE instid AND insttype` |
| 4 | 两步查询 | 第一步聚合不读 `sql_raw_text`；第二步用 `(database, md5) IN (...)` + PREWHERE 点查 `any(sql_raw_text)` |
| 5 | `argMin/argMax` + PREWHERE | 保留原始聚合函数，去掉 `sql_raw_text` 过滤条件，加 PREWHERE |

### 实例 A（小实例）结果

| 方案 | 结果 | 耗时 |
|---|---|---|
| 0. 基线(不读sql_raw_text) | **OK** (18行) | 0.16s |
| 1. 原始SQL | **OOM** | 1.75s |
| 2. any()无PREWHERE | **OOM** | 1.61s |
| 3. any()+PREWHERE | **OK** (18行) | 1.24s |
| 4. 两步查询 | **OK** (15行) | 1.31s |
| 5. argMin/Max+PREWHERE | **OK** (18行) | 1.06s |

### 实例 C（大实例）结果

| 方案 | 结果 | 耗时 |
|---|---|---|
| 0. 基线(不读sql_raw_text) | **OK** (27行) | 0.14s |
| 1. 原始SQL | **OOM** | 2.35s |
| 2. any()无PREWHERE | **OOM** | 2.28s |
| 3. any()+PREWHERE | **OOM** | 2.74s |
| 4. 两步查询(第二步OOM) | **OOM** | 3.15s |
| 5. argMin/Max+PREWHERE | **OOM** | 2.04s |

### 实验结论

1. **`any()` 替换 `argMin/argMax` 无效。** 方案 2 对两个实例均 OOM，与原始 SQL 表现一致。OOM 发生在 MergeTreeThread 列读取阶段，聚合函数类型不影响该阶段的内存占用。

2. **去掉 `sql_raw_text` 过滤条件无效。** 即使 WHERE 中不再引用 `sql_raw_text`（方案 2/3/5），只要 SELECT 聚合中引用了 `sql_raw_text`，CK 22.3 仍然在 MergeTreeThread 扫描阶段对所有 granule 解压 `sql_raw_text`，不会延迟到聚合阶段再读取。

3. **PREWHERE 只对小实例有效。** 小实例匹配行 ~7K，分布在少量 granule 中，PREWHERE 能有效裁剪；大实例匹配行 ~1.5M，覆盖大量 granule，PREWHERE 裁剪后仍然需要解压过多 `sql_raw_text` 数据。

4. **两步查询的第二步对大实例仍 OOM。** 第一步（不读 `sql_raw_text`）OK (0.11s)，但第二步用 `(database, md5) IN (...)` 点查时，由于 `md5` 不在主键 `(instid, insttype, log_timestamp)` 中，CK 无法通过主键裁剪 granule，仍然扫描该 instid 下的所有 granule 并解压 `sql_raw_text`。

5. **根因确认:** 对大实例，只要单条 SQL 需要读 `sql_raw_text` 且匹配行覆盖大量 granule，在当前内存限制 (9.31 GiB) 下必然 OOM。唯一确认不 OOM 的是完全不读 `sql_raw_text` 的基线查询 (方案 0)。

---

## 7. 纯代码侧解决方案验证

验证脚本: `scripts/test_ck_oom_solutions_v2.py`

约束：不改 CK 端配置/DDL，只改 SQL 和 Python 代码。

所有方案共享同一个第一步：聚合不读 `sql_raw_text`（PREWHERE instid + insttype），然后用不同策略获取 `sql_raw_text`，最后在 Python 端过滤 `length < 10176` 和 `LIKE '%select%'`。

### 测试方案说明

| # | 方案 | 第二步策略 |
|---|---|---|
| 0 | 对照 | 批量 `(database, md5) IN (...)` + PREWHERE，默认并发 |
| 1 | 逐组点查 | 每组单独 `LIMIT 1`，30 天窗口 |
| 2 | 降并发 | 批量 IN + `SETTINGS max_threads=1, max_block_size=2048` |
| 3 | 按天分批 | 逐天查询，利用 partition 裁剪，收集满即停 |
| 4 | 逐组+日期 | 每组查时用第一步的 `any(toDate(timestamp))` 定位单天 partition |

### 实例 A（小实例）结果

| 方案 | 结果 | 行数 | 总耗时 |
|---|---|---|---|
| 0. 对照(批量IN) | **OK** | 15 | 1.25s |
| 1. 逐组点查 | **OK** | 15 | 15.26s |
| 2. 降并发IN | **OK** | 15 | 2.97s |
| 3. 按天分批 | **OK** | 0 | 27.79s |
| 4. 逐组+日期 | **OK** | 0 | 20.92s |

### 实例 C（大实例）结果

| 方案 | 结果 | 行数 | 总耗时 |
|---|---|---|---|
| 0. 对照(批量IN) | **OOM** | 0 | 2.34s |
| 1. 逐组点查 | 1/27 成功 | 0 | 57.75s |
| 2. 降并发IN | **OK** | 24 | 58.30s |
| 3. 按天分批 | 30天全OOM | 0 | 87.23s |
| 4. 逐组+日期 | 27组全OOM | 0 | 182.64s |

### 实验结论

1. **方案 2（`SETTINGS max_threads=1`）是唯一对大实例成功的纯 SQL 方案。** 返回全部 27 组文本，Python 过滤后 24 行。证明 OOM 的直接原因是多线程并发解压 `sql_raw_text` granule —— 单线程顺序读时内存峰值可控制在 9.31 GiB 以内。

2. **逐组点查（方案 1）对大实例几乎全部 OOM (26/27)。** `LIMIT 1` 只控制返回行数，不控制扫描行为。CK 仍然用多线程扫描所有匹配 `instid` 的 granule 来寻找 `md5` 匹配行，每个线程都解压 `sql_raw_text`。

3. **按天分批（方案 3）对大实例 30 天全部 OOM。** 推翻了"partition 裁剪可以控制内存"的假设。该 instid 单天 partition 中仍有大量行覆盖多个 granule（单天约 5 万行），多线程并发解压 `sql_raw_text` 仍然超限。

4. **逐组+日期（方案 4）同样全部 OOM。** 指定单天 + 单组仍不够，因为 `md5` 不在主键中，CK 要扫描该天该 instid 的所有 granule。

5. **实例 A 方案 3/4 返回 0 行**（非 OOM），原因是 Distributed 表的 `timestamp` 列与物理表的 `PARTITION BY toDate(log_timestamp)` 使用不同列名，`timestamp = toDate('...')` 未命中 partition 裁剪，查询执行但未匹配到行。

### 最终方案

两步查询 + 降并发：

```
第一步: 聚合统计（不读 sql_raw_text）
  → PREWHERE instid + insttype, 约 0.15s

第二步: 批量获取 sql_raw_text
  → (database, md5) IN (...) + PREWHERE + SETTINGS max_threads=1
  → 约 58s (大实例)

Python 端: 过滤 length(sql_text) < 10176 AND 'select' in sql_text.lower()
```

待验证：`max_threads=2` 或 `max_threads=4` 是否仍在内存限制内（可缩短耗时）。

---

## 8. 一步查询 + 降并发验证

验证脚本: `scripts/test_ck_oom_solutions_v3.py`

验证 v7 的最终方案是否可以简化为一步查询：直接在原始 SQL 上加 `SETTINGS max_threads=N` 即可，无需拆分两步。同时测试 `max_threads` 的安全边界。

### 测试方案说明

| # | SQL 类型 | PREWHERE | sql_raw_text 过滤 | max_threads |
|---|---|---|---|---|
| 0a | 原始 argMin/argMax | 无 | 有 | 默认 |
| 0b | any() | 有 | 无 | 默认 |
| 1 | 原始 argMin/argMax | 无 | 有 | 1 |
| 2 | 原始 argMin/argMax | 无 | 有 | 2 |
| 3 | 原始 argMin/argMax | 无 | 有 | 4 |
| 4 | any() | 有 | 无 | 1 |
| 5 | any() | 有 | 无 | 2 |
| 6 | any() | 有 | 无 | 4 |
| 7 | argMin/argMax | 有 | 无 | 1 |
| 8 | argMin/argMax | 有 | 无 | 2 |

### 实例 A（小实例）结果

| 方案 | 结果 | 行数 | 耗时 |
|---|---|---|---|
| 0a. 原始SQL(默认并发) | **OOM** | 0 | 2.22s |
| 0b. any()+PW(默认并发) | **OK** | 18 | 9.27s |
| 1. 原始SQL + t=1 | **OK** | 15 | 75.52s |
| 2. 原始SQL + t=2 | **OK** | 15 | 19.67s |
| 3. 原始SQL + t=4 | **OK** | 15 | 7.43s |
| 4. any()+PW + t=1 | **OK** | 18 | 4.96s |
| 5. any()+PW + t=2 | **OK** | 18 | 1.36s |
| 6. any()+PW + t=4 | **OK** | 18 | 2.75s |
| 7. argMinMax+PW + t=1 | **OK** | 18 | 2.61s |
| 8. argMinMax+PW + t=2 | **OK** | 18 | 1.25s |

### 实例 C（大实例）结果

| 方案 | 结果 | 行数 | 耗时 |
|---|---|---|---|
| 0a. 原始SQL(默认并发) | **OOM** | 0 | 2.14s |
| 0b. any()+PW(默认并发) | **OOM** | 0 | 2.41s |
| 1. 原始SQL + t=1 | **OK** | 25 | 86.68s |
| 2. 原始SQL + t=2 | **OK** | 25 | 47.25s |
| 3. 原始SQL + t=4 | **OK** | 25 | 26.71s |
| 4. any()+PW + t=1 | **OK** | 29 | 53.35s |
| 5. any()+PW + t=2 | **OK** | 29 | 24.98s |
| 6. any()+PW + t=4 | **OK** | 29 | 13.95s |
| 7. argMinMax+PW + t=1 | **OK** | 29 | 50.48s |
| 8. argMinMax+PW + t=2 | **OK** | 29 | 31.75s |

### 实验结论

1. **`max_threads` 限制是决定性因素。** 所有方案只要加了 `max_threads ≤ 4`，无论是否加 PREWHERE、无论用 `any()` 还是 `argMin/argMax`、无论 WHERE 中是否有 `sql_raw_text` 过滤，对两个实例均不再 OOM。**原始 SQL 完全不改结构，仅追加 `SETTINGS max_threads=4` 即可解决。**

2. **PREWHERE 叠加降并发有显著加速效果。** 对大实例，同样 `max_threads=4`：原始 SQL 需要 26.71s，`any()+PW` 只需 13.95s（快约 2 倍）。PREWHERE 减少了需要扫描的 granule 数量，降并发控制了单次解压的内存峰值，两者叠加效果最优。

3. **`any()` 比 `argMin/argMax` 更快。** 对大实例 `max_threads=1`：`any()+PW` 53.35s vs `argMinMax+PW` 50.48s 差距不大；但 `max_threads=2` 下：`any()+PW` 24.98s vs `argMinMax+PW` 31.75s，`any()` 快约 21%。`any()` 聚合状态更小（不需要保存 `query_time` 用于比较），减少了内存占用。

4. **行数差异说明 `sql_raw_text` 过滤条件的影响。** 大实例原始 SQL 返回 25 行，`any()+PW`（无 `sql_raw_text` 过滤）返回 29 行。差出的 4 行是被 `lower(sql_raw_text) LIKE '%select%'` 和 `length(sql_raw_text) < 10176` 过滤掉的非 SELECT 类 SQL。如果将这两个过滤移到 Python 端，可以拿到完整的 29 行再做筛选。

5. **耗时与 max_threads 近似线性关系。** 大实例 `any()+PW`：t=1 → 53s, t=2 → 25s, t=4 → 14s，基本是 2 倍加速。说明该查询是 I/O bound，提高并发度能有效缩短耗时。

### 推荐方案

**最优方案：`any()` + PREWHERE + `max_threads=4` + Python 端过滤**

```sql
SELECT database,
       any(sql_raw_text) AS sql_text,
       COUNT(*) AS count_star,
       AVG(query_time) AS elapsed_time_avg,
       MIN(query_time) AS elapsed_time_min,
       MAX(query_time) AS elapsed_time_max,
       MAX(start_time) AS last_start_time,
       md5
FROM cdblog.slowlog
PREWHERE instid = '<INST_ID>' AND insttype = 'master'
WHERE user_name != 'tencentroot'
  AND timestamp >= toDate(now()) - 30
  AND query_time >= 0.1
GROUP BY database, md5
SETTINGS max_threads = 4, max_block_size = 2048
```

Python 端过滤：
```python
rows = [r for r in rows
        if len(r['sql_text']) < 10176
        and 'select' in r['sql_text'].lower()]
```

性能：大实例 ~14s，小实例 ~2.8s。

**最小改动方案：原始 SQL + `SETTINGS max_threads=4`**

不改 SQL 结构，仅追加 SETTINGS。大实例 ~27s，小实例 ~7.4s。适合不想改 SQL 逻辑的场景。

---

## 9. 原始 SQL + PREWHERE × max_threads 交叉验证

验证脚本: `scripts/test_ck_oom_solutions_v4.py`

聚焦原始业务 SQL（argMin/argMax + `sql_raw_text` 过滤条件完全保留），仅变化两个维度：是否加 PREWHERE、max_threads 取值。

### 实例 A（小实例）矩阵

|  | 默认并发 | t=1 | t=2 | t=4 |
|---|---|---|---|---|
| **无 PW** | **OOM** 2.0s | OK(15行) 22.6s | OK(15行) 11.9s | OK(15行) 24.7s |
| **有 PW** | OK(15行) 1.8s | OK(15行) 8.6s | OK(15行) 1.8s | OK(15行) 4.3s |

### 实例 C（大实例）矩阵

|  | 默认并发 | t=1 | t=2 | t=4 |
|---|---|---|---|---|
| **无 PW** | **OOM** 3.6s | OK(25行) 106.0s | OK(25行) 50.8s | OK(25行) 25.7s |
| **有 PW** | **OOM** 2.7s | OK(25行) 69.0s | OK(25行) 37.3s | OK(25行) 25.1s |

### 实验结论

1. **降并发是充分条件。** 原始 SQL 不做任何结构改动，仅追加 `SETTINGS max_threads=N`（N ≤ 4），对两个实例均不再 OOM。这是最小改动的解决方案。

2. **PREWHERE 对大实例有加速但不防 OOM。** 大实例默认并发下，有 PW 仍然 OOM（2.7s）。PREWHERE 减少了扫描 granule 数，但默认并发线程数过多，并发解压仍然超限。PREWHERE 的价值体现在与降并发叠加时缩短耗时。

3. **PREWHERE + 降并发叠加的加速效果。** 大实例 t=1：无 PW 106.0s → 有 PW 69.0s（快 35%）；t=2：50.8s → 37.3s（快 27%）。PREWHERE 裁剪掉无关 granule，让降并发后的单/双线程只扫描有效数据。

4. **t=4 时 PREWHERE 加速效果趋于消失。** 大实例 t=4：无 PW 25.7s vs 有 PW 25.1s，差距仅 2%。推测 t=4 时 I/O 吞吐已接近磁盘瓶颈，PREWHERE 减少的 granule 数不再是瓶颈。

5. **小实例存在耗时波动。** 小实例无 PW 下 t=1 (22.6s) 比 t=4 (24.7s) 更快，不符合预期。可能是 CK 缓存效应或集群负载波动导致。有 PW 下 t=2 (1.8s) 与默认并发 (1.8s) 持平且快于 t=4 (4.3s)，同样存在波动。小实例数据量小，受缓存影响更大，耗时数据参考价值有限。

### 最终推荐方案（更新）

综合 v3 和 v4 实验结果，最终推荐：

**方案 A（最小改动）：原始 SQL + `SETTINGS max_threads=4`**

```sql
-- 原始业务 SQL 完全不改，仅追加 SETTINGS
SELECT ...原始 SQL...
GROUP BY database, md5
SETTINGS max_threads = 4, max_block_size = 2048
```

- 大实例 ~25s，小实例 ~1.8s（有 PW）/ ~25s（无 PW）
- 零 SQL 结构改动，最低风险

**方案 B（最优性能）：`any()` + PREWHERE + `max_threads=4` + Python 过滤**

```sql
SELECT database,
       any(sql_raw_text) AS sql_text,
       COUNT(*) AS count_star,
       AVG(query_time) AS elapsed_time_avg,
       MIN(query_time) AS elapsed_time_min,
       MAX(query_time) AS elapsed_time_max,
       MAX(start_time) AS last_start_time,
       md5
FROM cdblog.slowlog
PREWHERE instid = '<INST_ID>' AND insttype = 'master'
WHERE user_name != 'tencentroot'
  AND timestamp >= toDate(now()) - 30
  AND query_time >= 0.1
GROUP BY database, md5
SETTINGS max_threads = 4, max_block_size = 2048
```

- 大实例 ~14s（v3 实验数据），小实例 ~2.8s
- 需要 Python 端过滤 `length < 10176` 和 `LIKE '%select%'`
- 去掉 `argMin/argMax` 重复列，返回行数更完整（29 行 vs 25 行）
