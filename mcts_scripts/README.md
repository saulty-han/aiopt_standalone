# MCTS Scripts

MCTS 搜索结果的离线分析与 SFT 训练数据生成工具集。

## 与根项目的关系

本目录依赖 [aiopt_standalone (adapt_for_ncdb3119_with_llm_dev_cache)](https://git.woa.com/OLTP/aiopt_standalone/tree/adapt_for_ncdb3119_with_llm_dev_cache) 根项目。使用前需将 `mcts_scripts/` 放在根项目目录下，以便 TPC-DS Runner 脚本能正确 import 根项目的 `db_controller`、`optimizer`、`data_models` 等模块。

> **注意**：`sft_data/run_sft.py`、`sft_data/run_posterior_correction.py` 和 `mcts_detailed_analyzer/analyze_mcts_nodes.py` 是完全独立的脚本，不依赖根项目，可以在任何环境下直接运行。

## 目录结构

```
mcts_scripts/
├── README.md
├── mcts_parallel_runner.py             # 1. 通用负载 — 多进程并行 MCTS 优化
├── mcts_analyze.py                     #    通用负载结果汇总分析
├── rollout_validation.py              #    单线程验证（被 tpcds_runner / benchmark 复用）
├── tpcds_runner/                       # 2. TPC-DS 执行 & 结果分析
│   ├── test_tpcds_queries_parallel.py  #    多进程并行 MCTS 优化（TPC-DS 专用）
│   ├── analyze_tpcds_results.py        #    结果汇总报表（TPC-DS 专用）
│   ├── aiopt_conf.tpcds.toml.tpl       #    TPC-DS 专用配置模版（cp 到 etc/aiopt_conf.toml）
│   └── queries_tpcds.txt               #    TPC-DS 99 条 SQL
├── mcts_detailed_analyzer/             # 3. MCTS 树节点详细分析
│   └── analyze_mcts_nodes.py           #    动作分布/改进/覆盖率/幻觉检测
├── sft_data/                           # 4. SFT 训练数据生成
│   ├── run_sft.py                      #    正向 SFT 数据一体化入口（推荐）
│   └── run_posterior_correction.py     #    后验思维链订正（生成高质量修正样本）
└── benchmark/                          # 5. Core-Set Benchmark 评测套件
    ├── benchmark_runner.py             #    多实例并行 MCTS 评测主程序
    ├── benchmark_sampler.py            #    Core-Set 采样脚本（Instance-Aware）
    ├── instance_lookup.py              #    实例 ID → IP/Port 映射查询模块
    ├── instance_lookup_table.txt       #    实例映射表
    ├── aiopt_conf.benchmark.toml.tpl   #    Benchmark 专用配置模版（cp 到 etc/aiopt_conf.toml）
    ├── core_set_1000_cdb.json          #    CDB 精选 1000 条评测集
    └── core_set_3000_cdb.json          #    CDB 精选 3000 条评测集
```

## 可选依赖

```bash
pip install tqdm   # 进度条（未安装则自动降级为简单输出）
```

## 配置（运行前必读）

TPC-DS Runner 和 Benchmark Runner 各自带一个专用配置模版，**运行前需把对应模版拷到根项目的 `etc/aiopt_conf.toml`**（runner 默认读取 `etc/aiopt_conf.toml`）：

```bash
# TPC-DS Runner
cp mcts_scripts/tpcds_runner/aiopt_conf.tpcds.toml.tpl etc/aiopt_conf.toml

# Benchmark Runner（二选一，按当前要跑的套件覆盖 etc/aiopt_conf.toml）
cp mcts_scripts/benchmark/aiopt_conf.benchmark.toml.tpl etc/aiopt_conf.toml
```

拷过去后，**唯一必须修改的是 `[mcts].llm_api_url_key`**（填入你的 LLM API URL key），其余字段开箱即用（已分别针对 TPC-DS / Benchmark 调好 `optimizer_type`、`default_plan_timeout_seconds`、remote cache 等）。

> 两个模版仅 `default_plan_timeout_seconds` 不同（TPC-DS=60s / Benchmark=300s），且都与 `etc/aiopt_conf.toml.tpl` 的其余默认保持一致。

---

## 一、通用负载 — mcts_parallel_runner & mcts_analyze

TPC-DS Runner 只能处理 TPC-DS 固定的 99 条 SQL。**`mcts_parallel_runner.py` + `mcts_analyze.py`** 是面向任意自定义负载的通用版本，适合把生产 Slow Log 或业务 SQL 直接丢进去跑。

### 1.1 准备 queries 文件

每行写一条 SQL，空行和 `--` 开头的行自动跳过：

```text
-- 查询 1：订单汇总
SELECT order_id, SUM(amount) FROM orders WHERE status='paid' GROUP BY order_id;
-- 查询 2：用户活跃度
SELECT uid, COUNT(*) cnt FROM events WHERE ts > '2024-01-01' GROUP BY uid ORDER BY cnt DESC LIMIT 100;
```

### 1.2 运行 MCTS 优化（mcts_parallel_runner.py）

`mcts_parallel_runner.py` 放在 `mcts_scripts/` 根目录，需要在**根项目目录**下执行，以便正确 import `db_controller`、`optimizer` 等模块。

```bash
# 基础用法：4 个 worker 并行优化 my_queries.txt 中的所有 SQL
python mcts_scripts/mcts_parallel_runner.py \
    --host 127.0.0.1 --port 13000 --user root \
    --db my_database \
    --queries my_queries.txt \
    --workers 4

# 带密码
python mcts_scripts/mcts_parallel_runner.py \
    --host 10.0.0.1 --port 3306 --user aiopt --password secret \
    --db prod_db \
    --queries slow_queries.txt \
    --workers 8

# 只跑前 10 条（调试用）
python mcts_scripts/mcts_parallel_runner.py \
    --host 127.0.0.1 --port 13000 --user root \
    --db my_database \
    --queries my_queries.txt \
    --workers 4 --limit 10
```

**参数说明：**

| 参数 | 必填 | 默认值 | 说明 |
|------|------|--------|------|
| `--host` | ✅ | — | 数据库 IP |
| `--port` | ✅ | — | 数据库端口 |
| `--user` | — | `root` | 数据库用户名 |
| `--password` | — | `""` | 数据库密码 |
| `--db` | ✅ | — | 目标数据库名 |
| `--queries` | ✅ | — | SQL 文件路径（每行一条） |
| `--workers` | — | `4` | 并行 worker 进程数（实际数 = min(workers, 查询数)） |
| `--limit` | — | `0` | 只跑前 N 条（0 = 全部） |

**运行机制：**

- 每个 worker 进程独立建立 DB 连接和 `LLMOptimizer` 实例，通过共享队列动态领取 SQL，保证负载均匀
- 支持 `Ctrl+C` 优雅中断，已完成的结果不会丢失
- MCTS 搜索结果写入配置文件（`aiopt_conf.toml`）中指定的 JSON 输出目录，每条 SQL 一个文件

**实时输出示例：**

```
[Worker-0] [1/20] SELECT order_id, SUM(amount) FROM orders ...
  [Worker-0] [1/20] 12.3s, solutions=3, candidates=8, llm_calls=15, db_executes=22
  [Worker-0] [1/20] Best: reward=0.82, time=0.45s, speedup=2.73x, hints=['USE_INDEX(orders idx_status)']
```

### 1.3 分析优化结果（mcts_analyze.py）

`mcts_analyze.py` 读取 MCTS JSON 输出目录，按 `query_digest` 去重并生成汇总报表。与 TPC-DS 版本不同，它不依赖 `q{N}` 编号映射，适用于任意 SQL。

```bash
# 终端预览（显示前 6 列 + SQL 摘要）
python mcts_scripts/mcts_analyze.py /path/to/mcts/output

# 导出完整 CSV 报表
python mcts_scripts/mcts_analyze.py /path/to/mcts/output -o report.csv

# 按 baseline 时间降序排序（找最慢的 SQL）
python mcts_scripts/mcts_analyze.py /path/to/mcts/output --sort baseline -o report.csv

# 按 min(baseline, best) 升序排序（找实际执行最快的 SQL）
python mcts_scripts/mcts_analyze.py /path/to/mcts/output --sort min -o report.csv
```

**参数说明：**

| 参数 | 必填 | 默认值 | 说明 |
|------|------|--------|------|
| `input_dir` | ✅ | — | MCTS JSON 结果目录 |
| `-o, --output` | — | 终端输出 | 输出 CSV 文件路径 |
| `--sort` | — | `speedup` | 排序方式：`speedup`（加速比降序）/ `baseline`（基线耗时降序）/ `min`（最优耗时升序） |

**CSV 输出列说明：**

| 列名 | 说明 |
|------|------|
| `Query Digest` | SQL 唯一标识（来自 `execution_info.query_digest`，缺失时降级为文件名） |
| `Best Plan ID` | 最优方案的执行计划摘要（`plan_digest`） |
| `Baseline(s)` | 原始执行时间（秒） |
| `Best(s)` | MCTS 找到的最优执行时间（秒） |
| `Min(Base,Best)` | `min(baseline, best)`，即实际可用的最优时间 |
| `Speedup` | `Baseline / Best`，加速比 |
| `Reward` | MCTS 最优解的奖励分 |
| `NewPlans` | MCTS 搜索中探索出的新执行计划数量 |
| `LLM` | LLM 调用次数 |
| `DB` | DB 实际执行次数 |
| `E2E(s)` | MCTS 端到端搜索耗时（秒） |
| `Cost(¥)` | 估算 LLM token 费用（按 ¥2/M tokens 计算） |
| `BestHints` | 最优方案使用的 Hint 列表 |
| `Full SQL` | 完整 SQL 内容 |

**去重逻辑：** 同一目录下如有重复 digest 的文件，保留**最新**（按文件修改时间）的结果。

### 1.4 典型工作流（通用负载）

```
[准备]  从 Slow Log / 业务系统导出 SQL → my_queries.txt
           │
           ▼
[Step 1]  mcts_parallel_runner.py
          多进程 MCTS 搜索 → /path/to/mcts/output/*.json
           │
           ▼
[Step 2a] mcts_analyze.py → report.csv
          查看加速比、Hints、费用估算
           │
           ▼（可选）
[Step 2b] mcts_detailed_analyzer/analyze_mcts_nodes.py
          深入分析搜索树动作分布、覆盖率、幻觉检测
           │
           ▼（可选）
[Step 2c] sft_data/run_sft.py → sft_samples.jsonl
          生成 SFT 训练数据
```

---

## 二、TPC-DS Runner — 执行优化 & 分析结果

### 2.1 运行 MCTS 优化

`test_tpcds_queries_parallel.py` 启动多个 worker 进程，每个 worker 独立创建 DB 连接和 LLM 优化器，通过共享队列逐条领取 SQL 执行 MCTS 搜索。

> **运行前**：`cp mcts_scripts/tpcds_runner/aiopt_conf.tpcds.toml.tpl etc/aiopt_conf.toml` 并填好 `[mcts].llm_api_url_key`（详见上文 [配置](#配置运行前必读)）。

```bash
# 基础用法：4 个 worker 跑全部 99 条查询
python mcts_scripts/tpcds_runner/test_tpcds_queries_parallel.py \
    --host 127.0.0.1 --port 13000 --user root \
    --db tpcds \
    --queries mcts_scripts/tpcds_runner/queries_tpcds.txt \
    --workers 4

# 只跑前 5 条（调试用）
python mcts_scripts/tpcds_runner/test_tpcds_queries_parallel.py \
    --host 127.0.0.1 --port 13000 --user root \
    --db tpcds \
    --queries mcts_scripts/tpcds_runner/queries_tpcds.txt \
    --workers 4 --limit 5
```

**参数说明：**

| 参数 | 必填 | 默认值 | 说明 |
|------|------|--------|------|
| `--host` | ✅ | — | 数据库 IP |
| `--port` | ✅ | — | 数据库端口 |
| `--user` | — | `root` | 数据库用户名 |
| `--password` | — | `""` | 数据库密码 |
| `--db` | ✅ | — | 目标数据库名 |
| `--queries` | ✅ | — | SQL 文件路径（每行一条） |
| `--workers` | — | `8` | 并行 worker 进程数 |
| `--limit` | — | `0` | 只跑前 N 条（0 = 全部） |
| `--mode` | — | `both` | `both`=优化+验证 / `optimize`=只优化 / `validate-only`=只验证（读 `--input-dir`） |
| `--no-validate` | — | 关闭 | 等价 `--mode optimize`：优化后不做验证 |
| `--input-dir` | — | — | `validate-only` 模式读取的 MCTS 输出 JSON 目录 |
| `--output-csv` | — | 自动命名 | 验证 CSV 输出路径（默认 `tpcds_runner/results/` 下） |
| `--validate-baseline` | — | 关闭 | 验证阶段额外重测 baseline（无 hints） |
| `--validate-timeout` | — | `0` | 验证单次 EXPLAIN ANALYZE 超时（秒），0=自动(baseline×1.1) |
| `--validate-warmup` | — | `0` | 验证每条 SQL 正式计时前的预热轮数 |

运行后会在 MCTS 配置的输出目录下生成 JSON 结果文件（每条 SQL 一个文件）。支持 `Ctrl+C` 优雅中断。

**查询标记：** 每条查询按 `q0001`–`q0099` 编号（零填充 4 位），该标记会写进结果 JSON 文件名（形如 `tpcds_q0001_<hash>_<时间戳>.json`），便于按 query 定位。

**实时输出示例（每条查询两行）：**

```
[W0] [1/99] digest=q0001_a1b2c3d4e5f6a7b8 SELECT i_item_id, ...
  [W0] [1/99] 12.3s, solutions=3, candidates=8, llm_calls=15, db_executes=22
  [W0] [1/99] Best: time=0.45s, speedup=2.73x, baseline=1.230s, hints=['USE_INDEX(...)']
```

启动 / 结束 banner 会打印关键配置（并行度、explain / plan 超时、remote cache 是否开启及其超时、是否按 baseline 收紧 cache 超时），结束时还会打印 MCTS 结果目录路径。

### 2.2 验证阶段（单线程重跑）

`--mode both`（默认）会在优化结束后进入**单线程验证阶段**（复用 `rollout_validation.py`），逐 rollout 串行重跑每个 rollout 的 best hint，输出宽表 CSV，并在每个 rollout 后打印「直接读取结果 → 单线程验证结果」的累计 Sum 变化：

```
  [validate][R0] measured=5 cached=0 skip_base=2 skip_worse=1 timeout=1  原结果Sum 120.500s -> 单线程Sum 95.200s
  [validate][R1] ...
  ------------------------------------------------------------------------
  [validate] 完成 耗时 170.9s | 实测 30 缓存 0 跳过(≥base) 38 跳过(worse) 64 错误 3
  [validate] BaselineSum=228.06s BestSum=64.82s Overall≈3.519x
  [validate] CSV: .../results/tpcds_validation_tpcds_<时间戳>.csv
```

- **原结果Sum** = MCTS JSON 记录值的累计最优之和（直接读取，不真跑）
- **单线程Sum** = 单线程重跑后的累计最优之和（真实 EXPLAIN ANALYZE）
- 两者对比可看出「记录值」与「实测值」的偏差。

### 2.3 分析优化结果

`analyze_tpcds_results.py` 读取 MCTS 输出目录，按文件名中的 `q{N}` 映射到 TPC-DS 99 条查询，生成汇总报表。

```bash
# 终端表格输出
python mcts_scripts/tpcds_runner/analyze_tpcds_results.py /path/to/mcts/output

# 导出 CSV
python mcts_scripts/tpcds_runner/analyze_tpcds_results.py /path/to/mcts/output -o report.csv

# 按加速比排序
python mcts_scripts/tpcds_runner/analyze_tpcds_results.py /path/to/mcts/output --sort speedup
```

**参数说明：**

| 参数 | 必填 | 默认值 | 说明 |
|------|------|--------|------|
| `input_dir` | ✅ | — | MCTS JSON 结果目录 |
| `-o, --output` | — | 终端输出 | 输出 CSV 文件路径 |
| `--sort` | — | `id` | 排序方式：`id` / `speedup` / `baseline` / `best` / `llm` |

**输出内容：**
- 每条查询：Baseline 时间、最优时间、加速比、Solutions 数、LLM 调用数、DB 执行数
- 汇总统计：完成率、优化成功率、平均加速比、整体加速比、总耗时

---

## 三、MCTS Detailed Analyzer — 搜索树节点分析

`analyze_mcts_nodes.py` 对 MCTS 搜索树的每个节点进行多维度统计分析，输出 7 大分析板块。

```bash
# 基础用法
python mcts_scripts/mcts_detailed_analyzer/analyze_mcts_nodes.py \
    --input-dir /path/to/mcts/output

# 指定并行 worker 数
python mcts_scripts/mcts_detailed_analyzer/analyze_mcts_nodes.py \
    --input-dir /path/to/mcts/output --workers 4
```

**参数说明：**

| 参数 | 必填 | 默认值 | 说明 |
|------|------|--------|------|
| `--input-dir` | ✅ | — | MCTS JSON 结果目录 |
| `--workers` | — | `8` | 并行进程数 |

**分析内容：**

1. **动作分布** — A1~A6 各动作的出现次数与比例（全量 vs SFT 筛选后）
2. **动作改进效果** — 每种动作类型的执行时间改进比例与幅度
3. **深度维度分析** — 不同搜索深度下的动作分布趋势
4. **轮次维度分析** — 各轮次（rollout）中最优解的动作类型变化
5. **Hint 类型统计** — Index / Join / Config 各类新增 Hint 的分布与改进效果
6. **探索覆盖率** — 候选 Hint 空间中实际被尝试的 INDEX、JOIN、SET_VAR 覆盖率
7. **A5/A6 特殊分析 & 幻觉检测** — 纠错成功率、最终答案有效性、幻觉 Hint 统计

---

## 四、SFT Data — 训练数据生成

### 4.1 正向 SFT 数据（run_sft.py）

`run_sft.py` 是完全独立的脚本（无项目依赖），将节点提取、去重、过滤、Prompt 填充整合为一条流水线。

```bash
# 基础用法
python mcts_scripts/sft_data/run_sft.py \
    --input-dir /path/to/mcts/output \
    --output-dir /path/to/sft/output

# 调整 baseline 过滤阈值（默认 0.1s）
python mcts_scripts/sft_data/run_sft.py \
    --input-dir /path/to/mcts/output \
    --output-dir /path/to/sft/output \
    --min-baseline 0.05

# 开启幻觉 Hint 过滤
python mcts_scripts/sft_data/run_sft.py \
    --input-dir /path/to/mcts/output \
    --output-dir /path/to/sft/output \
    --filter-phantom
```

**参数说明：**

| 参数 | 必填 | 默认值 | 说明 |
|------|------|--------|------|
| `--input-dir` | ✅ | — | MCTS JSON 结果目录 |
| `--output-dir` | — | `sft_output` | 输出目录 |
| `--min-baseline` | — | `0.1` | 过滤 baseline ≤ 此值（秒）的节点 |
| `--filter-phantom` | — | 关闭 | 开启幻觉 Hint 过滤（过滤不在候选列表中的 Hint） |

**处理流水线：**

```
[1] 提取节点  ──  从 MCTS JSON 中筛选「有改进且产生新执行计划」的节点
        ↓
[2] 去重      ──  按 (父计划, Hints, 当前计划) 三元组去重
        ↓
[3] Baseline 过滤  ──  去掉 baseline 时间过短（本身就快）的查询
        ↓
[4] 幻觉过滤（可选）──  去掉 Hint 不在候选列表中的节点
        ↓
[5] 填充 Prompt  ──  按动作类型（A1~A6）加载模板，生成 SFT 样本
```

**输出文件：**

| 文件 | 格式 | 说明 |
|------|------|------|
| `node_steps.json` | JSON | 中间结果：过滤后的节点步骤 |
| `sft_samples.jsonl` | JSONL | 最终结果：每行一条 SFT 训练样本 |

**SFT 样本格式（严格 6 字段）：**

```json
{
  "output": "LLM 推理 + <answer> /*+ hints */ </answer>",
  "history": [],
  "message": "",
  "system": "你是一个专业的SQL优化专家...",
  "instruction": "<任务A1：索引修改>...",
  "input": "<查询>: SELECT ... <候选Hints>: {...} <已有推理步骤>: ..."
}
```

### 4.2 后验思维链订正（run_posterior_correction.py）

`run_posterior_correction.py` 通过「裁判 LLM」对 MCTS 搜索节点进行后验订正，生成更高质量的 SFT 样本。

**核心思路：** 构建 `[A] 初始状态 → [B] 原始推理 → [C] 物理反馈` 三元组，由裁判 LLM 依据真实 EXPLAIN ANALYZE 结果订正原始推理（B → B'），使推理更加严谨可信，最终输出 `(A, B')` 作为 SFT 样本。

```bash
# 基础用法
python mcts_scripts/sft_data/run_posterior_correction.py \
    --input-dir /path/to/mcts/output \
    --output-dir /path/to/posterior/output

# 仅生成 correction_prompt（不调 LLM，用于人工抽检）
python mcts_scripts/sft_data/run_posterior_correction.py \
    --input-dir /path/to/mcts/output \
    --output-dir /path/to/posterior/output \
    --prompts-only

# 限制样本数 + 设置质量过滤阈值
python mcts_scripts/sft_data/run_posterior_correction.py \
    --input-dir /path/to/mcts/output \
    --output-dir /path/to/posterior/output \
    --limit 20 \
    --min-correction-score 7 \
    --min-speedup-pct 5.0
```

**参数说明：**

| 参数 | 必填 | 默认值 | 说明 |
|------|------|--------|------|
| `--input-dir` | ✅ | — | MCTS JSON 结果目录 |
| `--output-dir` | ✅ | — | 输出目录 |
| `--prompts-only` | — | 关闭 | 只生成 correction_prompt，不调 LLM |
| `--workers` | — | `0`（API 池大小） | 并发 worker 数 |
| `--limit` | — | `0`（全量） | 仅处理前 N 条 |
| `--min-correction-score` | — | `6` | 过滤裁判评分低于此值的样本 |
| `--min-speedup-pct` | — | 不过滤 | 仅保留加速比 ≥ 此百分比的节点 |
| `--min-step-improvement` | — | 不过滤 | 仅保留 step_improvement ≥ 此值的节点 |
| `--min-baseline` | — | `0.1` | 过滤 baseline ≤ 此值（秒）的 entry |
| `--filter-phantom` | — | 关闭 | 启用幻觉 Hint 过滤 |
| `--actions` | — | 全部 | 逗号分隔的动作过滤列表，如 `A1,A5,A6` |
| `--no-resume` | — | 不设置 | 不恢复已有进度，重跑全部 |

**输出文件：**

| 文件 | 格式 | 说明 |
|------|------|------|
| `sft_samples.jsonl` | JSONL | 订正后的 SFT 训练样本 |
| `comparison.jsonl` | JSONL | 原始推理 vs 订正推理对照表（含评分） |

---

## 五、Benchmark — Core-Set 评测套件

`benchmark/` 目录提供了一套基于真实业务 SQL 的标准化评测集，用于系统性评估 MCTS 优化效果。

### 5.1 评测集说明

| 文件 | 说明 |
|------|------|
| `core_set_1000_cdb.json` | CDB 精选 1000 条评测 SQL（覆盖多难度等级与查询模式） |
| `core_set_3000_cdb.json` | CDB 精选 3000 条评测 SQL（更大规模） |

评测集中每条 SQL 包含：`instance_id`、`query_digest`、`baseline_time`、`difficulty_level`（L1-Easy / L2-Medium / L3-Hard / L4-Expert）、`pattern_label` 等元数据。

### 5.2 运行评测（benchmark_runner.py）

`benchmark_runner.py` 读取评测 JSON，根据 `instance_lookup_table.txt` 自动查找对应克隆实例的 IP/Port，以实例为单位多进程并行跑 MCTS 优化。

> **运行前**：`cp mcts_scripts/benchmark/aiopt_conf.benchmark.toml.tpl etc/aiopt_conf.toml` 并填好 `[mcts].llm_api_url_key`（详见上文 [配置](#配置运行前必读)）。

```bash
# 基础用法：用默认 core_set_1000_cdb.json 跑全量评测
python mcts_scripts/benchmark/benchmark_runner.py

# 指定评测集文件
python mcts_scripts/benchmark/benchmark_runner.py \
    --bench-json mcts_scripts/benchmark/core_set_3000_cdb.json

# 按难度过滤（只跑 Hard 和 Expert 级别）
python mcts_scripts/benchmark/benchmark_runner.py \
    --difficulty L3-Hard L4-Expert

# 按查询模式过滤 + 只跑前 50 条
python mcts_scripts/benchmark/benchmark_runner.py \
    --pattern join-heavy \
    --limit 50

# 按实例 ID 过滤 + 指定 worker 数
python mcts_scripts/benchmark/benchmark_runner.py \
    --instance <instance_id_1> <instance_id_2> \
    --workers 8
```

**参数说明：**

| 参数 | 必填 | 默认值 | 说明 |
|------|------|--------|------|
| `--bench-json` | — | `core_set_1000_cdb.json` | 评测集 JSON 文件路径 |
| `--host` | — | `127.0.0.1` | 数据库 IP（查表命中后自动覆盖） |
| `--port` | — | `13000` | 数据库端口（查表命中后自动覆盖） |
| `--user` | — | `tencentroot` | 数据库用户名 |
| `--password` | — | `""` | 数据库密码 |
| `--db` | — | `None` | 未带 schema 时的兜底数据库名 |
| `--difficulty` | — | 全部 | 按难度等级过滤（可多选）|
| `--pattern` | — | 全部 | 按查询模式过滤（可多选）|
| `--instance` | — | 全部 | 按 instance_id 过滤（可多选）|
| `--limit` | — | `0`（全部） | 只优化前 N 条 |
| `--workers` | — | `0`（每实例一个） | 并行 worker 进程数上限 |
| `--no-validate` | — | 关闭 | 优化后不做验证（默认开启验证：单线程重跑每个 rollout 的 best hint 出 CSV） |
| `--validate-only` | — | 关闭 | 只验证不优化：复用 `[mcts].output_dir` 落盘的 JSON 重跑 best hints |
| `--input-dir` | — | — | `--validate-only` 模式读取的 MCTS 输出 JSON 目录 |
| `--output-csv` | — | 自动命名 | 验证 CSV 输出路径（默认 `benchmark/results/` 下） |
| `--validate-baseline` | — | 关闭 | 验证阶段额外重测 baseline（无 hints） |
| `--validate-timeout` | — | `0` | 验证单次 EXPLAIN ANALYZE 超时（秒），0=自动(baseline×1.1) |
| `--validate-warmup` | — | `0` | 验证每条 SQL 正式计时前的预热轮数 |

**查询标记：** 每条查询按 `q0001`、`q0002`… 编号（零填充 4 位）并入 digest，写进结果 JSON 文件名，便于定位。

**输出对齐：** 每条查询打印两行（结果行 + `Best:` 行，含 best hints），与 TPC-DS Runner 一致；启动 / 结束 banner 打印关键配置与 MCTS 结果目录。优化结束后默认进入与 TPC-DS 相同的**单线程验证阶段**（见 [2.2](#22-验证阶段单线程重跑)），逐 rollout 打印「原结果Sum → 单线程Sum」变化。

### 5.3 实例映射（instance_lookup.py / instance_lookup_table.txt）

`instance_lookup_table.txt` 记录了 benchmark instance_id → 克隆实例 IP/Port 的映射，`instance_lookup.py` 提供程序化查询接口：

```python
from mcts_scripts.benchmark.instance_lookup import InstanceLookup

lookup = InstanceLookup()
info = lookup.get("0802c835-1d11-11f1-aada-b8cef6dc748f")
# info.ip, info.port, info.host ("ip:port")
```

### 5.4 采样新评测集（benchmark_sampler.py）

若需从原始 benchmark JSON 中采样生成新的 Core-Set，可使用 `benchmark_sampler.py`：

```bash
python mcts_scripts/benchmark/benchmark_sampler.py \
    --input  data/benchmark_raw.json \
    --output benchmark/core_set_1000_new.json \
    --target 1000 \
    --seed 42
```

采样策略分三阶段：贪心集合覆盖（确保 instance 多样性）→ 稀疏 bucket 补充 → 分层配额采样，保证难度分布均衡。

---

## 典型工作流

```
          ┌─────────────────────────────────────┐
          │  任意 SQL 负载（Slow Log / 业务 SQL）│
          └─────────────────┬───────────────────┘
                            │ my_queries.txt
                            ▼
             ┌──────────────────────────────┐
  Step 1     │  mcts_parallel_runner.py     │
  执行优化   │  多进程 MCTS，生成 JSON 文件  │
             └──────────────┬───────────────┘
                            │
                   MCTS JSON 结果目录
                  /path/to/mcts/output/
                            │
          ┌─────────────────┼──────────────────────────┐
          ▼                 ▼                          ▼
 ┌─────────────────┐  ┌───────────────────┐  ┌────────────────────────┐
 │ mcts_analyze.py │  │ analyze_mcts      │  │ run_sft.py             │
 │                 │  │ _nodes.py         │  │ run_posterior_         │
 │ 汇总报表 / CSV  │  │                   │  │ correction.py          │
 │ 加速比 / 费用   │  │ 7 大分析板块      │  │ SFT / 订正样本         │
 └─────────────────┘  └───────────────────┘  └────────────────────────┘
     Step 1a (通用)       Step 1b                  Step 1c


          ┌──────────────────────┐
          │  TPC-DS 数据库实例    │
          └──────────┬───────────┘
                     │
      ┌──────────────────────────────────────┐
      │  tpcds_runner/                       │
      │  test_tpcds_queries_parallel.py      │
      │  多进程 MCTS 搜索，生成 JSON 结果文件 │
      └──────────────┬───────────────────────┘
                     │
            MCTS JSON 结果目录
           /path/to/mcts/output/
                     │
          ┌──────────┴──────────┐
          ▼                     ▼
 ┌─────────────────┐  ┌───────────────────┐
 │ analyze_tpcds   │  │ analyze_mcts      │
 │ _results.py     │  │ _nodes.py         │
 │                 │  │                   │
 │ 汇总报表 / CSV  │  │ 7 大分析板块      │
 └─────────────────┘  └───────────────────┘
     Step 2a (TPC-DS)    Step 2b


          ┌────────────────────────────────────────────┐
          │  Core-Set Benchmark（系统性效果评测）       │
          │  benchmark/core_set_1000_cdb.json 等       │
          └─────────────────────┬──────────────────────┘
                                │
              ┌─────────────────────────────────────┐
              │  benchmark_runner.py                │
              │  按实例并行 MCTS，自动查表 IP/Port   │
              │  支持按难度/模式/实例过滤            │
              └─────────────────────────────────────┘
```
