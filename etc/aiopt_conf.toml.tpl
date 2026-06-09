# AI Optimizer Configuration Template
# Copy this file to aiopt_conf.toml and fill in actual values

[oss]
oss_web_host = "127.0.0.1:8080"

[slowlog_clickhouse]
test_clickhouse_host = ""
gz_clickhouse_host = ""
bj_clickhouse_host = ""
sh_clickhouse_host = ""
sg_clickhouse_host = ""
user = "root"
password = ""

[awr_clickhouse]
test_clickhouse_host = ""
gz_clickhouse_host = ""
bj_clickhouse_host = ""
sh_clickhouse_host = ""
sg_clickhouse_host = ""
user = "root"
password = ""

[meta_server]
ip = "127.0.0.1"
port = 9999
user = "xxxx"
password = "xxxx"
database = "xxxx"

[logger]
log_level = "DEBUG"
log_dir = "logs"
max_bytes = 1073741824                       # 单个日志文件最大大小 (1GB)
backup_count = 10                            # 保留的旧日志文件数量

[optimizer]
optimizer_type = "small_model"               # 优化器类型: small_model, llm

# Training parameters (migrated from ai_config.py)
[training]
max_allowed_sql_length = 10176
index_hints_enumeration_limit = 1000
with_ignore_index_hints = true
default_plan_timeout_seconds = 60.0          # 默认执行计划评估超时（秒），超时则跳过该模板
better_plan_ratio = 0.2
feedback_timeout_rate = 1.1
# Parallel optimization
parallel_workers = 1                         # 并行 worker 数量 (1-16)
sql_template_limit_per_task = 100            # 单次任务最大训练 SQL 模板数 (0=不限制)

[workload]
window_days = 30                             # Slowlog/AWR 时间窗口（天）
min_query_time = 0.1                         # 最小查询时间过滤（秒）

[perfmon]
cluster_id = 0                               # 本集群的 cluster ID，用于在性能监控中标识集群
force_awr_perfmon = false                    # NOTE: 目前性能监控统一使用 AWR 数据源，这个配置项无效

# Scheduler configuration (管控层超时参数，由外部调度器使用)
# 执行模块不读取这些参数，管控层自行决定超时策略并通过 SIGUSR1 信号控制任务中止
# [scheduler]
# task_timeout_seconds = 3600.0              # 整体任务超时 (1h)，超时后管控发送 SIGUSR1
# slave_timeout_seconds = 1200.0             # Slave 环境超时 (20min)，超时后管控发送 SIGUSR1 并切换至 CLONE 环境续传

# Mock configuration (for local development/testing only)
# Uncomment this section to enable mock features
# [mock]
# interface_mocking_enabled = false          # 启用 mock 接口进行测试
# allow_mock_sources = false                 # 允许使用 mock 数据源 (AWR_TABLE)
# test_ck_cluster_id_override = 101          # 覆盖 test region 的 ClickHouse cluster ID（仅测试环境使用）

# LLM 优化器（MCTS）配置
[mcts]
# LLM API 资源池：三元组列表 [url, key, model]，可多组；示例见下行注释
# llm_api_url_key = [
#     ["https://api1.example.com/v1/chat/completions", "sk-key1", "deepseek-v3-0324"],
#     ["https://api2.example.com/v1/chat/completions", "sk-key2", "deepseek-v3-0324"],
# ]
llm_api_url_key = []
custom_cfg = "mcts/config/mcts_defaults.yaml"                    # MCTS YAML 配置路径（搜索/采样等）
output_dir = "mcts/eval_data/"                                   # MCTS 输出 JSON 目录（空字符串表示关闭）
iterations = 9                                                   # MCTS rollout 轮数
max_depth = 3                                                    # 单个 rollout 的最大 step 数；同时在 limit_global_depth=true 时也限制全局树深
limit_global_depth = false                                       # false (默认)：max_depth 同时限制 "全局树深度" 和 "单轮 step 数"；false：只限制 "单轮 step 数"，树本身可跨多轮不断加深
explain_timeout_seconds = 30.0                                   # 取 plan digest 的 EXPLAIN（非 EXPLAIN ANALYZE）墙钟超时（秒），默认 30.0
stop_mcts_search_plan_time_threshold_seconds = 0                 # 默认 0.1（秒）；<=0 关闭该提前结束条件
stop_mcts_search_estimated_tokens_budget = 0                     # 整数；0 表示不限制；>0 为预估 tokens 上限 (输入+输出字符)/2.5，超限则提前结束
include_early_stopping_metrics = false                           # 是否在 MCTS JSON 里输出 early_stopping_metrics（默认 on）
include_explain_analyze_info = false                             # 是否在 MCTS JSON 里输出 explain_analyze_info（按 plan_digest 聚合的 EXPLAIN ANALYZE；默认 off，体积较大）
remote_cache_enabled = true                                      # 是否启用 remote query_cache（{db}_cache.query_cache）；默认 on。关掉后 DBExecutor 不再读写远端 cache，内存 PlanDigestCache 仍然生效
remote_cache_timeout_seconds = 600                               # 单次 EXPLAIN ANALYZE 的墙钟上限（秒），同时作为 query_cache.timeout_time 的写入值；默认 600
cap_cache_timeout_by_baseline = true                             # 开启后，baseline probe 完成后将 remote cache timeout 收紧为 min(remote_cache_timeout_seconds, probed_baseline_time)，避免等待明显劣于 baseline 的执行计划直到全局上限才超时