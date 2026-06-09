# HOWTO: AI Optimizer 使用说明

## 1. 功能模块

AI Optimizer 由以下模块组成：

- 训练任务执行器：负责执行训练任务
- 性能监控：负责监控性能指标

## 2. 环境准备

### 运行环境
- Python 3.10, 部署方法可参考 <https://iwiki.woa.com/p/4015208104>
- 安装依赖：`pip install -i "https://mirrors.tencent.com/pypi/simple" -r requirements.txt`

### 配置文件
```bash
cp etc/aiopt_conf.toml.tpl etc/aiopt_conf.toml
# 编辑 etc/aiopt_conf.toml 填入实际配置值
```

需要配置的主要内容：元信息数据库连接 (`[meta_server]`)、ClickHouse 各地域连接 (`[slowlog_clickhouse]`/`[awr_clickhouse]`)、训练参数 (`[training]`)、性能监控参数 (`[perfmon]`)。详见模板文件中的注释说明。

### 数据库表初始化
按照 `schema/*.sql` 初始化数据库表和相关视图。

---

## 3. 训练任务执行器

### 前提条件

- 元信息数据库连接正常
- 管控 CK 采集模块工作正常，ClickHouse（SlowLog 或 AWR）可达，用于加载工作负载
- 训练环境（slave/ro/clone）可达
- 主节点（master）可达，用于规则应用

### 调用方式

```bash
# 从 JSON 文件读取输入
python executor.py --input input.json

# 从标准输入读取 JSON
echo '{"task_id": "...", ...}' | python executor.py --stdin
```

### 输入格式

```json
{
  "task_id": "task-001",
  "operator": "scheduler",
  "env_type": "slave",
  "env_config": {
    "node_ip": "[IP_ADDRESS]",
    "node_port": 3306,
    "node_uuid": "node-uuid-training",
    "username": "tencentroot",
    "password": ""
  },
  "master_node": {
    "node_ip": "[IP_ADDRESS]",
    "node_port": 3306,
    "node_uuid": "node-uuid-master",
    "username": "tencentroot",
    "password": ""
  },
  "instance_info": {
    "cluster_id": 1,
    "product_type": "ncdb",
    "instance_id": "instance-001",
    "node_uuid": "node-uuid-training",
    "workload_source": "awr",
    "outline_type": "spm",
    "region": "test"
  },
  "workload_set": null,
  "options": {
    "allow_resume": true,
    "resume_expiration_days": 7
  }
}
```

**字段说明：**

| 字段 | 类型 | 说明 |
|------|------|------|
| `task_id` | string | 任务唯一标识 |
| `operator` | string | 操作人标识 |
| `env_type` | `slave` / `ro` / `clone` | 训练环境类型 |
| `env_config` | object | 训练环境数据库连接（node_ip, node_port, node_uuid, username, password） |
| `master_node` | object | 主节点连接（格式同上），用于规则应用 |
| `instance_info.cluster_id` | int | 集群 ID |
| `instance_info.product_type` | `cdb` / `ncdb` | 产品类型，仅影响工作负载读取接口；优化能力通过 feature set 自动检测 |
| `instance_info.workload_source` | `slow_log` / `awr` | 工作负载数据源 |
| `instance_info.outline_type` | `statement_outline` / `spm` | Outline 类型 |
| `instance_info.region` | `bj` / `sh` / `gz` / `sg` / `test` | 地域 |
| `workload_set` | list 或 null | 增量训练 SQL 集 `[["db", "digest"], ...]`，null 表示全量训练 |
| `options` | object 或 null | 可选参数，省略或 null 表示全部使用默认值 |
| `options.allow_resume` | bool | 是否开启断点续传（默认 false），开启后自动检测上次 INTERRUPTED 任务并过滤已处理模板 |
| `options.resume_expiration_days` | int | 续传状态有效期（默认 7 天），超过此天数的 INTERRUPTED 记录不触发续传 |
| `options.sql_template_limit` | int 或 null | 最大训练模板数，null 或不传表示使用配置文件默认值，0 表示不限制。优先级: 任务输入 > 配置文件 `[training] sql_template_limit_per_task` |

### 输出格式

执行器将结果以 JSON 输出到 stdout：

```json
{
  "task_id": "task-001",
  "status": "completed",
  "duration_seconds": 120.5,
  "stats": {
    "total_templates": 10,
    "processed_templates": 10,
    "rules_generated": 5,
    "rules_applied": 3
  },
  "extra": {
    "stage": "finished"
  }
}
```

| 字段 | 说明 |
|------|------|
| `task_id` | 任务 ID |
| `status` | 执行终止状态：`completed`（成功）/ `interrupted`（被 SIGUSR1 中止，可续传）/ `failed`（异常） |
| `duration_seconds` | 执行总耗时（秒） |
| `stats.total_templates` | 总 SQL 模板数 |
| `stats.processed_templates` | 已处理的 SQL 模板数 |
| `stats.rules_generated` | 生成的优化规则数 |
| `stats.rules_applied` | 成功应用到在线实例的规则数 |
| `extra.stage` | 最终执行阶段（`initializing` / `loading_workload` / `optimizing` / `applying` / `finished`） |
| `extra.interrupt_stage` | 中断发生时的阶段（仅 `interrupted` / `failed` 时存在） |
| `extra.prev_task_ids` | 续传来源任务链 [最近 → 最早]（仅 `allow_resume=true` 且检测到可续传任务时存在） |
| `extra.error` | 错误信息（仅 `failed` 时存在） |

### 退出码

| 退出码 | 说明 |
|--------|------|
| 0 | 执行成功 |
| 1 | 执行失败 |

### 执行流程

1. 校验 JSON 输入（Pydantic 自动验证）
2. 若 `options.allow_resume=true`，查询 `task_execution_history` 检测该节点最近一次 INTERRUPTED 任务
3. 从 SlowLog/AWR 加载并聚合最新工作负载（续传时同样加载最新负载）
4. 自动检测训练环境的数据库 feature set（hints 提取、SPM、Statement Outline 等能力）
5. 过滤黑名单 SQL 模板
6. 若检测到可续传任务，过滤其已处理的模板（查 rules 表），仅保留未处理模板
7. 按 `(instance_id, db, digest)` 分组，并行优化各 SQL 模板
8. 将生成的优化规则应用到主节点
9. 输出 JSON 结果

### 信号控制

管控可通过 SIGUSR1 信号优雅中止正在运行的任务：

```bash
kill -SIGUSR1 <executor_pid>
```

收到信号后，执行器立即终止所有正在运行的 worker 进程，停止优化阶段。在信号到达前已完成优化的规则会正常保存并应用到主节点。输出结果中 `status` 为 `"interrupted"`，`extra["interrupt_stage"]` 指示中断发生时的执行阶段。

管控可使用新 `task_id` 加 `options.allow_resume: true` 发起续传：

```bash
echo '{"task_id": "task-002", "options": {"allow_resume": true}, ...}' | python executor.py --stdin
```

续传时执行器自动检测该节点最近一次 INTERRUPTED 任务，加载最新工作负载并过滤已完成的模板，仅优化剩余模板并应用规则。

---

## 4. 性能监控

### 概述

性能监控模块定期采集各 SQL 模板在规则变更前后的 AWR 性能指标，用于评估优化效果。

### 前提条件

- 元数据库中表和视图已初始化
- AWR ClickHouse 可达：性能监控统一使用 AWR 作为数据源

### 调用方式

```bash
# 生产环境：从管控接口获取本集群所有实例
python perfmon/update_metrics.py --from-api

# 测试阶段：从 JSON 文件读取实例列表
python perfmon/update_metrics.py --instances-file instances.json
```

> **注意：生产环境必须使用 `--from-api` 从管控接口获取实例列表。`--instances-file` 仅用于本地开发和测试，不得在生产环境使用！**

`--instances-file` 输入格式：

```json
[
  {
    "cluster_id": 1,
    "product_type": "ncdb",
    "instance_id": "instance-001",
    "node_uuid": "node-uuid-001",
    "region": "sh"
  }
]
```

### 退出码

| 退出码 | 说明 |
|--------|------|
| 0 | 全部成功 |
| 1 | 部分实例处理失败 |
| 2 | 未预期的错误 |
| 3 | 管控接口未实现（NotImplementedError） |

### 定时任务配置

**管控需设定定时任务定期执行性能监控更新：** 每天执行一次即可。

### 监控原理

性能监控以规则变更事件作为时间段分界点，对比各时间段内的 SQL 性能指标：

- **Period 0（基线期）**：首次规则变更前 30 天
- **Period 1 ~ N-1**：相邻规则变更之间（已固化，不再更新）
- **Period N（当前开放期）**：最后一次规则变更至今（每次运行时刷新）

### 查询监控结果

管控通过 `perf_result` 视图查询实例的性能监控数据：

```sql
SELECT * FROM perf_result
WHERE node_uuid = '<node_uuid>'
  AND operation != 'reset';
```

**注意事项：**

1. 查询时需过滤 `operation != 'reset'`。reset 操作表示 AI 优化器认为默认计划最优而撤销了先前的优化计划，不产生验证日志，无执行时间对比数据。
2. `optimized_status` 表示一条优化规则的当前生效状态（1=生效, 0=失效或者被禁用）。
  - 最近的一条查询结果中的 `optimized_status` 也可能为 0
    - 一种场景是，规则被用户手动禁用，处于黑名单中；
    - 另一种场景是，AI 优化器认为默认计划最优而撤销了先前的优化计划，此时实际的规则变更序列为 `optimize -> reset`，当前生效的是 reset 操作（已被查询过滤），而结果中展示的是上一次的 optimize 操作。
3. 执行时间对比（`query_time_before/after`）来自训练时的验证日志（单次样本），扫描行数对比（`sql_scan_rows_before/after`）来自 AWR 聚合的周期平均值。

`perf_result` 视图主要字段：

| 字段 | 说明 |
|------|------|
| `instance_id`, `db`, `digest` | SQL 模板标识 |
| `digest_text` | 归一化 SQL 模板 |
| `task_id` | 关联的训练任务 ID |
| `operation` | 操作类型：setup_plan / modify_plan / reset |
| `optimized_status` | 优化生效状态：1=生效, 0=失效 |
| `optimized_time` | 规则生效时间 |
| `query_time_before` / `query_time_after` | 优化前后执行时间 |
| `query_time_diff_rate` | 执行时间降低百分比（正数=优化） |
| `sql_scan_rows_before` / `sql_scan_rows_after` | 优化前后扫描行数 |
| `sql_scan_rows_diff_rate` | 扫描行数降低百分比（正数=优化） |
| `optimized_sql_count` | 当前周期执行次数 |
| `period_status` | 周期状态：固化 / 开放 |
| `is_blacklisted` | 是否在黑名单中 |
