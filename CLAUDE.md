# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## 项目概述

AI Optimizer — 基于 TXSQL 内核的 SQL 查询优化器。通过分析 workload 中的 SQL 模板，枚举候选执行计划，评估后将最优计划以 SPM 或 Statement Outline 规则写入在线实例。

## 核心执行流程

`executor.py` 是入口，接收 JSON（`ExecutorInput`），输出 JSON（`ExecutorResult`）。核心 pipeline 在 `task/task_executor.py`：

1. **加载 workload** — 从 ClickHouse（AWR/SlowLog）或本地 IS 表读取 SQL 统计，聚合过滤后存入元数据库
2. **并行优化** — 按 `(db, digest)` 分组，每个模板在 worker 进程中：收集候选计划 → 多样本评估 → 决策最优/重置
3. **应用规则** — 将决策结果写入在线实例（SPM: `dbms_admin.spm_alter_baseline`，Outline: `dbms_admin.statement_outline_add_rule_from_ai`）

支持 SIGUSR1 信号优雅中断，支持断点续传（`options.allow_resume`）。

## 关键模块

- `executor.py` — JSON-in/JSON-out 入口，信号处理，exit code 决策
- `task/task_executor.py` — 14 步 pipeline 编排，resume 逻辑，进度追踪
- `task/parallel_pool_manager.py` — 进程池管理，信号接管/恢复
- `task/parallel_worker.py` — worker 进程内的单模板优化
- `optimizer/basic_optimizer.py` — 候选收集 + 评估 + 决策的基类
- `optimizer/hints_enum_optimizer.py` — 索引 hints 枚举（`small_model` 模式）
- `optimizer/llm_optimizer.py` — MCTS + LLM 候选生成（`llm` 模式）
- `optimizer/spm_operator.py` / `statement_outline_operator.py` — 规则写入
- `workload/` — 多源 workload 加载（AWR ClickHouse、SlowLog ClickHouse、AWR_TABLE mock）
- `perfmon/` — 性能监控，从 ClickHouse 读取规则变更前后的指标对比
- `controller/` — 元数据库 CRUD（workload、rules、validation_logs、execution_history 等）
- `config/config.py` — TOML 配置读取，`GetConf(section, key)` 和 `GlobalConfig` 属性访问
- `data_models.py` — 所有 Pydantic 模型（`ExecutorInput`、`ExecutorResult`、`NodeConfig`、`InstanceInfo` 等）

## 配置

- `etc/aiopt_conf.toml`（从 `.tpl` 复制）— 元数据库连接、优化器类型、训练参数、ClickHouse 连接等
- `tests/profiles.json`（从 `.example` 复制）— E2E 测试用的实例连接配置

## 端到端测试

详见 `tests/CLAUDE.md`，包含运行命令、环境搭建和排查指南。

```bash
python tests/test_e2e.py --profile cdb20260331-spm
```
