# 可观测性内部表

本文档描述 AI 优化器内部的可观测性表，不作为公开接口。

---

## task_execution_history 表

### 用途

在任务执行结束之后，记录任务的输入和结果，供查看任务执行历史、任务续传可行性判断、收集续传任务链等场景使用。
注：仅在任务执行完之后才会记录，未完成的任务无法在这张表当中查到。

### 表结构

| 字段 | 类型 | 说明 |
|------|------|------|
| `id` | BIGINT AUTO_INCREMENT | 主键 |
| `task_id` | VARCHAR(100) | 任务 ID |
| `instance_id` | VARCHAR(100) | 实例 ID |
| `node_uuid` | VARCHAR(100) | 节点 UUID |
| `status` | ENUM('completed','interrupted','failed') | 执行终止状态 |
| `duration_seconds` | DOUBLE | 执行耗时（秒） |
| `executor_input` | JSON | 完整输入参数（密码已脱敏） |
| `executor_result` | JSON | 完整输出结果 |
| `created_at` | DATETIME(3) | 记录创建时间 |

索引：`idx_task_id(task_id)`、`idx_instance_node(instance_id, node_uuid)`

### 数据更新时机

- **写入时机**：任务执行完毕后的 finally 块中，无论成功、中断还是失败都会写入一条记录
- **写入频率**：每个 task_id 恰好对应 1 条记录。续传时管控使用新的 task_id 发起任务

### 局限性与查询注意事项

1. **进程异常退出不写入**：如果执行器进程被 SIGKILL 杀死或发生 OOM，finally 块不会执行，该次执行将没有记录。此时 `task_progress` 中的记录会停留在最后更新的状态，查询者需要结合进程存活状态判断。
2. **executor_input 中密码已脱敏**：`env_config` 和 `master_node` 中的 `password`、`username`、`user` 字段被移除，不可用于连接重建。
3. **续传查询按 (instance_id, node_uuid) 定位**：同一实例不同节点的 workload 来源不同，不能跨节点续传。查询时需同时指定两个字段。
4. **续传只看最近一条记录**：如果最近一条是 `completed` 或 `failed`，即使更早有 `interrupted` 记录也不触发续传。这是设计意图——新的完成/失败记录覆盖了续传需求。
5. **JSON 列无索引**：`executor_input` 和 `executor_result` 为 JSON 类型，不支持高效条件查询。按需解析时使用 `JSON_EXTRACT()`。

---

## task_progress 表

### 用途

粗略记录训练任务的执行进度，由于存在各种 corner case，不保证可靠性。

### 表结构

| 字段 | 类型 | 说明 |
|------|------|------|
| `task_id` | VARCHAR(100) | 主键 |
| `instance_id` | VARCHAR(100) | 实例 ID |
| `node_uuid` | VARCHAR(100) | 节点 UUID |
| `stage` | VARCHAR(50) | 当前阶段：initializing / loading_workload / optimizing / applying / finished |
| `total_templates` | INT / NULL | 总模板数（NULL = 尚未统计，如 loading_workload 阶段） |
| `completed_templates` | INT / NULL | 已完成模板数（含续传已处理数） |
| `started_at` | DATETIME(3) | 首次插入时间 |
| `updated_at` | DATETIME(3) | 最后更新时间 |

索引：PK(task_id)、`idx_instance_node(instance_id, node_uuid)`

### 数据更新时机

执行器在以下 **5 个时间点** 更新进度：

| 更新点 | stage | total_templates | completed_templates |
|--------|-------|-----------------|---------------------|
| 进入 LOADING_WORKLOAD | `loading_workload` | NULL | NULL |
| 进入 OPTIMIZING（优化开始前） | `optimizing` | 总数 | 续传已处理数 |
| 每完成一个 SQL 模板 | `optimizing` | 总数 | 续传已处理数 + 已完成数 |
| 进入 APPLYING | `applying` | 总数 | 已处理总数 |
| finally 块（最终状态） | 最终 stage | 最终 total | 最终 processed |

**更新方式**：INSERT ... ON DUPLICATE KEY UPDATE（upsert），每次覆盖 stage/total/completed。

### 局限性与查询注意事项

1. **不记录终止状态**：task_progress 只记录执行阶段，不记录 completed/interrupted/failed。判断任务是否结束需查 `task_execution_history`。
2. **进度不一定到 100%**：中断或失败时 `completed_templates` 可能小于 `total_templates`。
3. **进程异常退出时数据陈旧**：如果进程被 SIGKILL，finally 块不执行，task_progress 停留在最后一次成功更新的状态。查询者需结合 `updated_at` 时间戳判断是否陈旧（如 updated_at 超过合理时间未更新，可能进程已死）。
4. **INITIALIZING 阶段无记录**：首次写入发生在 LOADING_WORKLOAD 阶段。如果任务在 INITIALIZING 阶段失败，task_progress 中无记录。
5. **进度更新失败不影响主流程**：更新失败仅记录 warning 日志，不中断任务执行。因此 task_progress 数据是 best-effort 的。
6. **total_templates 和 completed_templates 均为 NULL 时**：表示任务处于早期阶段（loading_workload），模板数量尚未统计，不可用于计算进度百分比。

### 综合查询示例

详见 [`docs/task_progress.md`](./task_progress.md)。
