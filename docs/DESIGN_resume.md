# 断点续传设计

## 信号处理

`execute()` 入口安装 executor 级 SIGUSR1 handler，整个方法生命周期内 SIGUSR1 只设 flag 不杀进程。Pool manager 在 pool 执行期间临时替换为自己的 handler（terminate pool），完成后恢复 executor handler。finally 中恢复原始 handler。

```
execute() 入口 → 安装 executor handler (设 flag)
  → 预处理 (steps 2-7.6): 即使收到信号也跑完，完成后检查 flag
  → pool_manager.execute_parallel(): pool manager 临时替换 handler → 恢复
  → 规则应用 (steps 13-14): executor handler 生效，不中断
  → finally: 恢复原始 handler
```

| 阶段 | 收到 SIGUSR1 的行为 |
|---|---|
| 预处理 | 设 flag，不中断。预处理完成后检查 flag，返回 INTERRUPTED |
| Pool 执行 | Pool manager handler 生效，terminate pool |
| 规则应用 | Executor handler 生效，设 flag 但不中断，正常走完 |

预处理阶段不中断，保证 workload 完整存储。

## ExecutionStatus 与 ExecutionStage

```python
class ExecutionStatus(enum.Enum):
    COMPLETED = "completed"     # 全部完成
    INTERRUPTED = "interrupted" # 被外部信号中止，可续传
    FAILED = "failed"           # 异常退出

class ExecutionStage(enum.Enum):
    INITIALIZING = "initializing"         # 连接建立、特征检测
    LOADING_WORKLOAD = "loading_workload" # workload 预处理/加载/过滤
    OPTIMIZING = "optimizing"             # 并行优化
    APPLYING = "applying"                 # 规则应用
    FINISHED = "finished"                 # 全部完成
```

`stage` 跟踪代码执行位点，随代码推进自然更新。`status` 反映最终结果。两者解耦：pool 被中止后规则应用仍然完成 → `stage=FINISHED, status=INTERRUPTED`。

## ExecutorResult

```python
class ExecutorResult(BaseModel):
    task_id: str
    status: ExecutionStatus          # 续传决策依据
    duration_seconds: float
    stats: ExecutionStats
    error: str | None                # FAILED 时的错误信息
    extra: dict                      # 可观测性: stage, interrupt_stage, prev_task_ids
```

- `extra["stage"]`：最终执行位点（始终设置）
- `extra["interrupt_stage"]`：中断/异常发生时的阶段（仅 INTERRUPTED/FAILED 时设置）
- `extra["prev_task_ids"]`：续传来源任务链 [最近 → 最早]（仅 `allow_resume=true` 且检测到可续传任务时设置）

## 续传机制

### 触发条件

管控传入 `options.allow_resume=true` 时，执行器在初始化阶段查询 `task_execution_history`：

```sql
SELECT task_id, status FROM task_execution_history
WHERE instance_id = :instance_id AND node_uuid = :node_uuid
  AND created_at >= NOW() - INTERVAL :days DAY
ORDER BY created_at DESC LIMIT 1
```

仅当该节点最近一条记录状态为 `interrupted` 时返回其 `task_id`（触发续传）。
如果最近一条记录是 `completed` 或 `failed`，返回 None（不跳过它去找更早的 interrupted 记录）。

按 `(instance_id, node_uuid)` 定位节点：同一实例不同节点的 workload 来源不同，不能跨节点续传。

### 续传执行流程

1. 使用新 `task_id`（每次执行始终使用新 task_id）
2. 加载最新工作负载（总是从 SlowLog/AWR 获取，不复用上次的 workload）
3. 查询整条中断链在 rules 表中已处理的 `(db, digest)` 集合
4. 从当前 workload 中过滤掉已处理模板
5. 并行优化剩余模板
6. 应用本次生成的规则到主节点

### 关键简化

当 `status=INTERRUPTED` 时，所有已生成的规则都已被应用——优化被中断后代码仍会继续执行 APPLYING 阶段。因此续传任务只需关心 workload 过滤，不需要规则合并和已应用规则过滤。

### 链式续传

连续多次中断时，续传任务会收集完整的 INTERRUPTED 任务链，合并所有已处理模板：

```
task_1: 处理 A, B → INTERRUPTED
task_2: 续传 task_1, 处理 C, D → INTERRUPTED
task_3: 续传 task_2, get_interrupted_chain → [task_2, task_1]
         → get_processed_templates_batch([task_2, task_1]) → {A, B, C, D}
         → 只需处理 E
```

实现：
1. `get_interrupted_chain()` 从 `task_execution_history` 按 `created_at DESC` 遍历，遇到非 INTERRUPTED 状态停止，返回 task_id 列表
2. `get_processed_templates_batch()` 使用 `IN :task_ids` 批量查询 rules 表
3. `result.extra["prev_task_ids"]` 记录完整链 [最近 → 最早]，用于可观测性和审计

## 管控层参考

**续传决策**基于 `status`：

| status | 管控动作 |
|---|---|
| COMPLETED | 无需操作 |
| INTERRUPTED | 可续传（使用新 task_id + `options.allow_resume=true`） |
| FAILED | 排查错误 |

**可观测性**（`extra.stage` / `extra.interrupt_stage`，用于日志排查）：

| status | stage | interrupt_stage | prev_task_ids | 含义 |
|---|---|---|---|---|
| COMPLETED | finished | - | - 或 [task_ids] | 全部完成（有 prev_task_ids 表示是续传完成） |
| INTERRUPTED | loading_workload | loading_workload | - 或 [task_ids] | 信号在预处理后到达 |
| INTERRUPTED | finished | optimizing | - 或 [task_ids] | pool 被中止，规则应用已完成 |
| FAILED | (异常时的阶段) | (同左) | - 或 [task_ids] | 对应阶段异常 |
