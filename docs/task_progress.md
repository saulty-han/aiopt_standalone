# 训练进度查询

## 查询方式

```sql
SELECT task_id, stage, total_templates, completed_templates, updated_at
FROM task_progress
WHERE task_id = ?;
```

## 字段解读

`total_templates` 和 `completed_templates`：

| 条件 | 含义 |
|------|------|
| 两者均为 NULL | 任务处于早期阶段（初始化/加载工作负载），模板数量尚未统计 |
| 两者非 NULL | 进度 = `completed_templates / total_templates` |

注意：任务结束时进度不一定是 100%（中断或失败时 `completed_templates` 可能小于 `total_templates`）。判断任务是否结束需查 `task_execution_history`，见下文。

## 判断任务是否结束

`task_progress` 不记录终止状态。需查 `task_execution_history`：

```sql
SELECT status FROM task_execution_history WHERE task_id = ? LIMIT 1;
```

- 无记录 — 任务尚未正常终止（可能仍在运行，也可能异常退出未写入记录）
- `completed` — 所有模板已处理，规则已应用
- `interrupted` — 被 SIGUSR1 信号中断，已处理的规则仍会被应用，可通过续传恢复
- `failed` — 任务异常失败

## 综合查询示例

将进度与最终状态关联查询：

```sql
SELECT
    p.task_id,
    p.stage,
    p.total_templates,
    p.completed_templates,
    h.status,
    p.updated_at
FROM task_progress p
LEFT JOIN task_execution_history h ON p.task_id = h.task_id
WHERE p.task_id = ?;
```

`h.status` 为 NULL 表示任务仍在运行，此时 `completed_templates / total_templates` 即为实时进度。任务结束后（`h.status` 非 NULL），`completed_templates` 可能小于 `total_templates`（如被中断或失败）。
