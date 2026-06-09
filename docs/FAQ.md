# FAQ

---

## 元数据表：rules / validation_logs / rule_state_history

### 三张表各自的职责

| 表 | 定位 | 写入时机 |
|----|------|---------|
| `validation_logs` | **过程记录** — 每一次候选计划的评估尝试 | 优化阶段 |
| `rules` | **决策结论** — 每个 SQL 模板的最终优化决策 | 优化阶段 |
| `rule_state_history` | **操作日志** — 规则在用户实例上的实际变更记录 | 应用阶段 |

数据流方向：

```
优化阶段
    ├─→ validation_logs   （所有评估尝试）
    └─→ rules             （最终决策）
                │
                ▼
应用阶段
                │
                └─→ rule_state_history  （实际操作记录）
```

### 三张表的关联关系

三张表之间**不存在外键约束**，通过 `(instance_id, db, digest)` / `task_id` 等字段逻辑关联，各自独立写入、独立查询。

对于同一个 SQL 模板 `(db, digest)` 的一次训练任务：

| validation_logs | rules | rule_state_history |
|----------------|-------|--------------------|
| 0 ~ N 条（每个候选计划 × 每个样本 SQL = 一条） | 1 条 DEFAULT 或 1 ~ N 条 OPTIMIZE | 1 条（记录本次实际执行的操作） |

---

### Q: validation_logs 是否记录所有验证记录？

**是的。** 记录每一次候选计划的执行尝试，无论结果好坏、是否超时，全部写入。目的是提供完整的评估审计记录，用于调试和分析。

### Q: validation_logs 和 rules 总是关联在一起的吗？

**不是。** 两者独立存储，以下不对应场景均属正常：

- **有 validation_logs 但无对应 OPTIMIZE rule（常见）** — 评估了多个候选计划，只有最优的变成 rule，其余 log 没有对应的 rule
- **有 DEFAULT rule 但无 validation_logs（少见）** — 优化器在早期阶段就确定无候选计划，直接返回 DEFAULT rule，不产生 log
- **validation_logs 中 `is_best=True` 的记录无对应 rule** — 可靠性过滤可能丢弃原本胜出的 OPTIMIZE rule，但不修改已生成的 log。详见下方"可靠性过滤"

### Q: 会不会有孤立的 validation_logs（无对应 rule）？

**会，这是设计预期。** 产生场景包括：

1. **被淘汰的候选计划** — 评估了多个候选，只有最优的变成 rule
2. **默认测量失败后的部分记录** — 默认计划测量异常时，已产生的部分 log 仍会存储
3. **可靠性过滤淘汰的 rule** — OPTIMIZE rule 因验证数据不可靠被过滤，对应 log 保留
4. **全部回退为 DEFAULT** — 所有 OPTIMIZE rule 均不可靠，回退为 DEFAULT，原有 log 保留

这些孤立记录是有价值的——它们记录了"为什么没有选择这个计划"，对调试至关重要。

### Q: rules 和 rule_state_history 的区别是什么？

| 维度 | rules | rule_state_history |
|------|-------|--------------------|
| **写入时机** | 优化阶段 | 应用阶段 |
| **语义** | "优化器认为应该怎么做" | "实际在用户实例上做了什么" |
| **写入方式** | 按 task_id 追加写入 | 追加写入，保留完整历史 |
| **操作粒度** | `optimize` / `default` | `setup_plan` / `modify_plan` / `modify_timeout` / `reset` / `noop` |
| **变更对比** | 不记录 | 记录 `prev_plan_ids` → `curr_plan_ids` |

典型流转：

```
rules 中产生 action=optimize, plan_id=P1
    │
    ▼  应用阶段查询 rule_state_history 获取当前状态，决定操作
    │
    ├── 当前无规则      → setup_plan,   curr=["P1"]
    ├── 当前已有 P1     → noop（幂等跳过）
    ├── 当前已有 P2     → modify_plan,  prev=["P2"], curr=["P1"]
    └── 本次为 DEFAULT  → reset,        prev=["P2"], curr=null
```

---

## 可靠性过滤（Reliability Filter）

> 对应 commit: `feat(optimizer): filter unreliable OPTIMIZE rules with low default elapsed time`

### Q: 可靠性过滤会不会破坏 rules 和 validation_logs 的一致性？

**不会。** 两者之间本就没有强一致性约定。

可靠性过滤在决策完成后、返回结果前，丢弃验证数据不可靠的 OPTIMIZE rule（所有 validation 记录的 `default_elapsed_time` 均低于 `min_query_time`）；若全部被丢弃，回退为 DEFAULT rule。已生成的 `evaluated_logs` 不做修改。

这符合既有约定——代码库中已有多处"有 validation_logs 但 rule 为 DEFAULT"的路径，可靠性过滤只是新增了一个同类路径。

### Q: 为什么不在过滤后同步修正 evaluated_logs 中的 is_best 标记？

1. **无实际影响**。`is_best` 虽被部分查询 SELECT，但不用于排序、过滤或业务判断。
2. **保持过程记录的真实性**。`is_best` 反映的是"评估阶段的局部最优"，而非"最终决策"。修正它会让 validation_logs 从客观过程记录变成事后篡改的结果记录，降低调试价值。

---

## 设计原则

1. **validation_logs 是不可变的过程快照** — 一旦写入不修改、不删除，忠实记录所有评估尝试
2. **rules 按 task_id 隔离** — 每次训练追加写入，不同训练任务对同一 SQL 模板各自保留独立记录
3. **rule_state_history 是追加式操作日志** — 记录每次规则变更的前后状态，支持时间线回溯和幂等判断
4. **三者松耦合** — 通过共享字段逻辑关联，不强制数据一致性，各自服务于不同目的（调试 / 决策 / 操作审计）
