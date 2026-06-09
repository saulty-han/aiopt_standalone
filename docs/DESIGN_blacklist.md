# 管控 Disable/Enable SQL 模板完整方案（职责分离版）

## Context

管控系统需要对 SQL 模板执行两种操作：
- **停止优化（Disable）**：禁用某个 SQL 模板的 AI 优化规则，移除在线实例上的规则，更新元信息，阻止后续训练
- **恢复训练（Enable）**：解除禁用，允许后续训练重新优化该模板

当前缺失这一环节，导致：
1. 性能监控周期断裂：开放期无限延伸
2. 下次训练状态不一致：仍认为规则 ACTIVE
3. 审计历史缺口：缺少"被管控移除"事件

---

## 一、规则管理全面梳理

### 1.1 两层模型：逻辑状态 vs 操作记录

- **逻辑状态**：ACTIVE（有活跃规则）或 DEFAULT（无活跃规则/初始状态）
- **操作记录**：`rule_state_history.operation` ENUM，五种

| 操作类型 | 含义 | 结果逻辑状态 | 适用规则类型 |
|---------|------|------------|------------|
| `setup_plan` | 首次设置规则 | ACTIVE | SPM / Outline |
| `modify_plan` | 修改执行计划 | ACTIVE | SPM / Outline |
| `modify_timeout` | 修改超时时间 | ACTIVE | 仅 Outline |
| `reset` | 恢复默认 | DEFAULT | SPM / Outline |
| `noop` | 无操作 | 不变 | SPM / Outline |

**逻辑状态判定**（`rule_state_controller.py:219-222`）：
```python
has_current_rule = (
    current_state is not None and
    current_state.operation in (SETUP_PLAN, MODIFY_PLAN, MODIFY_TIMEOUT)
)
```

### 1.2 状态转移图

```
                     ┌──────────────────────────────────────────────┐
                     │              DEFAULT 逻辑状态                │
                     │  (current_state = None 或 operation = RESET) │
                     └──────┬─────────────────────────┬────────────┘
                            │                         │
                 OPTIMIZE 决策                  DEFAULT 决策
                            │                         │
                            ▼                         ▼
                      SETUP_PLAN                    NOOP
                   "New Optimization"           "Both DEFAULT"
                            │
                            ▼
                     ┌──────────────────────────────────────────────┐
                     │              ACTIVE 逻辑状态                 │
                     │  (operation ∈ {SETUP_PLAN, MODIFY_PLAN,      │
                     │                MODIFY_TIMEOUT})               │
                     └──┬────────┬────────┬────────────┬───────────┘
                        │        │        │            │
              OPTIMIZE  │OPTIMIZE│OPTIMIZE│     DEFAULT 决策
              S1≠S2     │S1=S2   │S1=S2   │
                        │T1≠T2   │T1=T2   │
                        ▼        ▼        ▼            ▼
                  MODIFY_PLAN MODIFY_   NOOP         RESET
                 "Plan Changed" TIMEOUT "No Change" "Reset Requested"
                        │    "T:old→new"  │            │
                        ▼        ▼        │            ▼
                  ┌────────────────┐      │     ┌────────────┐
                  │  仍在 ACTIVE   │      │     │ 回到 DEFAULT│
                  └────────────────┘      │     └────────────┘
                        ▲                 │
                        └─────────────────┘
                       (自环：NOOP 不改变状态)
```

### 1.3 完整决策矩阵（`decide_operation()` L218-246）

| # | 当前逻辑状态 | 本次决策 | 条件 | 返回操作 | 在线实例动作 |
|---|------------|---------|------|---------|------------|
| 1 | DEFAULT | OPTIMIZE | — | `SETUP_PLAN` | reject all + add new |
| 2 | DEFAULT | DEFAULT | — | `NOOP` | 无 |
| 3 | ACTIVE(S1) | OPTIMIZE(S2) | S1 ≠ S2 | `MODIFY_PLAN` | reject all + add new |
| 4 | ACTIVE(S1) | OPTIMIZE(S2) | S1=S2, T1≠T2 | `MODIFY_TIMEOUT` | delete old + add new |
| 5 | ACTIVE(S1) | OPTIMIZE(S2) | S1=S2, T1=T2 | `NOOP` | 无 |
| 6 | ACTIVE | DEFAULT | — | `RESET` | reject all / delete rule |

### 1.4 SPM baseline 的两维状态

SPM baseline 在 `information_schema.txsql_spm_plan_detail` 中有两个状态维度：

| accepted | enabled | 含义 |
|----------|---------|------|
| YES | YES | 活跃：优化器使用此计划 |
| YES | NO | 禁用：计划保留但不使用，可 enable 恢复 |
| NO | — | 拒绝：计划被移除 |

**当前代码只实现了 accept/reject，缺少 disable/enable。** 管控 disable 需要补充这两个操作。

### 1.5 `noop` 在各层面的处理

| 模块 | 处理方式 | 代码位置 |
|------|---------|---------|
| `get_latest_state()` | 过滤掉 | `rule_state_controller.py:74` |
| `get_all_changes_by_instance()` | 过滤掉 | `rule_state_controller.py:271` |
| `_identify_periods()` | 不会收到 | `performance_comparator.py:190` |
| `perf_result` VIEW | 不会收到 | `schema/perf_result.sql` |

### 1.6 Blacklist 语义

`blacklist.enabled` 字段语义在所有使用处完全一致：

| 位置 | 条件 | 语义 |
|------|------|------|
| `blacklist_controller.py:32` | `enabled = TRUE` | 查询有效黑名单条目 |
| `blacklist_controller.py:57` | `enabled = TRUE` | 检查单条是否在黑名单中 |
| `perf_result.sql:120` | `bl.enabled = 1` | 前端展示是否被黑名单 |
| `task_executor.py:196-209` | 使用 `get_blacklist_set()` | 过滤 workload |

`enabled=TRUE` 统一表示"此条目有效，该 SQL 模板被跳过训练"。无用反情况。

---

## 二、本模块仅操作 `rule_state_history` 一张表

### 2.1 与各表的耦合关系

`rule_state_history` 与其他表之间**不存在任何 JOIN 查询**。管控插入的 `task_id`（`mgmt_disable_xxx`）在其他表中不存在，不产生引用完整性问题。

| 表 | 是否需要操作 | 理由 |
|---|------------|------|
| `rules` | ❌ | 正常训练中 RESET/NOOP 虽有对应记录，但无代码用 `rule_state_history.task_id` JOIN `rules` 表 |
| `validation_logs` | ❌ | RESET 的 `curr_plan_ids=NULL`，perfmon 在 `performance_comparator.py:140` 跳过查询；NOOP 被过滤不进入 perfmon |
| `perf_metrics_history` | ❌ 自动更新 | 由 `perfmon/update_metrics.py` 定时任务根据 `rule_state_history` 重算 |
| `task_execution_history` | ❌ | 管控不经过 executor |
| `blacklist` | ✅ disable 时加入 / enable 时解除 | 阻止/恢复训练 |

### 2.2 Resume 逻辑不受影响

`prev_task_ids` 来自 `task_execution_history`（系统内部 UUID），不含管控 task_id，不影响 resume。

### 2.3 `perf_result` VIEW 不受影响

不 JOIN `rules` 表，不 JOIN `rule_state_history` 表。RESET 行 `best_validation_log_id=NULL`，COALESCE 处理为 0。

---

## 三、停止优化（Disable）— 职责分工

| 步骤 | 执行方 | 操作 |
|------|--------|------|
| 1 | 管控 | 修改 blacklist 状态（加入黑名单） |
| 2 | 管控 | SPM：查出所有 baseline，逐一 reject，逐一 disable |
| 3 | 管控 | Outline：删除所有 AI outline rules |
| 4 | 管控调用本模块脚本 | 添加一条 `rule_state_history` 记录（reset 或 noop） |

### 3.1 本模块提供的脚本

管控完成在线实例操作和 blacklist 后，调用本模块脚本写入规则状态变更记录：

```bash
echo '{
    "cluster_id": 123,
    "instance_id": "cdb-xxx",
    "db": "test_db",
    "digest": "a1b2c3d4",
    "task_id": "mgmt_disable_20260402120000",
    "reason": "性能回退"
}' | python scripts/reset_rule_state.py --stdin
```

脚本内部逻辑：
1. `RuleStateController.get_latest_state()` 查当前状态
2. `RuleStateController.decide_operation(current_state, is_reset=True)` 决策
3. `RuleStateController.record_operation()` 写入记录

| 当前状态 | 写入 operation | comments |
|---------|---------------|----------|
| ACTIVE（setup_plan / modify_plan / modify_timeout） | `reset` | `"Disabled by management: <reason>"` |
| DEFAULT（reset 或无记录） | `noop` | `"Disabled by management (already default): <reason>"` |

元数据库连接从 `etc/aiopt_conf.toml` 读取。

---

## 四、恢复训练（Enable）— 职责分工

| 步骤 | 执行方 | 操作 |
|------|--------|------|
| 1 | 管控 | 修改 blacklist 状态（解除黑名单） |

管控不操作在线实例，不写 `rule_state_history`。

Disable 后 baseline 处于 `accepted=NO, enabled=NO` 状态，管控解除 blacklist 后，下次训练流程会对该模板重新优化，在应用规则阶段执行 enable + accept baseline，届时自然产生 `setup_plan` 记录。

---

## 五、训练程序改动

### 5.1 SPM 规则应用：增加 enable 步骤

文件：`optimizer/spm_operator.py`

当前 SETUP_PLAN/MODIFY_PLAN 分支（`spm_operator.py:250-283`）只做 `reject_all_baselines()` + `add_baseline_from_sql()`/`accept_baseline()`。管控 disable 后 baseline 的 `enabled=NO`，训练应用规则时需要额外 enable。

新增 `SPMOperator.enable_baseline()`：
```python
@staticmethod
def enable_baseline(db_controller, db, digest, plan_id) -> None:
    call_sql = text("CALL dbms_admin.spm_alter_baseline(:db, :digest, :plan_id, 'enable')")
    db_controller.execute(call_sql, {"db": db, "digest": digest, "plan_id": plan_id})
```

在 accept/add baseline 后调用 `enable_baseline()`，确保 baseline 的 `enabled=YES`。

### 5.2 新增脚本：`scripts/reset_rule_state.py`

提供 `disable` 子命令，JSON stdin 输入，写入 `rule_state_history` 记录。详见 3.1。

---

## 六、插入记录的字段分析

| 字段 | NOT NULL? | Disable-RESET 取值 | Disable-NOOP 取值 | 来源/说明 |
|------|-----------|-------------------|-------------------|---------|
| `cluster_id` | ✅ | 管控已有 | 管控已有 | — |
| `instance_id` | ✅ | 管控已有 | 管控已有 | — |
| `db` | ✅ | 管控已有 | 管控已有 | — |
| `digest` | ✅ | 管控已有 | 管控已有 | — |
| `task_id` | ✅ | 管控传入 | 管控传入 | RESET 时传递到 `perf_metrics_history.task_id` 作展示，不参与 JOIN |
| `operation` | ✅ | `'reset'` | `'noop'` | ENUM 合法值 |
| `prev_plan_ids` | 可 NULL | `current_state.plan_ids` | `NULL` | 与 Outline operator（L274）对齐 |
| `curr_plan_ids` | 可 NULL | `NULL` | `NULL` | RESET 后无活跃计划 |
| `apply_time` | ✅ | `NOW(3)` | `NOW(3)` | 必须 > 已有最新记录 |
| `comments` | 可 NULL | `'Disabled by management: <reason>'` | `'Disabled by management (already default): <reason>'` | — |

---

## 七、方案依赖的隐含编程约定

### 约定 1：管控的 reset/noop 不需要关联 `rules` / `validation_logs` 表记录

正常训练中 RESET 本身就不产生评估样本。perfmon 靠 `curr_plan_ids=NULL` 跳过 validation_logs 查询。NOOP 被 `get_all_changes_by_instance()` 直接过滤。稳定性高。

### 约定 2：`task_id` 不做跨表 JOIN

管控的 `task_id` 传递到 `perf_metrics_history.task_id` 作展示。当前无跨表 JOIN。建议在 schema 注释中标注 `task_id` 不保证在其他表存在。稳定性中。

### 约定 3：`get_all_changes_by_instance()` 的 operation 过滤列表

硬编码 `operation IN ('setup_plan', 'modify_plan', 'reset')`。管控 `reset` 被包含，`noop` 被排除。新增操作类型需更新此列表。稳定性中。

### 约定 4：`apply_time` 严格递增

`_identify_periods()` 依赖排序。用 `NOW(3)` 自然满足。稳定性中。

### 约定 5：`prev_plan_ids` 取值对齐

管控取 `current_state.plan_ids if current_state else None`，与两个 operator 在实际运行中一致。稳定性高。

---

## 八、效果示例

### 示例 A：有活跃规则 → Disable

```
变更前:
  rule_state_history: t1: setup_plan, curr_plan_ids=["plan_A"]
  在线实例: baseline plan_A accepted=YES, enabled=YES
  blacklist: 无记录

管控操作后:
  在线实例: baseline plan_A accepted=NO, enabled=NO（reject + disable）
  blacklist: enabled=TRUE

脚本执行后:
  rule_state_history: t1: setup_plan | t2: reset, prev_plan_ids=["plan_A"], task_id='mgmt_disable_...'

perfmon:
  Period 0: [t1-30d, t1) baseline, finalized    ✅
  Period 1: [t1, t2)     setup_plan, finalized   ✅ 开放期正确固化
  Period 2: [t2, now)    reset, open              ✅ 新开放期

下次训练: blacklist 过滤跳过此模板 ✅
```

### 示例 B：无活跃规则 → Disable

```
变更前:
  rule_state_history: t1: setup_plan | t2: reset

管控操作后:
  blacklist: enabled=TRUE

脚本执行后:
  rule_state_history: t1: setup_plan | t2: reset | t3: noop, task_id='mgmt_disable_...'

perfmon: noop 被过滤，周期不变 ✅
下次训练: blacklist 过滤跳过此模板 ✅
```

### 示例 C：恢复训练（Enable）

```
变更前:
  rule_state_history: t1: setup_plan | t2: reset (mgmt_disable)
  在线实例: baseline plan_A accepted=NO, enabled=NO
  blacklist: enabled=TRUE

管控操作后:
  blacklist: enabled=FALSE

rule_state_history: 不变（仍为 t2: reset，规则未重新生效）
在线实例: 不变（accepted=NO, enabled=NO，管控不操作在线实例）

下次训练: blacklist 不再过滤 → 训练流程 enable + accept baseline → 产生 setup_plan ✅
```

---

## 九、约定

| 项目 | 约定 |
|------|------|
| `task_id` 格式 | 由管控传入，建议 `mgmt_disable_{YYYYMMDDHHmmss}` |
| `comments` 格式 | RESET: `"Disabled by management: "` + 原因；NOOP: `"Disabled by management (already default): "` + 原因 |
| `prev_plan_ids` | `current_state.plan_ids if current_state else None` |
| `apply_time` | 使用 `NOW(3)` |
| 并发安全 | 查 `task_progress` 表确认无活跃训练 |

---

## 十、验证方法

1. Disable 有活跃规则的模板：管控完成在线操作后调用脚本，确认 `rule_state_history` 写入 reset
2. Disable 无活跃规则的模板：确认 `rule_state_history` 写入 noop
3. 运行 `perfmon/update_metrics.py` 检查周期正确划分
4. 运行 E2E 训练（SPM）：确认训练流程对 disabled baseline 执行 enable + accept
5. 运行 E2E 训练：确认 blacklist 中的模板被跳过
6. Enable 恢复后运行训练：确认模板重新参与优化
7. 查询 `perf_result` VIEW 确认 reset 行和 blacklist 状态正常展示
