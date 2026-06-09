"""
Rule State Controller

追踪规则状态变更历史，用于：
1. 性能监控：按变更时间点划分 AWR 统计区间
2. 前端展示：SQL 模板的优化历史时间线
3. 操作精简：避免重复执行相同规则操作
"""

import datetime
import json
from dataclasses import dataclass, field
from enum import Enum

from sqlalchemy import text
from config.config import GlobalConfig
from db_controller import DBController
from ai_logger import aiopt_logger


class RuleOperation(Enum):
    """规则操作类型"""
    SETUP_PLAN = "setup_plan"          # 首次设置规则
    MODIFY_PLAN = "modify_plan"        # 修改执行计划
    MODIFY_TIMEOUT = "modify_timeout"  # 修改超时时间
    RESET = "reset"                    # 恢复默认
    NOOP = "noop"                      # 无操作


@dataclass
class RuleState:
    """规则状态"""
    operation: RuleOperation
    plan_ids: list[str] | None  # 当前计划 ID 集合
    apply_time: datetime.datetime


@dataclass
class RuleStateChange:
    """
    规则状态变更记录

    同时用于两个场景：
    - 写入：构造后传给 record_operation() 插入数据库。apply_time 由数据库
      DEFAULT CURRENT_TIMESTAMP(3) 自动生成，构造时无需手动传入。
    - 读取：get_state_history() / get_all_changes_by_instance() 从数据库填充，
      apply_time 来自数据库实际值，perfmon 依赖此字段划分监控周期。

    apply_time 这里给个默认值是因为，dataclass 要求构造时必须给无默认值的参数传值，
    但是我们的写入端实际上不需要此字段，apply time 由数据库 DEFAULT CURRENT_TIMESTAMP(3) 自动生成。
    这里设置默认值只是为了满足 dataclass 的构造要求，给个 dummy 值就不需要在构造时传入了，降低认知负担。
    
    """
    cluster_id: int
    instance_id: str
    db: str
    digest: str
    task_id: str
    operation: RuleOperation
    prev_plan_ids: list[str] | None
    curr_plan_ids: list[str] | None
    apply_time: datetime.datetime = field(default_factory=datetime.datetime.now)
    comments: str | None = None  # 变更原因/详情


class RuleStateController:
    """规则状态追踪控制器"""
    
    @staticmethod
    def get_latest_state(
        controller: DBController,
        instance_id: str,
        db: str,
        digest: str
    ) -> RuleState | None:
        """
        获取指定 SQL 模板的当前规则状态
        
        查询 rule_state_history 中 operation != 'noop' 的最新记录
        返回 None 表示初始状态（等同于 DEFAULT）
        """
        controller.use_db(GlobalConfig.ai_metadata_database)
        query = text("""
            SELECT operation, curr_plan_ids, apply_time
            FROM rule_state_history
            WHERE instance_id = :instance_id AND db = :db AND digest = :digest
              AND operation != 'noop'
            ORDER BY apply_time DESC
            LIMIT 1
        """)
        
        result = controller.execute(query, {
            "instance_id": instance_id,
            "db": db,
            "digest": digest
        })
        row = result.fetchone()
        
        if not row:
            return None
        
        plan_ids = json.loads(row[1]) if row[1] else None
        return RuleState(
            operation=RuleOperation(row[0]),
            plan_ids=plan_ids,
            apply_time=row[2]
        )
    
    @staticmethod
    def record_operation(
        controller: DBController,
        change: RuleStateChange
    ) -> None:
        """记录规则操作到历史表"""
        controller.use_db(GlobalConfig.ai_metadata_database)
        
        insert_sql = text("""
            INSERT INTO rule_state_history (
                cluster_id, instance_id, db, digest, task_id, operation,
                prev_plan_ids, curr_plan_ids, comments
            ) VALUES (
                :cluster_id, :instance_id, :db, :digest, :task_id, :operation,
                :prev_plan_ids, :curr_plan_ids, :comments
            )
        """)

        controller.execute(insert_sql, {
            "cluster_id": change.cluster_id,
            "instance_id": change.instance_id,
            "db": change.db,
            "digest": change.digest,
            "task_id": change.task_id,
            "operation": change.operation.value,
            "prev_plan_ids": json.dumps(change.prev_plan_ids) if change.prev_plan_ids else None,
            "curr_plan_ids": json.dumps(change.curr_plan_ids) if change.curr_plan_ids else None,
            "comments": change.comments
        })
        
        aiopt_logger.debug(
            f"[RuleStateController] Recorded {change.operation.value}: "
            f"db={change.db}, digest={change.digest}, "
            f"prev={change.prev_plan_ids}, curr={change.curr_plan_ids}, "
            f"comments={change.comments}"
        )
    
    @staticmethod
    def get_state_history(
        controller: DBController,
        instance_id: str,
        db: str,
        digest: str,
        include_noop: bool = True
    ) -> list[RuleStateChange]:
        """
        获取规则变更历史
        
        :param include_noop: 是否包含 noop 操作（完整审计用）
        """
        controller.use_db(GlobalConfig.ai_metadata_database)
        
        if include_noop:
            query = text("""
                SELECT cluster_id, instance_id, db, digest, task_id, operation,
                       prev_plan_ids, curr_plan_ids, apply_time, comments
                FROM rule_state_history
                WHERE instance_id = :instance_id AND db = :db AND digest = :digest
                ORDER BY apply_time ASC
            """)
        else:
            query = text("""
                SELECT cluster_id, instance_id, db, digest, task_id, operation,
                       prev_plan_ids, curr_plan_ids, apply_time, comments
                FROM rule_state_history
                WHERE instance_id = :instance_id AND db = :db AND digest = :digest
                  AND operation != 'noop'
                ORDER BY apply_time ASC
            """)
        
        result = controller.execute(query, {
            "instance_id": instance_id,
            "db": db,
            "digest": digest
        })
        
        history = []
        for row in result.fetchall():
            history.append(RuleStateChange(
                cluster_id=row[0],
                instance_id=row[1],
                db=row[2],
                digest=row[3],
                task_id=row[4],
                operation=RuleOperation(row[5]),
                prev_plan_ids=json.loads(row[6]) if row[6] else None,
                curr_plan_ids=json.loads(row[7]) if row[7] else None,
                apply_time=row[8],
                comments=row[9]
            ))
        
        return history
    
    @staticmethod
    def decide_operation(
        current_state: RuleState | None,
        new_plan_ids: list[str] | None,
        is_reset: bool,
        current_timeout: int | None = None,
        new_timeout: int | None = None
    ) -> tuple[RuleOperation, str]:
        """
        决策操作类型
        
        决策矩阵：
        | 当前状态 | 本次决策 | 条件 | 操作 | 原因 |
        |---------|---------|------|------|------|
        | DEFAULT | OPTIMIZE | - | SETUP_PLAN | "New Optimization" |
        | DEFAULT | DEFAULT | - | NOOP | "Both DEFAULT" |
        | ACTIVE(S1) | OPTIMIZE(S2) | S1 = S2, T1 = T2 | NOOP | "No Change" |
        | ACTIVE(S1) | OPTIMIZE(S2) | S1 ≠ S2 | MODIFY_PLAN | "Plan Changed" |
        | ACTIVE(S1) | OPTIMIZE(S2) | S1 = S2, T1 ≠ T2 | MODIFY_TIMEOUT | "Timeout Changed: old->new" |
        | ACTIVE | DEFAULT | - | RESET | "Reset Requested" |
        
        :param current_state: 当前状态，None 表示初始状态（等同于 DEFAULT）
        :param new_plan_ids: 新的计划 ID 集合，None 表示 DEFAULT
        :param is_reset: 本次决策是否为 DEFAULT
        :param current_timeout: 当前规则的超时时间 (ms)
        :param new_timeout: 新规则的超时时间 (ms)
        :return: (Operation, Reason)
        """
        # 判断当前是否有规则 (ACTIVE)
        has_current_rule = (
            current_state is not None and 
            current_state.operation in (RuleOperation.SETUP_PLAN, RuleOperation.MODIFY_PLAN, RuleOperation.MODIFY_TIMEOUT)
        )
        
        if is_reset:
            if has_current_rule:
                return RuleOperation.RESET, "Reset Requested"
            else:
                return RuleOperation.NOOP, "Both DEFAULT"
        else:
            # OPTIMIZE
            if not has_current_rule:
                return RuleOperation.SETUP_PLAN, "New Optimization"
            else:
                # 比较计划集合
                current_set = set(current_state.plan_ids) if current_state.plan_ids else set()
                new_set = set(new_plan_ids) if new_plan_ids else set()
                
                if current_set != new_set:
                    return RuleOperation.MODIFY_PLAN, "Plan Changed"
                
                # Plan 相同，检查 timeout 是否变化
                if current_timeout is not None and new_timeout is not None:
                    if current_timeout != new_timeout:
                        return RuleOperation.MODIFY_TIMEOUT, f"Timeout Changed: {current_timeout} -> {new_timeout}"
                
                return RuleOperation.NOOP, "No Change"
    
    @staticmethod
    def get_all_changes_by_instance(
        controller: DBController,
        instance_id: str
    ) -> list[RuleStateChange]:
        """
        获取实例所有有效规则变更历史（按时间排序）
        
        用于性能监控周期识别
        仅返回 setup_plan, modify_plan, reset 操作
        
        :param controller: 数据库控制器
        :param instance_id: 实例 ID
        :return: 按 (db, digest, apply_time) 排序的变更列表
        """
        controller.use_db(GlobalConfig.ai_metadata_database)
        
        query = text("""
            SELECT 
                cluster_id, instance_id, db, digest, task_id, operation,
                prev_plan_ids, curr_plan_ids, apply_time, comments
            FROM rule_state_history
            WHERE instance_id = :instance_id
              AND operation IN ('setup_plan', 'modify_plan', 'reset')
            ORDER BY db, digest, apply_time ASC
        """)
        
        result = controller.execute(query, {"instance_id": instance_id})
        
        changes = []
        for row in result.fetchall():
            changes.append(RuleStateChange(
                cluster_id=row[0],
                instance_id=row[1],
                db=row[2],
                digest=row[3],
                task_id=row[4],
                operation=RuleOperation(row[5]),
                prev_plan_ids=json.loads(row[6]) if row[6] else None,
                curr_plan_ids=json.loads(row[7]) if row[7] else None,
                apply_time=row[8],
                comments=row[9]
            ))
        
        aiopt_logger.debug(f"[RuleStateController] Found {len(changes)} rule changes for instance {instance_id}")
        return changes

