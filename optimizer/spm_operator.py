"""
SPM (Stored Plan Management) 操作模块

基于 NCDB 3.1.19.001 的 SPM 接口规范实现。
"""

from typing import List
from sqlalchemy import text

from ai_logger import aiopt_logger
from data_models import DecidedRule, RuleAction
from db_controller import DBController
from controller.rule_state_controller import (
    RuleStateController, RuleOperation, RuleStateChange
)


class SPMOperator:
    """
    SPM 操作器
    
    提供 SPM 计划管理的基础接口。
    """

    @staticmethod
    def get_all_baselines(db_controller: DBController, db: str, digest: str) -> List[dict]:
        """
        获取指定 SQL 的所有 SPM 基线
        
        :return: [{"db": str, "digest": str, "plan_id": str, "hints": str}, ...]
        """
        query = text("""
            SELECT db, digest, plan_id, PLAN_HINT
            FROM information_schema.txsql_spm_plan_detail 
            WHERE db = :db AND digest = :digest
        """)
        
        result = db_controller.execute(query, {"db": db, "digest": digest})
        baselines = []
        for row in result.fetchall():
            baselines.append({
                "db": row[0],
                "digest": row[1],
                "plan_id": row[2],
                "hints": row[3]
            })
        
        aiopt_logger.debug(f"SPM.get_all_baselines: db={db}, digest={digest}, found {len(baselines)} baselines")
        return baselines

    @staticmethod
    def reject_baseline(db_controller: DBController, db: str, digest: str, plan_id: str) -> None:
        """
        Reject 指定的 SPM 基线

        调用: CALL dbms_admin.spm_alter_baseline('db', 'digest', 'plan_id', 'reject')
        不捕获异常，失败直接向上抛出。
        """
        call_sql = text("CALL dbms_admin.spm_alter_baseline(:db, :digest, :plan_id, 'reject')")
        db_controller.execute(call_sql, {"db": db, "digest": digest, "plan_id": plan_id})
        aiopt_logger.debug(f"SPM.reject_baseline: db={db}, digest={digest}, plan_id={plan_id}")

    @staticmethod
    def accept_baseline(db_controller: DBController, db: str, digest: str, plan_id: str) -> None:
        """
        Accept 指定的 SPM 基线

        调用: CALL dbms_admin.spm_alter_baseline('db', 'digest', 'plan_id', 'accept')
        不捕获异常，失败直接向上抛出。
        """
        call_sql = text("CALL dbms_admin.spm_alter_baseline(:db, :digest, :plan_id, 'accept')")
        db_controller.execute(call_sql, {"db": db, "digest": digest, "plan_id": plan_id})
        aiopt_logger.debug(f"SPM.accept_baseline: db={db}, digest={digest}, plan_id={plan_id}")

    @staticmethod
    def enable_baseline(db_controller: DBController, db: str, digest: str, plan_id: str) -> None:
        """
        Enable 指定的 SPM 基线

        调用: CALL dbms_admin.spm_alter_baseline('db', 'digest', 'plan_id', 'enable')

        管控 disable 后 baseline 的 enabled=NO，训练应用规则时需要 enable 恢复。
        不捕获异常，失败直接向上抛出。
        """
        call_sql = text("CALL dbms_admin.spm_alter_baseline(:db, :digest, :plan_id, 'enable')")
        db_controller.execute(call_sql, {"db": db, "digest": digest, "plan_id": plan_id})
        aiopt_logger.debug(f"SPM.enable_baseline: db={db}, digest={digest}, plan_id={plan_id}")

    @staticmethod
    def disable_baseline(db_controller: DBController, db: str, digest: str, plan_id: str) -> None:
        """
        Disable 指定的 SPM 基线

        调用: CALL dbms_admin.spm_alter_baseline('db', 'digest', 'plan_id', 'disable')

        不捕获异常，失败直接向上抛出。
        """
        call_sql = text("CALL dbms_admin.spm_alter_baseline(:db, :digest, :plan_id, 'disable')")
        db_controller.execute(call_sql, {"db": db, "digest": digest, "plan_id": plan_id})
        aiopt_logger.debug(f"SPM.disable_baseline: db={db}, digest={digest}, plan_id={plan_id}")

    @staticmethod
    def add_baseline_from_sql(db_controller: DBController, db: str, sql_with_hints: str) -> bool:
        """
        通过 SQL 添加 SPM 基线
        
        调用: EXPLAIN txsql_spm type=apply <sql_with_hints>
        """
        try:
            # 切换到目标 DB (txsql_spm 语法需在对应 DB 上下文执行)
            try:
                db_controller.use_db(db)
            except Exception as e:
                aiopt_logger.warning(f"SPM.add_baseline_from_sql: failed to use db {db}: {e}")
                return False

            # 直接执行 EXPLAIN txsql_spm type=apply
            explain_sql = f"EXPLAIN txsql_spm type=apply {sql_with_hints}"
            db_controller.execute(text(explain_sql))
            aiopt_logger.debug(f"SPM.add_baseline_from_sql: success")
            return True
        except Exception as e:
            aiopt_logger.warning(f"SPM.add_baseline_from_sql failed: {e}")
            return False

    @staticmethod
    def reject_all_baselines(db_controller: DBController, db: str, digest: str) -> int:
        """
        Reject 指定 SQL 的所有基线

        单个 reject 失败不中断其余，容忍并发冲突（baseline 可能已被其他进程删除）。
        :return: 被 reject 的基线数量
        """
        baselines = SPMOperator.get_all_baselines(db_controller, db, digest)
        rejected_count = 0

        for baseline in baselines:
            try:
                SPMOperator.reject_baseline(
                    db_controller,
                    baseline["db"],
                    baseline["digest"],
                    baseline["plan_id"]
                )
                rejected_count += 1
            except Exception as e:
                aiopt_logger.warning(
                    f"SPM.reject_baseline failed for plan_id={baseline['plan_id']}: {e}"
                )

        aiopt_logger.info(
            f"SPM.reject_all_baselines: db={db}, digest={digest}, "
            f"rejected {rejected_count}/{len(baselines)}"
        )
        return rejected_count


class SPMRuleApplier:
    """
    SPM 规则应用器
    
    根据决策规则应用 SPM 计划管理，集成规则状态追踪。
    """

    def __init__(self, online_controller: DBController, meta_controller: DBController):
        """
        :param online_controller: 用户实例数据库控制器
        :param meta_controller: 元数据库控制器
        """
        self.online_controller = online_controller
        self.meta_controller = meta_controller

    def apply_rules(
        self,
        rules: List[DecidedRule],
        task_id: str,
        cluster_id: int,
        instance_id: str
    ) -> tuple[int, int, int]:
        """
        批量应用决策规则

        :param rules: 决策规则列表
        :param task_id: 任务 ID
        :param cluster_id: 集群 ID
        :param instance_id: 实例 ID
        :return: (成功数, 失败数, 跳过数)
        """
        success_count = 0
        fail_count = 0
        skip_count = 0

        # 按 (db, digest) 分组，因为一个 SQL 模板可能有多个规则
        from collections import defaultdict
        rules_by_template: dict[tuple[str, str], List[DecidedRule]] = defaultdict(list)
        for rule in rules:
            rules_by_template[(rule.db, rule.digest)].append(rule)

        for (db, digest), template_rules in rules_by_template.items():
            try:
                result = self._apply_template_rules(
                    db=db,
                    digest=digest,
                    rules=template_rules,
                    task_id=task_id,
                    cluster_id=cluster_id,
                    instance_id=instance_id
                )
                if result == "success":
                    success_count += 1
                elif result == "skip":
                    skip_count += 1
                else:
                    fail_count += 1
            except Exception as e:
                aiopt_logger.warning(f"Failed to apply rules for {db}/{digest}: {e}")
                fail_count += 1
        
        aiopt_logger.info(
            f"[SPMRuleApplier] Applied rules: {success_count} success, "
            f"{fail_count} failed, {skip_count} skipped"
        )
        return success_count, fail_count, skip_count

    def _apply_template_rules(
        self,
        db: str,
        digest: str,
        rules: List[DecidedRule],
        task_id: str,
        cluster_id: int,
        instance_id: str
    ) -> str:
        """
        应用单个 SQL 模板的规则

        :return: "success" | "skip" | "fail"
        """

        # 1. 获取当前状态
        current_state = RuleStateController.get_latest_state(
            self.meta_controller, instance_id, db, digest
        )
        
        # 2. 判断本次决策类型和计划集合
        # 检查是否全是 DEFAULT
        is_reset = all(r.action == RuleAction.DEFAULT for r in rules)
        
        # 收集新的 plan_ids（OPTIMIZE 规则的 plan_id）
        new_plan_ids = []
        if not is_reset:
            for r in rules:
                if r.action == RuleAction.OPTIMIZE and r.plan_id:
                    new_plan_ids.append(r.plan_id)
        
        # 3. 决策操作类型
        operation, reason = RuleStateController.decide_operation(
            current_state=current_state,
            new_plan_ids=new_plan_ids if new_plan_ids else None,
            is_reset=is_reset
        )
        
        # 获取上一次的 plan_ids
        prev_plan_ids = None
        if current_state and current_state.operation in (RuleOperation.SETUP_PLAN, RuleOperation.MODIFY_PLAN):
            prev_plan_ids = current_state.plan_ids
        
        # 4. 执行操作
        success = True
        if operation == RuleOperation.NOOP:
            aiopt_logger.debug(f"[SPMRuleApplier] NOOP: db={db}, digest={digest}, reason={reason}")
        elif operation == RuleOperation.RESET:
            # RESET: reject 所有基线
            aiopt_logger.info(f"[SPMRuleApplier] RESET: db={db}, digest={digest}")
            SPMOperator.reject_all_baselines(self.online_controller, db, digest)
        elif operation in (RuleOperation.SETUP_PLAN, RuleOperation.MODIFY_PLAN):
            # OPTIMIZE: reject 所有基线，然后添加新基线
            aiopt_logger.info(f"[SPMRuleApplier] {operation.value.upper()}: db={db}, digest={digest}")
            SPMOperator.reject_all_baselines(self.online_controller, db, digest)
            
            # 添加新基线
            for rule in rules:
                if rule.action == RuleAction.OPTIMIZE and rule.plan_id:
                    # 先检查 SPM 中是否已存在该 plan_id
                    existing_baselines = SPMOperator.get_all_baselines(
                        self.online_controller, db, digest
                    )
                    existing_plan_ids = {b["plan_id"] for b in existing_baselines}
                    
                    if rule.plan_id in existing_plan_ids:
                        # 规则已存在于 SPM（可能由其他程序插入），直接 accept
                        aiopt_logger.info(
                            f"[SPMRuleApplier] Plan {rule.plan_id} already exists in SPM, accepting..."
                        )
                        SPMOperator.accept_baseline(
                            self.online_controller, db, digest, rule.plan_id
                        )
                        SPMOperator.enable_baseline(
                            self.online_controller, db, digest, rule.plan_id
                        )
                    elif rule.sql_text_rewritten:
                        # 尝试通过 SQL 添加新基线
                        if SPMOperator.add_baseline_from_sql(
                            self.online_controller, db, rule.sql_text_rewritten
                        ):
                            # 新添加的 baseline 默认 enabled=YES，但为保险也 enable
                            SPMOperator.enable_baseline(
                                self.online_controller, db, digest, rule.plan_id
                            )
                        else:
                            # 添加失败，尝试 accept（可能是 add 过程中被其他程序添加了）
                            aiopt_logger.info(
                                f"[SPMRuleApplier] add_baseline_from_sql failed, trying accept..."
                            )
                            SPMOperator.accept_baseline(
                                self.online_controller, db, digest, rule.plan_id
                            )
                            SPMOperator.enable_baseline(
                                self.online_controller, db, digest, rule.plan_id
                            )
        
        # 5. 记录操作到历史表
        RuleStateController.record_operation(
            self.meta_controller,
            RuleStateChange(
                cluster_id=cluster_id,
                instance_id=instance_id,
                db=db,
                digest=digest,
                task_id=task_id,
                operation=operation,
                prev_plan_ids=prev_plan_ids,
                curr_plan_ids=new_plan_ids if new_plan_ids else None,
                comments=reason
            )
        )
        
        if operation == RuleOperation.NOOP:
            return "skip"
        return "success" if success else "fail"
