"""
Statement Outline 操作模块

基于 dbms_admin.statement_outline_* 系列存储过程实现。
"""

from typing import List, Optional
from sqlalchemy import text
from ai_logger import aiopt_logger
from db_controller import DBController

class StatementOutlineOperator:
    """
    Statement Outline 操作器
    
    封装 dbms_admin.statement_outline_* 接口
    """
    
    @staticmethod
    def add_rule(db_controller: DBController, default_db: str, hinted_query: str, timeout: int) -> int:
        """
        增加一条 AI 规则

        调用: CALL dbms_admin.statement_outline_add_rule_from_ai(default_db, hinted_query, timeout)
        :param timeout: 反馈超时时间(ms)，超时后自动禁用规则。0 表示无超时限制
        不捕获异常，失败直接向上抛出。
        :return: rule_id (int)
        """
        sql = text("CALL dbms_admin.statement_outline_add_rule_from_ai(:db, :query, :timeout)")
        result = db_controller.execute(sql, {"db": default_db, "query": hinted_query, "timeout": timeout})

        row = result.fetchone()
        if not row:
            raise RuntimeError(f"StatementOutline.add_rule: No rule_id returned for db={default_db}")

        rule_id = int(row[0])
        aiopt_logger.info(f"StatementOutline.add_rule: rule_id={rule_id} added for db={default_db}, timeout={timeout}")
        return rule_id

    @staticmethod
    def delete_rule(db_controller: DBController, rule_id: int) -> None:
        """
        删除 AI 规则

        调用: CALL dbms_admin.statement_outline_delete_rule_from_ai(rule_id)
        不捕获异常，失败直接向上抛出。
        """
        sql = text("CALL dbms_admin.statement_outline_delete_rule_from_ai(:id)")
        db_controller.execute(sql, {"id": rule_id})
        aiopt_logger.info(f"StatementOutline.delete_rule: rule_id={rule_id} deleted")

    @staticmethod
    def show_rules(db_controller: DBController) -> List[dict]:
        """
        列出所有 AI 规则

        调用: CALL dbms_admin.statement_outline_show_rules_from_ai()
        不捕获异常，失败直接向上抛出。
        :return: List of dicts representing rules
        """
        sql = text("CALL dbms_admin.statement_outline_show_rules_from_ai()")
        result = db_controller.execute(sql)
        columns = result.keys()
        rules = []
        for row in result.fetchall():
            rule = dict(zip(columns, row))
            rules.append(rule)
        return rules

    @staticmethod
    def delete_all_rules(db_controller: DBController) -> int:
        """
        删除所有规则 (主要用于测试清理)

        show_rules 失败直接冒泡。单个 delete 失败不中断其余，容忍并发冲突。
        :return: 删除的规则数量
        """
        rules = StatementOutlineOperator.show_rules(db_controller)
        count = 0
        for r in rules:
            r_id = int(r.get('ID', r.get('id', -1)))
            if r_id > 0:
                try:
                    StatementOutlineOperator.delete_rule(db_controller, r_id)
                    count += 1
                except Exception as e:
                    aiopt_logger.warning(f"StatementOutline.delete_rule failed for rule_id={r_id}: {e}")
        aiopt_logger.info(f"StatementOutline.delete_all_rules: deleted {count}/{len(rules)} rules")
        return count


from collections import defaultdict
from data_models import DecidedRule, RuleAction
from controller.rule_state_controller import (
    RuleStateController, RuleOperation, RuleStateChange
)

class StatementOutlineRuleApplier:
    """
    Statement Outline 规则应用器
    
    负责:
    1. 查询当前规则状态 (通过 (db, digest) 查找 rule_id)
    2. 决策操作类型 (ADD/UPDATE/REMOVE/SKIP)
    3. 调用 StatementOutlineOperator 执行具体操作
    """
    
    def __init__(self, online_controller: DBController, meta_controller: DBController):
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
        批量应用规则

        :param rules: 决策规则列表
        :param task_id: 任务 ID
        :param cluster_id: 集群 ID
        :param instance_id: 实例 ID
        :return: (成功数, 失败数, 跳过数)
        """
        success_count = 0
        fail_count = 0
        skip_count = 0
        
        # Group by (db, digest)
        rules_by_template: dict[tuple[str, str], List[DecidedRule]] = defaultdict(list)
        for rule in rules:
            rules_by_template[(rule.db, rule.digest)].append(rule)
            
        for (db, digest), template_rules in rules_by_template.items():
            try:
                result = self._apply_template_rules(
                    db=db, digest=digest, rules=template_rules,
                    task_id=task_id, cluster_id=cluster_id, instance_id=instance_id
                )
                if result == "success":
                    success_count += 1
                elif result == "skip":
                    skip_count += 1
                else:
                    fail_count += 1
            except Exception as e:
                aiopt_logger.warning(f"Failed to apply Outline rules for {db}/{digest}: {e}")
                fail_count += 1
                
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
        应用单个模板的规则

        :return: "success" | "skip" | "fail"
        """
        # 1. 获取当前状态
        current_state = RuleStateController.get_latest_state(
            self.meta_controller, instance_id, db, digest
        )
        
        # 2. 查找此 digest 下现有的 rules (用于决策和 cleanup)
        # TODO: show_rules 全量扫描可能慢，但在单实例层面通常规则不多
        all_rules = StatementOutlineOperator.show_rules(self.online_controller)
        existing_rule_ids = []
        current_timeout = None
        
        for r in all_rules:
            # 匹配 DB 和 Digest
            r_schema = r.get('SCHEMA', r.get('schema'))
            r_digest = r.get('DIGEST', r.get('digest'))
            r_id = int(r.get('ID', r.get('id', -1)))
            
            if r_schema == db and r_digest == digest:
                existing_rule_ids.append(r_id)
                # 获取现有规则的 timeout (假设同一 digest 只会有一条有效规则)
                # DB 可能返回 FEEDBACK_TIMEOUT, feedback_timeout 等
                current_timeout = int(r.get('FEEDBACK_TIMEOUT', r.get('feedback_timeout', 0)))

        # 3. 判断本次决策
        is_reset = all(r.action == RuleAction.DEFAULT for r in rules)
        
        # Statement Outline 不使用 plan_id, 而是每次生成新 rule_id
        new_plan_ids = []
        target_rule = None
        new_timeout = None
        
        if not is_reset:
            # 取第一个 OPTIMIZE 规则
            for r in rules:
                if r.action == RuleAction.OPTIMIZE:
                    # 我们使用 sql_text_rewritten 添加规则
                    target_rule = r
                    new_timeout = r.feedback_timeout
                    if r.plan_id:
                        new_plan_ids.append(r.plan_id)
                    break
        
        # 4. 决策操作
        operation, reason = RuleStateController.decide_operation(
            current_state=current_state,
            new_plan_ids=new_plan_ids if new_plan_ids else None,
            is_reset=is_reset,
            current_timeout=current_timeout,
            new_timeout=new_timeout
        )
        
        # 5. 执行操作
        result_status = "success"

        if operation == RuleOperation.NOOP:
            aiopt_logger.debug(f"[OutlineApplier] NOOP: db={db}, digest={digest}, reason={reason}")
            # Fall through to record the NOOP operation for audit trail
        elif operation in (RuleOperation.SETUP_PLAN, RuleOperation.RESET, RuleOperation.MODIFY_PLAN, RuleOperation.MODIFY_TIMEOUT):
            # 如果是 SETUP_PLAN, RESET, MODIFY_PLAN 或 MODIFY_TIMEOUT，先清理旧规则
            # 对于 Outline，每个 SQL 模板只能有一条规则，新增时也需要删除旧规则
            if existing_rule_ids:
                aiopt_logger.info(f"[OutlineApplier] Cleaning {len(existing_rule_ids)} existing rules for {digest}")
                for rid in existing_rule_ids:
                    try:
                        StatementOutlineOperator.delete_rule(self.online_controller, rid)
                    except Exception as e:
                        aiopt_logger.warning(f"[OutlineApplier] Failed to delete rule_id={rid}: {e}")

            # 如果是 SETUP_PLAN 或 MODIFY_PLAN 或 MODIFY_TIMEOUT，添加新规则
            if operation in (RuleOperation.SETUP_PLAN, RuleOperation.MODIFY_PLAN, RuleOperation.MODIFY_TIMEOUT):
                if target_rule and target_rule.sql_text_rewritten:
                    aiopt_logger.info(f"[OutlineApplier] Adding new rule for {digest}")
                    StatementOutlineOperator.add_rule(
                        self.online_controller, db, target_rule.sql_text_rewritten,
                        timeout=target_rule.feedback_timeout
                    )
                else:
                    aiopt_logger.warning(f"[OutlineApplier] Operation {operation} but no target rule found")
                    result_status = "fail"

        # 6. 记录状态变更
        # 如果是 MODIFY/SETUP/MODIFY_TIMEOUT, 记录 new_plan_ids
        # 如果是 RESET, plan_ids 为空
        record_plan_ids = new_plan_ids if operation in (RuleOperation.SETUP_PLAN, RuleOperation.MODIFY_PLAN, RuleOperation.MODIFY_TIMEOUT) else None
        
        change = RuleStateChange(
            cluster_id=cluster_id,
            instance_id=instance_id,
            db=db,
            digest=digest,
            task_id=task_id,
            operation=operation,
            prev_plan_ids=current_state.plan_ids if current_state else None,
            curr_plan_ids=record_plan_ids,
            comments=reason
        )

        RuleStateController.record_operation(
            self.meta_controller, change
        )

        if operation == RuleOperation.NOOP:
            return "skip"
        return result_status
