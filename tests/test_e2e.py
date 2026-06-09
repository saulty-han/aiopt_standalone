"""
统一端到端测试 (Unified E2E Test)

整合 SPM 和 Statement Outline 测试，通过 --profile 参数选择配置。

Usage:
    # 使用 profile 运行测试
    python tests/test_e2e.py --profile ncdb-spm
    python tests/test_e2e.py --profile ncdb-outline
    python tests/test_e2e.py --profile cdb-spm
    python tests/test_e2e.py --profile cdb-outline

    # 指定 workload 模式
    python tests/test_e2e.py --profile ncdb-spm --mode selective  # 使用预定义 workload set
    python tests/test_e2e.py --profile ncdb-spm --mode full       # 全量扫描

Profile 配置:
    所有 profile 定义在 tests/instances_config.py 中，包含完整的:
    - instance_info: 实例元信息
    - env_config: 训练环境连接配置
    - master_node: 主节点连接配置
    
    本地测试统一使用 awr_table 作为 workload_source。

验证项:
    1. executor.py 执行成功
    2. 生成规则数 > 0
    3. 规则状态历史记录正确 (setup_plan 操作的 prev/curr 逻辑)
    4. SPM/Outline 物理状态正确 (数据库中实际生效)
    5. 幂等性: 重复执行应全部 NOOP 或产生相同结果（有随机性不一定能保证，但可以运行两次测试多次训练正确性）
"""

import subprocess
import sys
import os
import time
import json
import argparse
import tempfile
from pathlib import Path
from collections import defaultdict

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from ai_logger import aiopt_logger
from db_controller import DBController
from config.config import generate_meta_server_config, GlobalConfig
from data_models import (
    ExecutorInput, ExecutorResult, ExecutionStatus, DecidedRule, RuleAction,
    OutlineType, TrainingEnvType, NodeConfig
)
from sqlalchemy import text

# 统一 workload 定义
from tests.e2e_workloads import get_workload_tuples, UNIFIED_WORKLOADS
from tests.instances_config import (
    get_available_profiles, get_profile, build_executor_input_from_profile
)


def setup_clean_environment(meta_controller: DBController, instance_id: str):
    """清理并初始化测试环境"""
    print(f"\n[Setup] 清理环境 instance_id={instance_id}")
    meta_controller.use_db(GlobalConfig.ai_metadata_database)
    
    tables = ["workload", "rules", "validation_logs", "rule_state_history", "blacklist"]
    for t in tables:
        try:
            meta_controller.execute(text(f"DELETE FROM {t} WHERE instance_id = :id"), {"id": instance_id})
        except:
            pass
            
    print("  ✓ 环境已清理")


def clean_rule_state(online_controller: DBController, outline_type: OutlineType, workload_set: list):
    """根据 outline_type 清理规则状态"""
    if outline_type == OutlineType.SPM:
        print(f"\n[Setup] 清理 SPM 基线状态")
        from optimizer.spm_operator import SPMOperator
        for db, digest in workload_set:
            baselines = SPMOperator.get_all_baselines(online_controller, db, digest)
            for b in baselines:
                SPMOperator.reject_baseline(online_controller, db, digest, b["plan_id"])
                SPMOperator.disable_baseline(online_controller, db, digest, b["plan_id"])
            if baselines:
                print(f"  Rejected + Disabled {len(baselines)} baselines for {db}/{digest}")
        print("  ✓ SPM 状态已清理 (rejected + disabled)")
    else:
        print(f"\n[Setup] 清理 Statement Outline 规则")
        from optimizer.statement_outline_operator import StatementOutlineOperator
        StatementOutlineOperator.delete_all_rules(online_controller)
        print("  ✓ Outline 规则已清理")


def run_executor(input_json: dict, timeout: int = 600) -> ExecutorResult:
    """运行 executor.py 并返回结果"""
    executor_path = Path(__file__).parent.parent / "executor.py"
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
        json.dump(input_json, f, ensure_ascii=False, default=str)
        input_file = f.name
    
    try:
        cmd = [sys.executable, str(executor_path), "--input", input_file]
        print(f"  命令: {' '.join(cmd)}")
        
        start_time = time.time()
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            cwd=str(Path(__file__).parent.parent)
        )
        
        # 等待进程完成
        while True:
            elapsed = time.time() - start_time
            ret = proc.poll()
            
            if ret is not None:
                print(f"  进程结束 (exit_code={ret}, elapsed={elapsed:.1f}s)")
                break
            
            if elapsed > timeout:
                proc.terminate()
                raise TimeoutError(f"executor.py 超时 ({timeout}s)")
            
            if int(elapsed) % 60 == 0 and elapsed > 0:
                print(f"  ... 已运行 {int(elapsed)}s")
            
            time.sleep(5)
        
        stdout, _ = proc.communicate()
        
        # 解析 JSON 结果
        if stdout:
            lines = stdout.strip().split('\n')
            json_start = None
            for i, line in enumerate(lines):
                if line.strip().startswith('{'):
                    json_start = i
                    break
            
            if json_start is not None:
                json_str = '\n'.join(lines[json_start:])
                try:
                    result_dict = json.loads(json_str)
                    return ExecutorResult.model_validate(result_dict)
                except json.JSONDecodeError:
                    pass
            
            # 打印完整输出
            print("\n[Executor Output]")
            for line in lines:
                print(f"  {line}")
        
        return ExecutorResult(
            task_id=input_json.get("task_id", "0"),
            status=ExecutionStatus.COMPLETED if ret == 0 else ExecutionStatus.FAILED,
            extra={"error": f"exit_code={ret}"} if ret != 0 else {}
        )
        
    finally:
        os.unlink(input_file)


def verify_results(meta_controller: DBController, task_id: str):
    """验证任务执行结果"""
    print(f"\n[Verification] 验证任务 {task_id} 结果")
    meta_controller.use_db(GlobalConfig.ai_metadata_database)

    # 验证 rules 表
    rule_count = meta_controller.execute(text("""
        SELECT COUNT(*) FROM rules WHERE task_id = :tid
    """), {"tid": task_id}).fetchone()[0]
    print(f"  生成规则数: {rule_count}")

    # 统计 SQL 模板数量（按 db, digest 分组）
    template_count = meta_controller.execute(text("""
        SELECT COUNT(DISTINCT CONCAT(db, '/', digest))
        FROM rules WHERE task_id = :tid
    """), {"tid": task_id}).fetchone()[0]
    print(f"  SQL 模板数: {template_count}")

    # 验证 rule_state_history 表
    history_rows = meta_controller.execute(text("""
        SELECT db, digest, operation, prev_plan_ids, curr_plan_ids
        FROM rule_state_history WHERE task_id = :tid ORDER BY id
    """), {"tid": task_id}).fetchall()
    history_count = len(history_rows)
    print(f"  状态历史记录数: {history_count}")

    # 显示历史记录详情
    if history_rows:
        print(f"  历史记录详情:")
        for r in history_rows:
            print(f"    - {r[0]}/{r[1]} : {r[2]} | {r[3]} -> {r[4]}")

    return rule_count, template_count, history_count


def verify_rule_state_history(meta_controller: DBController, task_id: str):
    """
    详细验证 rule_state_history 表逻辑
    
    验证规则：
    - setup_plan: prev_plan_ids 应为 None, curr_plan_ids 不应为空
    - modify_plan: prev_plan_ids 和 curr_plan_ids 都不应为空
    - reset: curr_plan_ids 应为 None 或空
    - noop: 无需操作，跳过验证
    """
    print(f"\n[Verification] 验证 rule_state_history 逻辑 (task={task_id})")
    meta_controller.use_db(GlobalConfig.ai_metadata_database)

    rows = meta_controller.execute(text("""
        SELECT db, digest, operation, prev_plan_ids, curr_plan_ids, comments
        FROM rule_state_history WHERE task_id = :tid ORDER BY id
    """), {"tid": task_id}).fetchall()

    print(f"  共 {len(rows)} 条历史记录:")
    
    errors = []
    for r in rows:
        db, digest, operation, prev_ids, curr_ids, comments = r
        
        if operation == 'setup_plan':
            # 新增规则: prev 应为空, curr 不应为空
            if prev_ids is not None:
                errors.append(f"setup_plan: {db}/{digest} prev_plan_ids 应为空")
            if curr_ids is None or curr_ids == '[]':
                errors.append(f"setup_plan: {db}/{digest} curr_plan_ids 不应为空")
            print(f"    ✓ [setup_plan] {db}/{digest} -> {curr_ids}")
            
        elif operation == 'modify_plan':
            # 修改规则: prev 和 curr 都不应为空
            if prev_ids is None or prev_ids == '[]':
                errors.append(f"modify_plan: {db}/{digest} prev_plan_ids 不应为空")
            if curr_ids is None or curr_ids == '[]':
                errors.append(f"modify_plan: {db}/{digest} curr_plan_ids 不应为空")
            print(f"    ✓ [modify_plan] {db}/{digest} {prev_ids} -> {curr_ids}")
            
        elif operation == 'reset':
            # 重置规则: curr 应为空
            print(f"    ✓ [reset] {db}/{digest} -> (reset)")
            
        elif operation == 'noop':
            # 无操作 (幂等性验证时会出现)
            print(f"    ○ [noop] {db}/{digest}")
    
    if errors:
        for e in errors:
            print(f"    ✗ {e}")
        raise AssertionError(f"rule_state_history 验证失败: {len(errors)} 个错误")
    
    print(f"  ✓ 历史记录逻辑验证通过")


def verify_rule_state(
    online_controller: DBController,
    meta_controller: DBController,
    task_id: str,
    outline_type: OutlineType
):
    """
    验证规则在数据库中的物理状态

    层 1: completeness — 该 task 的 rules 条数 == rule_state_history 条数
    层 2: correctness — 对该实例所有有 rule_state_history 的 (db, digest)，查跨所有
          task 的最新非 noop 记录，验证物理状态与之一致:
          - setup_plan / modify_plan / modify_timeout → outline/plan 存在且启用
          - reset → outline/plan 不应存在
          - 无非 noop 记录 → 跳过
    """
    print(f"\n[Verification] 验证 {outline_type.value} 物理状态 (task={task_id})")
    meta_controller.use_db(GlobalConfig.ai_metadata_database)

    # --- 层 1: 记录数一致 ---
    # SPM 下一个 digest 可有多个 plan_id（多条 rules 行），但只产生一条 rule_state_history
    # 因此用模板数（distinct db+digest）而非 rules 行数来比较
    template_count = meta_controller.execute(text(
        "SELECT COUNT(DISTINCT CONCAT(db, '/', digest)) FROM rules WHERE task_id = :tid"
    ), {"tid": task_id}).fetchone()[0]

    history_count = meta_controller.execute(text(
        "SELECT COUNT(*) FROM rule_state_history WHERE task_id = :tid"
    ), {"tid": task_id}).fetchone()[0]

    assert template_count == history_count, (
        f"SQL 模板数 ({template_count}) != rule_state_history 条数 ({history_count}) for task {task_id}"
    )
    print(f"  ✓ 层 1: templates({template_count}) == rule_state_history({history_count})")

    # --- 层 2: 物理状态验证 ---
    # 取该实例的 instance_id
    instance_row = meta_controller.execute(text(
        "SELECT DISTINCT instance_id FROM rules WHERE task_id = :tid LIMIT 1"
    ), {"tid": task_id}).fetchone()

    if not instance_row:
        print(f"  ⚠️ 无模板，跳过物理状态验证")
        return

    instance_id = instance_row[0]

    # 取该实例所有有 rule_state_history 的模板 (跨所有 task)
    all_templates = meta_controller.execute(text(
        "SELECT DISTINCT db, digest FROM rule_state_history WHERE instance_id = :iid"
    ), {"iid": instance_id}).fetchall()

    if not all_templates:
        print(f"  ⚠️ 该实例无 rule_state_history，跳过物理状态验证")
        return

    setup_templates = []   # (db, digest, curr_plan_ids) — 需要验证"存在且启用"
    reset_templates = []   # (db, digest) — 需要验证"不存在"

    for db, digest in all_templates:
        # 该模板全局最新的有效变更 (跨所有 task)
        latest = meta_controller.execute(text("""
            SELECT operation, curr_plan_ids
            FROM rule_state_history
            WHERE instance_id = :iid AND db = :db AND digest = :digest
              AND operation != 'noop'
            ORDER BY apply_time DESC LIMIT 1
        """), {"iid": instance_id, "db": db, "digest": digest}).fetchone()

        if latest is None:
            continue

        operation, curr_plan_ids = latest
        if operation in ('setup_plan', 'modify_plan', 'modify_timeout'):
            setup_templates.append((db, digest, curr_plan_ids))
        elif operation == 'reset':
            reset_templates.append((db, digest))

    total_checks = len(setup_templates) + len(reset_templates)
    print(f"  setup/modify 模板: {len(setup_templates)}, reset 模板: {len(reset_templates)}")

    if total_checks == 0:
        print(f"  ⚠️ 无需物理验证的模板，跳过")
        return

    verified = 0
    errors = []

    if outline_type == OutlineType.SPM:
        # --- 验证 setup_templates: plan 必须 ACCEPTED ---
        for db, digest, curr_plan_ids_json in setup_templates:
            try:
                plan_ids = json.loads(curr_plan_ids_json) if isinstance(curr_plan_ids_json, str) else (curr_plan_ids_json or [])
                if not plan_ids:
                    errors.append(f"SPM: {db}/{digest} curr_plan_ids 为空")
                    continue

                our_plan_ids = set(str(pid) for pid in plan_ids)

                result = online_controller.execute(text("""
                    SELECT plan_id, enabled, accepted
                    FROM information_schema.txsql_spm_plan_detail
                    WHERE db = :db AND digest = :digest
                """), {"db": db, "digest": digest}).fetchall()

                found_plan_ids = set()
                other_accepted_plans = []

                for plan in result:
                    db_plan_id = str(plan[0])
                    enabled, accepted = plan[1], plan[2]

                    is_our_plan = any(
                        (our_id in db_plan_id or db_plan_id in our_id)
                        for our_id in our_plan_ids
                    )

                    if is_our_plan:
                        if enabled == 'YES' and accepted == 'YES':
                            found_plan_ids.add(db_plan_id)
                        else:
                            errors.append(f"SPM: {db}/{digest} 优化 plan {db_plan_id} 未 ACCEPTED")
                    else:
                        if accepted == 'YES':
                            other_accepted_plans.append(db_plan_id)

                if found_plan_ids:
                    print(f"    ✓ SPM: {db}/{digest} 验证 {len(found_plan_ids)}/{len(our_plan_ids)} 个 plan ACCEPTED")
                    verified += 1
                else:
                    errors.append(f"SPM: {db}/{digest} 未找到任何优化的 plan")

                if other_accepted_plans:
                    errors.append(f"SPM: {db}/{digest} 存在非优化 plan 被 ACCEPTED: {other_accepted_plans}")

            except Exception as e:
                print(f"    ⚠️ SPM 查询失败: {db}/{digest} {e}")

        # --- 验证 reset_templates: 不应有 ACCEPTED plan ---
        for db, digest in reset_templates:
            try:
                result = online_controller.execute(text("""
                    SELECT plan_id, accepted, enabled
                    FROM information_schema.txsql_spm_plan_detail
                    WHERE db = :db AND digest = :digest
                      AND accepted = 'YES'
                """), {"db": db, "digest": digest}).fetchall()

                if result:
                    active_ids = [f"{r[0]}(accepted={r[1]},enabled={r[2]})" for r in result]
                    errors.append(f"SPM: {db}/{digest} reset 后仍有 ACCEPTED plan: {active_ids}")
                else:
                    print(f"    ✓ SPM: {db}/{digest} reset 验证通过 (无 ACCEPTED plan)")
                    verified += 1
            except Exception as e:
                print(f"    ⚠️ SPM reset 查询失败: {db}/{digest} {e}")

    else:
        # Statement Outline 验证
        from optimizer.statement_outline_operator import StatementOutlineOperator
        existing_rules = StatementOutlineOperator.show_rules(online_controller)

        # 构建 (schema, digest) -> [rules] 映射
        rules_by_template = defaultdict(list)
        for rule in existing_rules:
            r_norm = {k.upper(): v for k, v in rule.items()}
            schema = r_norm.get('SCHEMA')
            rule_digest = r_norm.get('DIGEST')
            if schema and rule_digest:
                rules_by_template[(schema, rule_digest)].append(r_norm)

        # --- 验证 setup_templates: 规则应存在且启用 ---
        for db, digest, curr_plan_ids_json in setup_templates:
            template_rules = rules_by_template.get((db, digest), [])

            if len(template_rules) == 0:
                print(f"    ✗ Outline: {db}/{digest} 未找到规则")
                errors.append(f"Outline: {db}/{digest} 未找到规则")
            elif len(template_rules) == 1:
                found_rule = template_rules[0]
                enabled = found_rule.get('ENABLED')
                if enabled in ('YES', 1, '1'):
                    print(f"    ✓ Outline: {db}/{digest} rule_id={found_rule.get('ID')} (唯一)")
                    verified += 1
                else:
                    errors.append(f"Outline: {db}/{digest} 规则未启用: enabled={enabled}")
            else:
                rule_ids = [r.get('ID') for r in template_rules]
                print(f"    ✗ Outline: {db}/{digest} 发现 {len(template_rules)} 条规则: {rule_ids}")
                errors.append(f"Outline: {db}/{digest} 存在 {len(template_rules)} 条规则，应该只有 1 条")

        # --- 验证 reset_templates: 规则不应存在 ---
        for db, digest in reset_templates:
            template_rules = rules_by_template.get((db, digest), [])
            if template_rules:
                rule_ids = [r.get('ID') for r in template_rules]
                errors.append(f"Outline: {db}/{digest} reset 后仍有规则: {rule_ids}")
            else:
                print(f"    ✓ Outline: {db}/{digest} reset 验证通过 (无规则)")
                verified += 1

    print(f"  ✓ 物理状态验证: {verified}/{total_checks}")

    if errors:
        print(f"\n  ❌ 物理状态验证失败:")
        for e in errors:
            print(f"    - {e}")
        raise AssertionError(f"物理状态验证失败: {len(errors)} 个错误")


def run_and_verify(
    run_name: str,
    profile_name: str,
    profile: dict,
    workload_set: list,
    meta_controller: DBController,
    online_controller: DBController,
    outline_type: OutlineType,
    timeout: int,
    is_first_run: bool = True
) -> ExecutorResult:
    """
    执行训练并进行完整验证 (可复用的核心逻辑)
    
    :param run_name: 运行名称 (用于日志)
    :param is_first_run: 是否为首次运行 (首次运行要求产生规则)
    :return: ExecutorResult
    """
    print("\n" + "=" * 60)
    print(f"[{run_name}] 开始执行")
    print("=" * 60)
    
    # [Step 1] 构建 ExecutorInput
    task_id = str(int(time.time()))
    print(f"\n[{run_name}] 构建 ExecutorInput (task_id={task_id})")
    
    input_json = build_executor_input_from_profile(
        profile_name=profile_name,
        task_id=task_id,
        workload_set=workload_set
    )
    
    # [Step 2] 执行
    print(f"\n[{run_name}] 调用 executor.py 进程")
    result = run_executor(input_json, timeout=timeout)
    
    print(f"\n[{run_name}] 执行结果:")
    print(f"  status: {result.status.value}")
    print(f"  rules_generated: {result.stats.rules_generated}")
    print(f"  rules_applied: {result.stats.rules_applied}")
    print(f"  duration: {result.duration_seconds:.1f}s")
    if result.extra.get("error"):
        print(f"  error: {result.extra['error']}")

    # [Step 3] 断言执行成功
    assert result.status == ExecutionStatus.COMPLETED, f"executor.py 执行失败: {result.extra.get('error')}"
    print(f"  ✓ executor.py 执行成功")
    
    # [Step 4] 验证结果统计
    rule_count, template_count, history_count = verify_results(meta_controller, task_id)

    if is_first_run:
        # 首次运行应该产生规则
        assert rule_count > 0 or history_count > 0, "未产生任何规则或历史记录"
        # 有规则时必须有对应的状态历史记录 (每个 SQL 模板至少产生一条历史记录)
        # 注意: SPM 模式下一个模板可能有多个 plan_id (多条规则)，但只产生一条历史记录
        if template_count > 0:
            assert history_count >= template_count, (
                f"模板数与历史记录数不匹配: templates={template_count}, history={history_count}. "
                f"每个 SQL 模板应至少有一条状态历史记录"
            )
        print(f"  ✓ 训练产生了结果: rules={rule_count}, templates={template_count}, history={history_count}")
    else:
        print(f"  ℹ 第二次运行: rules={rule_count}, templates={template_count}, history={history_count}")
    
    # [Step 5] 验证 rule_state_history 逻辑
    verify_rule_state_history(meta_controller, task_id)
    
    # [Step 6] 验证物理状态 (规则唯一性、ACCEPTED 状态)
    verify_rule_state(online_controller, meta_controller, task_id, outline_type)
    
    print(f"\n[{run_name}] ✓ 验证完成")
    
    return result


def main():
    parser = argparse.ArgumentParser(description="Unified E2E Test Runner")
    parser.add_argument("--profile", choices=get_available_profiles(), required=True,
                        help="Test profile (ncdb-spm, ncdb-outline, cdb-spm, cdb-outline)")
    parser.add_argument("--mode", choices=["selective", "full"], default="selective",
                        help="Test mode: selective (predefined workloads) or full (scan all)")
    parser.add_argument("--timeout", type=int, default=600,
                        help="Execution timeout in seconds (default: 600)")
    parser.add_argument("--skip-idempotency", action="store_true",
                        help="Skip idempotency test")
    args = parser.parse_args()
    
    profile = get_profile(args.profile)
    outline_type_str = profile["instance_info"]["outline_type"]
    outline_type = OutlineType(outline_type_str)
    
    print("=" * 60)
    print(f"统一端到端测试 (Profile: {args.profile}, Mode: {args.mode})")
    print("=" * 60)
    
    # 初始化 Controllers
    meta_config = generate_meta_server_config()
    meta_controller = DBController(meta_config)
    instance_id = profile["instance_info"]["instance_id"]
    
    master_node_config = NodeConfig.model_validate(profile["master_node"])
    online_instance_config = master_node_config.to_instance_config(
        read_only=False,
        with_ai_marker=True,
        allow_reconnect=True,
        is_meta_server=False
    )
    online_controller = DBController(online_instance_config)
    
    # [环境准备]
    setup_clean_environment(meta_controller, instance_id)
    
    # 选择 workload
    workload_set = None
    if args.mode == "selective":
        workload_set = get_workload_tuples()
        print(f"\n[Setup] Workload Set: {len(workload_set)} items")
    else:
        print(f"\n[Setup] Full scan mode (no workload set)")

    # 清理在线实例上的残留规则状态
    if workload_set:
        clean_rule_state(online_controller, outline_type, workload_set)

    # ========== 第一次执行 ==========
    run_and_verify(
        run_name="First Run",
        profile_name=args.profile,
        profile=profile,
        workload_set=workload_set,
        meta_controller=meta_controller,
        online_controller=online_controller,
        outline_type=outline_type,
        timeout=args.timeout,
        is_first_run=True
    )

    # ========== 第二次执行 (幂等性验证) ==========
    if not args.skip_idempotency:
        time.sleep(1)  # 确保 task_id 不同
        run_and_verify(
            run_name="Idempotency Run",
            profile_name=args.profile,
            profile=profile,
            workload_set=workload_set,
            meta_controller=meta_controller,
            online_controller=online_controller,
            outline_type=outline_type,
            timeout=args.timeout,
            is_first_run=False
        )
    
    print("\n" + "=" * 60)
    print(f"✓ 统一端到端测试完成 ({args.profile})")
    print("=" * 60)


if __name__ == "__main__":
    main()
