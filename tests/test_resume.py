"""
断点续传测试 (Resume Test)

验证 SIGUSR1 中断 + options.allow_resume=True 续传的完整流程。

通过轮询 rules 表感知子进程实际优化进度，
在部分模板完成后发送 SIGUSR1 中断，再以新 task_id + allow_resume=True 续传，
验证最终结果与正常执行一致。

Usage:
    python tests/test_resume.py --profile ncdb-spm
    python tests/test_resume.py --profile ncdb-outline
"""

import subprocess
import sys
import os
import signal
import time
import json
import argparse
import tempfile
import threading
from pathlib import Path

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from ai_logger import aiopt_logger
from db_controller import DBController
from config.config import generate_meta_server_config, GlobalConfig
from data_models import (
    ExecutorInput, ExecutorResult, ExecutionStatus, NodeConfig, OutlineType
)
from sqlalchemy import text

from tests.e2e_workloads import get_workload_tuples
from tests.instances_config import (
    get_available_profiles, get_profile, build_executor_input_from_profile
)
from tests.test_e2e import (
    setup_clean_environment, clean_rule_state, run_executor,
    verify_rule_state_history, verify_rule_state
)


def parse_executor_result(stdout_text: str, task_id: str) -> ExecutorResult:
    """从 executor.py 的 stdout 中解析 JSON 结果"""
    if stdout_text:
        lines = stdout_text.strip().split('\n')
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

    return ExecutorResult(
        task_id=task_id,
        status=ExecutionStatus.FAILED,
        extra={"error": "Failed to parse executor output"}
    )


def run_executor_with_interrupt(input_json: dict, task_id: str, trigger_template_count: int) -> ExecutorResult:
    """
    启动 executor.py 子进程，在监控线程中轮询 rules 表，
    当已处理的 distinct template 数量 >= trigger_template_count 时发送 SIGUSR1。

    :param input_json: ExecutorInput 的 dict 格式
    :param task_id: 任务 ID（用于查询 rules 表）
    :param trigger_template_count: 触发中断的模板数阈值
    :return: ExecutorResult
    """
    executor_path = Path(__file__).parent.parent / "executor.py"

    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
        json.dump(input_json, f, ensure_ascii=False, default=str)
        input_file = f.name

    try:
        cmd = [sys.executable, str(executor_path), "--input", input_file]
        print(f"  命令: {' '.join(cmd)}")

        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            cwd=str(Path(__file__).parent.parent)
        )

        signal_sent = threading.Event()
        output_lines = []

        def read_output():
            """持续读取 stdout，防止 pipe buffer 满导致死锁"""
            for line in iter(proc.stdout.readline, ''):
                output_lines.append(line)

        def monitor():
            """轮询 rules 表，达到阈值时发送 SIGUSR1"""
            monitor_controller = None
            try:
                monitor_controller = DBController(generate_meta_server_config())
                monitor_controller.use_db(GlobalConfig.ai_metadata_database)
                while proc.poll() is None:
                    count = monitor_controller.execute(text(
                        "SELECT COUNT(DISTINCT CONCAT(db, '/', digest)) "
                        "FROM rules WHERE task_id = :tid"
                    ), {"tid": task_id}).fetchone()[0]
                    if count >= trigger_template_count:
                        print(f"  [Monitor] rules 表已有 {count} 个模板，发送 SIGUSR1")
                        proc.send_signal(signal.SIGUSR1)
                        signal_sent.set()
                        return
                    time.sleep(2)
                print(f"  [Monitor] 进程已结束，未触发信号")
            except Exception as e:
                print(f"  [Monitor] 异常: {e}")
            finally:
                if monitor_controller:
                    monitor_controller.close()

        reader_thread = threading.Thread(target=read_output, daemon=True)
        monitor_thread = threading.Thread(target=monitor, daemon=True)
        reader_thread.start()
        monitor_thread.start()

        start_time = time.time()
        proc.wait()
        reader_thread.join()
        elapsed = time.time() - start_time
        print(f"  进程结束 (exit_code={proc.returncode}, elapsed={elapsed:.1f}s, signal_sent={signal_sent.is_set()})")

        stdout = ''.join(output_lines)
        return parse_executor_result(stdout, task_id)

    finally:
        os.unlink(input_file)


def run_executor_with_early_signal(input_json: dict, task_id: str, signal_delay: float = 1.0, preprocess_delay: int = 10) -> ExecutorResult:
    """
    启动 executor.py（通过 python -c 注入预处理延迟），在 signal_delay 秒后发送 SIGUSR1。

    通过 monkey-patch WorkloadPreprocessor.preprocess 注入 sleep，
    确保信号在预处理阶段到达，被 signal checkpoint 捕获。

    :param input_json: ExecutorInput 的 dict 格式
    :param task_id: 任务 ID
    :param signal_delay: 启动后多久发送 SIGUSR1（秒）
    :param preprocess_delay: 注入预处理的延迟（秒）
    :return: ExecutorResult
    """
    project_root = Path(__file__).parent.parent

    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
        json.dump(input_json, f, ensure_ascii=False, default=str)
        input_file = f.name

    try:
        code = (
            "import sys, time;"
            "from workload.workload_preprocessor import WorkloadPreprocessor;"
            "_orig = WorkloadPreprocessor.preprocess;"
            f"WorkloadPreprocessor.preprocess = lambda self, *a, **kw: (time.sleep({preprocess_delay}), _orig(self, *a, **kw))[1];"
            f"sys.argv = ['executor.py', '--input', '{input_file}'];"
            "from executor import main; main()"
        )
        cmd = [sys.executable, "-c", code]
        print(f"  命令: python -c '<monkey-patch preprocess +{preprocess_delay}s delay>' --input ...")

        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            cwd=str(project_root)
        )

        output_lines = []

        def read_output():
            for line in iter(proc.stdout.readline, ''):
                output_lines.append(line)

        reader_thread = threading.Thread(target=read_output, daemon=True)
        reader_thread.start()

        # 等待 signal_delay 后发送 SIGUSR1
        time.sleep(signal_delay)
        if proc.poll() is None:
            print(f"  [{signal_delay}s] 发送 SIGUSR1")
            proc.send_signal(signal.SIGUSR1)
        else:
            print(f"  进程已提前退出，未发送信号")

        start_time = time.time()
        proc.wait()
        reader_thread.join()
        elapsed = time.time() - start_time
        print(f"  进程结束 (exit_code={proc.returncode}, elapsed={elapsed:.1f}s after signal)")

        stdout = ''.join(output_lines)
        return parse_executor_result(stdout, task_id)

    finally:
        os.unlink(input_file)


def run_executor_with_applying_signal(input_json: dict, task_id: str, apply_delay: int = 20) -> ExecutorResult:
    """
    启动 executor.py 子进程，通过 monkey-patch 在 apply_rules 入口注入延迟，
    监控线程检测到 applying 开始后发送 SIGUSR1。

    验证: 信号在 applying 阶段到达时被忽略，apply_rules 正常完成，最终 status=COMPLETED。

    :param input_json: ExecutorInput 的 dict 格式
    :param task_id: 任务 ID
    :param apply_delay: apply_rules 入口注入的延迟（秒）
    :return: ExecutorResult
    """
    project_root = Path(__file__).parent.parent
    marker_file = f"/tmp/aiopt_applying_marker_{task_id}"

    # 清理旧 marker
    if os.path.exists(marker_file):
        os.unlink(marker_file)

    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
        json.dump(input_json, f, ensure_ascii=False, default=str)
        input_file = f.name

    try:
        code = (
            "import sys, time, os;"
            "from optimizer.spm_operator import SPMRuleApplier;"
            "from optimizer.statement_outline_operator import StatementOutlineRuleApplier;"
            "_spm_orig = SPMRuleApplier.apply_rules;"
            "_so_orig = StatementOutlineRuleApplier.apply_rules;"
            f"_marker = '{marker_file}';"
            f"_delay = {apply_delay};"
            "SPMRuleApplier.apply_rules = lambda self, *a, **kw: "
            "(open(_marker, 'w').close(), time.sleep(_delay), _spm_orig(self, *a, **kw))[-1];"
            "StatementOutlineRuleApplier.apply_rules = lambda self, *a, **kw: "
            "(open(_marker, 'w').close(), time.sleep(_delay), _so_orig(self, *a, **kw))[-1];"
            f"sys.argv = ['executor.py', '--input', '{input_file}'];"
            "from executor import main; main()"
        )
        cmd = [sys.executable, "-c", code]
        print(f"  命令: python -c '<monkey-patch apply_rules +{apply_delay}s delay>' --input ...")

        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            cwd=str(project_root)
        )

        signal_sent = threading.Event()
        output_lines = []

        def read_output():
            for line in iter(proc.stdout.readline, ''):
                output_lines.append(line)

        def monitor():
            """轮询 marker 文件，检测到 apply_rules 已进入后发送 SIGUSR1"""
            while proc.poll() is None:
                if os.path.exists(marker_file):
                    time.sleep(2)  # 确保已进入 sleep
                    if proc.poll() is None:
                        print(f"  [Monitor] 检测到 applying 阶段开始，发送 SIGUSR1")
                        proc.send_signal(signal.SIGUSR1)
                        signal_sent.set()
                    return
                time.sleep(1)
            print(f"  [Monitor] 进程已结束，未触发信号")

        reader_thread = threading.Thread(target=read_output, daemon=True)
        monitor_thread = threading.Thread(target=monitor, daemon=True)
        reader_thread.start()
        monitor_thread.start()

        start_time = time.time()
        proc.wait()
        reader_thread.join()
        elapsed = time.time() - start_time
        print(f"  进程结束 (exit_code={proc.returncode}, elapsed={elapsed:.1f}s, signal_sent={signal_sent.is_set()})")

        stdout = ''.join(output_lines)
        return parse_executor_result(stdout, task_id)

    finally:
        os.unlink(input_file)
        if os.path.exists(marker_file):
            os.unlink(marker_file)


def verify_execution_history(meta_controller: DBController, task_id: str, expected_count: int):
    """验证 task_execution_history 表记录数"""
    meta_controller.use_db(GlobalConfig.ai_metadata_database)
    count = meta_controller.execute(text(
        "SELECT COUNT(*) FROM task_execution_history WHERE task_id = :tid"
    ), {"tid": task_id}).fetchone()[0]
    assert count == expected_count, (
        f"task_execution_history 记录数不符: 期望 {expected_count}, 实际 {count}"
    )
    print(f"  ✓ execution_history: {count} 条记录")


def verify_task_state(
    meta_controller: DBController,
    online_controller: DBController,
    task_id: str,
    outline_type: OutlineType,
    label: str
):
    """
    验证单个任务的完整 DB 状态。

    :param label: 任务角色标签，如 "中断任务" / "续传任务"
    """
    print(f"\n[Verify] {label} (task_id={task_id})")
    verify_execution_history(meta_controller, task_id, expected_count=1)
    verify_rule_state_history(meta_controller, task_id)
    verify_rule_state(online_controller, meta_controller, task_id, outline_type)
    print(f"  ✓ {label} DB 状态验证通过")


def test_interrupt_and_resume(
    profile_name: str,
    profile: dict,
    workload_set: list,
    meta_controller: DBController,
    online_controller: DBController,
    outline_type: OutlineType
):
    """
    Test 1: 中断 → 续传完整生命周期

    Step 1: 清理环境
    Step 2: 首次执行 + 中断 (task_id_1)
    Step 3: 续传执行 (task_id_2, options.allow_resume=True, 自动检测 task_id_1)
    Step 4: 验证
    """
    instance_id = profile["instance_info"]["instance_id"]
    task_id_1 = str(int(time.time()))
    trigger_count = 2

    # ========== Step 1: 清理环境 ==========
    print("\n" + "=" * 60)
    print("[Test 1] 中断 → 续传完整生命周期")
    print("=" * 60)

    setup_clean_environment(meta_controller, instance_id)
    clean_rule_state(online_controller, outline_type, workload_set)

    # ========== Step 2: 首次执行 + 中断 ==========
    print(f"\n[Step 2] 首次执行 + 中断 (trigger_count={trigger_count}, task_id={task_id_1})")

    input_json = build_executor_input_from_profile(
        profile_name=profile_name,
        task_id=task_id_1,
        workload_set=workload_set
    )

    result1 = run_executor_with_interrupt(input_json, task_id_1, trigger_count)

    print(f"\n[Step 2] 中断结果:")
    print(f"  status: {result1.status.value}")
    print(f"  total_templates: {result1.stats.total_templates}")
    print(f"  processed_templates: {result1.stats.processed_templates}")
    print(f"  rules_generated: {result1.stats.rules_generated}")
    print(f"  rules_applied: {result1.stats.rules_applied}")
    print(f"  extra: {result1.extra}")
    if result1.extra.get("error"):
        print(f"  error: {result1.extra['error']}")

    # 断言: 中断状态
    assert result1.status == ExecutionStatus.INTERRUPTED, (
        f"期望 INTERRUPTED, 实际 {result1.status.value}"
    )
    assert result1.stats.processed_templates >= trigger_count, (
        f"已处理模板数 {result1.stats.processed_templates} < 触发阈值 {trigger_count}"
    )
    assert result1.stats.processed_templates < result1.stats.total_templates, (
        f"已处理模板数 {result1.stats.processed_templates} = 总数 {result1.stats.total_templates}, "
        f"中断未生效"
    )
    assert result1.extra.get("interrupt_stage") == "optimizing", (
        f"期望 interrupt_stage=optimizing, 实际 {result1.extra.get('interrupt_stage')}"
    )

    interrupt_processed = result1.stats.processed_templates
    total_templates = result1.stats.total_templates

    print(f"  ✓ 中断验证通过: {interrupt_processed}/{total_templates} 模板已处理, {result1.stats.rules_generated} 规则已生成")

    # 验证中断任务 DB 状态
    verify_task_state(meta_controller, online_controller, task_id_1, outline_type, "中断任务")

    # ========== Step 3: 续传执行 ==========
    task_id_2 = str(int(time.time()) + 1)
    print(f"\n[Step 3] 续传执行 (new task_id={task_id_2}, allow_resume=True, 自动检测 prev={task_id_1})")

    input_json_resume = build_executor_input_from_profile(
        profile_name=profile_name,
        task_id=task_id_2,
        workload_set=workload_set,
        options={"allow_resume": True, "resume_expiration_days": 7}
    )

    result2 = run_executor(input_json_resume)

    print(f"\n[Step 3] 续传结果 ({task_id_1} → {task_id_2}):")
    print(f"  status: {result2.status.value}")
    print(f"  total_templates: {result2.stats.total_templates}")
    print(f"  processed_templates: {result2.stats.processed_templates}")
    print(f"  rules_generated: {result2.stats.rules_generated}")
    print(f"  rules_applied: {result2.stats.rules_applied}")
    print(f"  extra: {result2.extra}")
    if result2.extra.get("error"):
        print(f"  error: {result2.extra['error']}")

    # 断言: 续传完成
    assert result2.status == ExecutionStatus.COMPLETED, (
        f"期望 COMPLETED, 实际 {result2.status.value}, error={result2.extra.get('error')}"
    )
    assert result2.stats.total_templates == total_templates, (
        f"续传 total_templates {result2.stats.total_templates} != 中断时 {total_templates}"
    )
    assert result2.stats.processed_templates == total_templates, (
        f"续传 processed_templates {result2.stats.processed_templates} != total {total_templates}"
    )
    assert result2.extra.get("prev_task_ids") == [task_id_1], (
        f"期望 prev_task_ids=[{task_id_1}], 实际 {result2.extra.get('prev_task_ids')}"
    )

    remaining_templates = total_templates - interrupt_processed
    print(
        f"  ✓ 续传验证通过: {result2.stats.processed_templates}/{total_templates} 模板全部完成 "
        f"(本次优化 {remaining_templates} 个新模板)"
    )

    # 验证续传任务 DB 状态
    verify_task_state(meta_controller, online_controller, task_id_2, outline_type, "续传任务")

    print(f"\n[Test 1] ✓ 中断 → 续传完整生命周期测试通过")


def test_resume_all_processed(
    profile_name: str,
    profile: dict,
    workload_set: list,
    meta_controller: DBController,
    online_controller: DBController,
    outline_type: OutlineType
):
    """
    Test 2: 上次任务已完成，续传不触发 (边界情况)

    先完整执行 (COMPLETED)，再用 allow_resume=True + 新 task_id 执行。
    由于最近一条记录是 COMPLETED（非 INTERRUPTED），find_resumable_task 返回 None，
    续传不触发，任务作为全新任务执行。
    """
    instance_id = profile["instance_info"]["instance_id"]
    task_id_1 = str(int(time.time()))

    print("\n" + "=" * 60)
    print("[Test 2] 上次任务已完成，续传不触发 (边界情况)")
    print("=" * 60)

    # 清理环境
    setup_clean_environment(meta_controller, instance_id)
    clean_rule_state(online_controller, outline_type, workload_set)

    # 正常完整执行
    print(f"\n[Step 1] 正常完整执行 (task_id={task_id_1})")

    input_json = build_executor_input_from_profile(
        profile_name=profile_name,
        task_id=task_id_1,
        workload_set=workload_set
    )

    result1 = run_executor(input_json)

    print(f"\n[Step 1] 执行结果:")
    print(f"  status: {result1.status.value}")
    print(f"  total_templates: {result1.stats.total_templates}")
    print(f"  processed_templates: {result1.stats.processed_templates}")
    print(f"  rules_generated: {result1.stats.rules_generated}")
    print(f"  rules_applied: {result1.stats.rules_applied}")
    print(f"  extra: {result1.extra}")
    if result1.extra.get("error"):
        print(f"  error: {result1.extra['error']}")

    assert result1.status == ExecutionStatus.COMPLETED, (
        f"首次执行失败: {result1.status.value}, error={result1.extra.get('error')}"
    )
    total_templates = result1.stats.total_templates
    print(f"  ✓ 首次执行完成: {total_templates} 模板, {result1.stats.rules_generated} 规则")

    # 验证首次任务 DB 状态
    verify_task_state(meta_controller, online_controller, task_id_1, outline_type, "首次任务")

    # 第二次执行 (allow_resume=True, 但上次是 COMPLETED，续传不应触发)
    # 不做任何清理（物理或 metadata），模拟生产行为：两轮连续训练之间无干预
    task_id_2 = str(int(time.time()) + 1)
    print(f"\n[Step 2] 第二次执行 (allow_resume=True, new task_id={task_id_2})")
    print(f"  上次任务 {task_id_1} 状态为 COMPLETED，续传不应触发")

    input_json_2 = build_executor_input_from_profile(
        profile_name=profile_name,
        task_id=task_id_2,
        workload_set=workload_set,
        options={"allow_resume": True, "resume_expiration_days": 7}
    )

    result2 = run_executor(input_json_2)

    print(f"\n[Step 2] 执行结果:")
    print(f"  status: {result2.status.value}")
    print(f"  total_templates: {result2.stats.total_templates}")
    print(f"  processed_templates: {result2.stats.processed_templates}")
    print(f"  rules_generated: {result2.stats.rules_generated}")
    print(f"  rules_applied: {result2.stats.rules_applied}")
    print(f"  extra: {result2.extra}")
    if result2.extra.get("error"):
        print(f"  error: {result2.extra['error']}")

    assert result2.status == ExecutionStatus.COMPLETED, (
        f"执行失败: {result2.status.value}, error={result2.extra.get('error')}"
    )
    assert "prev_task_ids" not in result2.extra, (
        f"续传不应触发，但 extra 中包含 prev_task_ids={result2.extra.get('prev_task_ids')}"
    )
    assert result2.stats.processed_templates == result2.stats.total_templates, (
        f"processed_templates {result2.stats.processed_templates} != total {result2.stats.total_templates}"
    )

    print(f"  ✓ 验证通过: 续传未触发，作为全新任务完成")

    # 验证第二次任务 DB 状态
    verify_task_state(meta_controller, online_controller, task_id_2, outline_type, "第二次任务(全新)")

    print(f"\n[Test 2] ✓ 边界情况测试通过")


def test_interrupt_during_preprocessing(
    profile_name: str,
    profile: dict,
    workload_set: list,
    meta_controller: DBController,
    online_controller: DBController,
    outline_type: OutlineType
):
    """
    Test 3: 预处理阶段中断

    通过 monkey-patch 注入预处理延迟，确保 SIGUSR1 在预处理阶段到达。
    验证 signal checkpoint 行为：预处理跑完，然后返回 INTERRUPTED。
    """
    instance_id = profile["instance_info"]["instance_id"]
    task_id = str(int(time.time()))

    print("\n" + "=" * 60)
    print("[Test 3] 预处理阶段中断")
    print("=" * 60)

    # 清理环境
    setup_clean_environment(meta_controller, instance_id)
    clean_rule_state(online_controller, outline_type, workload_set)

    # 执行：注入 10s 预处理延迟，1s 后发信号
    print(f"\n[Step 1] 执行 (预处理延迟 10s, 1s 后发送 SIGUSR1, task_id={task_id})")

    input_json = build_executor_input_from_profile(
        profile_name=profile_name,
        task_id=task_id,
        workload_set=workload_set
    )

    result = run_executor_with_early_signal(
        input_json, task_id,
        signal_delay=1.0,
        preprocess_delay=10
    )

    print(f"\n[Step 1] 结果:")
    print(f"  status: {result.status.value}")
    print(f"  total_templates: {result.stats.total_templates}")
    print(f"  processed_templates: {result.stats.processed_templates}")
    print(f"  rules_generated: {result.stats.rules_generated}")
    print(f"  extra: {result.extra}")
    if result.extra.get("error"):
        print(f"  error: {result.extra['error']}")

    # 断言: 预处理阶段中断
    assert result.status == ExecutionStatus.INTERRUPTED, (
        f"期望 INTERRUPTED, 实际 {result.status.value}"
    )
    assert result.extra.get("interrupt_stage") == "loading_workload", (
        f"期望 interrupt_stage=loading_workload, 实际 {result.extra.get('interrupt_stage')}"
    )
    assert result.stats.processed_templates == 0, (
        f"期望 processed_templates=0 (未进入优化阶段), 实际 {result.stats.processed_templates}"
    )
    assert result.stats.rules_generated == 0, (
        f"期望 rules_generated=0, 实际 {result.stats.rules_generated}"
    )

    # 验证: workload 表有数据 (预处理跑完了)
    meta_controller.use_db(GlobalConfig.ai_metadata_database)
    workload_count = meta_controller.execute(text(
        "SELECT COUNT(*) FROM workload WHERE task_id = :tid"
    ), {"tid": task_id}).fetchone()[0]
    print(f"  workload 表记录数: {workload_count}")
    assert workload_count > 0, "预处理应该跑完，workload 表应有数据"

    print(f"  ✓ 预处理阶段中断验证通过: workload 已存储, 0 模板处理, 0 规则生成")

    # 验证 DB: INTERRUPTED 记录已写入
    verify_execution_history(meta_controller, task_id, expected_count=1)

    print(f"\n[Test 3] ✓ 预处理阶段中断测试通过")


def test_signal_during_applying(
    profile_name: str,
    profile: dict,
    workload_set: list,
    meta_controller: DBController,
    online_controller: DBController,
    outline_type: OutlineType
):
    """
    Test 4: Applying 阶段收到 SIGUSR1

    通过 monkey-patch 在 apply_rules 入口注入延迟，
    监控线程检测到 applying 阶段开始后发送 SIGUSR1。

    验证: 信号在 applying 阶段被忽略（executor handler 仅设 flag，
    但 final status 只检查 was_aborted），apply_rules 正常完成，
    最终 status=COMPLETED。
    """
    instance_id = profile["instance_info"]["instance_id"]
    task_id = str(int(time.time()))

    print("\n" + "=" * 60)
    print("[Test 4] Applying 阶段收到 SIGUSR1")
    print("=" * 60)

    # 清理环境
    setup_clean_environment(meta_controller, instance_id)
    clean_rule_state(online_controller, outline_type, workload_set)

    # 执行: apply_rules 注入 20s 延迟，监控线程在 applying 开始后发 SIGUSR1
    print(f"\n[Step 1] 执行 (apply_rules +20s 延迟, applying 阶段发送 SIGUSR1, task_id={task_id})")

    input_json = build_executor_input_from_profile(
        profile_name=profile_name,
        task_id=task_id,
        workload_set=workload_set
    )

    result = run_executor_with_applying_signal(input_json, task_id, apply_delay=20)

    print(f"\n[Step 1] 结果:")
    print(f"  status: {result.status.value}")
    print(f"  total_templates: {result.stats.total_templates}")
    print(f"  processed_templates: {result.stats.processed_templates}")
    print(f"  rules_generated: {result.stats.rules_generated}")
    print(f"  rules_applied: {result.stats.rules_applied}")
    print(f"  extra: {result.extra}")
    if result.extra.get("error"):
        print(f"  error: {result.extra['error']}")

    # 断言: applying 阶段信号被忽略，最终 COMPLETED
    assert result.status == ExecutionStatus.COMPLETED, (
        f"期望 COMPLETED (applying 阶段信号应被忽略), 实际 {result.status.value}, error={result.extra.get('error')}"
    )
    assert result.stats.processed_templates == result.stats.total_templates, (
        f"所有模板应完成: processed={result.stats.processed_templates}, total={result.stats.total_templates}"
    )
    assert result.extra.get("stage") == "finished", (
        f"期望 stage=finished, 实际 {result.extra.get('stage')}"
    )
    assert "interrupt_stage" not in result.extra, (
        f"COMPLETED 时不应有 interrupt_stage, 实际 extra={result.extra}"
    )

    # 验证 rule_state_history 有记录（证明 apply_rules 已执行）
    meta_controller.use_db(GlobalConfig.ai_metadata_database)
    rsh_count = meta_controller.execute(text(
        "SELECT COUNT(*) FROM rule_state_history WHERE task_id = :tid"
    ), {"tid": task_id}).fetchone()[0]
    assert rsh_count > 0, (
        f"rule_state_history 应有记录 (证明 applying 阶段已执行), 实际 count={rsh_count}"
    )

    print(f"  ✓ Applying 阶段信号验证通过: status=COMPLETED, rule_state_history={rsh_count} 条记录")

    # 验证 DB 状态完整性
    verify_task_state(meta_controller, online_controller, task_id, outline_type, "Applying 阶段任务")

    print(f"\n[Test 4] ✓ Applying 阶段信号测试通过")


def test_chained_interrupt_resume(
    profile_name: str,
    profile: dict,
    workload_set: list,
    meta_controller: DBController,
    online_controller: DBController,
    outline_type: OutlineType
):
    """
    Test 5: 链式续传（连续两次中断后续传）

    Step 1: 清理环境
    Step 2: task_id_1 执行，trigger=1 模板后中断 → INTERRUPTED
    Step 3: task_id_2 续传 task_1，trigger=上次已处理+1 模板后中断 → INTERRUPTED
    Step 4: task_id_3 续传 → 应跳过 task_1+task_2 的所有已处理模板 → COMPLETED

    关键断言：
    - task_3 的 processed_templates == total_templates
    - task_3 的 prev_task_ids == [task_id_2, task_id_1]
    - task_3 实际处理的模板数 = total - task_1已处理 - task_2已处理
    """
    instance_id = profile["instance_info"]["instance_id"]

    print("\n" + "=" * 60)
    print("[Test 5] 链式续传（连续两次中断后续传）")
    print("=" * 60)

    # ========== Step 1: 清理环境 ==========
    setup_clean_environment(meta_controller, instance_id)
    clean_rule_state(online_controller, outline_type, workload_set)

    # ========== Step 2: 第一次执行 + 中断 ==========
    task_id_1 = str(int(time.time()))
    trigger_count_1 = 1
    print(f"\n[Step 2] 第一次执行 + 中断 (trigger_count={trigger_count_1}, task_id={task_id_1})")

    input_json_1 = build_executor_input_from_profile(
        profile_name=profile_name,
        task_id=task_id_1,
        workload_set=workload_set
    )

    result1 = run_executor_with_interrupt(input_json_1, task_id_1, trigger_count_1)

    print(f"\n[Step 2] 第一次中断结果:")
    print(f"  status: {result1.status.value}")
    print(f"  processed_templates: {result1.stats.processed_templates}/{result1.stats.total_templates}")
    print(f"  rules_generated: {result1.stats.rules_generated}")
    print(f"  extra: {result1.extra}")

    assert result1.status == ExecutionStatus.INTERRUPTED, (
        f"期望 INTERRUPTED, 实际 {result1.status.value}"
    )
    assert result1.stats.processed_templates >= trigger_count_1, (
        f"已处理模板数 {result1.stats.processed_templates} < 触发阈值 {trigger_count_1}"
    )

    task1_processed = result1.stats.processed_templates
    total_templates = result1.stats.total_templates
    print(f"  ✓ 第一次中断: {task1_processed}/{total_templates} 模板已处理")

    verify_task_state(meta_controller, online_controller, task_id_1, outline_type, "第一次中断")

    # ========== Step 3: 第二次执行 (续传 task_1) + 中断 ==========
    time.sleep(1)
    task_id_2 = str(int(time.time()))
    trigger_count_2 = task1_processed + 1
    print(f"\n[Step 3] 第二次执行 + 中断 (trigger_count={trigger_count_2}, task_id={task_id_2}, prev={task_id_1})")

    input_json_2 = build_executor_input_from_profile(
        profile_name=profile_name,
        task_id=task_id_2,
        workload_set=workload_set,
        options={"allow_resume": True, "resume_expiration_days": 7}
    )

    result2 = run_executor_with_interrupt(input_json_2, task_id_2, trigger_count_2)

    print(f"\n[Step 3] 第二次中断结果:")
    print(f"  status: {result2.status.value}")
    print(f"  processed_templates: {result2.stats.processed_templates}/{result2.stats.total_templates}")
    print(f"  rules_generated: {result2.stats.rules_generated}")
    print(f"  extra: {result2.extra}")

    assert result2.status == ExecutionStatus.INTERRUPTED, (
        f"期望 INTERRUPTED, 实际 {result2.status.value}"
    )
    assert result2.extra.get("prev_task_ids") == [task_id_1], (
        f"期望 prev_task_ids=[{task_id_1}], 实际 {result2.extra.get('prev_task_ids')}"
    )

    task2_processed = result2.stats.processed_templates
    print(f"  ✓ 第二次中断: {task2_processed}/{total_templates} 模板已处理 (累计)")

    verify_task_state(meta_controller, online_controller, task_id_2, outline_type, "第二次中断")

    # ========== Step 4: 第三次执行 (续传，应跳过 task_1 + task_2 的所有已处理模板) ==========
    time.sleep(1)
    task_id_3 = str(int(time.time()))
    print(f"\n[Step 4] 链式续传 (task_id={task_id_3}, 应跳过 task_1+task_2 的已处理模板)")

    input_json_3 = build_executor_input_from_profile(
        profile_name=profile_name,
        task_id=task_id_3,
        workload_set=workload_set,
        options={"allow_resume": True, "resume_expiration_days": 7}
    )

    result3 = run_executor(input_json_3)

    print(f"\n[Step 4] 链式续传结果:")
    print(f"  status: {result3.status.value}")
    print(f"  processed_templates: {result3.stats.processed_templates}/{result3.stats.total_templates}")
    print(f"  rules_generated: {result3.stats.rules_generated}")
    print(f"  rules_applied: {result3.stats.rules_applied}")
    print(f"  extra: {result3.extra}")

    # 断言: 链式续传完成
    assert result3.status == ExecutionStatus.COMPLETED, (
        f"期望 COMPLETED, 实际 {result3.status.value}, error={result3.extra.get('error')}"
    )
    assert result3.stats.total_templates == total_templates, (
        f"total_templates {result3.stats.total_templates} != {total_templates}"
    )
    assert result3.stats.processed_templates == total_templates, (
        f"processed_templates {result3.stats.processed_templates} != total {total_templates}"
    )
    assert result3.extra.get("prev_task_ids") == [task_id_2, task_id_1], (
        f"期望 prev_task_ids=[{task_id_2}, {task_id_1}], 实际 {result3.extra.get('prev_task_ids')}"
    )

    print(
        f"  ✓ 链式续传验证通过: {result3.stats.processed_templates}/{total_templates} 模板全部完成 "
        f"(链: task_1={task1_processed}模板, task_2={task2_processed - task1_processed}模板新增, "
        f"task_3={total_templates - task2_processed}模板新增)"
    )

    verify_task_state(meta_controller, online_controller, task_id_3, outline_type, "链式续传任务")

    print(f"\n[Test 5] ✓ 链式续传测试通过")


def main():
    parser = argparse.ArgumentParser(description="Resume (断点续传) Test Runner")
    parser.add_argument("--profile", choices=get_available_profiles(), required=True,
                        help="Test profile (ncdb-spm, ncdb-outline, cdb-spm, cdb-outline)")
    args = parser.parse_args()

    profile = get_profile(args.profile)
    outline_type_str = profile["instance_info"]["outline_type"]
    outline_type = OutlineType(outline_type_str)

    print("=" * 60)
    print(f"断点续传测试 (Profile: {args.profile})")
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

    workload_set = get_workload_tuples()
    print(f"\n[Setup] Workload Set: {len(workload_set)} items")

    # ========== Test 1: 中断 → 续传 ==========
    test_interrupt_and_resume(
        profile_name=args.profile,
        profile=profile,
        workload_set=workload_set,
        meta_controller=meta_controller,
        online_controller=online_controller,
        outline_type=outline_type
    )

    # ========== Test 2: 续传边界情况 ==========
    time.sleep(1)  # 确保 task_id 不同
    test_resume_all_processed(
        profile_name=args.profile,
        profile=profile,
        workload_set=workload_set,
        meta_controller=meta_controller,
        online_controller=online_controller,
        outline_type=outline_type
    )

    # ========== Test 3: 预处理阶段中断 ==========
    time.sleep(1)
    test_interrupt_during_preprocessing(
        profile_name=args.profile,
        profile=profile,
        workload_set=workload_set,
        meta_controller=meta_controller,
        online_controller=online_controller,
        outline_type=outline_type
    )

    # ========== Test 4: Applying 阶段信号 ==========
    time.sleep(1)
    test_signal_during_applying(
        profile_name=args.profile,
        profile=profile,
        workload_set=workload_set,
        meta_controller=meta_controller,
        online_controller=online_controller,
        outline_type=outline_type
    )

    # ========== Test 5: 链式续传 ==========
    time.sleep(1)
    test_chained_interrupt_resume(
        profile_name=args.profile,
        profile=profile,
        workload_set=workload_set,
        meta_controller=meta_controller,
        online_controller=online_controller,
        outline_type=outline_type
    )

    print("\n" + "=" * 60)
    print(f"✓ 断点续传测试全部通过 ({args.profile})")
    print("=" * 60)


if __name__ == "__main__":
    main()
