#!/usr/bin/env python3
"""
重置规则状态脚本

管控系统完成在线实例操作（reject/disable baseline 或删除 outline 规则）后，
调用本脚本向 rule_state_history 写入一条 reset 或 noop 记录。

Usage:
    echo '{"cluster_id":123,"instance_id":"cdb-xxx","db":"test_db","digest":"a1b2c3d4","task_id":"mgmt_disable_20260402120000","reason":"性能回退"}' \
      | python scripts/reset_rule_state.py --stdin

    python scripts/reset_rule_state.py --input request.json

返回值:
    exit code 0 + stdout JSON: 执行成功
        {"operation": "reset", "success": true}   — 有活跃规则，已写入 reset 记录
        {"operation": "noop", "success": true}     — 无活跃规则，已写入 noop 记录
    exit code 非零 + stderr 异常信息: 执行失败（输入校验失败、数据库连接失败等）
        管控应视为操作失败
"""

import sys
import os
import json
import argparse

# 将项目根目录加入 sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from pydantic import BaseModel

from config.config import generate_meta_server_config
from db_controller import DBController
from controller.rule_state_controller import (
    RuleStateController, RuleOperation, RuleStateChange
)


class ResetRuleStateRequest(BaseModel):
    """重置规则状态请求"""
    cluster_id: int
    instance_id: str
    db: str
    digest: str
    task_id: str
    reason: str


def main():
    parser = argparse.ArgumentParser(description="Reset rule state in rule_state_history")
    parser.add_argument("--stdin", action="store_true", help="Read input JSON from stdin")
    parser.add_argument("--input", type=str, help="Input JSON file path")
    args = parser.parse_args()

    # 1. 读取并校验输入
    if args.stdin:
        input_data = json.load(sys.stdin)
    elif args.input:
        with open(args.input, "r", encoding="utf-8") as f:
            input_data = json.load(f)
    else:
        raise ValueError("Must specify --stdin or --input")

    request = ResetRuleStateRequest.model_validate(input_data)

    # 2. 连接元数据库
    meta_config = generate_meta_server_config()
    meta_controller = DBController(meta_config)

    try:
        # 3. 查询当前状态
        current_state = RuleStateController.get_latest_state(
            meta_controller, request.instance_id, request.db, request.digest
        )

        # 4. 决策操作类型
        operation, reason = RuleStateController.decide_operation(
            current_state=current_state,
            new_plan_ids=None,
            is_reset=True
        )

        # 5. 构造 comments
        if operation == RuleOperation.RESET:
            comments = f"Disabled by management: {request.reason}"
        else:
            # NOOP
            comments = f"Disabled by management (already default): {request.reason}"

        # 6. 获取 prev_plan_ids
        prev_plan_ids = current_state.plan_ids if current_state else None

        # 7. 写入记录
        RuleStateController.record_operation(
            meta_controller,
            RuleStateChange(
                cluster_id=request.cluster_id,
                instance_id=request.instance_id,
                db=request.db,
                digest=request.digest,
                task_id=request.task_id,
                operation=operation,
                prev_plan_ids=prev_plan_ids,
                curr_plan_ids=None,
                comments=comments
            )
        )

        # 8. 输出结果
        result = {
            "operation": operation.value,
            "success": True
        }
        print(json.dumps(result, ensure_ascii=False))

    finally:
        meta_controller.close()


if __name__ == "__main__":
    main()
