#!/usr/bin/env python3
"""
AI Optimizer Executor - 独立执行器入口

解耦后的独立执行器，接收 JSON 输入，执行训练任务，输出 JSON 结果。

Usage:
    python executor.py --input input.json
    echo '{"..."}' | python executor.py --stdin
"""
import sys
import json
import argparse

from pydantic import ValidationError

from ai_logger import aiopt_logger
from db_controller import DBController
from data_models import ExecutorInput, ExecutorResult, ExecutionStatus
from task.task_executor import TrainingExecutor
from config.config import generate_meta_server_config


def parse_args():
    parser = argparse.ArgumentParser(description="AI Optimizer Executor")
    parser.add_argument("--input", type=str, help="Input JSON file path")
    parser.add_argument("--stdin", action="store_true", help="Read input JSON from stdin")
    return parser.parse_args()


def load_input(args) -> ExecutorInput:
    """Load and validate ExecutorInput from file or stdin."""
    if args.stdin:
        input_data = json.load(sys.stdin)
    elif args.input:
        with open(args.input, "r", encoding="utf-8") as f:
            input_data = json.load(f)
    else:
        raise ValueError("Must specify --input or --stdin")
    
    # Pydantic handles all validation and type conversion
    return ExecutorInput.model_validate(input_data)


def main():
    args = parse_args()
    
    # Default result for early failures
    result = ExecutorResult(
        task_id="0",
        status=ExecutionStatus.FAILED,
    )
    
    meta_controller = None
    try:
        # 1. Load input
        executor_input = load_input(args)
        result.task_id = executor_input.task_id
        
        aiopt_logger.info(f"[Executor] Starting task {executor_input.task_id}")
        aiopt_logger.info(f"[Executor] Instance: {executor_input.instance_info.instance_id}")
        aiopt_logger.info(f"[Executor] Operator: {executor_input.operator}")
        
        # 2. Initialize meta controller
        meta_config = generate_meta_server_config()
        meta_controller = DBController(meta_config)
        
        # 3. Execute training
        training_executor = TrainingExecutor(meta_controller)
        result = training_executor.execute(executor_input)
        
        aiopt_logger.info(f"[Executor] Task {executor_input.task_id} finished, status={result.status.value}")
        
    except ValidationError as e:
        result.extra["error"] = f"Input validation failed: {e}"
        aiopt_logger.error(f"[Executor] {result.extra['error']}")
    except Exception as e:
        result.extra["error"] = str(e)
        aiopt_logger.error(f"[Executor] Failed: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if meta_controller:
            try:
                meta_controller.close()
            except Exception:
                pass

    # Output result as JSON
    print(json.dumps(result.model_dump(mode='json'), indent=2, ensure_ascii=False))
    
    sys.exit(0 if result.status == ExecutionStatus.COMPLETED else 1)


if __name__ == "__main__":
    main()
