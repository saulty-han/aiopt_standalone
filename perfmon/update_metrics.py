"""
性能监控更新任务

执行并更新所有实例的性能指标

Usage:
    # 从文件读取实例列表（测试用）
    python perfmon/update_metrics.py --instances-file instances.json

    # 从管控接口获取本集群所有实例（生产用）
    python perfmon/update_metrics.py --from-api
"""
import sys
import os
import argparse
import json

# Add project root to path for standalone execution
if __name__ == "__main__":
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from datetime import datetime

from ai_logger import perf_logger
from db_controller import DBController
from data_models import PerfMonInstanceInfo
from perfmon.performance_comparator import PerformanceComparator
from config.config import generate_meta_server_config
import interfaces


def run_performance_update(instances: list[PerfMonInstanceInfo]) -> dict:
    """
    更新所有实例的性能指标

    :param instances: 性能监控节点列表
    :return: 执行结果统计
    """
    start_time = datetime.now()
    perf_logger.info(f"[PerfMon-Task] Starting performance update at {start_time}")

    # 创建元数据库连接
    meta_config = generate_meta_server_config()
    meta_controller = DBController(meta_config)

    try:
        perf_logger.info(f"[PerfMon-Task] Processing {len(instances)} instances")

        # 处理结果统计
        results = {
            "total_instances": len(instances),
            "success_count": 0,
            "error_count": 0,
            "total_updates": 0,
            "errors": []
        }

        comparator = PerformanceComparator(meta_controller)

        for instance_info in instances:
            try:
                updated, template_failures = comparator.update_instance_metrics(instance_info)
                results["total_updates"] += updated
                if template_failures > 0:
                    results["error_count"] += 1
                    error_msg = (
                        f"instance={instance_info.instance_id}, node={instance_info.node_uuid}: "
                        f"{template_failures} template(s) failed"
                    )
                    results["errors"].append(error_msg)
                    perf_logger.error(f"[PerfMon-Task] Partial failure: {error_msg}")
                else:
                    results["success_count"] += 1
            except Exception as e:
                results["error_count"] += 1
                error_msg = f"instance={instance_info.instance_id}, node={instance_info.node_uuid}: {str(e)}"
                results["errors"].append(error_msg)
                perf_logger.error(f"[PerfMon-Task] Error processing node: {error_msg}")

        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        perf_logger.info(
            f"[PerfMon-Task] Completed in {duration:.2f}s. "
            f"Success: {results['success_count']}/{results['total_instances']}, "
            f"Updates: {results['total_updates']}"
        )

        # 如果有任何错误，抛出异常以确保失败不被掩盖
        if results["error_count"] > 0:
            error_summary = (
                f"Performance update partially failed: "
                f"{results['error_count']} errors. "
                f"Errors: {results['errors']}"
            )
            raise RuntimeError(error_summary)

        return results

    finally:
        meta_controller.close()


def load_instances_from_file(file_path: str) -> list[PerfMonInstanceInfo]:
    """
    从 JSON 文件加载实例列表

    :param file_path: JSON 文件路径
    :return: 实例列表
    """
    with open(file_path, 'r') as f:
        data = json.load(f)

    instances = []
    for item in data:
        instances.append(PerfMonInstanceInfo(**item))

    return instances


def load_instances_from_api() -> list[PerfMonInstanceInfo]:
    """
    从管控接口获取本集群所有实例列表

    :return: 实例列表
    """
    return interfaces.get_perfmon_instances()


def main():
    parser = argparse.ArgumentParser(description="性能监控更新任务")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--instances-file",
        type=str,
        help="从 JSON 文件读取实例列表（测试用）"
    )
    group.add_argument(
        "--from-api",
        action="store_true",
        help="从管控接口获取本集群所有实例（生产用）"
    )

    args = parser.parse_args()

    try:
        if args.instances_file:
            instances = load_instances_from_file(args.instances_file)
        else:
            instances = load_instances_from_api()

        result = run_performance_update(instances)
        print(f"Performance update completed successfully: {result}")
        sys.exit(0)
    except NotImplementedError as e:
        print(f"Performance update failed: {e}", file=sys.stderr)
        sys.exit(3)
    except RuntimeError as e:
        print(f"Performance update failed: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Performance update unexpected error: {e}", file=sys.stderr)
        sys.exit(2)


if __name__ == "__main__":
    main()
