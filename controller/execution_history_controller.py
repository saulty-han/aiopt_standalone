"""
Execution History Controller

记录每次任务执行的结果，用于：
1. 可观测性：查看任务执行时间线
2. 管控层判断：是否需要续传、上次执行到哪里了

表结构使用 JSON 列存储完整的输入输出对象，
仅保留 task_id, instance_id, status, duration_seconds 作为可查询列。
"""

import json

from sqlalchemy import text
from config.config import GlobalConfig
from data_models import ExecutorInput, ExecutorResult
from db_controller import DBController
from ai_logger import aiopt_logger

# ExecutorInput 中需要脱敏的字段（含密码的 NodeConfig）
_SENSITIVE_INPUT_FIELDS = ("env_config", "master_node")


class ExecutionHistoryController:
    """任务执行历史控制器"""

    @staticmethod
    def _sanitize_input(executor_input: ExecutorInput) -> dict:
        """
        将 ExecutorInput 序列化为 dict，脱敏敏感字段。

        env_config 和 master_node 中包含数据库密码，
        仅保留非敏感的连接信息（ip, port, node_uuid）。
        """
        data = executor_input.model_dump(mode='json')
        for field in _SENSITIVE_INPUT_FIELDS:
            if field in data and isinstance(data[field], dict):
                node = data[field]
                data[field] = {
                    k: v for k, v in node.items()
                    if k not in ("password", "username", "user")
                }
        return data

    @staticmethod
    def record(
        controller: DBController,
        task_id: str,
        instance_id: str,
        node_uuid: str,
        executor_input: ExecutorInput,
        result: ExecutorResult
    ) -> None:
        """
        记录一次执行结果到 task_execution_history 表。
        """
        try:
            controller.use_db(GlobalConfig.ai_metadata_database)

            input_dict = ExecutionHistoryController._sanitize_input(executor_input)
            result_dict = result.model_dump(mode='json')

            insert_sql = text("""
                INSERT INTO task_execution_history (
                    task_id, instance_id, node_uuid, status, duration_seconds,
                    executor_input, executor_result
                ) VALUES (
                    :task_id, :instance_id, :node_uuid, :status, :duration_seconds,
                    :executor_input, :executor_result
                )
            """)
            controller.execute(insert_sql, {
                "task_id": task_id,
                "instance_id": instance_id,
                "node_uuid": node_uuid,
                "status": result.status.value,
                "duration_seconds": result.duration_seconds,
                "executor_input": json.dumps(input_dict, ensure_ascii=False),
                "executor_result": json.dumps(result_dict, ensure_ascii=False),
            })
            aiopt_logger.info(
                f"[ExecutionHistory] Recorded: task={task_id}, status={result.status.value}, "
                f"processed={result.stats.processed_templates}/{result.stats.total_templates}, "
                f"rules={result.stats.rules_generated} generated / {result.stats.rules_applied} applied, "
                f"duration={result.duration_seconds:.1f}s"
            )
        except Exception as e:
            aiopt_logger.error(f"[ExecutionHistory] Failed to record execution history: {e}")

    @staticmethod
    def find_resumable_task(
        controller: DBController,
        instance_id: str,
        node_uuid: str,
        expiration_days: int
    ) -> str | None:
        """
        查找该节点最近一个可续传的任务。

        按 (instance_id, node_uuid) 定位节点：同一实例不同节点的 workload 来源不同，不能跨节点续传。
        仅当该节点在 expiration_days 内的最近一条记录状态为 interrupted 时返回其 task_id。
        如果最近一条记录是 completed 或 failed，返回 None（不跳过它去找更早的 interrupted 记录）。
        """
        controller.use_db(GlobalConfig.ai_metadata_database)

        query = text("""
            SELECT task_id, status FROM task_execution_history
            WHERE instance_id = :instance_id AND node_uuid = :node_uuid
              AND created_at >= NOW() - INTERVAL :days DAY
            ORDER BY created_at DESC
            LIMIT 1
        """)
        row = controller.execute(query, {
            "instance_id": instance_id,
            "node_uuid": node_uuid,
            "days": expiration_days,
        }).fetchone()

        if row and row[1] == "interrupted":
            aiopt_logger.info(
                f"[ExecutionHistory] Found resumable task {row[0]} "
                f"for instance {instance_id}, node {node_uuid}"
            )
            return row[0]

        return None

    @staticmethod
    def get_interrupted_chain(
        controller: DBController,
        instance_id: str,
        node_uuid: str,
        expiration_days: int
    ) -> list[str]:
        """
        获取该节点最近连续的 INTERRUPTED 任务链。

        从最新记录向前遍历，遇到非 INTERRUPTED 状态时停止。
        返回 task_id 列表，顺序为 [最近 → 最早]。
        """
        controller.use_db(GlobalConfig.ai_metadata_database)

        query = text("""
            SELECT task_id, status FROM task_execution_history
            WHERE instance_id = :instance_id AND node_uuid = :node_uuid
              AND created_at >= NOW() - INTERVAL :days DAY
            ORDER BY created_at DESC
        """)
        rows = controller.execute(query, {
            "instance_id": instance_id,
            "node_uuid": node_uuid,
            "days": expiration_days,
        }).fetchall()

        chain = []
        for row in rows:
            if row[1] == "interrupted":
                chain.append(row[0])
            else:
                break

        if chain:
            aiopt_logger.info(
                f"[ExecutionHistory] Found interrupted chain of {len(chain)} tasks "
                f"for instance {instance_id}, node {node_uuid}: {chain}"
            )
        return chain
