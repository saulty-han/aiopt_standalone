"""
ClickHouse 查询工具函数

提供带重试机制的 ClickHouse 查询执行，解决并发场景下
"Too many simultaneous queries" (Code 202) 瞬态错误。
"""
import random
import time

from ai_logger import aiopt_logger
from clickhouse_driver import Client


# ClickHouse 可重试错误码
_RETRYABLE_ERROR_CODES = {
    202,  # TOO_MANY_SIMULTANEOUS_QUERIES
}


def _is_retryable(exc: Exception) -> bool:
    """判断 ClickHouse 异常是否为可重试错误"""
    msg = str(exc)
    for code in _RETRYABLE_ERROR_CODES:
        if f"Code: {code}." in msg:
            return True
    return False


def execute_with_retry(
    client: Client,
    sql: str,
    params: dict | None = None,
    *,
    max_retries: int = 5,
    context: str = ""
) -> list:
    """
    带重试的 ClickHouse 查询执行

    重试策略：指数退避 + 随机 jitter，仅对可重试错误重试。

    :param client: ClickHouse 客户端
    :param sql: 查询 SQL
    :param params: 查询参数
    :param max_retries: 最大重试次数 (含首次执行共 max_retries 次)
    :param context: 日志上下文标识 (如 "[Slowlog-CDB]")
    :return: 查询结果列表
    :raises: 最后一次重试仍失败时抛出原始异常
    """
    last_exception = None
    for attempt in range(max_retries):
        try:
            return client.execute(sql, params)
        except Exception as e:
            last_exception = e
            if attempt == max_retries - 1:
                aiopt_logger.error(
                    "%s ClickHouse query failed after %d attempts: %s",
                    context, max_retries, e
                )
                raise
            if not _is_retryable(e):
                aiopt_logger.error(
                    "%s ClickHouse query failed (non-retryable): %s", context, e
                )
                raise
            # 指数退避 + jitter: 2~3s, 4~5s, 6~7s, 8~9s
            wait_time = (attempt + 1) * 2 + random.uniform(0, 1)
            aiopt_logger.warning(
                "%s ClickHouse query failed (attempt %d/%d): %s, retrying in %.1fs",
                context, attempt + 1, max_retries, e, wait_time
            )
            time.sleep(wait_time)
