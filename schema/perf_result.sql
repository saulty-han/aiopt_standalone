-- ============================================================================
-- query_perf_metrics.sql - 性能监控可视化查询
-- ============================================================================
-- 
-- 功能概述:
--   为性能监控可视化工具 (perfmon_viewer.html) 提供数据，展示规则变更前后的性能对比。
--   仅返回有规则变更历史的 SQL 模板（不包含 baseline 周期）。
--
-- 数据来源:
--   1. perf_metrics_history (curr/prev) - 周期性能指标（来自 AWR 聚合）+ best_validation_log_id（物化）
--   2. validation_logs (best)          - 验证日志（训练时的执行样本，通过物化 ID 直接关联）
--   3. blacklist                       - 黑名单状态
--   4. sql_templates                   - SQL 模板文本（归一化 SQL，digest_text）
--
-- 字段说明:
--   | 字段                    | 来源               | 说明                                           |
--   |------------------------|--------------------|-------------------------------------------------|
--   | instance_id/db/digest  | perf_metrics_history | SQL 模板标识                                   |
--   | digest_text            | sql_templates      | 归一化 SQL 模板（literal 替换为 ?）              |
--   | task_id                | perf_metrics_history | 关联任务 ID                                    |
--   | operation              | perf_metrics_history | 操作类型: setup_plan/modify_plan/reset         |
--   | optimized_status       | 计算字段            | 优化生效状态: 1=新轮次且未黑名单, 0=旧轮次或已黑名单 |
--   | optimized_time         | perf_metrics_history | 周期开始时间（规则生效时间）                    |
--   | query_time_before      | validation_logs    | 默认计划执行时间（验证样本）                    |
--   | query_time_after       | validation_logs    | 优化计划执行时间（验证样本）                    |
--   | query_time_diff_rate   | 计算字段            | 执行时间降低百分比 (正数=优化)                  |
--   | sql_scan_rows_before   | perf_metrics_history prev | 上一周期平均扫描行数（AWR 聚合）          |
--   | sql_scan_rows_after    | perf_metrics_history curr | 当前周期平均扫描行数（AWR 聚合）          |
--   | sql_scan_rows_diff_rate| 计算字段            | 扫描行数降低百分比 (正数=优化)                  |
--   | optimized_sql_count    | perf_metrics_history | 当前周期优化 SQL 数量                          |
--   | plan_*_data_before/after | validation_logs  | EXPLAIN 执行计划（验证样本）                   |
--   | period_status          | perf_metrics_history | 周期状态: 固化/开放                            |
--   | is_blacklisted         | blacklist          | 是否在黑名单中                                 |
--
-- 重要注意事项:
--   1. operation='reset' 时，query_time_* 和 plan_* 字段为 0 或 NULL
--      原因: reset 操作不产生验证日志，无执行时间对比数据
--      前端处理: 应对 reset 类型特殊展示，不显示执行时间对比
--   
--   2. 执行时间与扫描行数数据来源不同:
--      - 执行时间: 来自 validation_logs，是训练时的单次验证样本
--      - 扫描行数: 来自 AWR 聚合的周期平均值
--   
--   3. period_index > 0 过滤:
--      已排除 baseline 周期 (period_index=0)，仅返回有规则变更的周期
--
--   4. best 验证样本:
--      best_validation_log_id 已物化到 perf_metrics_history，直接 LEFT JOIN。
--      当 best_validation_log_id IS NULL 时，LEFT JOIN 自然产生全 NULL 的 best.* 列，
--      COALESCE 已正确处理为 0。
--
-- ============================================================================
CREATE OR REPLACE VIEW perf_result AS
SELECT
    curr.cluster_id,
    curr.instance_id,
    curr.node_uuid,
    curr.db,
    curr.digest,
    st.digest_text,
    curr.task_id,
    curr.operation,
    -- optimized_status: 旧轮次=0, 新轮次(开放期)且未被黑名单=1
    CASE
        WHEN curr.is_finalized = FALSE
        AND bl.id IS NULL THEN 1
        ELSE 0
    END as optimized_status,
    curr.period_start_time as optimized_time,
    -- Performance Comparison (Time): Derived from the Best Optimized Sample in validation_logs
    -- Using COALESCE to ensure 0 is returned if no validation record exists, preventing NULL issues in visualization
    COALESCE(best.default_elapsed_time, 0) as query_time_before,
    COALESCE(best.elapsed_time, 0) as query_time_after,
    COALESCE(
        ROUND(
            IF (
                best.default_elapsed_time > 0,
                (best.default_elapsed_time - best.elapsed_time) / best.default_elapsed_time * 100,
                0
            ),
            1
        ),
        0
    ) as query_time_diff_rate,
    -- Performance Comparison (Rows): Derived from Performance Metrics History (Avg)
    COALESCE(ROUND(prev.rows_examined_avg, 0), 0) as sql_scan_rows_before,
    COALESCE(ROUND(curr.rows_examined_avg, 0), 0) as sql_scan_rows_after,
    COALESCE(
        ROUND(
            IF (
                prev.rows_examined_avg > 0,
                (prev.rows_examined_avg - curr.rows_examined_avg) / prev.rows_examined_avg * 100,
                0
            ),
            1
        ),
        0
    ) as sql_scan_rows_diff_rate,
    curr.execution_count as optimized_sql_count,
    -- Traditional EXPLAIN Plan (From Best Sample)
    best.default_explain_traditional as plan_traditional_data_before,
    best.explain_traditional as plan_traditional_data_after,
    -- JSON EXPLAIN Plan (From Best Sample)
    best.default_analyze_json as plan_json_data_before,
    best.analyze_json as plan_json_data_after,
    IF (curr.is_finalized, '固化', '开放') as period_status,
    IF (bl.id IS NOT NULL, 1, 0) as is_blacklisted
FROM
    perf_metrics_history curr
    LEFT JOIN perf_metrics_history prev ON curr.cluster_id = prev.cluster_id
    AND curr.instance_id = prev.instance_id
    AND curr.node_uuid = prev.node_uuid
    AND curr.db = prev.db
    AND curr.digest = prev.digest
    AND prev.period_index = curr.period_index - 1
    -- Join with blacklist to check status
    LEFT JOIN blacklist bl ON curr.instance_id = bl.instance_id
    AND curr.db = bl.db
    AND curr.digest = bl.digest
    AND bl.enabled = 1
    -- Join with sql_templates to get normalized SQL template text
    LEFT JOIN sql_templates st ON curr.cluster_id = st.cluster_id
    AND curr.instance_id = st.instance_id
    AND curr.db = st.db
    AND curr.digest = st.digest
    -- Join with the best validation result (materialized in perf_metrics_history)
    LEFT JOIN validation_logs best ON best.id = curr.best_validation_log_id
WHERE
    curr.period_index > 0
ORDER BY
    curr.cluster_id,
    curr.instance_id,
    curr.node_uuid,
    curr.db,
    curr.digest;