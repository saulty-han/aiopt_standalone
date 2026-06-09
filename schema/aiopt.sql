# Table: workload
CREATE TABLE IF NOT EXISTS workload (
    task_id VARCHAR(100) NOT NULL, /* 关联任务ID */
    instance_id VARCHAR(36) NOT NULL,
    db VARCHAR(255) NOT NULL,
    digest VARCHAR(64) NOT NULL,
    plan_id VARCHAR(64) NOT NULL DEFAULT '', /* Plan ID，仅 AWR 数据源有值 */
    sql_text TEXT NOT NULL,
    count_star BIGINT NOT NULL,
    elapsed_time_avg DOUBLE NOT NULL,
    elapsed_time_min DOUBLE NOT NULL,
    elapsed_time_max DOUBLE NOT NULL,
    last_start_time DOUBLE NOT NULL,
    md5 VARCHAR(64) DEFAULT NULL,
    sql_text_min TEXT DEFAULT NULL,
    sql_text_max TEXT DEFAULT NULL,
    is_processed TINYINT(1) NOT NULL DEFAULT 0,
    created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
    PRIMARY KEY (task_id, db, digest, plan_id),
    KEY idx_instance_id (instance_id)
) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


# Table: validation_logs
CREATE TABLE IF NOT EXISTS validation_logs (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    task_id VARCHAR(100) NOT NULL,
    instance_id VARCHAR(100) NOT NULL,
    db VARCHAR(255) NOT NULL,
    digest VARCHAR(64) NOT NULL,
    hints_text TEXT DEFAULT NULL,
    sql_text TEXT NOT NULL,
    sql_text_rewritten TEXT NOT NULL,
    default_plan_id VARCHAR(64) DEFAULT NULL,
    default_elapsed_time DOUBLE NOT NULL,
    default_explain_traditional LONGTEXT DEFAULT NULL,
    default_analyze_json LONGTEXT DEFAULT NULL,
    default_rows_examined BIGINT DEFAULT NULL,
    plan_id VARCHAR(64) NOT NULL,
    elapsed_time DOUBLE NOT NULL,
    explain_traditional LONGTEXT DEFAULT NULL,
    analyze_json LONGTEXT DEFAULT NULL,
    rows_examined BIGINT DEFAULT NULL,
    is_best BOOLEAN NOT NULL DEFAULT FALSE,
    is_better BOOLEAN NOT NULL DEFAULT FALSE,
    comments TEXT DEFAULT NULL,
    created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    speedup_ratio DOUBLE GENERATED ALWAYS AS
        (default_elapsed_time / GREATEST(elapsed_time, 0.000001)) VIRTUAL,
    KEY idx_task_id (task_id),
    KEY idx_instance_digest_task_ratio (instance_id, db, digest, task_id, speedup_ratio DESC)
) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


# Table: rules
CREATE TABLE IF NOT EXISTS rules (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    task_id VARCHAR(100) NOT NULL, /* 关联任务ID */
    cluster_id INT NOT NULL, /* 集群ID */
    instance_id VARCHAR(100) NOT NULL,
    db VARCHAR(255) NOT NULL,
    digest VARCHAR(64) NOT NULL,
    action ENUM('optimize', 'default') NOT NULL, /* OPTIMIZE=更优计划, DEFAULT=重置干预 */
    plan_id VARCHAR(64) DEFAULT NULL, /* 决策出的最优计划 ID, DEFAULT时为空 */
    hints_text TEXT DEFAULT NULL, /* 纯 hints 文本 */
    sql_text TEXT NOT NULL, /* SQL 样本 (Statement OUTLINE 用) */
    sql_text_rewritten TEXT NOT NULL, /* 干预后 SQL 样本 (Statement OUTLINE 用) */
    feedback_timeout BIGINT UNSIGNED NOT NULL DEFAULT 0, /* 单位为 ms */
    md5 VARCHAR(64) DEFAULT NULL, /* Slow Log 数据源的 MD5 哈希，用于性能监控查询 */
    comments TEXT DEFAULT NULL, /* 备注 */
    created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
    UNIQUE KEY uk_task_digest_plan (task_id, db, digest, plan_id),
    KEY idx_cluster_instance (cluster_id, instance_id),
    KEY idx_instance_id (instance_id),
    KEY idx_instance_db_digest (instance_id, db, digest)
) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


# Table: blacklist
CREATE TABLE IF NOT EXISTS blacklist (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    cluster_id INT NOT NULL, /* 集群ID */
    instance_id VARCHAR(100) NOT NULL,
    db VARCHAR(255) NOT NULL,
    digest VARCHAR(64) NOT NULL,
    enabled BOOLEAN NOT NULL, /* TRUE=此条目有效,该SQL模板将被跳过训练; FALSE=此条目无效 */
    comments TEXT,
    created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
    UNIQUE KEY uk_cluster_instance_db_digest (cluster_id, instance_id, db, digest),
    UNIQUE KEY uk_instance_db_digest (instance_id, db, digest),
    KEY idx_instance_id (instance_id)
) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


# Table: rule_state_history
CREATE TABLE IF NOT EXISTS rule_state_history (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    cluster_id INT NOT NULL, /* 集群ID */
    instance_id VARCHAR(100) NOT NULL,
    db VARCHAR(255) NOT NULL,
    digest VARCHAR(64) NOT NULL,
    task_id VARCHAR(100) NOT NULL,
    operation ENUM('setup_plan', 'modify_plan', 'modify_timeout', 'reset', 'noop') NOT NULL,
    prev_plan_ids JSON DEFAULT NULL,
    curr_plan_ids JSON DEFAULT NULL,
    apply_time DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    comments TEXT DEFAULT NULL,
    created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
    KEY idx_cluster_instance_db_digest (cluster_id, instance_id, db, digest),
    KEY idx_instance_db_digest (instance_id, db, digest),
    KEY idx_instance_id (instance_id),
    KEY idx_apply_time (apply_time)
) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


# Table: task_execution_history
# 追加记录每次执行的结果，同一 task_id 可能有多条（首次 + N 次续传）
# executor_input / executor_result 以 JSON 形式存储完整的输入输出（敏感字段已脱敏）
CREATE TABLE IF NOT EXISTS task_execution_history (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    task_id VARCHAR(100) NOT NULL,
    instance_id VARCHAR(100) NOT NULL,
    node_uuid VARCHAR(100) NOT NULL,
    status ENUM('completed', 'interrupted', 'failed') NOT NULL,
    duration_seconds DOUBLE NOT NULL DEFAULT 0.0,
    executor_input JSON DEFAULT NULL,
    executor_result JSON DEFAULT NULL,
    created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    KEY idx_task_id (task_id),
    KEY idx_instance_node (instance_id, node_uuid)
) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


# Table: task_progress
# 训练进度实时追踪，管控可随时轮询
CREATE TABLE IF NOT EXISTS task_progress (
    task_id             VARCHAR(100) NOT NULL,
    instance_id         VARCHAR(100) NOT NULL,
    node_uuid           VARCHAR(100) NOT NULL,
    stage               VARCHAR(50)  NOT NULL,                    -- initializing / loading_workload / optimizing / applying / finished
    total_templates     INT DEFAULT NULL,                         -- NULL = 尚未统计（如 loading_workload 阶段）
    completed_templates INT DEFAULT NULL,                         -- NULL = 尚未统计；非 NULL 时含续传已处理数
    started_at          DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    updated_at          DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
    PRIMARY KEY (task_id),
    KEY idx_instance_node (instance_id, node_uuid)
) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


# Table: sql_templates
# 存储 SQL 模板文本 (digest_text)，即 SQL 的参数化/归一化形式
# digest 是 statement_digest() 的输出，digest_text 是 statement_digest_text() 的输出
# 在优化训练流程中填充，每个 (cluster_id, instance_id, db, digest) 只存一行
CREATE TABLE IF NOT EXISTS sql_templates (
    cluster_id  INT          NOT NULL,   /* 集群ID */
    instance_id VARCHAR(100) NOT NULL,
    db          VARCHAR(255) NOT NULL,
    digest      VARCHAR(64)  NOT NULL,
    digest_text TEXT         NOT NULL,   /* statement_digest_text() 输出的归一化 SQL 模板 */
    created_at  DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    PRIMARY KEY (cluster_id, instance_id, db, digest)
) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
