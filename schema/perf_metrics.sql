# Table: perf_metrics_history
CREATE TABLE IF NOT EXISTS perf_metrics_history (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    cluster_id INT NOT NULL, /* 集群ID */
    instance_id VARCHAR(100) NOT NULL,
    node_uuid VARCHAR(100) NOT NULL, /* 监控节点 UUID */
    db VARCHAR(255) NOT NULL,
    digest VARCHAR(64) NOT NULL,
    period_index INT NOT NULL,
    period_start_time DATETIME(3) NOT NULL,
    period_end_time DATETIME(3) NOT NULL,
    task_id VARCHAR(100) DEFAULT NULL,
    operation VARCHAR(20) DEFAULT NULL,
    execution_count BIGINT NOT NULL DEFAULT 0,
    query_time_avg DOUBLE NOT NULL DEFAULT 0,
    query_time_min DOUBLE NOT NULL DEFAULT 0,
    query_time_max DOUBLE NOT NULL DEFAULT 0,
    rows_examined_avg DOUBLE NOT NULL DEFAULT 0,
    rows_examined_min DOUBLE NOT NULL DEFAULT 0,
    rows_examined_max DOUBLE NOT NULL DEFAULT 0,
    computed_at DATETIME(3) NOT NULL,
    is_finalized BOOLEAN NOT NULL DEFAULT FALSE,
    best_validation_log_id BIGINT DEFAULT NULL,  /* 最佳验证样本 ID (物化) */
    created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
    UNIQUE KEY uk_instance_node_digest_period (cluster_id, instance_id, node_uuid, db, digest, period_index),
    UNIQUE KEY uk_node_db_digest_period (node_uuid, db, digest, period_index),
    KEY idx_cluster_instance (cluster_id, instance_id),
    KEY idx_instance_id (instance_id)
) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
