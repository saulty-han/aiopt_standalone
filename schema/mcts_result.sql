# Table: mcts_result
# 存储 MCTS 优化过程的完整结果（JSON格式）
CREATE TABLE IF NOT EXISTS mcts_result (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    task_id VARCHAR(100) NOT NULL, /* 关联任务ID */
    instance_id VARCHAR(100) NOT NULL,
    db VARCHAR(255) NOT NULL,
    digest VARCHAR(64) NOT NULL,
    result LONGTEXT NOT NULL, /* MCTS 优化结果的完整 JSON 数据 */
    created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
    KEY idx_task_id (task_id),
    KEY idx_instance_digest (instance_id, db, digest),
    KEY idx_instance_id (instance_id)
) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
