import datetime
from dataclasses import dataclass, fields
from pydantic import BaseModel, Field
import enum


@dataclass
class FeatureFlags:
    """
    Feature flags determined by dynamic detection.

    Attributes:
        supports_hints_extraction: If True, can extract Outline Data hints from EXPLAIN.
        supports_spm: If True, supports SQL Plan Management.
        supports_statement_outline: If True, supports Statement Outline.
        supports_rows_examined: If True, supports rows_examined in EXPLAIN ANALYZE FORMAT=JSON_V2.
    """
    supports_hints_extraction: bool = False
    supports_spm: bool = False
    supports_statement_outline: bool = False
    supports_rows_examined: bool = False

    def __repr__(self) -> str:
        return (
            f"FeatureFlags("
            f"hints_extraction={self.supports_hints_extraction}, "
            f"spm={self.supports_spm}, "
            f"outline={self.supports_statement_outline}, "
            f"rows_examined={self.supports_rows_examined})"
        )

    def is_superset_of(self, other: 'FeatureFlags') -> bool:
        """检查 self 是否支持 other 所需的所有 feature"""
        for f in fields(self):
            if getattr(other, f.name) and not getattr(self, f.name):
                return False
        return True


class InstanceConfig(BaseModel):
    """数据库实例连接配置"""
    instance_id: str = Field(..., description="Unique identifier for the instance")
    ip: str = Field(..., description="IP address of the instance")
    port: int = Field(..., description="Port number of the instance")
    user: str = Field(..., description="Username for the instance")
    password: str = Field(..., description="Password for the instance")
    read_only: bool = Field(..., description="Whether the instance must be read-only")
    with_ai_marker: bool = Field(..., description="Whether to prefix with /* TxsqlAImarker */ for every SQL")
    allow_reconnect: bool = Field(..., description="Whether to allow reconnect to the instance")
    is_meta_server: bool = Field(default=False, description="Whether this is the AI metadata server (skip session optimizations)")


class SlowlogRowAggregated(BaseModel):
    """
    Aggregated WorkloadRow.
    """
    instance_id: str = Field(..., description="Unique identifier for the instance")
    db: str = Field(..., description="Database name")
    sql_text: str = Field(..., description="SQL text of the query (min query time)")
    sql_text_min: str | None = Field(None, description="SQL text of the min query time")
    sql_text_max: str | None = Field(None, description="SQL text of the max query time")
    count_star: int = Field(..., description="Number of times the query was executed")
    elapsed_time_avg: float = Field(..., description="Average Query execution time in seconds, including microseconds"),
    elapsed_time_min: float = Field(..., description="Minimum query execution time in seconds, including microseconds")
    elapsed_time_max: float = Field(..., description="Maximum query execution time in seconds, including microseconds")
    last_start_time: float = Field(..., description="Start time of the query execution in seconds, including microseconds")
    md5: str | None = Field(None, description="MD5 hash of the SQL text")

    class Config:
        from_attributes = True


class WorkloadRow(BaseModel):
    instance_id: str = Field(..., description="Unique identifier for the instance")
    db: str = Field(..., description="Database name")
    sql_text: str = Field(..., description="SQL text of the query")
    query_time: float = Field(..., description="Query execution time in seconds, including microseconds")
    start_time: float = Field(..., description="Start time of the query execution in seconds, including microseconds")
    md5: str | None = Field(None, description="MD5 hash of the SQL text")

    class Config:
        from_attributes = True


class WorkloadRowAggregated(BaseModel):
    """
    Aggregated WorkloadRow.
    """
    instance_id: str = Field(..., description="Unique identifier for the instance")
    db: str = Field(..., description="Database name")
    digest: str = Field(..., description="Statement digest of sql_text")
    sql_text: str = Field(..., description="SQL text of the query")
    count_star: int = Field(..., description="Number of times the query was executed")
    elapsed_time_avg: float = Field(..., description="Average Query execution time in seconds, including microseconds"),
    elapsed_time_min: float = Field(..., description="Minimum query execution time in seconds, including microseconds")
    elapsed_time_max: float = Field(..., description="Maximum query execution time in seconds, including microseconds")
    last_start_time: float = Field(..., description="Start time of the query execution in seconds, including microseconds")
    plan_id: str | None = Field(None, description="Plan ID (only available in AWR data)")
    md5: str | None = Field(None, description="MD5 hash of the SQL text")
    sql_text_min: str | None = Field(None, description="SQL text of the min query")
    sql_text_max: str | None = Field(None, description="SQL text of the max query")

    class Config:
        from_attributes = True



class RuleAction(enum.Enum):
    """规则动作类型"""
    OPTIMIZE = "optimize"  # 找到更优计划，需要固定/accept
    DEFAULT = "default"    # 需要重置干预规则（无法找到统一最优计划）


class DecidedRule(BaseModel):
    # 1. identification
    task_id: str = Field(..., description="关联任务ID")
    cluster_id: int = Field(..., description="集群ID")
    instance_id: str = Field(..., description="实例ID")
    db: str = Field(..., description="数据库名")
    digest: str = Field(..., description="SQL 模板 digest")
    
    # 2. 决策结论
    action: RuleAction = Field(..., description="规则动作: OPTIMIZE=找到更优计划, DEFAULT=使用默认计划")
    plan_id: str | None = Field(None, description="决策出的最优计划 ID, RESET时为空")
    hints_text: str | None = Field(None, description="纯 hints 文本")
    
    # 3. Text (Statement OUTLINE 需要)
    sql_text: str = Field(..., description="SQL 样本")
    sql_text_rewritten: str = Field(..., description="干预后 SQL 样本")
    
    # 4. State
    feedback_timeout: int = Field(default=0, description="Feedback timeout in ms")
    md5: str | None = Field(None, description="Slow Log 数据源的 MD5 哈希，用于性能监控查询")
    comments: str | None = Field(None, description="备注")

    class Config:
        from_attributes = True





class WorkloadSource(enum.Enum):
    SLOW_LOG = "slow_log"
    SLOW_LOG_TABLE = "slowlog_table"
    AWR = "awr"
    AWR_TABLE = "awr_table"
    AUDIT_LOG = "audit_log"

class OutlineType(enum.Enum):
    NOT_SUPPORT = "not_support"
    STATEMENT_OUTLINE = "statement_outline"
    SPM = "spm"

class Region(enum.Enum):
    test = "test"
    gz = "gz"
    sh = "sh"
    bj = "bj"
    sg = "sg"


class TrainingEnvType(enum.Enum):
    """训练环境类型枚举"""
    SLAVE = "slave"           # Slave 节点（可被抢占，支持超时切换）
    RO = "ro"                 # 内部只读实例（不可抢占）
    CLONE = "clone"           # 克隆环境（不可抢占，用于读写任务或超时切换）



class ProductType(enum.Enum):
    CDB = "cdb"
    NCDB = "ncdb"


class InstanceInfo(BaseModel):
    """
    实例节点信息模型 - 每个节点 (product_type, instance_id, node_uuid) 一条记录

    一个实例可能有多个节点，每个节点在表中存储为单独一行
    """
    cluster_id: int = Field(..., description="集群ID")
    product_type: ProductType = Field(..., description="产品类型: cdb/ncdb")
    instance_id: str = Field(..., description="节点所属的实例ID")
    node_uuid: str = Field(..., description="节点 UUID")
    workload_source: WorkloadSource = Field(..., description="数据源类型: awr/slow_log")
    outline_type: OutlineType = Field(..., description="Outline 类型: spm/statement_outline")
    region: Region = Field(..., description="地域: bj/sh/gz/sg/test")
    comments: str | None = Field(None, description="备注")

    class Config:
        from_attributes = True


class PerfMonInstanceInfo(BaseModel):
    """
    性能监控节点信息 - 从管控接口获取

    仅包含管控接口返回的字段，用于性能监控定时任务
    """
    cluster_id: int = Field(..., description="集群ID")
    product_type: ProductType = Field(..., description="产品类型: cdb/ncdb")
    instance_id: str = Field(..., description="节点所属的实例ID")
    node_uuid: str = Field(..., description="节点 UUID")
    region: Region = Field(..., description="地域: bj/sh/gz/sg/test")

    class Config:
        from_attributes = True

class ValidationLogEntry(BaseModel):
    """
    Validation log entry for storing evaluated plan results.
    Formerly defined in validation_logs_controller.py.
    """
    # 1. identification
    task_id: str = Field(..., description="Task ID")
    instance_id: str = Field(..., description="Instance ID")
    db: str = Field(..., description="Database name")
    digest: str = Field(..., description="SQL digest")
    
    # 2. Text / Hints
    hints_text: str = Field(..., description="Hints text")
    sql_text: str = Field(..., description="SQL text")
    sql_text_rewritten: str = Field(..., description="Rewritten SQL text")
    
    # 3. Default (Baseline)
    default_plan_id: str | None = Field(None, description="Default Plan ID")
    default_elapsed_time: float | None = Field(None, description="Default execution time")
    default_explain_traditional: str | None = Field(None, description="Default traditional explain")
    default_analyze_json: str | None = Field(None, description="Default analyze JSON")
    default_rows_examined: int | None = Field(None, description="Default rows examined")

    # 4. Candidate
    plan_id: str = Field(default="", description="Candidate Plan ID")
    elapsed_time: float | None = Field(None, description="Candidate execution time")
    explain_traditional: str | None = Field(None, description="Candidate traditional explain")
    analyze_json: str | None = Field(None, description="Candidate analyze JSON")
    rows_examined: int | None = Field(None, description="Candidate rows examined")
    
    # 5. Conclusion
    is_best: bool = Field(default=False, description="Is this the best plan?")
    is_better: bool = Field(default=False, description="Is this plan better than default?")
    comments: str | None = Field(None, description="Comments")

    class Config:
        from_attributes = True


# =============================================================================
# Executor Interface Models (解耦执行器接口)
# =============================================================================

class NodeConfig(BaseModel):
    """
    节点连接配置 (管控 JSON 输入格式)
    
    直接匹配管控传入的 JSON 格式，无需转换
    """
    node_ip: str = Field(..., description="节点 IP")
    node_port: int = Field(..., description="节点端口")
    node_uuid: str = Field(..., description="节点 UUID")
    username: str = Field(..., description="用户名")
    password: str = Field(..., description="密码")
    
    def to_instance_config(
        self, 
        *, 
        read_only: bool,
        with_ai_marker: bool,
        allow_reconnect: bool,
        is_meta_server: bool = False
    ) -> InstanceConfig:
        """
        转换为内部 InstanceConfig 格式
        
        所有参数必须显式指定，禁止使用默认值
        """
        return InstanceConfig(
            instance_id=self.node_uuid,
            ip=self.node_ip,
            port=self.node_port,
            user=self.username,
            password=self.password,
            read_only=read_only,
            with_ai_marker=with_ai_marker,
            allow_reconnect=allow_reconnect,
            is_meta_server=is_meta_server
        )

    class Config:
        from_attributes = True


class ExecutorOptions(BaseModel):
    """
    可选参数

    控制续传等行为，允许为空值，允许不传递 options 以及其中的内容。
    """
    allow_resume: bool = Field(default=False, description="是否开启断点续传")
    resume_expiration_days: int = Field(default=7, description="续传状态有效期（天）")
    sql_template_limit: int | None = Field(default=None, description="最大训练模板数, None 表示使用配置文件默认值")


class ExecutorInput(BaseModel):
    """
    执行器输入参数

    管控通过 JSON 传递给执行器的所有必要信息
    所有字段通过 Pydantic 验证，保证类型安全
    """
    # 1. 训练环境信息
    env_config: NodeConfig = Field(..., description="训练环境连接配置")
    env_type: TrainingEnvType = Field(..., description="训练环境类型")

    # 2. 实例信息
    instance_info: InstanceInfo = Field(..., description="实例信息")

    # 3. 在线实例连接 (master/rw)
    master_node: NodeConfig = Field(..., description="主节点连接配置")

    # 4. 任务信息
    task_id: str = Field(..., description="任务 ID")
    workload_set: list[tuple[str, str]] | None = Field(None, description="增量训练 SQL 集 [(db, digest)], None 表示全量")

    # 5. 操作人
    operator: str = Field(..., description="操作人标识")

    # 6. 可选参数
    options: ExecutorOptions | None = Field(default=None, description="可选参数（续传控制等）")

    class Config:
        from_attributes = True


class ExecutionStatus(enum.Enum):
    """执行终止状态"""
    COMPLETED = "completed"     # 全部模板处理完成，规则应用完成
    INTERRUPTED = "interrupted" # 被外部信号中止 (SIGUSR1)，结果有效，可续传
    FAILED = "failed"           # 异常退出


class ExecutionStage(enum.Enum):
    """执行阶段（细粒度位点追踪）"""
    INITIALIZING = "initializing"         # 连接建立、特征检测 (steps 2-4)
    LOADING_WORKLOAD = "loading_workload" # workload 预处理/加载/过滤 (steps 5-7.6)
    OPTIMIZING = "optimizing"             # 构建 payloads + 并行优化 (steps 8-12.6)
    APPLYING = "applying"                 # 规则应用 (steps 13-14)
    FINISHED = "finished"                 # 全部流程执行完毕


class ExecutionStats(BaseModel):
    """执行统计"""
    total_templates: int = Field(default=0, description="总 SQL 模板数")
    processed_templates: int = Field(default=0, description="已处理的 SQL 模板数")
    failed_templates: int = Field(default=0, description="失败的 SQL 模板数")
    rules_generated: int = Field(default=0, description="生成的规则数量")
    rules_applied: int = Field(default=0, description="成功应用的规则数量")


class ExecutorResult(BaseModel):
    """
    执行器输出结果

    执行器完成后返回的 JSON 结果，供管控解析。

    管控层根据 status 判断后续动作：
    - COMPLETED: 正常完成，无需额外处理
    - INTERRUPTED: 被 SIGUSR1 中止，结果有效，可续传
    - FAILED: 异常退出，查看 extra["error"]

    extra 字段用于可观测性（日志、监控、排查）：
    - extra["stage"]: 最终执行位点 (ExecutionStage.value)，始终设置
    - extra["interrupt_stage"]: 中断/异常发生时的阶段 (仅 INTERRUPTED/FAILED 时设置)
    - extra["error"]: 错误信息 (仅 FAILED 时设置)
    """
    task_id: str = Field(..., description="任务 ID")
    status: ExecutionStatus = Field(..., description="执行终止状态")
    duration_seconds: float = Field(default=0.0, description="执行耗时（秒）")
    stats: ExecutionStats = Field(default_factory=ExecutionStats, description="执行统计")
    extra: dict = Field(default_factory=dict, description="扩展信息")

    class Config:
        from_attributes = True


# =============================================================================
# Parallel Optimization Models (并行优化数据模型)
# =============================================================================

class OptimizationTaskPayload(BaseModel):
    """
    可序列化的优化任务负载

    用于跨进程传递优化任务，所有字段必须可序列化。
    env_config 由主进程预先计算为 InstanceConfig，Worker 直接使用。
    """
    # 任务标识
    task_id: str = Field(..., description="任务 ID")
    instance_id: str = Field(..., description="实例 ID")
    cluster_id: int = Field(..., description="集群 ID")
    sql_progress: str = Field(..., description="SQL 进度标识，如 '[SQL 3/10]'")

    # SQL 模板标识
    db: str = Field(..., description="数据库名")
    digest: str = Field(..., description="SQL digest")

    # 工作负载
    workloads: list[WorkloadRowAggregated] = Field(..., description="工作负载列表")

    # 环境配置（由主进程预先计算好的完整 InstanceConfig）
    env_config: InstanceConfig = Field(..., description="训练环境连接配置")
    env_type: TrainingEnvType = Field(..., description="训练环境类型（元信息）")

    # 实例信息
    instance_info: InstanceInfo = Field(..., description="实例信息")

    # Feature flags
    feature_flags: FeatureFlags = Field(..., description="Feature flags")

    # Outline 类型
    outline_type: OutlineType = Field(..., description="Outline 类型")

    class Config:
        from_attributes = True


class OptimizationTaskResult(BaseModel):
    """
    可序列化的优化任务结果

    Worker 进程执行 optimize_template 后返回的结果
    """
    # 标识
    db: str = Field(..., description="数据库名")
    digest: str = Field(..., description="SQL digest")
    sql_progress: str = Field(..., description="SQL 进度标识")

    # 执行状态
    success: bool = Field(..., description="是否执行成功")
    error_type: str | None = Field(None, description="异常类名")
    error_message: str | None = Field(None, description="错误信息")

    # 结果数据
    rules: list[DecidedRule] = Field(default_factory=list, description="生成的规则列表")
    evaluated_logs: list[ValidationLogEntry] = Field(default_factory=list, description="评估日志列表")
    mcts_results: list[dict] | None = Field(None, description="MCTS优化结果（JSON格式，仅LLM优化器使用）")

    # digest_text
    digest_text: str | None = Field(None, description="归一化 SQL 模板 (statement_digest_text 输出)")

    # 统计信息
    training_time: float = Field(default=0.0, description="训练耗时（秒）")

    class Config:
        from_attributes = True
