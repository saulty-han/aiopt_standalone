class InstanceNonReadonlyException(Exception):
  def __init__(self, message="Instance is not in readonly mode"):
    self.message = message
    super().__init__(self.message)

  def __str__(self):
    return f"InstanceNonReadonlyException: {self.message}"


class AiOptError(Exception):
    """AI 优化器基础异常"""
    pass


class PlanCaptureError(AiOptError):
    """计划捕获失败异常

    当 SQL 不支持计划捕获（如引用系统表/I_S表）时抛出
    """
    pass


class WorkloadLoadError(AiOptError):
    """Workload 加载失败异常
    
    当从数据源（如 ClickHouse, MySQL）加载 Workload 失败时抛出
    """
    pass


class DBConnectionError(AiOptError):
    """数据库连接/会话异常
    
    当数据库连接建立失败、SESSION 配置失败或训练环境 feature 禁用失败时抛出
    """
    pass