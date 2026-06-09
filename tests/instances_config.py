"""
Test Instance Configuration

从 tests/profiles.json 读取测试 profile 配置。

首次使用:
    cp tests/profiles.json.example tests/profiles.json
    # 按实际环境修改 ip/port/username/password
"""

import json
from pathlib import Path

# ---------------------------------------------------------------------------
# 配置路径
# ---------------------------------------------------------------------------

_PROFILES_PATH = Path(__file__).parent / "profiles.json"
_EXAMPLE_PATH = Path(__file__).parent / "profiles.json.example"

# ---------------------------------------------------------------------------
# 内部: 读取 & 缓存
# ---------------------------------------------------------------------------

_cached_profiles: dict | None = None


def _load_profiles() -> dict:
    global _cached_profiles
    if _cached_profiles is not None:
        return _cached_profiles

    if not _PROFILES_PATH.exists():
        raise FileNotFoundError(
            f"Profile 配置文件不存在: {_PROFILES_PATH}\n"
            f"请先复制模板并按实际环境修改:\n"
            f"  cp {_EXAMPLE_PATH} {_PROFILES_PATH}"
        )
    _cached_profiles = json.loads(_PROFILES_PATH.read_text(encoding="utf-8"))
    return _cached_profiles


# ---------------------------------------------------------------------------
# 公开 API
# ---------------------------------------------------------------------------

def get_available_profiles() -> list[str]:
    """返回可用的 profile 名称列表"""
    return list(_load_profiles().keys())


def get_profile(name: str) -> dict:
    """
    获取指定的测试 profile。

    :param name: profile 名称 (如 ncdb-spm, cdb-outline)
    :return: profile 配置字典 (env_type + env_config + instance_info + master_node)
    :raises ValueError: 如果 profile 不存在
    """
    profiles = _load_profiles()
    if name not in profiles:
        available = ", ".join(profiles.keys())
        raise ValueError(f"Unknown profile '{name}'. Available: {available}")
    return profiles[name]


def build_executor_input_from_profile(
    profile_name: str, task_id: str,
    workload_set: list = None, options: dict = None
) -> dict:
    """
    从 profile 构建 ExecutorInput 的 JSON dict。

    :param profile_name: profile 名称
    :param task_id: 任务 ID
    :param workload_set: 可选的 workload set，格式为 [(db, digest), ...]
    :param options: 可选参数 dict，如 {"allow_resume": True, "resume_expiration_days": 7}
    :return: ExecutorInput 的 dict 格式（可序列化为 JSON）
    """
    profile = get_profile(profile_name)

    result = {
        "env_config": profile["env_config"],
        "env_type": profile["env_type"],
        "instance_info": profile["instance_info"],
        "master_node": profile["master_node"],
        "task_id": task_id,
        "workload_set": workload_set,
        "operator": "e2e_test",
    }
    if options is not None:
        result["options"] = options
    return result
