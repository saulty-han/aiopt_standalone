#!/usr/bin/env python3
"""
Instance Lookup Table 解析模块

解析 instance_lookup_table.txt，提供按 benchmark instance_id 查找克隆实例 IP/Port 的能力。

文件格式 (每行):
    benchmark_instance_id --- original_id --- clone_id    IP:Port

用法:
    from instance_lookup import InstanceLookup

    lookup = InstanceLookup()  # 自动加载同目录下的 instance_lookup_table.txt

    # 按 benchmark 中的 instance_id 查找
    info = lookup.get("0802c835-1d11-11f1-aada-b8cef6dc748f")
    # info = InstanceInfo(benchmark_id=..., original_id=..., clone_id=..., ip=..., port=...)

    # 也可以按 original_id 查找
    info = lookup.get_by_original("bbcc6cb9-30a2-11f0-aada-b8cef6dc748f")
"""

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional, Set


@dataclass(frozen=True)
class InstanceInfo:
    """一条实例映射记录"""
    benchmark_id: str      # benchmark JSON 中的 instance_id
    original_id: str       # 真实原始业务实例 ID (node_uuid)
    clone_id: str          # 克隆测试实例 ID
    ip: str
    port: int

    @property
    def host(self) -> str:
        """返回 ip:port 格式的连接地址"""
        return f"{self.ip}:{self.port}"


# 匹配一行: UUID --- UUID --- UUID   IP:PORT
_LINE_RE = re.compile(
    r"^\s*"
    r"(?P<benchmark>[0-9a-f\-]+)"
    r"\s*---\s*"
    r"(?P<original>[0-9a-f\-]+)"
    r"\s*---\s*"
    r"(?P<clone>[0-9a-f\-]+)"
    r"\s+"
    r"(?P<ip>[\d.]+):(?P<port>\d+)"
    r"\s*$"
)


class InstanceLookup:
    """
    结构化的实例查找表。

    加载 instance_lookup_table.txt 后，支持:
      - get(benchmark_id)          -> InstanceInfo | None  (按 benchmark instance_id 查)
      - get_by_original(orig_id)   -> InstanceInfo | None  (按原始实例 ID 查)
      - __getitem__(benchmark_id)  -> InstanceInfo (KeyError if missing)
      - __contains__(benchmark_id) -> bool
      - 迭代所有记录
    """

    def __init__(self, path: Optional[str] = None):
        if path is None:
            path = str(Path(__file__).parent / "instance_lookup_table.txt")
        self._path = path
        self._by_benchmark: Dict[str, InstanceInfo] = {}
        self._by_original: Dict[str, InstanceInfo] = {}
        self._load()

    def _load(self):
        with open(self._path, "r") as f:
            for lineno, line in enumerate(f, 1):
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                m = _LINE_RE.match(line)
                if not m:
                    raise ValueError(
                        f"instance_lookup_table.txt 第 {lineno} 行格式错误: {line!r}"
                    )
                info = InstanceInfo(
                    benchmark_id=m.group("benchmark").strip(),
                    original_id=m.group("original").strip(),
                    clone_id=m.group("clone").strip(),
                    ip=m.group("ip"),
                    port=int(m.group("port")),
                )
                if info.benchmark_id in self._by_benchmark:
                    raise ValueError(
                        f"instance_lookup_table.txt 第 {lineno} 行: "
                        f"benchmark_id {info.benchmark_id} 重复"
                    )
                self._by_benchmark[info.benchmark_id] = info
                self._by_original[info.original_id] = info

    # ── 按 benchmark instance_id 查询 ──

    def get(self, benchmark_id: str) -> Optional[InstanceInfo]:
        """根据 benchmark JSON 中的 instance_id 查找"""
        return self._by_benchmark.get(benchmark_id)

    def __getitem__(self, benchmark_id: str) -> InstanceInfo:
        return self._by_benchmark[benchmark_id]

    def __contains__(self, benchmark_id: str) -> bool:
        return benchmark_id in self._by_benchmark

    # ── 按 original_id 查询 ──

    def get_by_original(self, original_id: str) -> Optional[InstanceInfo]:
        """根据原始业务实例 ID (node_uuid) 查找"""
        return self._by_original.get(original_id)

    # ── 通用 ──

    def __len__(self) -> int:
        return len(self._by_benchmark)

    def __iter__(self):
        return iter(self._by_benchmark.values())

    @property
    def benchmark_ids(self) -> Set[str]:
        """返回所有 benchmark instance_id 集合"""
        return set(self._by_benchmark.keys())

    @property
    def original_ids(self) -> Set[str]:
        """返回所有原始实例 ID 集合"""
        return set(self._by_original.keys())

    def get_ip_port(self, benchmark_id: str) -> Optional[tuple]:
        """便捷方法: 按 benchmark_id 返回 (ip, port) 元组"""
        info = self._by_benchmark.get(benchmark_id)
        if info is None:
            return None
        return (info.ip, info.port)

    def summary(self) -> str:
        """打印汇总信息"""
        lines = [f"InstanceLookup: {len(self._by_benchmark)} entries from {self._path}"]
        ip_counts: Dict[str, int] = {}
        for info in self._by_benchmark.values():
            ip_counts[info.ip] = ip_counts.get(info.ip, 0) + 1
        for ip, cnt in sorted(ip_counts.items(), key=lambda x: -x[1]):
            lines.append(f"  {ip}: {cnt} instances")
        return "\n".join(lines)


# ── 快捷全局实例 (懒加载) ──
_global_lookup: Optional[InstanceLookup] = None


def get_lookup(path: Optional[str] = None) -> InstanceLookup:
    """获取全局单例 InstanceLookup（首次调用时加载）"""
    global _global_lookup
    if _global_lookup is None:
        _global_lookup = InstanceLookup(path)
    return _global_lookup


if __name__ == "__main__":
    lookup = InstanceLookup()
    print(lookup.summary())
    print()
    for i, info in enumerate(lookup):
        if i >= 3:
            break
        print(f"  benchmark: {info.benchmark_id}")
        print(f"  original:  {info.original_id}")
        print(f"  clone:     {info.clone_id}")
        print(f"  地址:      {info.host}")
        print()
