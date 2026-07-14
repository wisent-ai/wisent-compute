"""Live host telemetry used by local admission.

Linux ``MemAvailable`` and NVML/nvidia-smi free memory already account for
Stado slots and external tenants exactly once.
"""
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from .admission import HostSnapshot

_GIB_KIB = 1024.0 * 1024.0


def _proc_meminfo(path: str = "/proc/meminfo") -> dict[str, float]:
    try:
        lines = Path(path).read_text(encoding="utf-8").splitlines()
    except OSError:
        return {}
    values: dict[str, float] = {}
    for line in lines:
        key, separator, raw = line.partition(":")
        if not separator:
            continue
        fields = raw.strip().split()
        if not fields:
            continue
        try:
            kib = float(fields[0])
        except ValueError:
            continue
        values[key] = kib / _GIB_KIB
    return values


def collect_host_snapshot(
    *,
    gpu_total_gb: float,
    gpu_free_gb: float,
    meminfo_path: str = "/proc/meminfo",
) -> HostSnapshot:
    """Collect one immutable, fail-closed admission snapshot."""
    memory = _proc_meminfo(meminfo_path)
    required = ("MemTotal", "MemAvailable", "SwapFree")
    memory_complete = all(key in memory for key in required)
    gpu_complete = gpu_total_gb >= 0 and gpu_free_gb >= 0
    quality = "live" if memory_complete and gpu_complete else "missing"
    return HostSnapshot(
        mem_total_gb=memory.get("MemTotal", -1.0),
        mem_available_gb=memory.get("MemAvailable", -1.0),
        swap_free_gb=memory.get("SwapFree", -1.0),
        gpu_total_gb=float(gpu_total_gb),
        gpu_free_gb=float(gpu_free_gb),
        timestamp=datetime.now(timezone.utc).isoformat(),
        telemetry_quality=quality,
    )
