"""Diagnostics and opt-in policy for workloads outside Stado cgroups."""
from __future__ import annotations

import os
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

_GIB_KIB = 1024.0 * 1024.0


@dataclass(frozen=True)
class ExternalProcess:
    pid: int
    name: str
    pss_gb: float
    rss_gb: float
    vram_gb: float
    protected: bool = True
    policy_name: str = "default-protected-external"

    def diagnostic(self) -> dict[str, object]:
        return {
            "pid": self.pid,
            "name": self.name,
            "pss_gb": round(self.pss_gb, 3),
            "rss_gb": round(self.rss_gb, 3),
            "vram_gb": round(self.vram_gb, 3),
            "protected": self.protected,
            "policy_name": self.policy_name,
        }


def _status_memory(status_path: Path) -> tuple[str, float, float]:
    name = "unknown"
    rss_kib = 0.0
    try:
        lines = status_path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return name, 0.0, 0.0
    for line in lines:
        key, separator, raw = line.partition(":")
        if not separator:
            continue
        if key == "Name":
            name = raw.strip()[:64] or "unknown"
        elif key == "VmRSS":
            try:
                rss_kib = float(raw.strip().split()[0])
            except (ValueError, IndexError):
                pass
    pss_kib = 0.0
    try:
        for line in status_path.with_name("smaps_rollup").read_text(
            encoding="utf-8",
        ).splitlines():
            if line.startswith("Pss:"):
                pss_kib = float(line.split()[1])
                break
    except (OSError, ValueError, IndexError):
        pass
    return name, pss_kib / _GIB_KIB, rss_kib / _GIB_KIB


def _process_cgroup(proc_dir: Path) -> str:
    try:
        lines = (proc_dir / "cgroup").read_text(encoding="utf-8").splitlines()
    except OSError:
        return ""
    for line in lines:
        fields = line.split(":", 2)
        if len(fields) == 3 and fields[0] == "0":
            return fields[2]
    return ""


def _matching_policy(
    pid: int,
    cgroup: str,
    policies: Iterable[dict[str, object]],
) -> dict[str, object] | None:
    for policy in policies:
        if policy.get("pid") == pid:
            return policy
        expected = str(policy.get("cgroup", "") or "")
        if expected and (
            cgroup == expected or cgroup.startswith(expected.rstrip("/") + "/")
        ):
            return policy
        unit = str(policy.get("systemd_unit", "") or "")
        if unit and cgroup.endswith("/" + unit):
            return policy
    return None


def _gpu_usage_by_pid() -> dict[int, float]:
    try:
        result = subprocess.run(
            [
                "nvidia-smi",
                "--query-compute-apps=pid,used_memory",
                "--format=csv,noheader,nounits",
            ],
            capture_output=True,
            text=True,
        )
    except OSError:
        return {}
    if result.returncode != 0:
        return {}
    usage: dict[int, float] = {}
    for line in result.stdout.splitlines():
        fields = [field.strip() for field in line.split(",")]
        if len(fields) != 2:
            continue
        try:
            pid, mib = int(fields[0]), float(fields[1])
        except ValueError:
            continue
        usage[pid] = usage.get(pid, 0.0) + mib / 1024.0
    return usage


def collect_external_processes(
    owned_pids: Iterable[int],
    *,
    proc_root: str = "/proc",
    policies: Iterable[dict[str, object]] = (),
    gpu_usage: dict[int, float] | None = None,
) -> list[ExternalProcess]:
    """Attribute processes by cgroup ownership; never by command strings."""
    owned = set(owned_pids)
    gpu = _gpu_usage_by_pid() if gpu_usage is None else gpu_usage
    processes: list[ExternalProcess] = []
    root = Path(proc_root)
    try:
        entries = list(root.iterdir())
    except OSError:
        return []
    for entry in entries:
        if not entry.name.isdigit():
            continue
        pid = int(entry.name)
        if pid in owned:
            continue
        name, pss_gb, rss_gb = _status_memory(entry / "status")
        vram_gb = gpu.get(pid, 0.0)
        if pss_gb <= 0 and rss_gb <= 0 and vram_gb <= 0:
            continue
        policy = _matching_policy(pid, _process_cgroup(entry), policies)
        processes.append(
            ExternalProcess(
                pid=pid,
                name=name,
                pss_gb=pss_gb,
                rss_gb=rss_gb,
                vram_gb=vram_gb,
                protected=bool(policy.get("protected", True)) if policy else True,
                policy_name=(
                    str(policy.get("name", ""))
                    if policy
                    else "default-protected-external"
                ),
            ),
        )
    return processes


def occupancy_diagnostics(
    slots: list[dict],
    *,
    policies: Iterable[dict[str, object]] = (),
    top_n: int = 5,
) -> dict[str, object]:
    owned_pids: set[int] = set()
    slot_ram = 0.0
    for slot in slots:
        owned_pids.update(int(pid) for pid in slot.get("scope_pids", ()))
        slot_ram += float(slot.get("memory_current_gb", 0.0) or 0.0)
    gpu = _gpu_usage_by_pid()
    slot_vram = sum(value for pid, value in gpu.items() if pid in owned_pids)
    external = collect_external_processes(
        owned_pids, policies=policies, gpu_usage=gpu,
    )
    external.sort(key=lambda proc: proc.pss_gb + proc.vram_gb, reverse=True)
    return {
        "slot_owned_ram_gb": round(slot_ram, 3),
        "slot_owned_vram_gb": round(slot_vram, 3),
        "external_pss_gb": round(sum(proc.pss_gb for proc in external), 3),
        "external_rss_gb": round(sum(proc.rss_gb for proc in external), 3),
        "external_vram_gb": round(sum(proc.vram_gb for proc in external), 3),
        "external_process_count": len(external),
        "top_external_processes": [proc.diagnostic() for proc in external[:top_n]],
        "default_external_policy": "protected",
    }


def cooperative_reclaim(
    workload: dict[str, object],
    *,
    reason: str,
) -> bool:
    """Run an operator-declared reclaim hook; absent opt-in is a hard no-op."""
    if workload.get("protected", True):
        return False
    if not workload.get("cooperative_reclaim", False):
        return False
    command = str(workload.get("reclaim_command", "") or "").strip()
    if not command:
        return False
    env = {**os.environ, "WC_RECLAIM_REASON": reason}
    result = subprocess.run(command, shell=True, env=env, timeout=30)
    return result.returncode == 0
