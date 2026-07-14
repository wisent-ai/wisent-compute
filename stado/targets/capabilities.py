"""Provider-neutral workload admission against declared target capabilities."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class TargetCapabilities:
    target_id: str
    operating_system: str
    architecture: str
    cpu_cores: int
    memory_gb: int
    disk_gb: int
    accelerator: str = ""
    execution_modes: frozenset[str] = frozenset({"stado-agent"})
    supports_preemptible: bool = False
    region_selectable: bool = False
    supports_system_packages: bool = False


@dataclass(frozen=True)
class AdmissionDecision:
    accepted: bool
    reasons: tuple[str, ...] = ()

    def require(self) -> None:
        if not self.accepted:
            raise ValueError("; ".join(self.reasons))


def admit_job(job: Any, target: TargetCapabilities) -> AdmissionDecision:
    """Return every incompatibility instead of failing at the first field."""
    reasons: list[str] = []
    required_os = str(getattr(job, "platform_os", "") or "").lower()
    required_arch = str(getattr(job, "architecture", "") or "").lower()
    required_cpu = int(getattr(job, "cpu_cores", int()) or int())
    required_memory = int(getattr(job, "memory_gb", int()) or int())
    required_disk = int(getattr(job, "disk_gb", int()) or int())
    executor = str(getattr(job, "executor", "stado-agent") or "stado-agent")
    gpu_mem = int(getattr(job, "gpu_mem_gb", int()) or int())
    gpu_type = str(getattr(job, "gpu_type", "") or "")

    if required_os and required_os != target.operating_system:
        reasons.append(f"requires os={required_os}, target is {target.operating_system}")
    if required_arch and required_arch != target.architecture:
        reasons.append(f"requires architecture={required_arch}, target is {target.architecture}")
    if required_cpu > target.cpu_cores:
        reasons.append(f"requires {required_cpu} CPU cores, target has {target.cpu_cores}")
    if required_memory > target.memory_gb:
        reasons.append(f"requires {required_memory} GB memory, target has {target.memory_gb}")
    if required_disk > target.disk_gb:
        reasons.append(f"requires {required_disk} GB disk, target has {target.disk_gb}")
    if gpu_mem > int() or gpu_type:
        reasons.append("target has no accelerator")
    if executor not in target.execution_modes:
        reasons.append(f"executor {executor!r} is unsupported")
    if bool(getattr(job, "preemptible", False)) and not target.supports_preemptible:
        reasons.append("target does not support preemptible lifecycle")
    if str(getattr(job, "region", "") or "") and not target.region_selectable:
        reasons.append("target region is not selectable")
    if getattr(job, "apt_packages", None) and not target.supports_system_packages:
        reasons.append("target does not support provider-managed system packages")
    return AdmissionDecision(not reasons, tuple(reasons))


BOX_CAPABILITIES = TargetCapabilities(
    target_id="box-linux-sandbox",
    operating_system="linux",
    architecture="x86_64",
    cpu_cores=int("4"),
    memory_gb=int("8"),
    disk_gb=int("80"),
    accelerator="",
    execution_modes=frozenset({"stado-agent", "box-command", "box-prompt"}),
    supports_preemptible=False,
    region_selectable=False,
    supports_system_packages=False,
)
