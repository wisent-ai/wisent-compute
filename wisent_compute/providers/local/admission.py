"""Pure local-agent resource admission decisions.

The kernel's ``MemAvailable`` and NVML's live free VRAM already include every
running Stado slot and external tenant. Existing occupancy is therefore
reported as diagnostics only; it is never added back into an admission
threshold.
"""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any


class AdmissionReason(str, Enum):
    ACCEPTED = "ACCEPTED"
    INSUFFICIENT_HOST_RAM = "INSUFFICIENT_HOST_RAM"
    INSUFFICIENT_VRAM = "INSUFFICIENT_VRAM"
    EXCLUSIVE_CONFLICT = "EXCLUSIVE_CONFLICT"
    MISSING_TELEMETRY_FAIL_CLOSED = "MISSING_TELEMETRY_FAIL_CLOSED"
    HARD_SLOT_CAP = "HARD_SLOT_CAP"
    ASSIGNED_TO_OTHER_AGENT = "ASSIGNED_TO_OTHER_AGENT"
    DISK_GATE = "DISK_GATE"


@dataclass(frozen=True)
class HostSnapshot:
    mem_total_gb: float
    mem_available_gb: float
    swap_free_gb: float
    gpu_total_gb: float
    gpu_free_gb: float
    timestamp: str
    telemetry_quality: str = "live"

    @property
    def complete(self) -> bool:
        return all(
            value >= 0
            for value in (
                self.mem_total_gb,
                self.mem_available_gb,
                self.swap_free_gb,
                self.gpu_total_gb,
                self.gpu_free_gb,
            )
        )


@dataclass(frozen=True)
class JobRequest:
    ram_request_gb: float
    vram_request_gb: float
    ram_estimation_source: str
    vram_estimation_source: str
    exclusive: bool = False
    priority: int = 0

    def __post_init__(self) -> None:
        if self.ram_request_gb < 0 or self.vram_request_gb < 0:
            raise ValueError("resource requests cannot be negative")
        if not self.ram_estimation_source or not self.vram_estimation_source:
            raise ValueError("resource estimation sources are required")


@dataclass
class ProjectedClaims:
    ram_gb: float = 0.0
    vram_gb: float = 0.0

    def reserve(self, request: JobRequest) -> None:
        self.ram_gb += request.ram_request_gb
        self.vram_gb += request.vram_request_gb


@dataclass(frozen=True)
class AdmissionPolicy:
    ram_safety_headroom_gb: float
    vram_safety_headroom_gb: float
    fail_closed_on_missing_telemetry: bool = True

    def __post_init__(self) -> None:
        if self.ram_safety_headroom_gb < 0 or self.vram_safety_headroom_gb < 0:
            raise ValueError("safety headroom cannot be negative")


@dataclass(frozen=True)
class AdmissionDecision:
    accepted: bool
    reason_code: AdmissionReason
    mem_available_gb: float
    gpu_free_gb: float
    projected_ram_before_gb: float
    projected_vram_before_gb: float
    ram_request_gb: float
    vram_request_gb: float
    ram_safety_headroom_gb: float
    vram_safety_headroom_gb: float
    projected_mem_after_gb: float
    projected_vram_after_gb: float

    def as_dict(self) -> dict[str, Any]:
        return {
            "accepted": self.accepted,
            "reason_code": self.reason_code.value,
            "mem_available_gb": round(self.mem_available_gb, 3),
            "gpu_free_gb": round(self.gpu_free_gb, 3),
            "projected_ram_before_gb": round(self.projected_ram_before_gb, 3),
            "projected_vram_before_gb": round(self.projected_vram_before_gb, 3),
            "ram_request_gb": round(self.ram_request_gb, 3),
            "vram_request_gb": round(self.vram_request_gb, 3),
            "ram_safety_headroom_gb": round(self.ram_safety_headroom_gb, 3),
            "vram_safety_headroom_gb": round(self.vram_safety_headroom_gb, 3),
            "projected_mem_after_gb": round(self.projected_mem_after_gb, 3),
            "projected_vram_after_gb": round(self.projected_vram_after_gb, 3),
        }


def evaluate_admission(
    snapshot: HostSnapshot,
    request: JobRequest,
    projected: ProjectedClaims,
    policy: AdmissionPolicy,
) -> AdmissionDecision:
    """Evaluate one claim without mutating ``projected``.

    Callers reserve an accepted request immediately, before spawning it, so a
    later candidate in the same queue tick cannot reuse the same snapshot.
    """
    projected_mem_after = (
        snapshot.mem_available_gb - projected.ram_gb - request.ram_request_gb
    )
    projected_vram_after = (
        snapshot.gpu_free_gb - projected.vram_gb - request.vram_request_gb
    )

    reason = AdmissionReason.ACCEPTED
    if not snapshot.complete and policy.fail_closed_on_missing_telemetry:
        reason = AdmissionReason.MISSING_TELEMETRY_FAIL_CLOSED
    elif projected_mem_after < policy.ram_safety_headroom_gb:
        reason = AdmissionReason.INSUFFICIENT_HOST_RAM
    elif projected_vram_after < policy.vram_safety_headroom_gb:
        reason = AdmissionReason.INSUFFICIENT_VRAM

    return AdmissionDecision(
        accepted=reason is AdmissionReason.ACCEPTED,
        reason_code=reason,
        mem_available_gb=snapshot.mem_available_gb,
        gpu_free_gb=snapshot.gpu_free_gb,
        projected_ram_before_gb=projected.ram_gb,
        projected_vram_before_gb=projected.vram_gb,
        ram_request_gb=request.ram_request_gb,
        vram_request_gb=request.vram_request_gb,
        ram_safety_headroom_gb=policy.ram_safety_headroom_gb,
        vram_safety_headroom_gb=policy.vram_safety_headroom_gb,
        projected_mem_after_gb=projected_mem_after,
        projected_vram_after_gb=projected_vram_after,
    )
