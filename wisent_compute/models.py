"""Job data model and state definitions."""
from __future__ import annotations

import os
import json
from dataclasses import dataclass, field, asdict
from enum import Enum
from datetime import datetime, timezone


class JobState(str, Enum):
    QUEUED = "queued"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


GPU_SIZING = {
    "gcp": {
        16: ("n1-standard-4", "nvidia-tesla-t4"),
        24: ("g2-standard-4", "nvidia-l4"),
        40: ("a2-highgpu-1g", "nvidia-tesla-a100"),
        80: ("a2-highgpu-2g", "nvidia-tesla-a100"),
    },
}

# On-demand $ per accelerator-hour. Used for cost-aware dispatch.
# Source: GCP us-central1 list pricing (subject to change; refresh quarterly).
GPU_HOURLY_RATE_USD = {
    "nvidia-tesla-t4": 0.35,
    "nvidia-l4": 0.71,
    "nvidia-tesla-a100": 2.93,        # 40GB
    "nvidia-tesla-a100-80gb": 3.67,
    "nvidia-h100-80gb": 11.06,
}
# Spot/preemptible discount factor (multiply on-demand by this to get Spot rate).
# Empirically ~0.5x for A100, ~0.4x for L4, ~0.3x for T4.
SPOT_DISCOUNT = {
    "nvidia-tesla-t4": 0.30,
    "nvidia-l4": 0.40,
    "nvidia-tesla-a100": 0.49,
    "nvidia-tesla-a100-80gb": 0.49,
    "nvidia-h100-80gb": 0.45,
}


@dataclass
class Job:
    job_id: str
    command: str
    gpu_mem_gb: int = 0
    gpu_type: str = ""
    machine_type: str = ""
    provider: str = "gcp"
    batch_id: str = ""
    state: str = JobState.QUEUED.value
    created_at: str = ""
    started_at: str | None = None
    completed_at: str | None = None
    failed_at: str | None = None
    instance_ref: str | None = None
    restarts: int = 0
    max_restarts: int = 20
    last_restart: str | None = None
    image: str = "pytorch-2-9-cu129-ubuntu-2204-nvidia-580-v20260408"
    image_project: str = "deeplearning-platform-release"
    boot_disk_gb: int = 200
    startup_script_uri: str = ""
    error: str | None = None
    # New routing/cost fields. Default values keep all existing jobs behaving
    # exactly as before — older clients that don't set them keep working.
    preemptible: bool = False              # if true, dispatch on Spot
    pin_to_provider: bool = False          # if true, only the named provider claims
    max_cost_per_hour_usd: float = 0.0     # 0 = no cap
    preempt_count: int = 0                 # # times this job was preempted on Spot
    max_preempts_before_ondemand: int = 3  # after N preempts, fall back to on-demand
    priority: int = 0                      # higher = scheduled first within FIFO bucket
    # Tracks failed create_instance calls so a job that can't currently be
    # dispatched (zone exhausted, quota error, etc.) backs off instead of
    # being retried on every tick. Resets on successful dispatch.
    dispatch_attempts: int = 0
    last_dispatch_attempt: str | None = None
    # Submitter provenance. Populated by submit_job from $USER + os.uname().nodename
    # at submit time so every job in queue/running/completed/failed records who
    # put it there. Defaults are empty string for back-compat with old records.
    submitted_by: str = ""        # username on the submitting machine
    submitted_from: str = ""      # hostname of the submitting machine
    submitted_via: str = ""       # cli | api | other

    def __post_init__(self):
        if not self.created_at:
            self.created_at = datetime.now(timezone.utc).isoformat()

    def to_dict(self) -> dict:
        return asdict(self)

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), indent=2)

    @classmethod
    def from_dict(cls, d: dict) -> Job:
        return cls(**{k: v for k, v in d.items() if k in cls.__dataclass_fields__})

    @classmethod
    def from_json(cls, s: str) -> Job:
        return cls.from_dict(json.loads(s))
