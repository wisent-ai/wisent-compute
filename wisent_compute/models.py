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
