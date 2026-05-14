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


# Full GPU SKU catalog (Azure NC/ND/NV/NCC + GCE T4/L4/A100/H100/H200/B200/MI300X)
# lives in _catalog/gpu_sku.py to keep this file under the 300-line cap.
# Re-export at module scope so existing callers
# (`from wisent_compute.models import GPU_SIZING`, etc.) keep working.
from ._catalog.gpu_sku import (  # noqa: E402
    AZURE_QUOTA_FAMILY_TO_ACCEL,
    AZURE_VM_HOURLY_RATE_USD,
    AZURE_VM_TO_ACCEL,
    GPU_HOURLY_RATE_USD,
    GPU_SIZING,
    GPU_TYPE_TO_MACHINE_TYPE,
    SPOT_DISCOUNT,
    VM_BUNDLE_HOURLY_RATE_USD,
)


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
    boot_disk_gb: int = 500
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
    # Optional source repo to git clone before running command. The startup
    # script clones repo into ./{repo_workdir} and pip-installs the [train]
    # extra so 'cd {repo_workdir} && python -m train.foo' works without the
    # caller having to glue clone+install onto every command. Empty = skip.
    repo: str = ""                # https git URL (or git@host:owner/repo.git)
    repo_workdir: str = ""        # subdir name; defaults to repo basename
    repo_extras: str = "train"    # pip-install extras name; "" to skip install
    # Verification hook. If set, runs as a shell command AFTER the subprocess
    # exits 0; non-zero exit reverses the COMPLETED → FAILED. Catches the
    # class of bug where the subprocess swallows errors and exits 0 despite
    # not having done its work (e.g. wisent's extract_and_upload reports
    # "5/7 strategies failed" + HF 429 rate-limit errors but exits 0,
    # marking the job COMPLETED while no usable output was produced).
    # Confirmed live on 2026-05-06: 20/20 sampled "completed" jobs had this
    # exact failure pattern. Empty string = skip verify (back-compat).
    verify_command: str = ""
    # Centralized fleet-aware assignment: when set, ONLY the agent whose
    # consumer_id matches may claim this queued job. Coordinator's
    # _assign_jobs_to_agents fills this on every tick. Empty string means
    # unassigned and any-eligible-agent may claim (pre-0.4.100 behavior).
    assigned_to: str = ""
    # Optional runtime hint (seconds). When > 0 the makespan scheduler
    # uses this directly; otherwise it falls back to historical mean
    # runtime grouped by (model, task), then by model, then global. This
    # is what lets the scheduler optimize for total time-to-finish
    # rather than VRAM balance.
    runtime_seconds_estimate: float = 0.0
    # Shell snippet prefixed to job.command at agent runtime. Runs in the
    # same bash invocation so it can `export` env vars that affect the
    # subprocess (e.g. LD_LIBRARY_PATH for cu128/cu129 cuBLAS reconciliation
    # on DLVM images). Empty = no prelude.
    pre_command: str = ""
    # Apt packages to install on the agent VM before spawning the subprocess.
    # Applied on cloud-kind agents only (gcp/azure/aws); local-kind agents
    # refuse the job to avoid surprise installs on operator workstations.
    apt_packages: list = field(default_factory=list)
    # Additional GCS URI to mirror job output to after completion. Default
    # location at gs://$WC_BUCKET/status/<id>/output/ is always written;
    # this is an additive sync so artifacts can land in a project bucket
    # (e.g. gs://wisent-images-bucket/Jakubs-lora/run01_2026-05-14/)
    # without the job command itself having to gsutil cp them.
    output_uri: str = ""

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
