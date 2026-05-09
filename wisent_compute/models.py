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
        80: ("a2-ultragpu-1g", "nvidia-a100-80gb"),
    },
    # Azure NCas_T4_v3 (T4 16 GiB) and NC_A100_v4 (A100 80 GiB) are the
    # supported GPU SKUs. Azure has no native L4 SKU, so the 24 GiB tier
    # rolls up to the larger NC8as_T4_v3 host (still 1×T4 16 GiB but more
    # vCPU/RAM than NC4as so the OS doesn't choke on dataset preprocessing).
    # Anything > 80 GiB lands on the dual-A100 NC48ads SKU.
    "azure": {
        16: ("Standard_NC4as_T4_v3", "nvidia-tesla-t4"),
        24: ("Standard_NC8as_T4_v3", "nvidia-tesla-t4"),
        40: ("Standard_NC24ads_A100_v4", "nvidia-a100-80gb"),
        80: ("Standard_NC24ads_A100_v4", "nvidia-a100-80gb"),
        160: ("Standard_NC48ads_A100_v4", "nvidia-a100-80gb"),
    },
}

# Azure VM SKU → (accel_type, count). Used by AzureProvider.list_running_instances
# to count GPUs from VM size alone (Azure VMs bundle the GPU into the SKU
# rather than expose it as a separate attached_accelerator like GCE does).
AZURE_VM_TO_ACCEL = {
    "Standard_NC4as_T4_v3": ("nvidia-tesla-t4", 1),
    "Standard_NC8as_T4_v3": ("nvidia-tesla-t4", 1),
    "Standard_NC16as_T4_v3": ("nvidia-tesla-t4", 1),
    "Standard_NC64as_T4_v3": ("nvidia-tesla-t4", 4),
    "Standard_NC24ads_A100_v4": ("nvidia-a100-80gb", 1),
    "Standard_NC48ads_A100_v4": ("nvidia-a100-80gb", 2),
    "Standard_NC96ads_A100_v4": ("nvidia-a100-80gb", 4),
}

# Azure quota family name → accel_type. Used by scheduler/quota.py to map
# `compute.usage.list(location)` rows. Azure exposes quota at the SKU-family
# level (one count covers every NC*as_T4_v3 size) so a saturated quota in
# one family blocks every member SKU regardless of which size we'd pick.
AZURE_QUOTA_FAMILY_TO_ACCEL = {
    "standardNCASv3Family": "nvidia-tesla-t4",
    "standardNCASv4Family": "nvidia-tesla-t4",        # newer T4 family if added
    "standardNCADSA100v4Family": "nvidia-a100-80gb",
}

# On-demand $ for the GPU accelerator only. Source: GCP us-central1 list price.
# A VM bundle adds vCPU + RAM costs on top — see VM_BUNDLE_USD below.
GPU_HOURLY_RATE_USD = {
    "nvidia-tesla-t4": 0.35,
    "nvidia-l4": 0.71,
    "nvidia-tesla-a100": 2.93,        # 40GB
    "nvidia-a100-80gb": 3.67,
    "nvidia-h100-80gb": 11.06,
}
# Spot/preemptible multiplier on the GPU rate. Calibrated 2026-05-05 against
# real GCP billing_export.gcp_billing_export_v1_017364_D3B657_F207B5:
#   A100-40: $287.81 / 722480s = $1.434/hr → 1.434/2.93 = 0.489
#   A100-80: $23.60  / 43256s  = $1.964/hr → 1.964/3.67 = 0.535
#   T4:      $15.66  / 327808s = $0.172/hr → 0.172/0.35 = 0.491
SPOT_DISCOUNT = {
    "nvidia-tesla-t4": 0.49,         # was 0.30 — observed 0.49
    "nvidia-l4": 0.40,
    "nvidia-tesla-a100": 0.49,
    "nvidia-a100-80gb": 0.54,        # was 0.49 — observed 0.54
    "nvidia-h100-80gb": 0.45,
}

# Per-machine-bundle CPU + RAM rate (NO GPU). Real billing showed the GPU
# attached-to-VM SKU is only ~50-65% of the actual VM cost; the rest is the
# A2/N1/G2 Core + Ram SKUs which get billed separately. Without these the
# `wc cost` and `--max-cost-per-hour` paths systematically undercount by
# ~30-40% on a2-* and ~50%+ on T4/L4 (where the bundle dominates).
#
# Format: (on_demand_usd_per_hour, spot_usd_per_hour).
# Spot/A2 bundle calibrated 2026-05-05:
#   A2 Core spot: $38.50 / (8767329s / 3600) = $0.0158/vCPU-hr × 12 = $0.190
#   A2 RAM spot:  $38.49 / 18178 GB-hr      = $0.00212/GB-hr  × 85 = $0.180
#   a2-highgpu-1g spot bundle = 0.190 + 0.180 = $0.370/hr
#   a2-ultragpu-1g spot bundle = 0.190 + 0.00212×170 = $0.190 + 0.360 = $0.550/hr
VM_BUNDLE_HOURLY_RATE_USD = {
    # machine_type: (on_demand, spot)
    "a2-highgpu-1g":  (1.50, 0.37),  # 12 vCPU + 85 GiB
    "a2-ultragpu-1g": (1.85, 0.55),  # 12 vCPU + 170 GiB
    "n1-standard-4":  (0.20, 0.06),  # 4 vCPU + 15 GiB (T4 host)
    "g2-standard-4":  (0.30, 0.12),  # 4 vCPU + 16 GiB (L4 host, custom rates)
    "a3-highgpu-8g":  (8.00, 3.20),  # 208 vCPU + 1872 GiB (H100 host) — list-derived
}

# Map GPU type → host machine type so cost-aware code that only sees gpu_type
# can still reach the bundle rate. Default: a 1×accelerator host.
GPU_TYPE_TO_MACHINE_TYPE = {
    "nvidia-tesla-t4": "n1-standard-4",
    "nvidia-l4": "g2-standard-4",
    "nvidia-tesla-a100": "a2-highgpu-1g",
    "nvidia-a100-80gb": "a2-ultragpu-1g",
    "nvidia-h100-80gb": "a3-highgpu-8g",
}

# Azure VM SKU → (on_demand_usd_per_hour, spot_usd_per_hour). Unlike GCP,
# Azure bundles GPU into the SKU price (NC* families bill the entire VM
# including the attached GPU as a single line item), so cost.py's Azure
# branch reads this directly instead of summing GPU + bundle.
# Source: Azure retail prices for eastus, captured 2026-05-08. Override
# via env if your subscription uses an EA/savings-plan negotiated rate.
AZURE_VM_HOURLY_RATE_USD = {
    "Standard_NC4as_T4_v3":      (0.526, 0.105),
    "Standard_NC8as_T4_v3":      (0.752, 0.150),
    "Standard_NC16as_T4_v3":     (1.204, 0.241),
    "Standard_NC64as_T4_v3":     (4.352, 0.870),
    "Standard_NC24ads_A100_v4":  (3.673, 0.735),
    "Standard_NC48ads_A100_v4":  (7.346, 1.470),
    "Standard_NC96ads_A100_v4":  (14.692, 2.940),
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
