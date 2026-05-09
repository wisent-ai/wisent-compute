"""Agent-mode VM dispatch.

For each (accel, machine_type) bucket of queued work that isn't already
yielded to a local consumer, launch enough agent VMs to fill remaining
quota — but no more than the bucket's job count. Each VM runs
`wc agent --idle-shutdown`, polls the queue, packs jobs by nvidia-smi
VRAM, and self-terminates when no eligible queued job remains.

Replaces the legacy 1-VM-per-job dispatch path. VRAM (read live from the
hardware) is the only admission constant; there is no per-VM slot count.
"""
from __future__ import annotations

import time
from datetime import datetime
from pathlib import Path

from ...config import INSTANCE_PREFIX
from ...models import GPU_HOURLY_RATE_USD, SPOT_DISCOUNT
from ...providers.base import Provider


_TEMPLATES_DIR = Path(__file__).parent.parent.parent / "templates"
# Per-provider agent startup-script templates. Each launches `wc agent
# --kind <provider> --gpu-type <accel> --idle-shutdown` after installing
# wisent-compute, but with provider-specific bootstrap (gsutil vs azcopy,
# managed identity vs SA JSON, etc.).
_TEMPLATES_BY_PROVIDER = {
    "gcp": "startup_gpu_agent.sh",
    "azure": "startup_gpu_agent_azure.sh",
}


def _accel_hourly_rate(accel: str, preemptible: bool) -> float:
    base = GPU_HOURLY_RATE_USD.get(accel, 0.0)
    if not preemptible:
        return base
    return base * SPOT_DISCOUNT.get(accel, 0.5)


def dispatch_agent_vms(
    *,
    queued: list,
    yield_targets: dict,
    available: dict,
    accel_dispatched: dict,
    per_accel_share: int,
    per_tick_cap: int,
    scheduled_so_far: int,
    provider: Provider,
    provider_name: str,
    secrets: dict,
    backoff_due,
    log_fn,
    now_utc: datetime,
) -> int:
    """Group queued jobs by (accel, machine_type) and launch agent VMs.

    Returns the number of agent VMs created. Mutates `available` and
    `accel_dispatched` in-place so the caller's per-tick budgets stay
    consistent with cloud reality.
    """
    template_name = _TEMPLATES_BY_PROVIDER.get(provider_name, "startup_gpu_agent.sh")
    template = (_TEMPLATES_DIR / template_name).read_text()

    # Re-derive (machine_type, accel) per job from current GPU_SIZING rather
    # than trust job.machine_type / job.gpu_type, which may be stale (a job
    # submitted before a GPU_SIZING tier swap would still carry the old VM
    # spec — e.g. a2-highgpu-2g + nvidia-tesla-a100 for 60GB jobs that GCP
    # rejects with 'Invalid accelerator specs for accelerator optimized
    # instances').
    from ...config import lookup_instance_type
    buckets: dict[tuple[str, str], list] = {}
    for j in queued:
        if getattr(j, "pin_to_provider", False) and j.provider != provider_name:
            continue
        if j.job_id in yield_targets:
            continue
        if not backoff_due(j, now_utc):
            continue
        gpu_mem = int(getattr(j, "gpu_mem_gb", 0) or 0)
        if gpu_mem <= 0:
            continue
        mt, accel = lookup_instance_type(provider_name, gpu_mem)
        if not (accel and mt):
            continue
        cap = getattr(j, "max_cost_per_hour_usd", 0.0) or 0.0
        if cap > 0 and accel:
            preempt = getattr(j, "preemptible", False)
            rate = _accel_hourly_rate(accel, preempt)
            if rate > 0 and rate > cap:
                continue
        buckets.setdefault((accel, mt), []).append(j)

    tick_tag = str(int(time.time()))
    created = 0
    scheduled = scheduled_so_far
    for (accel, mt), jobs in buckets.items():
        if scheduled >= per_tick_cap:
            break
        quota_left = available.get(accel, 0)
        if quota_left <= 0:
            log_fn(f"Skip bucket accel={accel} machine={mt}: 0 quota slots")
            continue
        share_left = per_accel_share - accel_dispatched.get(accel, 0)
        if share_left <= 0:
            continue
        n_to_dispatch = min(len(jobs), quota_left, share_left,
                            per_tick_cap - scheduled)
        biggest = max(jobs, key=lambda j: int(getattr(j, "gpu_mem_gb", 0) or 0))
        # No-preemptible policy: per user instruction (2026-05-06), this
        # codebase is NOT to dispatch Spot/preemptible VMs even when the
        # job's `preemptible` field is True. Repeated Spot reclaims of
        # A100-80 capacity in us-central1 caused 8 cloud-agent VMs to be
        # deleted under instance_termination_action=DELETE in a single
        # 3-second window (22:21:10-13Z), forcing requeues that burned
        # restart-budget on misclassified jobs (since fixed in 0.4.55,
        # but the underlying preemption noise persists). Override the
        # job-level flag and force every dispatch to STANDARD.
        preemptible_for_call = False
        for i in range(n_to_dispatch):
            script = template.replace("${ACCEL_TYPE}", accel)
            for key, val in secrets.items():
                script = script.replace(f"${{{key}}}", val)
            instance_name = (
                f"{INSTANCE_PREFIX}-agent-{accel.split('-')[-1]}-{tick_tag}-{i}"
            )
            ref = provider.create_instance(
                name=instance_name,
                machine_type=mt,
                accel_type=accel,
                boot_disk_gb=biggest.boot_disk_gb,
                image=biggest.image,
                image_project=biggest.image_project,
                startup_script=script,
                preemptible=preemptible_for_call,
            )
            if ref is None:
                log_fn(f"Agent VM create failed accel={accel} machine={mt}")
                continue
            available[accel] = available.get(accel, 0) - 1
            accel_dispatched[accel] = accel_dispatched.get(accel, 0) + 1
            scheduled += 1
            created += 1
            log_fn(
                f"Dispatched agent VM {ref} accel={accel} machine={mt} "
                f"preemptible={preemptible_for_call}"
            )
            if scheduled >= per_tick_cap:
                break
    return created
