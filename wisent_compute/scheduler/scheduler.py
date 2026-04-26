"""Job scheduler: pick queued jobs and create instances.

Routing rules:
- job.pin_to_provider=True + job.provider="local" -> only local agent claims
- job.pin_to_provider=True + job.provider=<X>     -> only provider X claims
- job.pin_to_provider=False (default)             -> any consumer with capacity
  can claim. The Cloud Function (this file) skips a job ONLY if its capacity
  cannot satisfy the job (no quota, or cost cap exceeds available SKU rate);
  the local agent then has a chance.

Dispatch backoff:
A job whose create_instance call failed gets dispatch_attempts++ and a
last_dispatch_attempt timestamp. It is then skipped for a backoff window
that grows with attempt count. This prevents a wedged job (e.g. quota
exhausted in every zone) from slamming the API on every 3-min tick AND
gives the local agent a clean shot at the same job in the meantime.
"""
from __future__ import annotations

import sys
from datetime import datetime, timezone, timedelta

from ..config import MAX_SCHEDULE_PER_TICK, INSTANCE_PREFIX
from ..models import Job, JobState, GPU_HOURLY_RATE_USD, SPOT_DISCOUNT
from ..queue.capacity import (
    consumers_by_free_vram, read_consumer_capacity, total_free_by_accel,
)
from ..queue.storage import JobStorage
from ..providers.base import Provider
from .quota import get_available_slots


# Backoff schedule by attempt count; index = attempt count.
# Each entry is the minimum minutes since last_dispatch_attempt before we retry.
DISPATCH_BACKOFF_MINUTES = [0, 1, 5, 15, 30, 60, 120]
MAX_DISPATCH_BACKOFF_MINUTES = 240


def _log(msg):
    sys.stderr.write(f"[scheduler] {msg}\n")
    sys.stderr.flush()


def _accel_hourly_rate(accel_type: str, preemptible: bool) -> float:
    """Return $/hour for one accelerator of this type at given pricing model."""
    base = GPU_HOURLY_RATE_USD.get(accel_type, 0.0)
    if not preemptible:
        return base
    return base * SPOT_DISCOUNT.get(accel_type, 0.5)


def _backoff_due(job: Job, now_utc: datetime) -> bool:
    """True if this job is past its dispatch-backoff window."""
    attempts = getattr(job, "dispatch_attempts", 0)
    if attempts <= 0:
        return True
    idx = min(attempts, len(DISPATCH_BACKOFF_MINUTES) - 1)
    wait_minutes = min(DISPATCH_BACKOFF_MINUTES[idx], MAX_DISPATCH_BACKOFF_MINUTES)
    last = getattr(job, "last_dispatch_attempt", None)
    if not last:
        return True
    try:
        last_dt = datetime.fromisoformat(last.replace("Z", "+00:00"))
    except ValueError:
        return True
    return now_utc - last_dt >= timedelta(minutes=wait_minutes)


def _dynamic_per_tick_cap(queue_depth: int) -> int:
    """Autoscale dispatch cap with queue depth.

    Defaults to MAX_SCHEDULE_PER_TICK (4) for shallow queues, scales up for
    larger bursts so a 723-job batch doesn't drip-feed at 4-per-tick. Hard
    upper bound to avoid quota-thundering-herd.
    """
    base = MAX_SCHEDULE_PER_TICK
    if queue_depth <= base * 2:
        return base
    return min(50, base + (queue_depth - base * 2) // 4 + 4)


def schedule_queued_jobs(
    store: JobStorage,
    provider: Provider,
    provider_name: str,
    secrets: dict,
) -> int:
    """Pick queued jobs that fit available GPU slots and cost caps; create instances."""
    available = get_available_slots(store, provider, provider_name)
    _log(f"Available slots: {available}")

    if all(v == 0 for v in available.values()):
        _log("No GPU slots available")
        return 0

    queued = store.list_jobs("queue")
    queued.sort(key=lambda j: (-getattr(j, "priority", 0), j.created_at))

    now_utc = datetime.now(timezone.utc)
    per_tick_cap = _dynamic_per_tick_cap(len(queued))

    # Per-accelerator fairness: when a heterogeneous batch is queued
    # (e.g. T4 + A100-40 + A100-80 jobs all waiting), pure FIFO means the
    # first-submitted accel hogs every tick until its quota saturates while
    # other accels sit idle. Compute a soft per-accel per-tick share so each
    # accel makes progress concurrently. Round up so distinct_accels=3 with
    # cap=50 gives 17 each (the leftover 1 falls to whichever accel comes
    # first in the sorted queue). The pass after this loop fills any
    # remaining budget without per-accel limits, so we don't underuse.
    distinct_accels = {(j.gpu_type or "_cpu") for j in queued}
    if distinct_accels:
        per_accel_share = max(1, -(-per_tick_cap // len(distinct_accels)))
    else:
        per_accel_share = per_tick_cap
    accel_dispatched: dict[str, int] = {}

    # Read live consumer capacity. Any local agent reporting a free slot for
    # an accelerator is a free-hardware peer we should yield to before paying
    # for a fresh GCE VM. We track yields by accel so a job we yielded in
    # this tick doesn't burn the local agent's capacity in our internal book
    # before it actually claims.
    consumer_caps = read_consumer_capacity(store)
    local_free = total_free_by_accel(consumer_caps, kinds=("local",))
    # Per-consumer free VRAM (gigabytes). Scheduler yields a job iff its
    # gpu_mem_gb fits in some consumer's free_vram_gb. Decrement that
    # consumer's pool when yielding so we don't double-yield within a tick.
    local_vram_pool: dict[str, int] = {
        cid: vram for cid, vram in consumers_by_free_vram(consumer_caps, kinds=("local",))
    }
    if local_free:
        _log(f"Live local-agent slots: {local_free}")
    if local_vram_pool:
        _log(f"Live local-agent free_vram_gb: {local_vram_pool}")
    if per_tick_cap != MAX_SCHEDULE_PER_TICK:
        _log(f"Autoscale per-tick cap: {MAX_SCHEDULE_PER_TICK} -> {per_tick_cap} (queue={len(queued)})")

    scheduled = 0

    def _attempt(job: Job, enforce_accel_share: bool) -> str:
        """Try to dispatch one job. Returns 'ok'/'yield'/'skip'/'cap'/'fail'."""
        nonlocal scheduled
        pinned = getattr(job, "pin_to_provider", False)
        if pinned and job.provider != provider_name:
            return "skip"
        if not _backoff_due(job, now_utc):
            return "skip"
        # Guard against malformed jobs (empty machine_type from older versions
        # of lookup_instance_type when gpu_mem exceeded all tiers). Mark them
        # failed so they leave the queue instead of looping every tick.
        if not (job.machine_type or "").strip():
            job.state = JobState.FAILED.value
            job.failed_at = now_utc.isoformat()
            job.error = "machine_type is empty (job was created before the lookup_instance_type fix)"
            store.move_job(job, "queue", "failed")
            _log(f"{job.job_id}: failed (empty machine_type)")
            return "fail"
        accel = job.gpu_type or ""
        if accel and available.get(accel, 0) <= 0:
            return "skip"
        if enforce_accel_share and accel and accel_dispatched.get(accel, 0) >= per_accel_share:
            return "skip"
        # VRAM-first yield: pick the local consumer with the most free VRAM
        # whose pool still admits this job's gpu_mem_gb. Falls back to the
        # legacy slot-counted yield if no consumer publishes free_vram_gb.
        need_gb = int(getattr(job, "gpu_mem_gb", 0) or 0)
        if not pinned and need_gb > 0 and local_vram_pool:
            best_cid = max(local_vram_pool, key=lambda c: local_vram_pool[c])
            if local_vram_pool[best_cid] >= need_gb:
                local_vram_pool[best_cid] -= need_gb
                _log(f"Yielding {job.job_id} to {best_cid} ({need_gb}GB; free_vram remaining={local_vram_pool[best_cid]})")
                return "yield"
        if not pinned and accel and local_free.get(accel, 0) > 0 and not local_vram_pool:
            local_free[accel] -= 1
            _log(f"Yielding {job.job_id} to local agent ({accel}, slots remaining={local_free[accel]})")
            return "yield"
        cap = getattr(job, "max_cost_per_hour_usd", 0.0) or 0.0
        if cap > 0 and accel:
            preemptible = getattr(job, "preemptible", False)
            rate = _accel_hourly_rate(accel, preemptible)
            if rate > 0 and rate > cap:
                _log(f"Skip {job.job_id}: ${rate:.2f}/hr > cap ${cap:.2f}/hr")
                return "cap"
        script = store.download_script(job.job_id)
        for key, val in secrets.items():
            script = script.replace(f"${{{key}}}", val)
        instance_name = f"{INSTANCE_PREFIX}-{job.job_id}"
        switch_to_ondemand = (
            getattr(job, "preemptible", False)
            and getattr(job, "preempt_count", 0)
               >= getattr(job, "max_preempts_before_ondemand", 3)
        )
        preemptible_for_call = (
            getattr(job, "preemptible", False) and not switch_to_ondemand
        )
        if switch_to_ondemand:
            _log(f"{job.job_id}: preempt cap reached ({job.preempt_count}); dispatching on-demand this attempt")
        ref = provider.create_instance(
            name=instance_name,
            machine_type=job.machine_type,
            accel_type=accel,
            boot_disk_gb=job.boot_disk_gb,
            image=job.image,
            image_project=job.image_project,
            startup_script=script,
            preemptible=preemptible_for_call,
        )
        if ref is None:
            job.dispatch_attempts = getattr(job, "dispatch_attempts", 0) + 1
            job.last_dispatch_attempt = now_utc.isoformat()
            store.write_job("queue", job)
            wait_idx = min(job.dispatch_attempts, len(DISPATCH_BACKOFF_MINUTES) - 1)
            wait = DISPATCH_BACKOFF_MINUTES[wait_idx]
            _log(f"Failed to create instance for {job.job_id} (attempt {job.dispatch_attempts}); backing off {wait}m")
            return "fail"
        job.dispatch_attempts = 0
        job.last_dispatch_attempt = None
        job.instance_ref = ref
        job.state = JobState.RUNNING.value
        job.started_at = now_utc.isoformat()
        store.move_job(job, "queue", "running")
        if accel:
            available[accel] = available.get(accel, 0) - 1
            accel_dispatched[accel] = accel_dispatched.get(accel, 0) + 1
        scheduled += 1
        _log(f"Scheduled {job.job_id} on {ref} (preemptible={preemptible_for_call})")
        return "ok"

    # Pass 1 — fairness: each accel limited to per_accel_share dispatches so
    # heterogeneous batches make concurrent progress instead of one accel
    # hogging the whole tick.
    for job in queued:
        if scheduled >= per_tick_cap:
            break
        _attempt(job, enforce_accel_share=True)

    # Pass 2 — fill any remaining tick budget without the per-accel cap so
    # we don't underuse capacity when one accel still has plenty of room.
    if scheduled < per_tick_cap:
        for job in queued:
            if scheduled >= per_tick_cap:
                break
            if job.state != JobState.QUEUED.value:
                continue  # was claimed in pass 1
            _attempt(job, enforce_accel_share=False)

    return scheduled
