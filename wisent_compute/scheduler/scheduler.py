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
SECONDS_PER_HOUR = 3600.0
GPU_MEM_GB_KEY = "gpu_mem_gb"
GPU_TYPE_KEY = "gpu_type"
PRIORITY_KEY = "priority"


def _log(msg):
    sys.stderr.write(f"[scheduler] {msg}\n")
    sys.stderr.flush()


def _accel_hourly_rate(accel_type: str, preemptible: bool) -> float:
    """Return $/hour for one accelerator of this type at given pricing model."""
    if accel_type not in GPU_HOURLY_RATE_USD:
        raise KeyError(f"missing hourly GPU rate for {accel_type}")
    base = GPU_HOURLY_RATE_USD[accel_type]
    if not preemptible:
        return base
    if accel_type not in SPOT_DISCOUNT:
        raise KeyError(f"missing spot discount for {accel_type}")
    return base * SPOT_DISCOUNT[accel_type]


def _dict_value(data: dict, key, default):
    return data[key] if key in data else default


def _dict_number(data: dict, key, default=0) -> int:
    value = _dict_value(data, key, default)
    return int(value if value is not None else default)


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
    last_dt = datetime.fromisoformat(last.replace("Z", "+00:00"))
    return now_utc - last_dt >= timedelta(minutes=wait_minutes)


def _dynamic_per_tick_cap(queue_depth: int) -> int:
    """Autoscale dispatch cap with queue depth.

    Defaults to MAX_SCHEDULE_PER_TICK (4) for shallow queues, scales up for
    larger bursts so a 723-job batch doesn't drip-feed at 4-per-tick. Upper
    bound aligned with the multi-region preemptible quota envelope (5
    regions x ~36 spot GPUs = ~180 ceiling).
    """
    base = MAX_SCHEDULE_PER_TICK
    if queue_depth <= base * 2:
        return base
    return min(25, base + (queue_depth - base * 2) // 4 + 4)  # cap=25 fits 60s tick budget


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

    # Cap the listing in JobStorage so we never download more than we'd
    # dispatch this tick. queue/ holds 14k+ blobs after a big batch submit
    # and downloading every JSON blew the 60s function timeout. Pick by
    # GCS time_created ascending (FIFO) — anything past _dynamic_per_tick_cap's
    # ceiling × 8 wouldn't fit in this tick's budget anyway.
    window_budget = _dynamic_per_tick_cap(10**9) * 8

    # Metadata-only prefilter (NO body downloads): keep only jobs whose
    # accelerator has available quota this tick, so a backlog of UNDISPATCHABLE
    # jobs cannot saturate the per-tick window and starve dispatchable work.
    # Confirmed live 2026-06-01: 435 jobs sized to nvidia-tesla-k80 (0 fleet
    # k80 quota) filled the 200-job FIFO window every tick -> the only bucket
    # formed was k80 -> "Skip: 0 quota" -> scheduled 0 for the WHOLE fleet,
    # including brand-new t4/l4 jobs queued behind the stuck backlog. write_job
    # stamps gpu_mem_gb, gpu_type, and priority into blob metadata, so this
    # filters + orders the whole queue cheaply and we read only the surviving
    # window's bodies.
    # The stuck backlog stays queued and untouched — it just stops blocking.
    from ..config import lookup_instance_type as _lookup_it
    in_quota = {a for a, v in available.items() if v > 0}
    cand: list[tuple[int, float, str]] = []  # (-priority, updated_ts, job_id)
    skipped_no_quota = 0
    for info in store.list_blobs_with_meta("queue/"):
        if not info.name.endswith(".json"):
            continue
        meta = info.metadata or {}
        gm = _dict_number(meta, GPU_MEM_GB_KEY)
        explicit_accel = (_dict_value(meta, GPU_TYPE_KEY, "") or "").strip()
        # gm<=0 jobs are kept: dispatch_agent_vms re-sizes them via its own
        # observed/smallest_live_vram recovery. Only skip jobs with a concrete
        # size that maps to an accelerator with zero available quota.
        accel_for_filter = explicit_accel or (_lookup_it(provider_name, gm)[1] if gm > 0 else "")
        if accel_for_filter and accel_for_filter not in in_quota:
            skipped_no_quota += 1
            continue
        prio = _dict_number(meta, PRIORITY_KEY)
        ts = info.updated.timestamp() if getattr(info, "updated", None) else 0.0
        cand.append((-prio, ts, info.name.split("/")[-1][:-5]))
    if skipped_no_quota:
        _log(f"window: skipped {skipped_no_quota} undispatchable (0-quota-accel) queued jobs")
    cand.sort(key=lambda t: (t[0], t[1]))  # priority desc, then oldest-first (FIFO)
    queued = []
    for _, _, jid in cand[:window_budget]:
        j = store.read_job("queue", jid)
        if j is not None:
            queued.append(j)
    now_utc = datetime.now(timezone.utc)
    full_queue_depth = len(queued)
    per_tick_cap = _dynamic_per_tick_cap(full_queue_depth)
    queued = queued[: per_tick_cap * 8]
    # filter_already_done was disabled: HfApi.list_repo_files on the 184k-file
    # wisent-ai/activations repo takes 50+s, eating the 60s function timeout
    # before any dispatch fires. Wrapper still short-circuits per-strategy on
    # the box so the cost is only VM boot for results-already-uploaded jobs.

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
    local_vram_pool: dict[str, int] = {
        cid: vram for cid, vram in consumers_by_free_vram(consumer_caps, kinds=("local",))
    }
    if local_free:
        _log(f"Live local-agent slots: {local_free}")
    if local_vram_pool:
        _log(f"Live local-agent free_vram_gb: {local_vram_pool}")

    # COST-OPTIMAL LOCAL PACK: knapsack over queued jobs by
    # $-saved-per-GB-of-local-VRAM, weighted by per-job wall-time so the
    # score reflects total dollars-saved-per-GB on this specific job (not
    # per-hour-of-running). Wall-time comes from the median of past
    # completed jobs of the same (model, gpu_type); when that bucket is
    # empty, a model-size heuristic is used. Best-fit-decreasing packing.
    yield_targets: dict[str, str] = {}  # job_id -> consumer_id
    if local_vram_pool:
        from .cost import collect_completed, estimate_wall_time, wall_time_table
        wt_table = wall_time_table(collect_completed(store))
        scored: list[tuple[float, int, Job]] = []
        for j in queued:
            need = int(getattr(j, "gpu_mem_gb", 0) or 0)
            if need <= 0 or getattr(j, "pin_to_provider", False):
                continue
            if not _backoff_due(j, now_utc):
                continue
            rate = _accel_hourly_rate(
                j.gpu_type or "", getattr(j, "preemptible", False)
            )
            if rate <= 0:
                continue
            wall_s = estimate_wall_time(j.command, j.gpu_type or "", need, wt_table)
            score = (wall_s / SECONDS_PER_HOUR) * rate / need  # $-saved per GB on this job
            scored.append((score, need, j))
        scored.sort(key=lambda t: -t[0])
        # Reserve the local agent's admission safety buffer (VRAM_SAFETY_BUFFER_GB
        # = 8 in providers/local_agent.py) so we don't yield a job the agent then
        # REFUSES at admission (it rejects when projected_used > total - buffer).
        # Over-committing on raw broadcast free_vram stranded jobs: yielded to the
        # local agent but rejected by it, AND skipped by cloud dispatch because
        # they were yielded. Confirmed live 2026-06-01: a 16GB job yielded to
        # local-ubuntu-server (75/98 GB used, ~22 free) sat unclaimed forever
        # (22 - 8 = 14 < 16). Reserving the buffer routes such jobs to cloud.
        LOCAL_ADMISSION_BUFFER_GB = 8
        local_remaining = {cid: max(0, v - LOCAL_ADMISSION_BUFFER_GB)
                           for cid, v in local_vram_pool.items()}
        for score, need, j in scored:
            best_cid = None
            best_free = -1
            for cid, free_gb in local_remaining.items():
                if free_gb >= need and free_gb > best_free:
                    best_cid, best_free = cid, free_gb
            if best_cid is None:
                continue
            yield_targets[j.job_id] = best_cid
            local_remaining[best_cid] -= need
        if yield_targets:
            _log(
                f"Cost-optimal local pack: {len(yield_targets)} jobs yielded; "
                f"remaining_vram={local_remaining}"
            )
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
        if accel and _dict_number(available, accel) <= 0:
            return "skip"
        if enforce_accel_share and accel and _dict_number(accel_dispatched, accel) >= per_accel_share:
            return "skip"
        # Honour the cost-optimal pre-pass first.
        if not pinned and job.job_id in yield_targets:
            _log(f"Yielding {job.job_id} to {yield_targets[job.job_id]} (cost-optimal pack)")
            return "yield"
        if not pinned and accel and _dict_number(local_free, accel) > 0 and not local_vram_pool:
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
        # No-preemptible policy (matches dispatch/agent.py 0.4.56). Per
        # user instruction (2026-05-06) the dispatcher does NOT use Spot
        # /preemptible provisioning even if the job's preemptible field
        # is True. Force STANDARD on every dispatch.
        preemptible_for_call = False
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
            available[accel] = _dict_number(available, accel) - 1
            accel_dispatched[accel] = _dict_number(accel_dispatched, accel) + 1
        scheduled += 1
        _log(f"Scheduled {job.job_id} on {ref} (preemptible={preemptible_for_call})")
        return "ok"

    # Agent-mode dispatch: launch agent VMs that poll the queue and pack
    # jobs by VRAM. Replaces the per-job VM dispatch — per-VM concurrency
    # is now bounded by nvidia-smi readout, not a constant.
    from .dispatch.agent import dispatch_agent_vms
    scheduled += dispatch_agent_vms(
        queued=queued,
        yield_targets=yield_targets,
        available=available,
        accel_dispatched=accel_dispatched,
        per_accel_share=per_accel_share,
        per_tick_cap=per_tick_cap,
        scheduled_so_far=scheduled,
        provider=provider,
        provider_name=provider_name,
        secrets=secrets,
        backoff_due=_backoff_due,
        log_fn=_log,
        now_utc=now_utc,
    )
    return scheduled
