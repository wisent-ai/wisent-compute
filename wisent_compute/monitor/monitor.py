"""Monitor running jobs: check heartbeat, status, cleanup.

Handles four exit conditions for a running job:
  COMPLETED         -> finalize success path
  FAILED            -> finalize failure path + alert
  preempted (Spot)  -> instance is TERMINATED but the Job is otherwise healthy.
                      Delete the GCE instance, increment preempt_count, requeue.
                      preempt_count is separate from restarts so a Spot-heavy
                      job doesn't burn the restart budget on preemptions alone.
  instance gone OR
  stale heartbeat   -> requeue (counted against restarts).
"""
from __future__ import annotations

import sys
from datetime import datetime, timezone

from ..config import ALERTS_TOPIC
from ..models import Job, JobState
from ..queue.storage import JobStorage
from ..providers.base import Provider
from .alerts import send_alert


def _log(msg):
    sys.stderr.write(f"[monitor] {msg}\n")
    sys.stderr.flush()


def check_running_jobs(store: JobStorage, provider: Provider, publisher=None):
    """Check all running jobs. Handle completion, failure, preemption, stale."""
    running = store.list_jobs("running")
    _log(f"Checking {len(running)} running jobs")

    _live_consumers_cache: dict | None = None
    _running_vm_names_cache: set | None = None
    for job in running:
        job_id = job.job_id
        ref = job.instance_ref
        if not ref:
            _requeue(store, job, "no instance ref")
            continue

        status = store.read_status(job_id)

        if status == "COMPLETED":
            job.state = JobState.COMPLETED.value
            job.completed_at = datetime.now(timezone.utc).isoformat()
            provider.delete_instance(ref)
            store.move_job(job, "running", "completed")
            store.cleanup_status(job_id)
            _log(f"{job_id}: COMPLETED")

        elif status == "FAILED":
            job.state = JobState.FAILED.value
            job.failed_at = datetime.now(timezone.utc).isoformat()
            provider.delete_instance(ref)
            store.move_job(job, "running", "failed")
            store.cleanup_status(job_id)
            send_alert(publisher, ALERTS_TOPIC, f"Job {job_id} FAILED: {job.command[:100]}")
            _log(f"{job_id}: FAILED")

        else:
            if (ref or "").startswith("local@"):
                hostname = ref[len("local@"):]
                if _live_consumers_cache is None:
                    from ..queue.capacity import read_consumer_capacity
                    _live_consumers_cache = read_consumer_capacity(store)
                if any(f"{prefix}-{hostname}" in _live_consumers_cache
                       for prefix in ("local", "gcp", "azure", "aws")):
                    continue
                is_cloud_agent_name = hostname.startswith("wisent-agent-")
                if is_cloud_agent_name:
                    if _running_vm_names_cache is None:
                        _running_vm_names_cache = {
                            r.split("@", 1)[0]
                            for r, _ in provider.list_running_instance_refs_with_age()
                        }
                    if hostname not in _running_vm_names_cache:
                        if getattr(job, "preemptible", False):
                            _requeue_preempted(store, job, "Spot preempted (cloud agent gone)")
                        else:
                            _requeue(store, job, "VM gone (cloud agent missing from fleet)")
                        continue
                continue

            alive = provider.instance_exists(ref)
            lifecycle = provider.instance_lifecycle_state(ref)

            if not alive and lifecycle == "TERMINATED" and getattr(job, "preemptible", False):
                _requeue_preempted(store, job, "Spot preempted")
                provider.delete_instance(ref)
            elif not alive:
                _requeue(store, job, f"instance gone (lifecycle={lifecycle})")
                provider.delete_instance(ref)


def _requeue(store: JobStorage, job: Job, reason: str):
    """Move job back to queue or fail if max restarts exceeded."""
    job.restarts += 1
    if job.restarts > job.max_restarts:
        job.state = JobState.FAILED.value
        job.failed_at = datetime.now(timezone.utc).isoformat()
        job.error = f"Exceeded {job.max_restarts} restarts ({reason})"
        store.move_job(job, "running", "failed")
        _log(f"{job.job_id}: FAILED (restart cap, {reason})")
        return

    job.state = JobState.QUEUED.value
    job.instance_ref = None
    job.started_at = None
    job.last_restart = datetime.now(timezone.utc).isoformat()
    store.move_job(job, "running", "queue")
    store.cleanup_status(job.job_id)
    _log(f"{job.job_id}: requeued ({reason}, restart {job.restarts})")


def reap_dead_agents(store: JobStorage, provider: Provider, kind: str = "gcp") -> int:
    """Delete RUNNING VMs whose `wc agent` has stopped publishing capacity.

    The agent's main loop publishes a freshness-stamped JSON to
    gs://<bucket>/capacity/<kind>-<hostname>.json on every iteration. If the
    process crashes (OOM, segfault, uncaught exception) the GCE instance keeps
    running, holding GPU + disk quota with zero work output. read_consumer_capacity
    filters to broadcasts younger than CAPACITY_STALE_SECONDS. Any RUNNING VM
    whose corresponding consumer_id is missing from that filtered set has a
    dead agent and gets deleted here so the dispatcher can spawn a fresh
    replacement.
    """
    from ..queue.capacity import read_consumer_capacity
    live = read_consumer_capacity(store)  # consumer_id -> payload, fresh only
    refs = provider.list_running_instance_refs_with_age()
    deleted = 0
    # Two reap conditions, each with an age guard so a VM that's still in its
    # startup-script install phase (pip install wisent-compute + transformers +
    # aux model download takes ~5-10 minutes before the agent's first broadcast)
    # doesn't get killed before it can do any work.
    #
    #   Branch A (dead-agent): VM age > BOOT_GRACE_SECONDS AND no fresh
    #     capacity broadcast. Covers crashed agents AND startup-script
    #     failures. The age guard prevents reaping fresh VMs that just
    #     haven't finished installing yet.
    #   Branch B (never-worked): VM age > IDLE_GRACE_SECONDS AND broadcasting
    #     normally AND zero completions in completed/ for this instance_ref.
    #     Covers agents that broadcast but cannot claim (some upstream
    #     failure in the claim path).
    BOOT_GRACE_SECONDS = 900       # 15-window grace for startup script + first broadcast
    IDLE_GRACE_SECONDS = 1800      # half-window grace for first completion
    # Build the completed-refs set ONLY if any VM is old enough to need it.
    # Iterating completed/ at fleet scale (~11k blobs) blows the 60s tick
    # budget every time, returning 504 and pausing Cloud Scheduler. Cheap
    # short-circuit: if no VM has crossed IDLE_GRACE_SECONDS, branch B
    # cannot fire anyway.
    needs_completions_scan = any(
        age_seconds > IDLE_GRACE_SECONDS for _, age_seconds in refs
    )
    completed_refs = (
        _instance_refs_with_completions(store, kind=kind)
        if needs_completions_scan else set()
    )
    # ALSO build the set of VMs that currently have a job in running/. A VM
    # mid-extraction on its FIRST big job (e.g. gpt-oss-20b 80GB shards)
    # legitimately exceeds IDLE_GRACE_SECONDS=1800 before producing its
    # first completion. Without this check, the never-worked reaper kills
    # healthy VMs and the parent jobs ricochet through restart cycles.
    # Confirmed live on 2026-05-07: reaper killed 23+ working VMs in one
    # hour, triggering the "never-worked reap (>5 in 1h)" alert email
    # storm.
    active_refs: set = set()
    if needs_completions_scan:
        for j in store.list_jobs("running"):
            r = getattr(j, "instance_ref", None)
            if r:
                active_refs.add(r)
    # Second signal: per-job heartbeat. Defers the reap when the agent's
    # capacity blob is stale BUT a running job assigned to its VM still
    # has a fresh heartbeat — agent is alive, just starved on its
    # broadcast tick by a training subprocess. Without this guard the
    # reaper destroys productive VMs (Llama-1B 5k run was reaped 3 times
    # mid-training on 2026-05-12 because rollout steps exceeded
    # CAPACITY_STALE_SECONDS).
    from ..queue.capacity import CAPACITY_STALE_SECONDS as _CAP_STALE
    from .heartbeat_guard import any_job_heartbeat_fresh, build_ref_to_jids
    _ref_to_jids = build_ref_to_jids(store)
    _hb_threshold = 2 * _CAP_STALE
    for ref, age_seconds in refs:
        name = ref.split("@", 1)[0]
        consumer_id = f"{kind}-{name}"
        instance_ref = f"local@{name}"
        if consumer_id not in live:
            if age_seconds < BOOT_GRACE_SECONDS:
                continue  # still installing, give it time
            _jids = _ref_to_jids.get(ref, []) + _ref_to_jids.get(instance_ref, [])
            if any_job_heartbeat_fresh(store, _jids, _hb_threshold):
                _log(
                    f"defer reap of {ref}: capacity stale "
                    f"(age={age_seconds:.0f}s) but job heartbeat fresh for {_jids}"
                )
                continue
            provider.delete_instance(ref)
            _log(
                f"reaped dead-agent VM {ref} (no fresh capacity broadcast, "
                f"age={age_seconds:.0f}s > boot grace {BOOT_GRACE_SECONDS}s, "
                f"no fresh job heartbeat either)"
            )
            deleted += 1
            _requeue_jids_after_reap(store, _jids, f"VM reaped (dead agent, age={age_seconds:.0f}s)")
            continue
        if (age_seconds > IDLE_GRACE_SECONDS
                and instance_ref not in completed_refs
                and instance_ref not in active_refs):
            provider.delete_instance(ref)
            _log(
                f"reaped never-worked VM {ref} (broadcasting but 0 completions "
                f"AND no active running job in age={age_seconds:.0f}s, "
                f"> grace {IDLE_GRACE_SECONDS}s)"
            )
            deleted += 1
            _jids = _ref_to_jids.get(ref, []) + _ref_to_jids.get(instance_ref, [])
            _requeue_jids_after_reap(store, _jids, "VM reaped (never-worked)")
    if deleted:
        _log(f"reap_dead_agents: deleted {deleted} VM(s)")
    return deleted


def _instance_refs_with_completions(store: JobStorage, kind: str = "gcp") -> set:
    """Return set of instance_ref strings that appear in the completed/ bucket.
    Used to detect VMs that broadcast capacity but never finish a job."""
    refs = set()
    for j in store.list_jobs("completed"):
        r = getattr(j, "instance_ref", None)
        if r:
            refs.add(r)
    return refs


def _requeue_jids_after_reap(store: JobStorage, jids, reason: str):
    if not jids:
        return
    running = {j.job_id: j for j in store.list_jobs("running")}
    for jid in jids:
        job = running.get(jid)
        if job is None:
            continue
        _requeue(store, job, reason)


def _requeue_preempted(store: JobStorage, job: Job, reason: str):
    """Move job back to queue, counting preemption separately from restarts.

    Preemptions are an expected part of Spot lifecycle, not a fault. They
    accumulate in preempt_count; once that exceeds max_preempts_before_ondemand
    the scheduler dispatches the next attempt on-demand instead.
    """
    job.preempt_count = getattr(job, "preempt_count", 0) + 1
    job.state = JobState.QUEUED.value
    job.instance_ref = None
    job.started_at = None
    job.last_restart = datetime.now(timezone.utc).isoformat()
    store.move_job(job, "running", "queue")
    store.cleanup_status(job.job_id)
    _log(f"{job.job_id}: requeued ({reason}, preempts={job.preempt_count})")
