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

from ..config import HEARTBEAT_STALE_MINUTES, ALERTS_TOPIC
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
            alive = provider.instance_exists(ref)
            lifecycle = provider.instance_lifecycle_state(ref)
            stale = store.heartbeat_stale(job_id, HEARTBEAT_STALE_MINUTES)

            if not alive and lifecycle == "TERMINATED" and getattr(job, "preemptible", False):
                # GCE has TERMINATED a Spot/preemptible VM but the boot disk and
                # name still exist (lifecycleState=TERMINATED, instance.exists=True
                # in raw GET, but our instance_exists() returns False because
                # status isn't RUNNING/STAGING). Treat as preemption: delete +
                # requeue with preempt_count++ instead of restarts++.
                _requeue_preempted(store, job, "Spot preempted")
                provider.delete_instance(ref)
            elif not alive:
                _requeue(store, job, f"instance gone (lifecycle={lifecycle})")
                provider.delete_instance(ref)
            elif stale:
                _requeue(store, job, "stale heartbeat")
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
