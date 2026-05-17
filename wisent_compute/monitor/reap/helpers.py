"""Requeue + completed-ref helpers for monitor.py.

Extracted out of wisent_compute/monitor/monitor.py so that module
returns under the 300-line file-size cap after reaper Branch C
(wedged-agent reap) was added (2026-05-17). These are independent
concerns (job requeue accounting, the cached completed/ instance-ref
scan) with NO dependency on monitor.py, so the import is strictly
one-directional and cannot cycle. Functions moved verbatim.
"""
from __future__ import annotations

import sys
import time as _time
from datetime import datetime, timezone

from ...models import Job, JobState
from ...queue.storage import JobStorage


def _log(msg):
    sys.stderr.write(f"[monitor] {msg}\n")
    sys.stderr.flush()


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


_COMPLETION_REFS_TTL_S = 300
_completion_refs_cache: set = set()
_completion_refs_built_at: float = 0.0


def _instance_refs_with_completions(store: JobStorage, kind: str = "gcp") -> set:
    """Return set of instance_ref strings appearing in completed/.

    Iterating list_jobs("completed") downloads every completed blob (13,500+
    in production) which took ~75s per tick — confirmed live 03:27Z
    2026-05-15. Cache the result with a 5-minute TTL: the set only grows
    when new jobs complete, and the never-worked reaper branch's accuracy
    is unchanged because a VM that NEVER worked stays out of the set
    regardless of cache age.
    """
    global _completion_refs_cache, _completion_refs_built_at
    if (_time.time() - _completion_refs_built_at) < _COMPLETION_REFS_TTL_S and _completion_refs_cache:
        return _completion_refs_cache
    refs = set()
    for j in store.list_jobs("completed"):
        r = getattr(j, "instance_ref", None)
        if r:
            refs.add(r)
    _completion_refs_cache = refs
    _completion_refs_built_at = _time.time()
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
