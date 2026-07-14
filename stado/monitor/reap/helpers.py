"""Requeue + completed-ref helpers for monitor.py.

Extracted out of stado/monitor/monitor.py so that module
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


def safe_delete_vm_by_hostname(provider, hostname, vm_cache, log_fn) -> bool:
    """Best-effort delete of a GCE VM named `hostname`.

    The requeue paths in check_running_jobs (orphan + VM-gone) previously
    moved running -> queue without calling provider.delete_instance, so
    the prior agent's training subprocess kept running on the
    supposedly-gone VM and producing duplicate writes against the same
    gs://wisent-compute/ckpts/<run>/ path. Confirmed live 2026-05-18 for
    job 724084db: 4 concurrent trainers (workstation + 3 GCP VMs) all
    racing on the same ckpt prefix because each transient "VM missing
    from fleet listing" requeue spawned a new dispatch without
    terminating the old subprocess.

    Looks up the full <name>@<zone> ref from `vm_cache` (a dict
    {hostname: full_ref} built by the caller) and falls back to a fresh
    list-and-search on cache miss (handles the exact transient-miss case
    that caused the ghost-trainer bug). Never raises; returns True iff
    a delete call returned cleanly. Idempotent and safe on a VM that is
    truly gone (returns False).
    """
    full_ref = vm_cache.get(hostname) if isinstance(vm_cache, dict) else None
    if not full_ref:
        try:
            for r, _age in provider.list_running_instance_refs_with_age():
                if r.split("@", 1)[0] == hostname:
                    full_ref = r
                    break
        except Exception as e:
            log_fn(f"safe_delete: fresh list failed for {hostname}: "
                   f"{type(e).__name__}: {e}")
            return False
    if not full_ref:
        return False
    try:
        provider.delete_instance(full_ref)
        log_fn(f"safe_delete: killed ghost VM {full_ref}")
        return True
    except Exception as e:
        log_fn(f"safe_delete({full_ref}) failed: "
               f"{type(e).__name__}: {e}")
        return False


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


def safety_is_real_race(store: JobStorage, jids, hb_threshold: float) -> bool:
    """Decide whether a freshly re-listed running/ jid set
    (fresh_jids_pointing_to_ref) is a GENUINE active_refs race that must
    defer a reap, vs a set of confirmed ORPHANS safe to reap+requeue.

    fresh_jids_pointing_to_ref's "fresh" means "re-listed at call time"
    (beats the cached-listing race), NOT "the job is alive" — it returns
    every running/ blob pointing at the ref with zero liveness check. On
    a CONFIRMED-dead agent (reaper Branch A: consumer_id absent from live
    capacity) that made the guard defer on mere blob existence forever:
    0db3438b/6a0fceba sat ~3h on agents that were gone (no capacity
    broadcast, heartbeats ~2.5h stale), never requeued, and the whole
    gpt-oss-20b queue totally stalled (2026-05-19).

    A real race is only when the job is plausibly alive: the GCS re-list
    itself failed (fail-safe defer), OR some jid still heartbeats / writes
    checkpoints fresh, OR some jid started so recently it has not had time
    to heartbeat yet (boot grace). Otherwise every jid is a stale orphan
    on a dead VM -> return False so the caller reaps and requeues them.
    """
    if not jids:
        return False
    if "__list_failed__" in jids:
        return True  # GCS re-list failure: never reap on unknown state
    from ..heartbeat_guard import (
        any_job_heartbeat_fresh,
        any_job_checkpoint_fresh_jids,
    )
    if (any_job_heartbeat_fresh(store, jids, hb_threshold)
            or any_job_checkpoint_fresh_jids(store, jids, 5400)):
        return True
    running = {j.job_id: j for j in store.list_jobs("running")}
    now = datetime.now(timezone.utc)
    for jid in jids:
        j = running.get(jid)
        sa = getattr(j, "started_at", None) if j else None
        if not sa:
            continue
        try:
            if (now - datetime.fromisoformat(sa)).total_seconds() < 1800:
                return True  # just dispatched, no heartbeat yet (real race)
        except (ValueError, TypeError):
            continue
    return False


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


def requeue_dead_local_host_orphan(store: JobStorage, job, job_id, log_fn):
    """Requeue a job orphaned on a stale non-cloud local@ agent.

    reap_dead_agents only iterates GCP provider VMs, so a local@<host>
    that is NOT a wisent-agent-* cloud VM (e.g. the ubuntu-server lab
    box) is never reaped there. In check_running_jobs the agent_live
    block is skipped once that agent's capacity broadcast goes stale,
    and the is_cloud_agent_name block does not match, so control fell
    to a bare `continue` and the job wedged in running/ forever
    (0db3438b: hb 03:28:42, command_output.log 03:25:46,
    local-ubuntu-server capacity stale, coordinator logged 'dead host
    ubuntu-server', never requeued, 2026-05-19). Leave the job alone
    only if it is demonstrably alive (fresh heartbeat / fresh
    checkpoint) or its command self-terminates the agent (kill is the
    success condition); otherwise requeue it. No VM delete — the local
    host is operator-owned and must not be touched.
    """
    from ..heartbeat_guard import (
        any_job_heartbeat_fresh,
        any_job_checkpoint_fresh,
        finalize_if_self_terminating,
    )
    if any_job_heartbeat_fresh(store, [job_id], 1800):
        return
    if any_job_checkpoint_fresh(store, job, 5400):
        return
    if finalize_if_self_terminating(store, job, log_fn):
        return
    _requeue(store, job,
             "local agent capacity stale & job heartbeat stale "
             "(dead local host orphan)")
