"""Evaluate schedules and submit due jobs. Called once per coordinator tick.

A schedule is "due" when now >= next_due_at. Firing is:
  1. compute the next future occurrence,
  2. atomically claim it (store.claim_due — advances next_due_at FIRST,
     under a GCS generation-match, so an overlapping CF invocation can't
     double-fire),
  3. submit the job tagged with schedule_id + a fresh run_id,
  4. record last_fired_at / last_run_id / last_job_id / fire_count.

catchup_policy is "skip" only for now: if the coordinator was down across
several occurrences, step 1 jumps straight to the next future slot, so the
backlog collapses to a single fire rather than a burst.
"""
from __future__ import annotations

from datetime import datetime, timezone

from .model import Schedule, compute_next_due
from . import store as sched_store


def _prev_instance_live(store, sched: Schedule) -> bool:
    """True iff this schedule's most recent fire is still queued/running.
    Two direct reads by id — cheap, unlike scanning queue/ (14k+ blobs)."""
    jid = sched.last_job_id
    if not jid:
        return False
    try:
        if store.read_job("queue", jid) is not None:
            return True
        if store.read_job("running", jid) is not None:
            return True
    except Exception:
        # A read error is not proof the prior instance is gone; be
        # conservative and treat it as live so overlap_policy=skip holds.
        return True
    return False


def fire_due_schedules(store, log_fn=None, now: datetime | None = None) -> int:
    """Fire every due+enabled schedule once. Returns the number fired."""
    def _log(m):
        if log_fn:
            log_fn(m)

    now = now or datetime.now(timezone.utc)
    fired = 0
    for sid in sched_store.list_schedule_ids(store):
        sched = sched_store.read_schedule(store, sid)
        if sched is None or not sched.enabled or not sched.next_due_at:
            continue
        try:
            due = datetime.fromisoformat(sched.next_due_at)
        except (ValueError, TypeError):
            _log(f"schedule {sid}: unparseable next_due_at={sched.next_due_at!r}; skipping")
            continue
        if due.tzinfo is None:
            due = due.replace(tzinfo=timezone.utc)
        if due > now:
            continue

        next_due = compute_next_due(sched.cron, now, sched.tz).isoformat()

        if sched.overlap_policy == "skip" and _prev_instance_live(store, sched):
            # Don't fire on top of a still-running prior instance — but DO
            # advance next_due_at so we re-evaluate cleanly next tick instead
            # of re-firing the same overdue slot every tick.
            sched.next_due_at = next_due
            sched_store.write_schedule(store, sched)
            _log(f"schedule {sid}: skip fire (prior job {sched.last_job_id} still live)")
            continue

        # Claim the occurrence before submitting (CF double-fire guard).
        if not sched_store.claim_due(store, sched, next_due):
            _log(f"schedule {sid}: lost claim race; another coordinator fired it")
            continue

        try:
            from ..queue.submit import submit_job
            from ..queue.runs import generate_run_id
            run_id = generate_run_id()
            job = submit_job(
                sched.command,
                bucket=store.bucket_name,
                run_id=run_id,
                schedule_id=sid,
                **sched.submit_kwargs(),
            )
        except Exception as exc:
            # next_due_at is already advanced (occurrence consumed). Record
            # the miss rather than retry-storming; the next occurrence fires
            # normally.
            _log(f"schedule {sid}: submit FAILED ({type(exc).__name__}: {exc}); occurrence skipped")
            continue

        sched.last_fired_at = now.isoformat()
        sched.last_run_id = run_id
        sched.last_job_id = job.job_id
        sched.fire_count += 1
        sched_store.write_schedule(store, sched)
        fired += 1
        _log(f"schedule {sid}: fired job {job.job_id} (run {run_id}); next_due={next_due}")
    return fired
