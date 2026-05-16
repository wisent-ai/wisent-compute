"""Per-job heartbeat freshness check used by the reaper to avoid
destroying productive VMs.

The reaper's primary signal is the agent's capacity broadcast in
gs://<bucket>/capacity/<consumer_id>.json. When the agent runs a
long training subprocess the broadcast loop can starve past
CAPACITY_STALE_SECONDS even though the agent process is alive and
the training is actively producing checkpoints. Reaping that VM
destroys hours of work and forces the job to restart from the last
checkpoint (or step 0 if no checkpoints exist).

This module provides a second signal — the per-job heartbeat at
gs://<bucket>/status/<job_id>/heartbeat — that is written by the
running job itself (via the agent's status-watchdog cron) and is
NOT coupled to the agent's broadcast loop. If ANY job assigned to
a VM has a fresh heartbeat, the agent is alive and the reap is
deferred.
"""
from __future__ import annotations

import re
import time
from typing import Iterable


_TS_RE = re.compile(
    r"(\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(?:\.\d+)?Z?)"
)


def _parse_heartbeat_ts(text: str) -> float | None:
    """Parse an ISO-8601 timestamp from the heartbeat blob and return
    unix seconds. The agent writes lines like:
        RUNNING 2026-05-13T00:26:33.130155+00:00
    Returns None if no parseable timestamp is found.
    """
    if not text:
        return None
    m = _TS_RE.search(text)
    if not m:
        return None
    raw = m.group(1).replace(" ", "T").rstrip("Z")
    if "." in raw:
        head, frac = raw.split(".", 1)
        tz_idx = 0
        for i, c in enumerate(frac):
            if c in "+-":
                tz_idx = i
                break
        if tz_idx:
            tz = frac[tz_idx:]
            frac_digits = frac[:tz_idx]
        else:
            tz = ""
            frac_digits = frac
        frac_digits = frac_digits[:6]
        raw = f"{head}.{frac_digits}{tz}"
    if "+" not in raw and not raw.endswith("Z"):
        raw = raw + "+00:00"
    from datetime import datetime
    dt = datetime.fromisoformat(raw)
    return dt.timestamp()


def any_job_heartbeat_fresh(
    store, jids: Iterable[str], threshold_seconds: float
) -> bool:
    """True iff ANY job in jids has a heartbeat blob whose embedded
    timestamp is younger than threshold_seconds. Used by the reaper:
    if the agent's capacity blob is stale but a job assigned to its
    VM is still heartbeating, the agent is alive — busy in the
    training subprocess — and the VM should NOT be deleted.
    """
    now = time.time()
    for jid in jids:
        if not jid:
            continue
        try:
            text = store._download_text(f"status/{jid}/heartbeat")
        except Exception:
            # A coordinator-side GCS read failure is NOT proof the job
            # is dead. The old `except: text=None` path made a transient
            # Cloud-Function storage hiccup on one monitor tick read as
            # "no heartbeat" for EVERY running job, requeuing them all
            # in the same pass — the synchronized orphan churn (3ef705b2
            # + 724084db both requeued 2026-05-16T04:09:19, restart 11,
            # on freshly-written heartbeat blobs). Fail safe: a read
            # error defers (treat as alive); never let the coordinator's
            # own read failure destroy a running job. A genuinely dead
            # job is still caught when the read succeeds (stale ts) or
            # via the TERMINATED/absent VM path.
            return True
        if not text:
            continue
        ts = _parse_heartbeat_ts(text)
        if ts is None:
            continue
        if now - ts < threshold_seconds:
            return True
    return False


def is_self_terminating_command(cmd: str) -> bool:
    """True if the job command kills the `wc agent` process itself
    (e.g. an upgrade-then-restart maintenance job:
    `pip install --upgrade ... ; pkill -f "wc agent"`).

    For such a command the agent's disappearance is the SUCCESS
    condition, not an orphan failure. The agent dies before it can
    write a COMPLETED status, so the job is stranded in running/ and
    the orphan-reaper requeues it — which re-runs the kill on the
    next agent generation, an infinite crash loop. Confirmed live
    2026-05-15: job 435b184e crash-looped ubuntu-server
    (wisent-agent.service n_restarts=7) until removed by operator.
    """
    if not cmd:
        return False
    return ("pkill" in cmd or "kill " in cmd) and "wc agent" in cmd


def finalize_if_self_terminating(store, job, log_fn) -> bool:
    """If job.command self-terminates the agent, finalize the job as
    COMPLETED (running/ -> completed/) and return True. Otherwise
    return False so the caller proceeds with its normal requeue path.
    """
    from datetime import datetime, timezone
    from ..models import JobState
    if not is_self_terminating_command(getattr(job, "command", "") or ""):
        return False
    job.state = JobState.COMPLETED.value
    job.completed_at = datetime.now(timezone.utc).isoformat()
    job.instance_ref = None
    store.move_job(job, "running", "completed")
    store.cleanup_status(job.job_id)
    log_fn(f"{job.job_id}: COMPLETED (self-terminating maintenance cmd; "
           f"agent kill is the success condition, not an orphan)")
    return True


def build_ref_to_jids(store) -> dict:
    """Build instance_ref -> list[job_id] from store.list_jobs('running').
    Used by the reaper to find which jobs claim each VM ref before the
    heartbeat freshness check.
    """
    out: dict = {}
    running = store.list_jobs("running")
    for j in running:
        ref = getattr(j, "instance_ref", None)
        jid = getattr(j, "job_id", None)
        if ref and jid:
            out.setdefault(ref, []).append(jid)
    return out
