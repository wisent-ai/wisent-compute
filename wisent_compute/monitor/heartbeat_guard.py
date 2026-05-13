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
        text = store.read_text(f"status/{jid}/heartbeat")
        if not text:
            continue
        ts = _parse_heartbeat_ts(text)
        if now - ts < threshold_seconds:
            return True
    return False


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
