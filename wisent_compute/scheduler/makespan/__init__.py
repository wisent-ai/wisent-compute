"""Makespan-minimizing job-to-agent matcher. Sorts queue by
(-priority, -runtime) (LPT in time, runtime from completed-job
history keyed by (model, task)), then assigns each job to the
eligible agent that finishes it earliest under a VRAM-concurrency
model. Writes assigned_to on the queue blob; agent-side enforcement
lives in providers/local/helpers/_job_eligible. No runtime guesses:
jobs without history AND without an explicit runtime_seconds_estimate
stay unassigned and the operator sees a log naming them. The runtime-
history machinery lives in ._history (split out to keep this module
under the 300-line cap).
"""
from __future__ import annotations

import datetime as dt
import json
import re
from typing import Callable, Optional

from ..._catalog.gpu_sku import GPU_SIZING  # noqa: F401 (used implicitly through eligibility downstream)
from ...queue.storage import JobStorage
from ._history import _extract_model_task, _history


HEARTBEAT_TTL_S = 180

_INSTANCE_HOST_RE = re.compile(r"^[^@]+@(.+)$")


def _estimate_runtime(job, history: dict[tuple[str, str], float]) -> float | None:
    """Per-job runtime estimate in seconds. Returns None when neither an
    explicit estimate nor a matching history entry is available; the
    caller leaves the job unassigned with a log naming the missing
    (model, task). A new (model, task) combo must either set
    runtime_seconds_estimate at submit time or wait for a sibling job
    to complete and seed history; the matcher refuses to guess."""
    explicit = float(getattr(job, "runtime_seconds_estimate", 0.0) or 0.0)
    if explicit > 0:
        return explicit
    model, task = _extract_model_task(getattr(job, "command", "") or "")
    if not model or not task:
        return None
    if (model, task) not in history:
        return None
    return history[(model, task)]


def _live_agents(store: JobStorage, now: dt.datetime) -> dict[str, dict]:
    from google.api_core.exceptions import NotFound
    bucket = getattr(store, "_sdk_bucket", None)
    if bucket is None:
        return {}
    agents: dict[str, dict] = {}
    for blob in bucket.list_blobs(prefix="capacity/"):
        try:  # capacity blobs can vanish between list and download
            text = blob.download_as_text()
        except NotFound:
            continue
        doc = json.loads(text)
        cid = doc.get("consumer_id") or ""
        pub = doc.get("published_at") or ""
        age = (now - dt.datetime.fromisoformat(pub.replace("Z", "+00:00"))).total_seconds()
        if age > HEARTBEAT_TTL_S:
            continue
        # Capacity guard: _assign_one gates on total_vram_gb only, so a
        # wedged agent (free_vram_gb=0 / free_slots={} but total 80) was a
        # valid target and the 127 gpt-oss-20b jobs dead-pinned to agents
        # that could never claim them (q pinned 348, c frozen 14069,
        # 2026-05-17). Skip an agent with no free VRAM AND no free slots.
        if int(doc.get("free_vram_gb") or 0) <= 0 and not (doc.get("free_slots") or {}):
            continue
        agents[cid] = {"total_vram_gb": int(doc.get("total_vram_gb") or 0),
                       "active_slots": []}
    return agents


def _seed_running_jobs(store: JobStorage, agents: dict[str, dict],
                       now: dt.datetime, history: dict,
                       log_fn: Callable[[str], None]) -> None:
    """For each running/ blob, locate the executing agent (by hostname
    in instance_ref) and add an active_slot covering its remaining
    runtime. Without this, a freshly-claimed long job looks invisible
    and the matcher would pile more work onto an already-loaded agent.
    """
    bucket = getattr(store, "_sdk_bucket", None)
    if bucket is None:
        return
    # consumer_id is "<kind>-<hostname>" (queue/capacity.publish_capacity).
    host_to_cid: dict[str, str] = {}
    for cid in agents:
        parts = cid.split("-", 1)
        if len(parts) == 2:
            host_to_cid[parts[1]] = cid
    from google.api_core.exceptions import NotFound as _NotFound2
    for blob in bucket.list_blobs(prefix="running/"):
        try:  # running blob can be moved to completed/failed mid-tick
            text = blob.download_as_text()
        except _NotFound2:
            continue
        doc = json.loads(text)
        iref = doc.get("instance_ref") or ""
        m = _INSTANCE_HOST_RE.match(iref)
        if not m:
            raise RuntimeError(
                f"makespan: running blob {blob.name} has malformed "
                f"instance_ref {iref!r}; cannot map to consumer_id"
            )
        host = m.group(1)
        cid = host_to_cid.get(host)
        if cid is None:
            # Running job points to an agent we no longer track as live.
            # Leave its VRAM out of the projection; the reaper will move
            # the running blob to failed on its own pass. Logging only.
            log_fn(f"makespan: running {doc.get('job_id')} on dead host {host}; skipping projection")
            continue
        st = doc.get("started_at")
        if not st:
            raise RuntimeError(
                f"makespan: running blob {blob.name} has no started_at"
            )
        elapsed = (now - dt.datetime.fromisoformat(
            st.replace("Z", "+00:00")
        )).total_seconds()

        class _Shim:
            command = doc.get("command", "")
            runtime_seconds_estimate = doc.get("runtime_seconds_estimate", 0)
        est = _estimate_runtime(_Shim(), history)
        if est is None:
            # Admin/maintenance commands have no parseable (model, task); the
            # matcher can't predict their finish time. Agent-side smi_free
            # enforces actual VRAM at claim time.
            log_fn(f"makespan: skip running {doc.get('job_id')} for seeding")
            continue
        remaining = max(0.0, est - max(0.0, elapsed))
        vram = int(doc.get("gpu_mem_gb") or 0)
        agents[cid]["active_slots"].append((remaining, vram))


def _earliest_start(slots: list, new_vram: int, total_vram: int) -> float:
    """Earliest start time (seconds from now) at which new_vram GB
    becomes free on an agent with the given total VRAM and active slots
    [(finish_offset_seconds, vram_gb), ...]."""
    used_now = sum(v for _, v in slots)
    available_now = total_vram - used_now
    if available_now >= new_vram:
        return 0.0
    by_end = sorted(slots, key=lambda s: s[0])
    freed = available_now
    for end_time, vram in by_end:
        freed += vram
        if freed >= new_vram:
            return float(end_time)
    return float(by_end[-1][0]) if by_end else 0.0


def _assign_one(job, agents: dict[str, dict], runtime: float, vram: int) -> Optional[str]:
    """Pick the eligible agent that finishes this job earliest; update
    its active_slots in place. Returns the chosen consumer_id, or None
    if no agent has enough total VRAM to host the job."""
    if bool(getattr(job, "exclusive", False)):
        return None
    best_cid: Optional[str] = None
    best_finish: Optional[float] = None
    for cid, info in agents.items():
        if info["total_vram_gb"] < vram:
            continue
        start = _earliest_start(info["active_slots"], vram, info["total_vram_gb"])
        finish = start + runtime
        if best_finish is None or finish < best_finish:
            best_finish = finish
            best_cid = cid
    if best_cid is None or best_finish is None:
        return None
    agents[best_cid]["active_slots"].append((best_finish, vram))
    return best_cid


def assign_jobs(store: JobStorage, log_fn: Optional[Callable[[str], None]] = None) -> int:
    """One pass of makespan-minimizing assignment. Returns the number of
    queue blobs whose assigned_to changed this tick."""
    if log_fn is None:
        log_fn = lambda _msg: None  # noqa: E731
    now = dt.datetime.now(dt.timezone.utc)
    history = _history(store, log_fn)
    agents = _live_agents(store, now)
    if not agents:
        return 0
    _seed_running_jobs(store, agents, now, history, log_fn)
    # Aggregate skip counts + parallel writes: 900+ per-job skip log lines
    # were eating ~300s of the 540s tick budget; serial assignment writes
    # added ~10s. Confirmed live 02:54Z 2026-05-15.
    from concurrent.futures import ThreadPoolExecutor
    schedulable: list[tuple[int, float, object]] = []
    skip_by_key: dict[tuple[str, str], int] = {}
    for j in store.list_jobs("queue"):
        rt = _estimate_runtime(j, history)
        if rt is None:
            mt = _extract_model_task(getattr(j, "command", "") or "")
            if int(getattr(j, "priority", 0) or 0) <= 0:
                skip_by_key[mt] = skip_by_key.get(mt, 0) + 1
                continue
            # High-priority no-history job (one-off training run, e.g.
            # free_chat_pd GRPO) must not be silently dropped: priority
            # =999999 jobs were starved in queue indefinitely behind the
            # history-backed benchmark backlog (Qwen3 724084db queued
            # 30min+, zero dispatch, 2026-05-15). Conservative long
            # runtime so it still enters schedulable and the priority
            # sort below places it first.
            rt = 6 * 3600.0
        schedulable.append((-int(getattr(j, "priority", 0) or 0), -rt, j))
    schedulable.sort(key=lambda t: (t[0], t[1]))
    to_write: list[object] = []
    unassigned = 0
    for _p, neg_rt, job in schedulable:
        vram = int(getattr(job, "gpu_mem_gb", 0) or 0)
        chosen = _assign_one(job, agents, -neg_rt, vram)
        if chosen is None:
            unassigned += 1
            if getattr(job, "assigned_to", ""):
                job.assigned_to = ""
                to_write.append(job)
            continue
        if getattr(job, "assigned_to", "") == chosen:
            continue
        job.assigned_to = chosen
        to_write.append(job)
    if to_write:
        with ThreadPoolExecutor(max_workers=16) as ex:
            list(ex.map(lambda j: store.write_job("queue", j), to_write))
    skipped = sum(skip_by_key.values())
    if skipped:
        top = sorted(skip_by_key.items(), key=lambda kv: -kv[1])[:5]
        log_fn(f"makespan: {skipped} skipped; top: " + ", ".join(f"({m},{t}):{n}" for (m,t),n in top))
    if unassigned:
        log_fn(f"makespan: {unassigned} unassigned (no eligible agent)")
    return len(to_write)
