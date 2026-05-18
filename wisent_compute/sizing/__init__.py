"""Fleet-learned per-model GPU sizing — purely from MEASURED peaks.

For a model, the maximum ACTUAL peak_vram_gb the agent measured via
nvidia-smi per-process during a completed run. No formula, no per-model
constant, no minimum-sample gate: a single real measurement is the
truth and is used immediately. max (not mean/mode) because one run that
legitimately needed the peak must still be sized for.

If a model has ZERO measured completions, observed_vram_gb returns None
and the caller does NOT fabricate a number — the job starts on the
smallest GPU tier and escalates up the hardware ladder on OOM until it
runs, at which point its real peak is measured and every later job of
that model is sized from that measurement. There is no hardcoded VRAM
guess anywhere in this path; the only inputs are measured peaks and
hardware GPU-class capacities.

completed/ is thousands of blobs; building the per-model map on every
estimate call would blow the tick budget, so it is built once and
cached in process for _TTL_S, the same amortization makespan._history
and the reaper completion-ref scan already use.
"""
from __future__ import annotations

import json
import re
import time

from ..config import BUCKET

_MODEL_RE = re.compile(r"--model\s+(\S+)")

_COMPLETED_SAMPLE_CAP = 6000
_TTL_S = 600
_cache: dict = {"map": None, "built_at": 0.0}


def _model_of(command: str) -> str:
    m = _MODEL_RE.search(command or "")
    return m.group(1).strip("'\"") if m else ""


def _build_observed_map() -> dict[str, int]:
    """model -> max measured peak_vram_gb over its completed runs.

    Any model with >= 1 real measurement is included; there is no
    minimum-sample gate. A coordinator-side GCS list/read outage must
    not silently erase the map fleet-wide, so a hard failure here
    propagates; the caller's cache keeps the last good map until a
    later rebuild succeeds.
    """
    from concurrent.futures import ThreadPoolExecutor
    from google.cloud import storage
    from google.api_core.exceptions import NotFound

    bucket = storage.Client().bucket(BUCKET)
    blobs = []
    for blob in bucket.list_blobs(prefix="completed/"):
        blobs.append(blob)
        if len(blobs) >= _COMPLETED_SAMPLE_CAP:
            break
    if not blobs:
        return {}

    def _fetch(b):
        # TOCTOU: a completed/ blob can be moved (verify_command rc!=0)
        # between list and download. Skip just that entry; any other
        # GCS error propagates so a real outage is visible.
        try:
            return b.download_as_text()
        except NotFound:
            return None

    with ThreadPoolExecutor(max_workers=32) as ex:
        texts = list(ex.map(_fetch, blobs))

    peaks: dict[str, list[int]] = {}
    for text in texts:
        if text is None:
            continue
        doc = json.loads(text)
        if doc.get("state") != "completed":
            continue
        peak = doc.get("peak_vram_gb")
        if not isinstance(peak, int) or peak <= 0:
            continue  # unmeasured / CPU job — not a usable observation
        if doc.get("peak_vram_per_gpu") is not True:
            # Legacy record from the pre-0.4.241 probe that summed
            # used_memory ACROSS GPUs. On a multi-GPU host that is the
            # cross-GPU total, not the per-card requirement (gpt-oss-20b
            # ~89 summed vs ~50-74 real), and max() lets one such sample
            # pin every job above the single-GPU fleet. Only peaks the
            # corrected per-GPU probe produced are a valid single-GPU
            # sizing signal. Until a model has one, observed_vram_gb
            # returns None and the job sizes via the smallest-live-GPU +
            # OOM-escalate path (no fabricated number).
            continue
        model = _model_of(doc.get("command") or "")
        if not model:
            continue
        peaks.setdefault(model, []).append(peak)

    return {model: max(samples) for model, samples in peaks.items()}


def observed_vram_gb(model: str) -> int | None:
    """Max MEASURED peak_vram_gb for model, or None if the model has no
    measured completion yet (caller must NOT fabricate a number — start
    on the smallest ACTUAL fleet GPU and escalate via live capacities)."""
    now = time.time()
    if _cache["map"] is None or now - _cache["built_at"] > _TTL_S:
        _cache["map"] = _build_observed_map()
        _cache["built_at"] = now
    return _cache["map"].get(model)


# Agent-liveness window: a capacity broadcast older than this means the
# agent is gone, so its GPU is not part of "the actual fleet" right now.
# This is a staleness threshold, not a VRAM figure.
_LIVE_TTL_S = 180
_caps_cache: dict = {"vrams": None, "built_at": 0.0}


def _live_total_vrams() -> list[int]:
    """Ascending, de-duplicated list of the REAL total_vram_gb values the
    fleet is broadcasting right now — i.e. the actual GPUs that exist,
    read from gs://<bucket>/capacity/ (each agent publishes its own
    nvidia-smi total_vram_gb). No catalog, no hand-written tier list.
    Stale broadcasts (older than _LIVE_TTL_S) are excluded. Cached 30s
    so the agent claim loop / submit path does not relist every call.
    """
    now = time.time()
    if (_caps_cache["vrams"] is not None
            and now - _caps_cache["built_at"] < 30):
        return _caps_cache["vrams"]
    import datetime as _dt
    from google.cloud import storage
    from google.api_core.exceptions import NotFound

    vrams: set[int] = set()
    bucket = storage.Client().bucket(BUCKET)
    for blob in bucket.list_blobs(prefix="capacity/"):
        try:
            doc = json.loads(blob.download_as_text())
        except NotFound:
            continue
        pub = doc.get("published_at") or ""
        try:
            age = (_dt.datetime.now(_dt.timezone.utc)
                   - _dt.datetime.fromisoformat(
                       pub.replace("Z", "+00:00"))).total_seconds()
        except Exception:
            continue
        if age > _LIVE_TTL_S:
            continue
        tv = doc.get("total_vram_gb")
        if isinstance(tv, int) and tv > 0:
            vrams.add(tv)
    out = sorted(vrams)
    _caps_cache["vrams"] = out
    _caps_cache["built_at"] = now
    return out


def smallest_live_vram() -> int | None:
    """Smallest REAL GPU total_vram_gb currently in the fleet, or None if
    no live agent is broadcasting (then the caller must not invent a
    number — the job stays unsized until a real GPU appears)."""
    v = _live_total_vrams()
    return v[0] if v else None


def next_live_vram(current: int) -> int | None:
    """Smallest REAL fleet total_vram_gb strictly greater than `current`,
    or None if no live GPU is bigger (genuine ceiling — not a guess)."""
    for v in _live_total_vrams():
        if v > current:
            return v
    return None


_OOM_RE = re.compile(
    r"out of memory|OutOfMemoryError|CUDA error: out of memory|"
    r"CUDA_ERROR_OUT_OF_MEMORY|cuBLAS.*alloc|cudaErrorMemoryAllocation",
    re.IGNORECASE,
)


def escalate_on_oom(store, job, error_text: str) -> bool:
    """A job that OOMed while sized at some GPU — and has NO measured
    peak yet — is moved to the next-larger REAL GPU currently in the
    fleet and requeued (running -> queue) instead of failed. Returns
    True iff requeued. If no live GPU is larger, or the model already
    has a measured peak, this does nothing and the caller fails it.

    No hand-written tier ladder: the next size comes from the actual
    GPUs the fleet is broadcasting (next_live_vram). An unmeasured model
    starts on the smallest real fleet GPU and climbs the real observed
    GPUs one OOM at a time until it runs; that run's measured nvidia-smi
    peak then sizes every later job of the model.
    """
    if not _OOM_RE.search(error_text or ""):
        return False
    model = _model_of(getattr(job, "command", "") or "")
    if not model:
        return False
    if observed_vram_gb(model) is not None:
        return False  # measured already; a real OOM is a real failure
    from ..models import JobState

    cur = int(getattr(job, "gpu_mem_gb", 0) or 0)
    nxt = next_live_vram(cur)
    if nxt is None:
        return False  # no live GPU bigger than current — genuine failure
    job.gpu_mem_gb = nxt
    job.state = JobState.QUEUED.value
    job.failed_at = None
    job.error = None
    job.instance_ref = None
    job.started_at = None
    store.move_job(job, "running", "queue")
    store.cleanup_status(job.job_id)
    return True


def normalize_queue_sizing(store, log_fn=None) -> int:
    """Coordinator-authoritative sizing pass, run once per tick BEFORE
    assignment.

    A queued job's gpu_mem_gb is owned by the sizing path, not by the
    agent that last touched it. An agent still on pre-0.4.237
    wisent-compute (not yet drifted) requeues jobs writing the OLD
    hardcoded estimate_gpu_memory output (gpt-oss-20b -> 64/12/80); the
    0.4.238 makespan _apply_assignment then faithfully PRESERVES that
    stale value because it only rewrites assigned_to. So the queue keeps
    re-accumulating hardcoded sizes until every agent has drifted.

    This pass closes that gap structurally: for every queued job whose
    model has NO measured peak yet, force gpu_mem_gb back to 0 — the
    canonical "no stored size, sized live at claim time" state. For a
    model WITH a measured peak, stamp that measured peak (the ground
    truth). Either way the stored number is never a hardcoded guess.
    A lagging agent's stale write is corrected within one tick instead
    of persisting until fleet-wide drift completes.

    Fresh read-modify-write of ONLY gpu_mem_gb so a concurrent
    makespan assigned_to write on the same blob is not lost. Returns
    the number of queue blobs corrected this tick.
    """
    if log_fn is None:
        log_fn = lambda _m: None  # noqa: E731
    corrected = 0
    for j in store.list_jobs("queue"):
        model = _model_of(getattr(j, "command", "") or "")
        if not model:
            continue
        peak = observed_vram_gb(model)
        desired = peak if peak is not None else 0
        if int(getattr(j, "gpu_mem_gb", 0) or 0) == desired:
            continue
        fresh = store.read_job("queue", j.job_id)
        if fresh is None:
            continue  # claimed/moved since tick start
        if int(getattr(fresh, "gpu_mem_gb", 0) or 0) == desired:
            continue
        fresh.gpu_mem_gb = desired
        store.write_job("queue", fresh)
        corrected += 1
    if corrected:
        log_fn(
            f"sizing: normalized {corrected} queue jobs "
            f"(unmeasured->0 / measured->peak); stale-agent clobber corrected"
        )
    return corrected
