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
        model = _model_of(doc.get("command") or "")
        if not model:
            continue
        peaks.setdefault(model, []).append(peak)

    return {model: max(samples) for model, samples in peaks.items()}


def observed_vram_gb(model: str) -> int | None:
    """Max MEASURED peak_vram_gb for model, or None if the model has no
    measured completion yet (caller must NOT fabricate a number — start
    smallest GPU tier and escalate on OOM)."""
    now = time.time()
    if _cache["map"] is None or now - _cache["built_at"] > _TTL_S:
        _cache["map"] = _build_observed_map()
        _cache["built_at"] = now
    return _cache["map"].get(model)


_OOM_RE = re.compile(
    r"out of memory|OutOfMemoryError|CUDA error: out of memory|"
    r"CUDA_ERROR_OUT_OF_MEMORY|cuBLAS.*alloc|cudaErrorMemoryAllocation",
    re.IGNORECASE,
)


def escalate_on_oom(store, job, error_text: str) -> bool:
    """A job that OOMed while sized at a GPU tier — and has NO measured
    peak yet — is moved up exactly one hardware tier and requeued
    (running -> queue) instead of being failed. Returns True iff it was
    requeued. At the largest tier, or once the model has a measured
    peak, this does nothing and the caller fails the job normally.

    This is the no-hardcode escalation: an unmeasured model starts on
    the smallest GPU (config.estimate_gpu_memory) and climbs the real
    hardware ladder one OOM at a time until it runs; that run's measured
    nvidia-smi peak then sizes every later job of the model.
    """
    if not _OOM_RE.search(error_text or ""):
        return False
    model = _model_of(getattr(job, "command", "") or "")
    if not model:
        return False
    if observed_vram_gb(model) is not None:
        return False  # measured already; a real OOM is a real failure
    from ..models import GPU_SIZING, JobState

    tiers = sorted(GPU_SIZING.get("gcp", {}).keys())
    cur = int(getattr(job, "gpu_mem_gb", 0) or 0)
    bigger = [t for t in tiers if t > cur]
    if not bigger:
        return False  # already at the largest tier — genuine failure
    job.gpu_mem_gb = bigger[0]
    job.state = JobState.QUEUED.value
    job.failed_at = None
    job.error = None
    job.instance_ref = None
    job.started_at = None
    store.move_job(job, "running", "queue")
    store.cleanup_status(job.job_id)
    return True
