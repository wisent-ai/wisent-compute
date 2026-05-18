"""Fleet-learned per-model GPU sizing.

Replaces the hand-maintained per-model VRAM constant table with a value
derived from the fleet's own MEASURED history: for a model, the maximum
ACTUAL peak_vram_gb (sampled by the agent from nvidia-smi per-process
during the run, written onto the Job at completion) across that model's
successful completions. A size at or above every actual peak the fleet
has survived for a model is, by construction, sufficient — no human
picks the number, and it self-corrects: a size that starts OOMing puts
those jobs in failed/ not completed/, so the learned high-water mark
only ever reflects sizes that actually ran to completion.

max (not mean/mode) because the number must not under-provision the
tail: one run that legitimately needed the peak must still be sized for.
The earlier declared-gpu_mem_gb signal was circular (it just read back
whatever the heuristic declared); peak_vram_gb is the measured truth.

Models with fewer than _MIN_OBSERVATIONS measured completions return
None so config.estimate_gpu_memory uses its params heuristic — a
never-seen model has no measured size yet and one sample is noise.

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

# Confidence threshold (global, not per-model, so it adds no per-model
# hardcoding): below this many measured completions a model's observed
# size is not yet trustworthy.
_MIN_OBSERVATIONS = 20

_COMPLETED_SAMPLE_CAP = 6000
_TTL_S = 600
_cache: dict = {"map": None, "built_at": 0.0}


def _model_of(command: str) -> str:
    m = _MODEL_RE.search(command or "")
    return m.group(1).strip("'\"") if m else ""


def _build_observed_map() -> dict[str, int]:
    """model -> max measured peak_vram_gb over its successful completions,
    keeping only models with >= _MIN_OBSERVATIONS measured samples.

    A coordinator-side GCS list/read outage must not silently degrade
    every model to the params heuristic fleet-wide, so a hard failure
    here propagates; the caller's cache keeps the last good map until a
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

    return {
        model: max(samples)
        for model, samples in peaks.items()
        if len(samples) >= _MIN_OBSERVATIONS
    }


def observed_vram_gb(model: str) -> int | None:
    """Fleet-learned VRAM size for model from MEASURED peaks, or None if
    not enough measured completions to trust a value yet."""
    now = time.time()
    if _cache["map"] is None or now - _cache["built_at"] > _TTL_S:
        _cache["map"] = _build_observed_map()
        _cache["built_at"] = now
    return _cache["map"].get(model)
