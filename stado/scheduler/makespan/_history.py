"""Runtime-history machinery for the makespan matcher, split out of
makespan/__init__.py so that module stays under the 300-line file-size
limit (the guard fired when the capacity-aware assignment guard was
added 2026-05-17). Mean per-(model,task) runtime is rebuilt from
completed/ blobs on a TTL and used to order the queue (LPT) and project
agent finish times. This module has NO dependency on makespan.__init__
so the import is one-directional and cannot cycle.
"""
from __future__ import annotations

import datetime as dt
import json
import re
import time as _time
from typing import Callable

from ...queue.storage import JobStorage

HISTORY_TTL_S = 600
COMPLETED_SAMPLE_CAP = 4000  # don't scan every completed/ blob each refresh

_MODEL_RE = re.compile(r"--model\s+(\S+)")
_TASK_RE = re.compile(r"--task\s+(\S+)")

_history_cache: dict[tuple[str, str], float] = {}
_history_cache_built_at: float = 0.0


def _extract_model_task(command: str) -> tuple[str, str]:
    m = _MODEL_RE.search(command or "")
    t = _TASK_RE.search(command or "")
    model = m.group(1).strip("'\"") if m else ""
    task = t.group(1).strip("'\"") if t else ""
    return model, task


def _build_history(store: JobStorage, log_fn: Callable[[str], None]) -> dict[tuple[str, str], float]:
    """Mean runtime in seconds per (model, task), from completed/ blobs.

    Reads at most COMPLETED_SAMPLE_CAP blobs in parallel via a
    ThreadPoolExecutor. Sequential per-blob downloads at ~50-100ms each
    are too slow for the Cloud Function's 540s timeout when the cap is
    in the thousands; parallelism brings the wall time down to seconds.
    """
    from concurrent.futures import ThreadPoolExecutor
    bucket = getattr(store, "_sdk_bucket", None)
    if bucket is None:
        return {}
    blobs = []
    for blob in bucket.list_blobs(prefix="completed/"):
        blobs.append(blob)
        if len(blobs) >= COMPLETED_SAMPLE_CAP:
            break
    if not blobs:
        return {}

    from google.api_core.exceptions import NotFound

    def _fetch(blob):
        # TOCTOU race: list_blobs returns a name, then move_job (completed
        # -> failed when verify_command rc != 0, or manual cleanup) deletes
        # the blob before we get here. Return None so the loop below skips
        # that entry. Only NotFound is caught -- any other GCS error
        # propagates so the tick fails visibly on a real problem.
        try:
            return blob.download_as_text()
        except NotFound:
            return None

    with ThreadPoolExecutor(max_workers=32) as ex:
        texts = list(ex.map(_fetch, blobs))

    by_key: dict[tuple[str, str], list[float]] = {}
    for text in texts:
        if text is None:
            continue
        doc = json.loads(text)
        st = doc.get("started_at")
        ct = doc.get("completed_at")
        if not st or not ct:
            continue
        elapsed = (
            dt.datetime.fromisoformat(ct.replace("Z", "+00:00"))
            - dt.datetime.fromisoformat(st.replace("Z", "+00:00"))
        ).total_seconds()
        if elapsed <= 0:
            continue
        model, task = _extract_model_task(doc.get("command") or "")
        if not model or not task:
            continue
        by_key.setdefault((model, task), []).append(elapsed)
    out = {k: sum(v) / len(v) for k, v in by_key.items() if v}
    log_fn(f"makespan: history rebuilt from {len(blobs)} completed/ blobs, {len(out)} (model,task) keys")
    return out


def _history(store: JobStorage, log_fn: Callable[[str], None]) -> dict[tuple[str, str], float]:
    global _history_cache, _history_cache_built_at
    if _time.time() - _history_cache_built_at > HISTORY_TTL_S or not _history_cache:
        _history_cache = _build_history(store, log_fn)
        _history_cache_built_at = _time.time()
    return _history_cache
