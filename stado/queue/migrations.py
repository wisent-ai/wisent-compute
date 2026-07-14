"""Idempotent priority-marker backfill for pre-0.4.26 queued jobs.

The priority-marker index introduced in 0.4.26 is auto-populated by
JobStorage.write_job() at submit time. Jobs already sitting in queue/
when 0.4.26 deployed have no marker and would remain stranded behind
the FIFO listing window because the scheduler only loads the oldest N
queue/ blobs by GCS time_created.

This module fixes that automatically: list_jobs_priority_first calls
backfill_priority_markers() before doing its priority-listing pass.
The backfill is resumable via a queue_priority/.migration.json sentinel
that records (cursor, done). Each call processes BACKFILL_BATCH blobs
to fit comfortably inside a Cloud Function 60s tick; subsequent calls
resume past the recorded cursor. Once cursor reaches the end of queue/,
sentinel.done=True and every future call returns immediately.
"""
from __future__ import annotations

import json
from bisect import bisect_right
from concurrent.futures import ThreadPoolExecutor

from ..models import Job

_SENTINEL_PATH = "queue_priority/.migration.json"
BACKFILL_BATCH = 500
_DOWNLOAD_WORKERS = 10


def _read_sentinel(store) -> dict:
    raw = store._download_text(_SENTINEL_PATH)
    if not raw:
        return {"cursor": "", "done": False}
    return json.loads(raw)


def _write_sentinel(store, *, cursor: str, done: bool) -> None:
    store._upload_text(_SENTINEL_PATH,
                       json.dumps({"cursor": cursor, "done": done}))


def _existing_marker_job_ids(store) -> set[str]:
    """All job_ids that already have a priority marker. Each marker name
    ends in '-{job_id}.json'."""
    out: set[str] = set()
    for path in store._list_paths("queue_priority/"):
        if not path.endswith(".json"):
            continue
        # path layout: queue_priority/{inv}-{ts}-{job_id}.json (skip sentinel)
        name = path.rsplit("/", 1)[-1]
        if name.startswith("."):
            continue
        out.add(name.rsplit("-", 1)[-1].removesuffix(".json"))
    return out


def backfill_priority_markers(store, *, batch: int = BACKFILL_BATCH) -> bool:
    """Scan queue/ and write missing markers for priority>0 jobs. Returns
    True iff migration is complete (sentinel.done set). Idempotent: safe
    to call repeatedly. Each call processes at most `batch` queue/ blobs."""
    state = _read_sentinel(store)
    if state.get("done"):
        return True
    paths = sorted(p for p in store._list_paths("queue/") if p.endswith(".json"))
    cursor = state.get("cursor", "") or ""
    if cursor:
        paths = paths[bisect_right(paths, cursor):]
    if not paths:
        _write_sentinel(store, cursor="", done=True)
        return True
    chunk = paths[:batch]
    have = _existing_marker_job_ids(store)
    with ThreadPoolExecutor(max_workers=_DOWNLOAD_WORKERS) as pool:
        bodies = list(pool.map(store._download_text, chunk))
    for body in bodies:
        if not body:
            continue
        job = Job.from_json(body)
        if int(getattr(job, "priority", 0) or 0) <= 0:
            continue
        if job.job_id in have:
            continue
        store.write_priority_marker(job)
    new_cursor = chunk[-1]
    is_done = len(chunk) < batch
    _write_sentinel(store, cursor=new_cursor, done=is_done)
    return is_done
