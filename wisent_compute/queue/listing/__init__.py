"""Job-listing helpers extracted from queue/storage.py.

Holds the priority-marker index code (queue_priority/<inv_prio>-<ts>-<jid>.json
markers so name-ascending sort = priority-desc + FIFO), the priority-aware
fitting-jobs listing, and the parallel bulk-fetch listing. Functions take
JobStorage as the first argument; JobStorage retains thin delegate methods.

Extraction split keeps storage.py under the 300-line cap, which is what
unblocked removing the previously-protected broad `except Exception: pass`
blocks in the per-blob JSON decode and Job.from_json paths. The new code
strict-raises on corrupt blobs so the operator sees the failure.
"""
from __future__ import annotations

import json
from concurrent.futures import ThreadPoolExecutor

from ...models import Job

MAX_PRIORITY_SORT_KEY = 99999999
QUEUE_PREFIX = "queue"
QUEUE_PRIORITY_PREFIX = "queue_priority/"
MARKER_SCAN_MIN_CHUNK = 50
MARKER_SCAN_MULTIPLIER = 20
MARKER_DOWNLOAD_WORKERS = 10
DEFAULT_FITTING_CAP = 4000
FITTING_DOWNLOAD_WORKERS = 32
LIST_DOWNLOAD_WORKERS = 10


def _download_or_none(store, path: str) -> str | None:
    try:
        return store._download_text(path)
    except Exception:
        return None


def priority_key(job: Job) -> str:
    """Sortable name component: lower = higher real priority + older."""
    prio = max(0, min(MAX_PRIORITY_SORT_KEY, int(getattr(job, "priority", 0) or 0)))
    inv = MAX_PRIORITY_SORT_KEY - prio
    ts = ""
    ca = getattr(job, "created_at", None)
    if ca is not None:
        ts = ca.isoformat() if hasattr(ca, "isoformat") else str(ca)
    return f"{inv:08d}-{ts}"


def write_marker(store, job: Job) -> None:
    """Index entry for priority>0 jobs."""
    name = f"{QUEUE_PRIORITY_PREFIX}{priority_key(job)}-{job.job_id}.json"
    body = json.dumps({"job_id": job.job_id, "priority": int(job.priority)})
    store._upload_text(name, body)


def delete_marker(store, job_id: str) -> None:
    """Remove any priority marker(s) for this job_id."""
    suffix = f"-{job_id}.json"
    for path in store._list_paths(QUEUE_PRIORITY_PREFIX):
        if path.endswith(suffix):
            store._delete_blob(path)


def list_top_n(store, prefix: str, *, top_n: int) -> list[Job]:
    """Fetch the top_n highest-priority jobs from prefix/ via the
    queue_priority/ index. Markers sort ascending by name = (inv_priority,
    created_at), so the first top_n give priority-desc + FIFO."""
    if top_n <= 0:
        return []
    marker_paths = sorted(
        p for p in store._list_paths(QUEUE_PRIORITY_PREFIX) if p.endswith(".json")
    )
    if not marker_paths:
        return []

    # Stale priority markers are expected: a job can move out of queue/ after
    # its marker was written, and older versions only deleted markers on the
    # queue -> running path. Do not let stale top markers consume the whole
    # top_n budget, or high-priority fresh jobs disappear behind dead markers.
    # Keep the scan bounded because agents call this in their polling loop.
    out: list[Job] = []
    chunk = max(MARKER_SCAN_MIN_CHUNK, top_n)
    max_scan = min(len(marker_paths), max(top_n * MARKER_SCAN_MULTIPLIER, top_n))
    for i in range(0, max_scan, chunk):
        paths = marker_paths[i:min(i + chunk, max_scan)]
        with ThreadPoolExecutor(max_workers=min(MARKER_DOWNLOAD_WORKERS, len(paths))) as pool:
            bodies = list(pool.map(lambda p: _download_or_none(store, p), paths))
        job_ids: list[tuple[str, str]] = []
        for path, body in zip(paths, bodies):
            if not body:
                continue
            jid = json.loads(body).get("job_id")
            if jid:
                job_ids.append((path, jid))
        if not job_ids:
            continue

        job_paths = [f"{prefix}/{jid}.json" for _, jid in job_ids]
        with ThreadPoolExecutor(max_workers=min(MARKER_DOWNLOAD_WORKERS, len(job_paths))) as pool:
            blobs = list(pool.map(lambda p: _download_or_none(store, p), job_paths))
        for (marker_path, _jid), data in zip(job_ids, blobs):
            if data:
                out.append(Job.from_json(data))
                if len(out) >= top_n:
                    return out
    return out


def list_priority_first(store, prefix: str, *, cap: int) -> list[Job]:
    """Priority markers first, then FIFO oldest_first. Deduped by job_id.
    Calls migrations.backfill_priority_markers up-front so any pre-0.4.26
    queued jobs get retroactive markers."""
    from .. import migrations as _mig
    _mig.backfill_priority_markers(store)
    pri = list_top_n(store, prefix, top_n=cap)
    fifo = list_jobs(store, prefix, oldest_first=cap)
    seen: set[str] = set()
    out: list[Job] = []
    for j in (pri + fifo):
        if j.job_id in seen:
            continue
        seen.add(j.job_id)
        out.append(j)
    return out


def list_fitting(store, prefix: str, *, max_gpu_mem_gb: int, cap: int = DEFAULT_FITTING_CAP) -> list[Job]:
    """Priority-aware fitting jobs: queue_priority/ markers first, then FIFO
    from queue/. Metadata stamping filters non-fitting blobs before download."""
    if store._azure_backend is None and store._sdk_bucket is None:
        # gsutil-only mode: no metadata reads available, so download all.
        jobs = list_jobs(store, prefix, oldest_first=cap)
        return [j for j in jobs if int(getattr(j, "gpu_mem_gb", 0) or 0) <= max_gpu_mem_gb]
    out: list[Job] = []
    seen: set[str] = set()
    if prefix == QUEUE_PREFIX:
        for j in list_top_n(store, prefix, top_n=cap):
            if int(getattr(j, "gpu_mem_gb", 0) or 0) <= max_gpu_mem_gb and j.job_id not in seen:
                out.append(j)
                seen.add(j.job_id)
    eligible_paths: list[str] = []
    for blob in store.list_blobs_with_meta(f"{prefix}/"):
        jid = blob.name.split("/")[-1].removesuffix(".json")
        if not blob.name.endswith(".json") or jid in seen:
            continue
        md = blob.metadata
        mem_str = md.get("gpu_mem_gb")
        # mem_str is None on blobs that predate the metadata stamp -> treat
        # as eligible. A corrupt int now raises ValueError so the operator
        # sees that GCS metadata is misbehaving.
        if mem_str is None or int(mem_str) <= max_gpu_mem_gb:
            eligible_paths.append(blob.name)
        if len(eligible_paths) + len(out) >= cap:
            break

    def _read(path: str) -> Job | None:
        txt = store._download_text(path)
        return Job.from_json(txt) if txt else None

    with ThreadPoolExecutor(max_workers=FITTING_DOWNLOAD_WORKERS) as ex:
        for j in ex.map(_read, eligible_paths):
            if j is not None and int(getattr(j, "gpu_mem_gb", 0) or 0) <= max_gpu_mem_gb:
                out.append(j)
    return out


def list_jobs(store, prefix: str, *, oldest_first: int = 0) -> list[Job]:
    """Parallel-fetch job JSONs under prefix/ via a thread pool.

    oldest_first>0 caps to that many blobs picked by GCS time_created —
    required for queue/ where 14k+ blobs would otherwise force the
    scheduler to download every JSON before slicing per_tick_cap and
    blow the 60s function timeout.
    """
    paths = [
        p for p in store._list_paths(f"{prefix}/", oldest_first=oldest_first)
        if p.endswith(".json")
    ]
    if not paths:
        return []
    workers = min(LIST_DOWNLOAD_WORKERS, max(1, len(paths)))
    with ThreadPoolExecutor(max_workers=workers) as pool:
        blobs = list(pool.map(store._download_text, paths))
    jobs: list[Job] = []
    for data in blobs:
        if data:
            jobs.append(Job.from_json(data))
    return jobs
