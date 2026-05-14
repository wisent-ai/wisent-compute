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


def priority_key(job: Job) -> str:
    """Sortable name component: lower = higher real priority + older."""
    prio = max(0, min(99999999, int(getattr(job, "priority", 0) or 0)))
    inv = 99999999 - prio
    ts = ""
    ca = getattr(job, "created_at", None)
    if ca is not None:
        ts = ca.isoformat() if hasattr(ca, "isoformat") else str(ca)
    return f"{inv:08d}-{ts}"


def write_marker(store, job: Job) -> None:
    """Index entry for priority>0 jobs."""
    name = f"queue_priority/{priority_key(job)}-{job.job_id}.json"
    body = json.dumps({"job_id": job.job_id, "priority": int(job.priority)})
    store._upload_text(name, body)


def delete_marker(store, job_id: str) -> None:
    """Remove any priority marker(s) for this job_id."""
    suffix = f"-{job_id}.json"
    for path in store._list_paths("queue_priority/"):
        if path.endswith(suffix):
            store._delete_blob(path)


def list_top_n(store, prefix: str, *, top_n: int) -> list[Job]:
    """Fetch the top_n highest-priority jobs from prefix/ via the
    queue_priority/ index. Markers sort ascending by name = (inv_priority,
    created_at), so the first top_n give priority-desc + FIFO."""
    if top_n <= 0:
        return []
    marker_paths = sorted(
        p for p in store._list_paths("queue_priority/") if p.endswith(".json")
    )[:top_n]
    if not marker_paths:
        return []
    workers = min(10, max(1, len(marker_paths)))
    with ThreadPoolExecutor(max_workers=workers) as pool:
        bodies = list(pool.map(store._download_text, marker_paths))
    job_ids: list[str] = []
    for b in bodies:
        if not b:
            continue
        jid = json.loads(b).get("job_id")
        if jid:
            job_ids.append(jid)
    if not job_ids:
        return []
    job_paths = [f"{prefix}/{j}.json" for j in job_ids]
    with ThreadPoolExecutor(max_workers=min(10, len(job_paths))) as pool:
        blobs = list(pool.map(store._download_text, job_paths))
    out: list[Job] = []
    for data in blobs:
        if data:
            out.append(Job.from_json(data))
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


def list_fitting(store, prefix: str, *, max_gpu_mem_gb: int, cap: int = 4000) -> list[Job]:
    """Priority-aware fitting jobs: queue_priority/ markers first, then FIFO
    from queue/. Metadata stamping filters non-fitting blobs before download."""
    if store._azure_backend is None and store._sdk_bucket is None:
        # gsutil-only mode: no metadata reads available, so download all.
        jobs = list_jobs(store, prefix, oldest_first=cap)
        return [j for j in jobs if int(getattr(j, "gpu_mem_gb", 0) or 0) <= max_gpu_mem_gb]
    out: list[Job] = []
    seen: set[str] = set()
    if prefix == "queue":
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

    with ThreadPoolExecutor(max_workers=32) as ex:
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
    workers = min(10, max(1, len(paths)))
    with ThreadPoolExecutor(max_workers=workers) as pool:
        blobs = list(pool.map(store._download_text, paths))
    jobs: list[Job] = []
    for data in blobs:
        if data:
            jobs.append(Job.from_json(data))
    return jobs
