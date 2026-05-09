"""GCS storage operations for job queue.

Uses gsutil subprocess for CLI (gcloud auth), Python SDK for Cloud Function (ADC).
"""
from __future__ import annotations

import json
import os
import subprocess
import tempfile
from datetime import datetime, timezone
from pathlib import Path

from ..models import Job

_USE_SDK = None


def _has_adc() -> bool:
    """Check if Application Default Credentials are available."""
    global _USE_SDK
    if _USE_SDK is not None:
        return _USE_SDK
    try:
        from google.cloud import storage as _s
        _s.Client()
        _USE_SDK = True
    except Exception as _adc_exc:
        import sys as _sys
        _sys.stderr.write(f"[has_adc] SDK Client() failed: {_adc_exc!r}\n")
        _sys.stderr.flush()
        _USE_SDK = False
    return _USE_SDK


def _find_gsutil() -> str:
    """Find gsutil binary, checking common install locations."""
    import shutil
    found = shutil.which("gsutil")
    if found:
        return found
    for path in [
        os.path.expanduser("~/google-cloud-sdk/bin/gsutil"),
        "/opt/google-cloud-sdk/bin/gsutil",
        "/usr/lib/google-cloud-sdk/bin/gsutil",
        "/snap/google-cloud-cli/current/bin/gsutil",
    ]:
        if os.path.isfile(path):
            return path
    return "gsutil"


_GSUTIL = _find_gsutil()


def _gsutil_cp(src: str, dst: str):
    subprocess.run([_GSUTIL, "cp", src, dst], capture_output=True)


def _gsutil_cat(path: str) -> str | None:
    r = subprocess.run([_GSUTIL, "cat", path], capture_output=True, text=True)
    return r.stdout if r.returncode == 0 else None


def _gsutil_ls(prefix: str) -> list[str]:
    r = subprocess.run([_GSUTIL, "ls", prefix], capture_output=True, text=True)
    return [l.strip() for l in r.stdout.splitlines() if l.strip()] if r.returncode == 0 else []


def _gsutil_rm(path: str):
    subprocess.run([_GSUTIL, "rm", path], capture_output=True)


class JobStorage:
    def __init__(self, bucket_name: str):
        self.bucket_name = bucket_name
        self.gs = f"gs://{bucket_name}"
        self._sdk_bucket = None
        self._azure_backend = None

        # Storage backend selector. Default "gcs" preserves every existing
        # call site (gs:// path, _sdk_bucket, gsutil fallback). When set to
        # "azure" the same JobStorage routes uploads/downloads/listings
        # through AzureBlobBackend; the gs URI is left dangling but no GCS
        # call is ever made from this instance.
        from ..config import (
            WC_AZURE_CONTAINER,
            WC_AZURE_STORAGE_ACCOUNT,
            WC_STORAGE_BACKEND,
        )
        if WC_STORAGE_BACKEND == "azure":
            from .azure_blob import AzureBlobBackend
            self._azure_backend = AzureBlobBackend(
                WC_AZURE_STORAGE_ACCOUNT, WC_AZURE_CONTAINER,
            )
        elif _has_adc():
            from google.cloud import storage
            client = storage.Client()
            self._sdk_bucket = client.bucket(bucket_name)

    @property
    def bucket(self):
        # Returned only for the GCS path; Azure-backed JobStorage exposes
        # bucket=None and callers go through the abstract methods below.
        return self._sdk_bucket

    @property
    def backend_name(self) -> str:
        """'azure' or 'gcs' — useful for log lines and conditional branches."""
        return "azure" if self._azure_backend is not None else "gcs"

    # Internal helpers — three execution paths:
    #   1. Azure backend (WC_STORAGE_BACKEND=azure): all I/O via Azure SDK.
    #   2. GCS SDK (ADC available, default Cloud Function path).
    #   3. gsutil subprocess (CLI usage without ADC).
    def _upload_text(self, blob_path: str, content: str):
        if self._azure_backend is not None:
            self._azure_backend.upload_text(blob_path, content)
            return
        if self._sdk_bucket:
            self._sdk_bucket.blob(blob_path).upload_from_string(content)
            return
        with tempfile.NamedTemporaryFile(mode="w", suffix=".tmp", delete=False) as f:
            f.write(content)
            tmp = f.name
        _gsutil_cp(tmp, f"{self.gs}/{blob_path}")
        Path(tmp).unlink(missing_ok=True)

    def _download_text(self, blob_path: str) -> str | None:
        if self._azure_backend is not None:
            return self._azure_backend.download_text(blob_path)
        if self._sdk_bucket:
            blob = self._sdk_bucket.blob(blob_path)
            if not blob.exists():
                return None
            return blob.download_as_text()
        return _gsutil_cat(f"{self.gs}/{blob_path}")

    def _delete_blob(self, blob_path: str):
        if self._azure_backend is not None:
            self._azure_backend.delete(blob_path)
            return
        if self._sdk_bucket:
            blob = self._sdk_bucket.blob(blob_path)
            if blob.exists():
                blob.delete()
            return
        _gsutil_rm(f"{self.gs}/{blob_path}")

    def _list_paths(self, prefix: str, *, oldest_first: int = 0) -> list[str]:
        """Return blob names under `prefix`. When oldest_first>0, return only
        that many names sorted by creation time ascending — bounded
        listing for hot prefixes (queue/ has 14k+ blobs, fetching all of
        them every Cloud Function tick blew the 60s timeout)."""
        if self._azure_backend is not None:
            return self._azure_backend.list_paths(prefix, oldest_first=oldest_first)
        if self._sdk_bucket:
            blobs = list(self._sdk_bucket.list_blobs(prefix=prefix))
            if oldest_first > 0:
                blobs.sort(key=lambda b: b.time_created)
                blobs = blobs[:oldest_first]
            return [b.name for b in blobs]
        return [
            p.removeprefix(self.gs + "/")
            for p in _gsutil_ls(f"{self.gs}/{prefix}")
        ]

    def list_blobs_with_meta(self, prefix: str):
        """Iterate (name, updated_ts, metadata, download_text(), delete()) for
        every blob under prefix. Used by capacity.py and list_jobs_fitting
        to filter on metadata before downloading the full body."""
        if self._azure_backend is not None:
            yield from self._azure_backend.list_blobs_with_meta(prefix)
            return
        if self._sdk_bucket is None:
            return
        from .azure_blob import BlobInfo
        for blob in self._sdk_bucket.list_blobs(prefix=prefix):
            # Capture per-iteration so the closure doesn't see a mutating var.
            blob_obj = blob
            yield BlobInfo(
                name=blob_obj.name,
                updated=blob_obj.updated,
                metadata=dict(blob_obj.metadata or {}),
                _download=self._download_text,
                _delete=lambda name, b=blob_obj: b.delete(),
            )

    def write_job(self, prefix: str, job: Job):
        blob_path = f"{prefix}/{job.job_id}.json"
        self._upload_text(blob_path, job.to_json())
        meta = {
            "gpu_mem_gb": str(int(getattr(job, "gpu_mem_gb", 0) or 0)),
            "priority": str(int(getattr(job, "priority", 0) or 0)),
        }
        if self._azure_backend is not None:
            self._azure_backend.set_metadata(blob_path, meta)
        elif self._sdk_bucket:
            try:
                blob = self._sdk_bucket.blob(blob_path); blob.reload()
                blob.metadata = {**(blob.metadata or {}), **meta}
                blob.patch()
            except Exception:
                pass
        if prefix == "queue" and int(getattr(job, "priority", 0) or 0) > 0:
            self.write_priority_marker(job)

    def read_job(self, prefix: str, job_id: str) -> Job | None:
        data = self._download_text(f"{prefix}/{job_id}.json")
        return Job.from_json(data) if data else None

    def delete_job(self, prefix: str, job_id: str):
        self._delete_blob(f"{prefix}/{job_id}.json")
        if prefix == "queue":
            self.delete_priority_marker(job_id)

    def move_job(self, job: Job, from_prefix: str, to_prefix: str):
        self.write_job(to_prefix, job)
        self._delete_blob(f"{from_prefix}/{job.job_id}.json")
        if from_prefix == "queue":
            self.delete_priority_marker(job.job_id)

    def _priority_key(self, job: Job) -> str:
        """Sortable name component: lower = higher real priority + older."""
        prio = max(0, min(99999999, int(getattr(job, "priority", 0) or 0)))
        inv = 99999999 - prio
        ts = ""
        ca = getattr(job, "created_at", None)
        if ca is not None:
            ts = ca.isoformat() if hasattr(ca, "isoformat") else str(ca)
        return f"{inv:08d}-{ts}"

    def write_priority_marker(self, job: Job):
        """Index entry for priority>0 jobs. The blob NAME encodes
        (inv_priority, created_at) so name-ascending sort = priority-desc
        then FIFO. Body holds {job_id, priority} for unambiguous resolve."""
        name = f"queue_priority/{self._priority_key(job)}-{job.job_id}.json"
        body = json.dumps({"job_id": job.job_id, "priority": int(job.priority)})
        self._upload_text(name, body)

    def delete_priority_marker(self, job_id: str):
        """Remove any priority marker(s) for this job_id. Safe no-op if absent."""
        suffix = f"-{job_id}.json"
        for path in self._list_paths("queue_priority/"):
            if path.endswith(suffix):
                self._delete_blob(path)

    def list_priority_jobs(self, prefix: str, *, top_n: int) -> list[Job]:
        """Fetch the top_n highest-priority jobs from prefix/ via the
        queue_priority/ index. Markers sort ascending by name = (inv_priority,
        created_at), so the first top_n give priority-desc + FIFO."""
        if top_n <= 0:
            return []
        from concurrent.futures import ThreadPoolExecutor
        marker_paths = sorted(
            p for p in self._list_paths("queue_priority/") if p.endswith(".json")
        )[:top_n]
        if not marker_paths:
            return []
        workers = min(10, max(1, len(marker_paths)))
        with ThreadPoolExecutor(max_workers=workers) as pool:
            bodies = list(pool.map(self._download_text, marker_paths))
        job_ids = []
        for b in bodies:
            if not b:
                continue
            try:
                jid = json.loads(b).get("job_id")
            except Exception:
                continue
            if jid:
                job_ids.append(jid)
        if not job_ids:
            return []
        job_paths = [f"{prefix}/{j}.json" for j in job_ids]
        with ThreadPoolExecutor(max_workers=min(10, len(job_paths))) as pool:
            blobs = list(pool.map(self._download_text, job_paths))
        out = []
        for data in blobs:
            if data:
                try:
                    out.append(Job.from_json(data))
                except Exception:
                    pass
        return out

    def list_jobs_priority_first(self, prefix: str, *, cap: int) -> list[Job]:
        """Combined listing: priority markers first, then FIFO oldest_first
        over the same prefix. Deduped by job_id. The whole point of this
        method is that high-priority jobs jump the FIFO listing window.
        Calls migrations.backfill_priority_markers up-front so any pre-0.4.26
        queued jobs get retroactive markers without manual intervention."""
        from . import migrations as _mig
        _mig.backfill_priority_markers(self)
        pri = self.list_priority_jobs(prefix, top_n=cap)
        fifo = self.list_jobs(prefix, oldest_first=cap)
        seen: set[str] = set()
        out: list[Job] = []
        for j in (pri + fifo):
            if j.job_id in seen:
                continue
            seen.add(j.job_id)
            out.append(j)
        return out

    def list_jobs_fitting(self, prefix: str, *, max_gpu_mem_gb: int, cap: int = 4000) -> list[Job]:
        """Priority-aware fitting jobs: queue_priority/ markers first, then FIFO
        from queue/. Metadata stamping filters non-fitting blobs before download."""
        # gsutil-only mode (no SDK on either side) can't read metadata, so
        # fall back to downloading all jobs and filtering in Python.
        if self._azure_backend is None and self._sdk_bucket is None:
            jobs = self.list_jobs(prefix, oldest_first=cap)
            return [j for j in jobs if int(getattr(j, "gpu_mem_gb", 0) or 0) <= max_gpu_mem_gb]
        out: list[Job] = []
        seen: set[str] = set()
        if prefix == "queue":
            for j in self.list_priority_jobs(prefix, top_n=cap):
                if int(getattr(j, "gpu_mem_gb", 0) or 0) <= max_gpu_mem_gb and j.job_id not in seen:
                    out.append(j); seen.add(j.job_id)
        eligible_paths: list[str] = []
        for blob in self.list_blobs_with_meta(f"{prefix}/"):
            jid = blob.name.split("/")[-1].removesuffix(".json")
            if not blob.name.endswith(".json") or jid in seen:
                continue
            md = blob.metadata
            mem_str = md.get("gpu_mem_gb")
            try:
                if mem_str is None or int(mem_str) <= max_gpu_mem_gb:
                    eligible_paths.append(blob.name)
            except ValueError:
                eligible_paths.append(blob.name)
            if len(eligible_paths) + len(out) >= cap: break
        from concurrent.futures import ThreadPoolExecutor
        def _read(path):
            try: txt = self._download_text(path); return Job.from_json(txt) if txt else None
            except Exception: return None
        with ThreadPoolExecutor(max_workers=32) as ex:
            for j in ex.map(_read, eligible_paths):
                if j is not None and int(getattr(j, "gpu_mem_gb", 0) or 0) <= max_gpu_mem_gb:
                    out.append(j)
        return out

    def list_jobs(self, prefix: str, *, oldest_first: int = 0) -> list[Job]:
        """Parallel-fetch job JSONs under prefix/ via a thread pool.

        oldest_first>0 caps to that many blobs picked by GCS time_created
        — required for queue/ where 14k+ blobs would otherwise force the
        scheduler to download every JSON before slicing per_tick_cap and
        blow the 60s function timeout. Threads parallelise the I/O-bound
        downloads. Workers scale with path count up to the GCS client's
        connection pool size to avoid 'pool full' churn.
        """
        from concurrent.futures import ThreadPoolExecutor
        paths = [
            p for p in self._list_paths(f"{prefix}/", oldest_first=oldest_first)
            if p.endswith(".json")
        ]
        if not paths:
            return []
        workers = min(10, max(1, len(paths)))
        with ThreadPoolExecutor(max_workers=workers) as pool:
            blobs = list(pool.map(self._download_text, paths))
        jobs = []
        for data in blobs:
            if data:
                jobs.append(Job.from_json(data))
        return jobs

    def upload_script(self, job_id: str, content: str):
        self._upload_text(f"scripts/{job_id}.sh", content)

    def download_script(self, job_id: str) -> str:
        return self._download_text(f"scripts/{job_id}.sh") or ""

    def read_status(self, job_id: str) -> str | None:
        data = self._download_text(f"status/{job_id}/status")
        return data.strip().split()[0] if data else None

    def heartbeat_stale(self, job_id: str, threshold_minutes: int) -> bool:
        path = f"status/{job_id}/heartbeat"
        if self._azure_backend is not None:
            updated = self._azure_backend.updated_at(path)
            if updated is None:
                return False
            age = (datetime.now(timezone.utc) - updated).total_seconds() / 60
            return age > threshold_minutes
        if self._sdk_bucket:
            blob = self._sdk_bucket.blob(path)
            if not blob.exists():
                return False
            blob.reload()
            if blob.updated is None:
                return False
            age = (datetime.now(timezone.utc) - blob.updated).total_seconds() / 60
            return age > threshold_minutes
        return False

    def cleanup_status(self, job_id: str):
        for suffix in ("status", "heartbeat"):
            self._delete_blob(f"status/{job_id}/{suffix}")

    def list_all_jobs(self) -> dict[str, list[Job]]:
        result = {}
        for prefix in ("queue", "running", "completed", "failed"):
            result[prefix] = self.list_jobs(prefix)
        return result
