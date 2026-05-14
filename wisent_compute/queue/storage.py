"""GCS storage operations for job queue.

Uses gsutil subprocess for CLI (gcloud auth), Python SDK for Cloud Function (ADC).
Job-listing logic lives in queue/listing/ (priority markers, fitting,
priority-then-FIFO). JobStorage methods on this class are thin delegates.
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
        listing for hot prefixes (queue/ has 14k+ blobs)."""
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
            blob = self._sdk_bucket.blob(blob_path); blob.reload()
            blob.metadata = {**(blob.metadata or {}), **meta}
            blob.patch()
        if prefix == "queue" and int(getattr(job, "priority", 0) or 0) > 0:
            self.write_priority_marker(job)

    def read_job(self, prefix: str, job_id: str) -> Job | None:
        data = self._download_text(f"{prefix}/{job_id}.json")
        return Job.from_json(data) if data else None

    def update_priority(self, job_id: str, prefix: str, new_priority: int) -> bool:
        """Rewrite a queued job priority in-place."""
        job = self.read_job(prefix, job_id)
        if job is None:
            return False
        job.priority = int(new_priority)
        self.write_job(prefix, job)
        return True

    def delete_job(self, prefix: str, job_id: str):
        self._delete_blob(f"{prefix}/{job_id}.json")
        if prefix == "queue":
            self.delete_priority_marker(job_id)

    def move_job(self, job: Job, from_prefix: str, to_prefix: str):
        self.write_job(to_prefix, job)
        self._delete_blob(f"{from_prefix}/{job.job_id}.json")
        if from_prefix == "queue":
            self.delete_priority_marker(job.job_id)

    # ---- delegates to queue/listing/ ----

    def write_priority_marker(self, job: Job):
        from .listing import write_marker
        write_marker(self, job)

    def delete_priority_marker(self, job_id: str):
        from .listing import delete_marker
        delete_marker(self, job_id)

    def list_priority_jobs(self, prefix: str, *, top_n: int) -> list[Job]:
        from .listing import list_top_n
        return list_top_n(self, prefix, top_n=top_n)

    def list_jobs_priority_first(self, prefix: str, *, cap: int) -> list[Job]:
        from .listing import list_priority_first
        return list_priority_first(self, prefix, cap=cap)

    def list_jobs_fitting(self, prefix: str, *, max_gpu_mem_gb: int, cap: int = 4000) -> list[Job]:
        from .listing import list_fitting
        return list_fitting(self, prefix, max_gpu_mem_gb=max_gpu_mem_gb, cap=cap)

    def list_jobs(self, prefix: str, *, oldest_first: int = 0) -> list[Job]:
        from .listing import list_jobs as _list_jobs
        return _list_jobs(self, prefix, oldest_first=oldest_first)

    # ---- end delegates ----

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
