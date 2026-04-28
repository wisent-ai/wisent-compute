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
    except Exception:
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
        if _has_adc():
            from google.cloud import storage
            client = storage.Client()
            self._sdk_bucket = client.bucket(bucket_name)

    @property
    def bucket(self):
        return self._sdk_bucket

    # Internal helpers — use SDK when ADC available (Cloud Function path),
    # fall back to gsutil subprocess for CLI usage.
    def _upload_text(self, blob_path: str, content: str):
        if self._sdk_bucket:
            self._sdk_bucket.blob(blob_path).upload_from_string(content)
            return
        with tempfile.NamedTemporaryFile(mode="w", suffix=".tmp", delete=False) as f:
            f.write(content)
            tmp = f.name
        _gsutil_cp(tmp, f"{self.gs}/{blob_path}")
        Path(tmp).unlink(missing_ok=True)

    def _download_text(self, blob_path: str) -> str | None:
        if self._sdk_bucket:
            blob = self._sdk_bucket.blob(blob_path)
            if not blob.exists():
                return None
            return blob.download_as_text()
        return _gsutil_cat(f"{self.gs}/{blob_path}")

    def _delete_blob(self, blob_path: str):
        if self._sdk_bucket:
            blob = self._sdk_bucket.blob(blob_path)
            if blob.exists():
                blob.delete()
            return
        _gsutil_rm(f"{self.gs}/{blob_path}")

    def _list_paths(self, prefix: str, *, oldest_first: int = 0) -> list[str]:
        """Return blob names under `prefix`. When oldest_first>0, return only
        that many names sorted by GCS time_created ascending — bounded
        listing for hot prefixes (queue/ has 14k+ blobs, fetching all of
        them every Cloud Function tick blew the 60s timeout). The single
        list_blobs call already returns blob metadata so sorting is free."""
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

    def write_job(self, prefix: str, job: Job):
        self._upload_text(f"{prefix}/{job.job_id}.json", job.to_json())

    def read_job(self, prefix: str, job_id: str) -> Job | None:
        data = self._download_text(f"{prefix}/{job_id}.json")
        return Job.from_json(data) if data else None

    def delete_job(self, prefix: str, job_id: str):
        self._delete_blob(f"{prefix}/{job_id}.json")

    def move_job(self, job: Job, from_prefix: str, to_prefix: str):
        self.write_job(to_prefix, job)
        self._delete_blob(f"{from_prefix}/{job.job_id}.json")

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
        if self._sdk_bucket:
            blob = self._sdk_bucket.blob(f"status/{job_id}/heartbeat")
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
