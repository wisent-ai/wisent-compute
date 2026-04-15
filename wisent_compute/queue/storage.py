"""GCS storage operations for job queue.

Uses gsutil subprocess for CLI (gcloud auth), Python SDK for Cloud Function (ADC).
"""
from __future__ import annotations

import json
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


def _gsutil_cp(src: str, dst: str):
    subprocess.run(["gsutil", "cp", src, dst], capture_output=True)


def _gsutil_cat(path: str) -> str | None:
    r = subprocess.run(["gsutil", "cat", path], capture_output=True, text=True)
    return r.stdout if r.returncode == 0 else None


def _gsutil_ls(prefix: str) -> list[str]:
    r = subprocess.run(["gsutil", "ls", prefix], capture_output=True, text=True)
    return [l.strip() for l in r.stdout.splitlines() if l.strip()] if r.returncode == 0 else []


def _gsutil_rm(path: str):
    subprocess.run(["gsutil", "rm", path], capture_output=True)


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

    def write_job(self, prefix: str, job: Job):
        path = f"{self.gs}/{prefix}/{job.job_id}.json"
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            f.write(job.to_json())
            tmp = f.name
        _gsutil_cp(tmp, path)
        Path(tmp).unlink(missing_ok=True)

    def read_job(self, prefix: str, job_id: str) -> Job | None:
        data = _gsutil_cat(f"{self.gs}/{prefix}/{job_id}.json")
        return Job.from_json(data) if data else None

    def delete_job(self, prefix: str, job_id: str):
        _gsutil_rm(f"{self.gs}/{prefix}/{job_id}.json")

    def move_job(self, job: Job, from_prefix: str, to_prefix: str):
        self.write_job(to_prefix, job)
        _gsutil_rm(f"{self.gs}/{from_prefix}/{job.job_id}.json")

    def list_jobs(self, prefix: str) -> list[Job]:
        paths = _gsutil_ls(f"{self.gs}/{prefix}/")
        jobs = []
        for p in paths:
            if p.endswith(".json"):
                data = _gsutil_cat(p)
                if data:
                    jobs.append(Job.from_json(data))
        return jobs

    def upload_script(self, job_id: str, content: str):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".sh", delete=False) as f:
            f.write(content)
            tmp = f.name
        _gsutil_cp(tmp, f"{self.gs}/scripts/{job_id}.sh")
        Path(tmp).unlink(missing_ok=True)

    def download_script(self, job_id: str) -> str:
        data = _gsutil_cat(f"{self.gs}/scripts/{job_id}.sh")
        return data or ""

    def read_status(self, job_id: str) -> str | None:
        data = _gsutil_cat(f"{self.gs}/status/{job_id}/status")
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
            _gsutil_rm(f"{self.gs}/status/{job_id}/{suffix}")

    def list_all_jobs(self) -> dict[str, list[Job]]:
        result = {}
        for prefix in ("queue", "running", "completed", "failed"):
            result[prefix] = self.list_jobs(prefix)
        return result
