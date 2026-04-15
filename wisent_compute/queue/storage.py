"""GCS storage operations for job queue."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from google.cloud import storage

from ..models import Job


class JobStorage:
    def __init__(self, bucket_name: str):
        self.client = storage.Client()
        self.bucket = self.client.bucket(bucket_name)

    def write_job(self, prefix: str, job: Job):
        blob = self.bucket.blob(f"{prefix}/{job.job_id}.json")
        blob.upload_from_string(job.to_json(), content_type="application/json")

    def read_job(self, prefix: str, job_id: str) -> Job | None:
        blob = self.bucket.blob(f"{prefix}/{job_id}.json")
        if not blob.exists():
            return None
        return Job.from_json(blob.download_as_text())

    def delete_job(self, prefix: str, job_id: str):
        blob = self.bucket.blob(f"{prefix}/{job_id}.json")
        blob.delete(if_exists=True)

    def move_job(self, job: Job, from_prefix: str, to_prefix: str):
        src = self.bucket.blob(f"{from_prefix}/{job.job_id}.json")
        dst = self.bucket.blob(f"{to_prefix}/{job.job_id}.json")
        dst.upload_from_string(job.to_json(), content_type="application/json")
        src.delete()

    def list_jobs(self, prefix: str) -> list[Job]:
        blobs = self.bucket.list_blobs(prefix=f"{prefix}/")
        jobs = []
        for blob in blobs:
            if blob.name.endswith(".json"):
                jobs.append(Job.from_json(blob.download_as_text()))
        return jobs

    def upload_script(self, job_id: str, content: str):
        blob = self.bucket.blob(f"scripts/{job_id}.sh")
        blob.upload_from_string(content)

    def download_script(self, job_id: str) -> str:
        blob = self.bucket.blob(f"scripts/{job_id}.sh")
        return blob.download_as_text()

    def read_status(self, job_id: str) -> str | None:
        blob = self.bucket.blob(f"status/{job_id}/status")
        if not blob.exists():
            return None
        return blob.download_as_text().strip().split()[0]

    def heartbeat_stale(self, job_id: str, threshold_minutes: int) -> bool:
        blob = self.bucket.blob(f"status/{job_id}/heartbeat")
        if not blob.exists():
            return False
        blob.reload()
        if blob.updated is None:
            return False
        age = (datetime.now(timezone.utc) - blob.updated).total_seconds() / 60
        return age > threshold_minutes

    def cleanup_status(self, job_id: str):
        for suffix in ("status", "heartbeat"):
            blob = self.bucket.blob(f"status/{job_id}/{suffix}")
            if blob.exists():
                blob.delete()

    def list_all_jobs(self) -> dict[str, list[Job]]:
        result = {}
        for prefix in ("queue", "running", "completed", "failed"):
            result[prefix] = self.list_jobs(prefix)
        return result
