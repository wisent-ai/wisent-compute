"""GCS storage operations for job queue.

Uses gsutil subprocess for CLI (gcloud auth), Python SDK for Cloud Function (ADC).
Job-listing logic lives in queue/listing/ (priority markers, fitting,
priority-then-FIFO). JobStorage methods on this class are thin delegates.
"""
from __future__ import annotations

import json
import os
import re
import subprocess
import tempfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from ..models import Job

_USE_SDK = None


@dataclass(frozen=True)
class VersionedText:
    content: str
    version: str


class StorageConflict(RuntimeError):
    """A conditional write lost a race with another writer."""


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
    # Raise on failure rather than swallowing it: a silent gsutil cp failure
    # used to leave callers believing a blob was written when it wasn't.
    r = subprocess.run([_GSUTIL, "cp", src, dst], capture_output=True)
    if r.returncode:
        raise RuntimeError(
            f"gsutil cp failed rc={r.returncode}: "
            f"{r.stderr.decode(errors='replace').strip()}"
        )



def _gsutil_cat_bytes(path: str) -> bytes | None:
    result = subprocess.run([_GSUTIL, "cat", path], capture_output=True)
    return result.stdout if result.returncode == 0 else None
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
            from requests.adapters import HTTPAdapter
            client = storage.Client()
            # Default urllib3 pool is 10 connections, which throttles every
            # parallel scan (history rebuild fans out 32 downloads, queue
            # listing fans out 32, dispatcher tick spawns concurrent
            # reads). Without a larger pool, the Cloud Function spent
            # ~6.5 minutes on a single history rebuild because 22 of 32
            # threads were waiting on connection-pool tickets, blowing
            # past the 540s tick timeout. Mount adapters with pool_maxsize
            # matched to the largest fan-out so all concurrent fetches
            # actually proceed in parallel.
            adapter = HTTPAdapter(pool_connections=32, pool_maxsize=64)
            client._http.mount("https://", adapter)
            client._http.mount("http://", adapter)
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
            from google.api_core.exceptions import NotFound
            blob = self._sdk_bucket.blob(blob_path)
            try:
                blob.reload()
            except NotFound:
                return None
            return blob.download_as_text()
        return _gsutil_cat(f"{self.gs}/{blob_path}")

    def read_bytes(self, blob_path: str) -> bytes | None:
        if self._azure_backend is not None:
            return self._azure_backend.download_bytes(blob_path)
        if self._sdk_bucket:
            blob = self._sdk_bucket.blob(blob_path)
            if not blob.exists():
                return None
            return blob.download_as_bytes()
        return _gsutil_cat_bytes(f"{self.gs}/{blob_path}")

    def download_blob(self, blob_path: str, filename: str) -> bool:
        """Download one blob to filename, returning False when it is absent."""
        if self._azure_backend is not None:
            return self._azure_backend.download_to_filename(blob_path, filename)
        if self._sdk_bucket:
            blob = self._sdk_bucket.blob(blob_path)
            if not blob.exists():
                return False
            blob.download_to_filename(filename)
            return True
        result = subprocess.run(
            [_GSUTIL, "cp", f"{self.gs}/{blob_path}", filename],
            capture_output=True,
        )
        if result.returncode:
            return False
        return True

    def create_text_if_absent(self, blob_path: str, content: str) -> bool:
        """Atomically create a text blob; return False if it already exists."""
        if self._azure_backend is not None:
            return self._azure_backend.upload_text_if_absent(blob_path, content)
        if self._sdk_bucket:
            from google.api_core.exceptions import PreconditionFailed
            try:
                self._sdk_bucket.blob(blob_path).upload_from_string(
                    content, if_generation_match=0,
                )
                return True
            except PreconditionFailed:
                return False
        with tempfile.NamedTemporaryFile(mode="w", suffix=".tmp", delete=False) as file:
            file.write(content)
            tmp = file.name
        try:
            result = subprocess.run(
                [_GSUTIL, "cp", "-n", tmp, f"{self.gs}/{blob_path}"],
                capture_output=True,
            )
            if result.returncode:
                raise RuntimeError(
                    "gsutil conditional create failed: "
                    + result.stderr.decode(errors="replace").strip()
                )
            output = result.stdout + result.stderr
            return b"Skipping existing item" not in output
        finally:
            Path(tmp).unlink(missing_ok=True)

    def read_text_versioned(self, blob_path: str) -> VersionedText | None:
        """Read text together with the backend generation/ETag used for CAS."""
        if self._azure_backend is not None:
            value = self._azure_backend.download_text_versioned(blob_path)
            return VersionedText(*value) if value is not None else None
        if self._sdk_bucket:
            from google.api_core.exceptions import PreconditionFailed

            blob = self._sdk_bucket.blob(blob_path)
            for attempt in range(3):
                if not blob.exists():
                    return None
                blob.reload()
                generation = str(blob.generation)
                try:
                    content = blob.download_as_text(
                        if_generation_match=blob.generation,
                    )
                    return VersionedText(content, generation)
                except PreconditionFailed:
                    if attempt == 2:
                        raise
            raise RuntimeError("unreachable versioned GCS read")

        uri = f"{self.gs}/{blob_path}"
        stat = subprocess.run(
            [_GSUTIL, "stat", uri],
            capture_output=True,
            text=True,
        )
        if stat.returncode:
            return None
        match = re.search(r"^\s*Generation:\s*(\d+)\s*$", stat.stdout, re.MULTILINE)
        if match is None:
            raise RuntimeError(f"gsutil stat did not report a generation for {uri}")
        content = _gsutil_cat(uri)
        if content is None:
            raise RuntimeError(f"gsutil could not read versioned object {uri}")
        return VersionedText(content, match.group(1))

    def compare_and_swap_text(
        self,
        blob_path: str,
        expected_version: str,
        content: str,
    ) -> str | None:
        """Replace text iff generation/ETag matches; return a known new version."""
        if not expected_version:
            raise ValueError("expected_version is required for compare-and-swap")
        try:
            if self._azure_backend is not None:
                return self._azure_backend.compare_and_swap_text(
                    blob_path, expected_version, content,
                )
            if self._sdk_bucket:
                blob = self._sdk_bucket.blob(blob_path)
                blob.upload_from_string(
                    content,
                    if_generation_match=int(expected_version),
                )
                # google-cloud-storage updates the Blob properties from the
                # successful upload response. Do not reload: a later writer
                # could otherwise make us return its generation.
                return str(blob.generation) if blob.generation is not None else None
        except Exception as exc:
            if type(exc).__name__ in {
                "PreconditionFailed",
                "ResourceModifiedError",
                "ResourceNotFoundError",
            }:
                raise StorageConflict(f"{blob_path} changed concurrently") from None
            raise

        uri = f"{self.gs}/{blob_path}"
        with tempfile.NamedTemporaryFile(mode="w", suffix=".tmp", delete=False) as file:
            file.write(content)
            tmp = file.name
        try:
            result = subprocess.run(
                [
                    _GSUTIL,
                    "-h",
                    f"x-goog-if-generation-match:{expected_version}",
                    "cp",
                    tmp,
                    uri,
                ],
                capture_output=True,
                text=True,
            )
            if result.returncode:
                detail = (result.stderr or result.stdout).strip()
                if "412" in detail or "Precondition" in detail:
                    raise StorageConflict(f"{blob_path} changed concurrently")
                raise RuntimeError(
                    f"gsutil compare-and-swap failed rc={result.returncode}: {detail}"
                )
            # gsutil does not expose the generation created by `cp`. Returning
            # a subsequent `stat` generation would be unsafe if another writer
            # won a race immediately after this write. One-shot callers (for
            # example alias updates) must reload before attempting another CAS.
            return None
        finally:
            Path(tmp).unlink(missing_ok=True)

    def upload_file_if_absent(self, blob_path: str, filename: str) -> bool:
        """Atomically upload a local file; return False if the blob exists."""
        if self._azure_backend is not None:
            return self._azure_backend.upload_file_if_absent(blob_path, filename)
        if self._sdk_bucket:
            from google.api_core.exceptions import PreconditionFailed
            try:
                self._sdk_bucket.blob(blob_path).upload_from_filename(
                    filename, if_generation_match=0,
                )
                return True
            except PreconditionFailed:
                return False
        result = subprocess.run(
            [_GSUTIL, "cp", "-n", filename, f"{self.gs}/{blob_path}"],
            capture_output=True,
        )
        if result.returncode:
            raise RuntimeError(
                "gsutil conditional upload failed: "
                + result.stderr.decode(errors="replace").strip()
            )
        output = result.stdout + result.stderr
        return b"Skipping existing item" not in output

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
            "gpu_type": str(getattr(job, "gpu_type", "") or ""),
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
        # Live re-submission tracking: write a fixed/ or failed_again/
        # tombstone when this move terminates a re-submitted job. Never
        # raises into the caller (the agent loop) — see tracking.tombstone.
        from .tracking import on_transition
        on_transition(self, job, to_prefix)

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
        for prefix in ("queue", "running", "completed", "uploaded", "failed"):
            result[prefix] = self.list_jobs(prefix)
        return result
