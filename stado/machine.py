"""Stable JSON automation facade for Stado job lifecycle operations."""
from __future__ import annotations

import hashlib
import json
import os
import re
import tarfile
import stat
import tempfile
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from typing import Any, Callable

import click

from .config import BUCKET
from .models import Job, JobState
from .queue.storage import JobStorage
from .queue.submit import submit_job

SCHEMA_VERSION = 1
JOB_PREFIXES = ("queue", "running", "completed", "uploaded", "failed", "cancelled")
TERMINAL_STATES = {
    JobState.COMPLETED.value,
    JobState.UPLOADED.value,
    JobState.FAILED.value,
    JobState.CANCELLED.value,
}
REQUEST_FIELDS = {
    "client_request_id",
    "command",
    "provider",
    "gpu_type",
    "vram_gb",
    "max_cost_per_hour_usd",
    "pin_to_provider",
    "priority",
    "repo",
    "repo_workdir",
    "repo_extras",
    "pre_command",
    "apt_packages",
    "output_uri",
    "verify_command",
    "exclusive",
    "source_archive_path",
}
_REQUEST_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
_APT_PACKAGE_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9+._:-]*$")
MAX_SOURCE_ARCHIVE_BYTES = 512 * 1024 * 1024
MAX_SOURCE_EXTRACTED_BYTES = 2 * 1024 * 1024 * 1024
MAX_SOURCE_MEMBERS = 100_000


class MachineError(Exception):
    def __init__(self, code: str, message: str, *, retryable: bool = False):
        super().__init__(message)
        self.code = code
        self.message = message
        self.retryable = retryable


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


def normalize_job(job: Job) -> dict[str, Any]:
    state = "queued" if job.state == "queue" else job.state
    return {
        "job_id": job.job_id,
        "run_id": job.run_id,
        "batch_id": job.batch_id,
        "state": state,
        "command": job.command,
        "provider": job.provider,
        "gpu_type": job.gpu_type,
        "gpu_mem_gb": job.gpu_mem_gb,
        "machine_type": job.machine_type,
        "created_at": job.created_at,
        "started_at": job.started_at,
        "completed_at": job.completed_at,
        "failed_at": job.failed_at,
        "error": job.error,
        "output_uri": job.output_uri,
    }


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _request_digest(request: dict[str, Any]) -> str:
    return hashlib.sha256(_canonical_json(request).encode("utf-8")).hexdigest()


def _validate_source_archive(value: Any) -> tuple[Path, str] | None:
    if value in (None, ""):
        return None
    if not isinstance(value, str):
        raise MachineError("INVALID_SOURCE_ARCHIVE", "source_archive_path must be a string")
    path = Path(value).expanduser()
    try:
        info = path.lstat()
    except OSError as exc:
        raise MachineError("INVALID_SOURCE_ARCHIVE", f"source archive is not readable: {exc}") from exc
    if stat.S_ISLNK(info.st_mode) or not stat.S_ISREG(info.st_mode):
        raise MachineError("INVALID_SOURCE_ARCHIVE", "source archive must be a regular non-symlink file")
    if info.st_size <= 0 or info.st_size > MAX_SOURCE_ARCHIVE_BYTES:
        raise MachineError(
            "INVALID_SOURCE_ARCHIVE",
            f"source archive must be between 1 and {MAX_SOURCE_ARCHIVE_BYTES} bytes",
        )
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    try:
        with tarfile.open(path, mode="r:gz") as archive:
            total_size = 0
            seen: set[str] = set()
            for index, member in enumerate(archive):
                if index >= MAX_SOURCE_MEMBERS:
                    raise MachineError("INVALID_SOURCE_ARCHIVE", "source archive has too many entries")
                name = member.name
                pure = PurePosixPath(name)
                if (
                    not name or "\\" in name or pure.is_absolute()
                    or ".." in pure.parts or any(part in {"", "."} for part in pure.parts)
                ):
                    raise MachineError("INVALID_SOURCE_ARCHIVE", f"unsafe archive entry: {name!r}")
                if name in seen:
                    raise MachineError("INVALID_SOURCE_ARCHIVE", f"duplicate archive entry: {name!r}")
                seen.add(name)
                if not (member.isdir() or member.isreg()):
                    raise MachineError("INVALID_SOURCE_ARCHIVE", f"non-regular archive entry: {name!r}")
                if member.isreg():
                    total_size += member.size
                    if total_size > MAX_SOURCE_EXTRACTED_BYTES:
                        raise MachineError("INVALID_SOURCE_ARCHIVE", "source archive expands beyond the safety limit")
    except (tarfile.TarError, OSError) as exc:
        raise MachineError("INVALID_SOURCE_ARCHIVE", f"invalid tar.gz archive: {exc}") from exc
    return path, digest.hexdigest()


def _validate_request(request: Any) -> dict[str, Any]:
    if not isinstance(request, dict):
        raise MachineError("INVALID_REQUEST", "request file must contain one JSON object")
    unknown = sorted(set(request) - REQUEST_FIELDS)
    if unknown:
        raise MachineError("INVALID_REQUEST", f"unknown request field(s): {', '.join(unknown)}")
    missing = [name for name in ("client_request_id", "command") if name not in request]
    if missing:
        raise MachineError("INVALID_REQUEST", f"missing required field(s): {', '.join(missing)}")

    request_id = request["client_request_id"]
    command = request["command"]
    if not isinstance(request_id, str) or not _REQUEST_ID_RE.fullmatch(request_id):
        raise MachineError(
            "INVALID_REQUEST",
            "client_request_id must be 1-128 path-safe ASCII characters",
        )
    if not isinstance(command, str) or not command.strip():
        raise MachineError("INVALID_REQUEST", "command must be a non-empty string")

    defaults: dict[str, Any] = {
        "provider": "gcp",
        "gpu_type": "",
        "vram_gb": 0,
        "max_cost_per_hour_usd": 0.0,
        "pin_to_provider": False,
        "priority": 0,
        "repo": "",
        "repo_workdir": "",
        "repo_extras": "train",
        "pre_command": "",
        "apt_packages": [],
        "output_uri": "",
        "verify_command": "",
        "exclusive": False,
        "source_archive_path": "",
    }
    normalized = {**defaults, **request}
    for name in (
        "provider", "gpu_type", "repo", "repo_workdir", "repo_extras",
        "pre_command", "output_uri", "verify_command", "source_archive_path",
    ):
        if not isinstance(normalized[name], str):
            raise MachineError("INVALID_REQUEST", f"{name} must be a string")
    for name in ("vram_gb", "priority"):
        if isinstance(normalized[name], bool) or not isinstance(normalized[name], int):
            raise MachineError("INVALID_REQUEST", f"{name} must be an integer")
    if normalized["vram_gb"] < 0:
        raise MachineError("INVALID_REQUEST", "vram_gb must not be negative")
    cost = normalized["max_cost_per_hour_usd"]
    if isinstance(cost, bool) or not isinstance(cost, (int, float)) or cost < 0:
        raise MachineError("INVALID_REQUEST", "max_cost_per_hour_usd must be non-negative")
    normalized["max_cost_per_hour_usd"] = float(cost)
    for name in ("pin_to_provider", "exclusive"):
        if not isinstance(normalized[name], bool):
            raise MachineError("INVALID_REQUEST", f"{name} must be a boolean")
    packages = normalized["apt_packages"]
    if (
        not isinstance(packages, list)
        or any(not isinstance(item, str) or not _APT_PACKAGE_RE.fullmatch(item) for item in packages)
    ):
        raise MachineError("INVALID_REQUEST", "apt_packages must contain only valid apt package names")
    return normalized


class MachineFacade:
    def __init__(
        self,
        store: JobStorage | None = None,
        submitter: Callable[..., Job] = submit_job,
    ):
        self.store = store or JobStorage(BUCKET)
        self.submitter = submitter

    def lookup_job(self, job_id: str) -> Job:
        if not job_id or "/" in job_id or "\\" in job_id:
            raise MachineError("NOT_FOUND", f"job {job_id!r} was not found")
        for prefix in JOB_PREFIXES:
            job = self.store.read_job(prefix, job_id)
            if job is not None:
                job.state = "queued" if prefix == "queue" else prefix
                return job
        raise MachineError("NOT_FOUND", f"job {job_id!r} was not found")

    def submit_request(self, request: dict[str, Any]) -> dict[str, Any]:
        request = _validate_request(request)
        request_id = request["client_request_id"]
        if os.environ.get("COMPUTE_API_KEY", "").strip() and self.submitter is submit_job:
            raise MachineError(
                "UNSUPPORTED_BACKEND",
                "machine submission requires direct durable storage; the legacy compute API drops lifecycle fields",
            )
        archive = _validate_source_archive(request.get("source_archive_path"))
        source_uri = ""
        source_sha = ""
        digest_request = dict(request)
        if archive is not None:
            if self.store.backend_name != "gcs":
                raise MachineError(
                    "SOURCE_ARCHIVE_UNSUPPORTED",
                    "source archive bootstrap currently requires the GCS storage backend",
                )
            archive_path, source_sha = archive
            source_blob = f"machine_inputs/{request_id}/{source_sha}.tar.gz"
            source_uri = f"gs://{self.store.bucket_name}/{source_blob}"
            try:
                self.store.upload_file_if_absent(source_blob, str(archive_path))
            except Exception as exc:
                raise MachineError("SOURCE_UPLOAD_FAILED", str(exc), retryable=True) from exc
            digest_request["source_archive_path"] = source_sha

        record_path = f"machine_requests/{request_id}.json"
        digest = _request_digest(digest_request)
        reservation = {
            "schema_version": SCHEMA_VERSION,
            "client_request_id": request_id,
            "request_digest": digest,
            "state": "submitting",
            "created_at": _utcnow(),
        }
        if source_uri:
            reservation["source_archive_uri"] = source_uri
            reservation["source_sha256"] = source_sha
        created = self.store.create_text_if_absent(record_path, _canonical_json(reservation))
        if not created:
            raw = self.store._download_text(record_path)
            if not raw:
                raise MachineError("REQUEST_IN_PROGRESS", "request reservation is not readable", retryable=True)
            try:
                existing = json.loads(raw)
            except (TypeError, json.JSONDecodeError) as exc:
                raise MachineError("INTERNAL", "stored idempotency record is invalid") from exc
            if existing.get("request_digest") != digest:
                raise MachineError(
                    "IDEMPOTENCY_CONFLICT",
                    "client_request_id was already used with a different request",
                )
            stored_result = existing.get("result")
            if isinstance(stored_result, dict) and isinstance(stored_result.get("job"), dict):
                return stored_result
            stored_job = existing.get("job")
            if isinstance(stored_job, dict):
                result = {"job": stored_job}
                if existing.get("source_archive_uri"):
                    result["source_archive_uri"] = existing["source_archive_uri"]
                    result["source_sha256"] = existing.get("source_sha256", "")
                return result
            raise MachineError("REQUEST_IN_PROGRESS", "matching request is still being submitted", retryable=True)

        kwargs = {
            key: value
            for key, value in request.items()
            if key not in {"client_request_id", "command", "source_archive_path"}
        }
        if source_uri:
            workdir = f"/tmp/stado-machine-source/{request_id}-{source_sha}"
            local_archive = f"/tmp/stado-machine-source/{source_sha}.tar.gz"
            bootstrap = "\n".join((
                "set -e",
                "mkdir -p /tmp/stado-machine-source",
                f"rm -rf {workdir}",
                f"mkdir -p {workdir}",
                f"gsutil cp {source_uri} {local_archive}",
                f"tar --extract --gzip --file={local_archive} --directory={workdir} --no-same-owner --no-same-permissions",
                f"cd {workdir}",
            ))
            caller_pre_command = kwargs.get("pre_command", "")
            kwargs["pre_command"] = bootstrap + ("\n" + caller_pre_command if caller_pre_command else "")
            kwargs["repo"] = ""
            kwargs["repo_workdir"] = ""
            kwargs["repo_extras"] = ""
        try:
            job = self.submitter(request["command"], bucket=self.store.bucket_name, **kwargs)
        except Exception as exc:
            self.store._delete_blob(record_path)
            raise MachineError("SUBMIT_FAILED", str(exc), retryable=True) from exc
        normalized = normalize_job(job)
        result = {"job": normalized}
        if source_uri:
            result["source_archive_uri"] = source_uri
            result["source_sha256"] = source_sha
        completed = {
            **reservation,
            "state": "submitted",
            "job": normalized,
            "result": result,
            "completed_at": _utcnow(),
        }
        try:
            self.store._upload_text(record_path, _canonical_json(completed))
        except Exception as exc:
            raise MachineError(
                "INTERNAL",
                f"job {job.job_id} was submitted but its idempotency record could not be finalized: {exc}",
                retryable=True,
            ) from exc
        return result

    def status(self, job_id: str) -> dict[str, Any]:
        return {"job": normalize_job(self.lookup_job(job_id))}

    def read_logs(self, job_id: str, cursor: int, limit: int) -> dict[str, Any]:
        if cursor < 0:
            raise MachineError("INVALID_CURSOR", "cursor must not be negative")
        if limit <= 0:
            raise MachineError("INVALID_CURSOR", "limit must be positive")
        self.lookup_job(job_id)
        payload = self.store.read_bytes(f"status/{job_id}/output/command_output.log") or b""
        if cursor > len(payload):
            raise MachineError("INVALID_CURSOR", "cursor is beyond the end of the log")
        end = min(len(payload), cursor + limit)
        return {
            "job_id": job_id,
            "cursor": cursor,
            "next_cursor": end,
            "eof": end == len(payload),
            "text": payload[cursor:end].decode("utf-8", errors="replace"),
        }

    def cancel_job(self, job_id: str) -> dict[str, Any]:
        job = self.lookup_job(job_id)
        if job.state in TERMINAL_STATES:
            return {"job": normalize_job(job)}

        marker_path = f"cancellations/{job_id}.json"
        marker = _canonical_json({"job_id": job_id, "requested_at": _utcnow()})
        self.store.create_text_if_absent(marker_path, marker)

        if job.state == JobState.QUEUED.value:
            job.state = JobState.CANCELLED.value
            job.completed_at = _utcnow()
            job.error = "cancelled"
            self.store.write_job("cancelled", job)
            self.store.delete_job("queue", job_id)
            raced = self.store.read_job("running", job_id)
            if raced is None:
                return {"job": normalize_job(job)}
            job = raced

        if job.state == JobState.RUNNING.value:
            if job.instance_ref and not job.instance_ref.startswith("local@"):
                try:
                    from .providers import get_provider
                    get_provider(job.provider).delete_instance(job.instance_ref)
                except Exception as exc:
                    raise MachineError("CANCEL_FAILED", str(exc), retryable=True) from exc
            job.state = JobState.CANCELLED.value
            job.completed_at = _utcnow()
            job.error = "cancelled"
            job.instance_ref = None
            self.store.write_job("cancelled", job)
            self.store.delete_job("running", job_id)
            self.store.delete_job("failed", job_id)
            return {"job": normalize_job(job)}

        raise MachineError("CANCEL_FAILED", f"job {job_id!r} is in an unsupported state", retryable=True)

    def download_artifacts(self, job_id: str, output_dir: str | Path) -> dict[str, Any]:
        job = self.lookup_job(job_id)
        if job.state not in TERMINAL_STATES:
            raise MachineError("NOT_TERMINAL", f"job {job_id!r} is not terminal")
        requested_root = Path(output_dir).expanduser().absolute()
        if requested_root.exists() and requested_root.is_symlink():
            raise MachineError("ARTIFACT_SECURITY", "output directory must not be a symlink")
        temp_root = Path(tempfile.gettempdir()).absolute()
        trusted_system_aliases = {temp_root, *temp_root.parents}
        for component in reversed(requested_root.parents):
            if component.exists() and component.is_symlink() and component not in trusted_system_aliases:
                raise MachineError("ARTIFACT_SECURITY", "output path must not contain symlinks")
        if requested_root.exists() and not requested_root.is_dir():
            raise MachineError("ARTIFACT_SECURITY", "output directory path is not a directory")
        requested_root.mkdir(parents=True, exist_ok=True)
        root = requested_root.resolve()
        prefix = f"status/{job_id}/output/"
        paths = sorted(path for path in self.store._list_paths(prefix) if path != prefix)
        if not paths:
            raise MachineError("NO_ARTIFACTS", f"job {job_id!r} has no canonical output artifacts")

        artifacts: list[dict[str, Any]] = []
        for blob_path in paths:
            if not blob_path.startswith(prefix):
                raise MachineError("ARTIFACT_SECURITY", "storage returned an artifact outside the job output prefix")
            relative = blob_path[len(prefix):]
            pure = PurePosixPath(relative)
            if not relative or "\\" in relative or pure.is_absolute() or ".." in pure.parts or any(part in {"", "."} for part in pure.parts):
                raise MachineError("ARTIFACT_SECURITY", f"unsafe artifact path: {relative!r}")
            destination = root.joinpath(*pure.parts)
            current = root
            for part in pure.parts[:-1]:
                current = current / part
                if current.exists() and (current.is_symlink() or not current.is_dir()):
                    raise MachineError("ARTIFACT_SECURITY", f"unsafe output path component: {part!r}")
                current.mkdir(exist_ok=True)
            if destination.exists() and (destination.is_symlink() or not destination.is_file()):
                raise MachineError("ARTIFACT_SECURITY", f"unsafe artifact destination: {relative!r}")
            with tempfile.NamedTemporaryFile(
                dir=destination.parent, prefix=".stado-", suffix=".download", delete=False,
            ) as temporary_file:
                temporary = Path(temporary_file.name)
            try:
                downloaded = self.store.download_blob(blob_path, str(temporary))
                if not downloaded:
                    raise MachineError("NO_ARTIFACTS", f"artifact disappeared while downloading: {relative}", retryable=True)
                digest = hashlib.sha256()
                size = 0
                with temporary.open("rb") as stream:
                    for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                        size += len(chunk)
                        digest.update(chunk)
                os.replace(temporary, destination)
            finally:
                temporary.unlink(missing_ok=True)
            mode = destination.lstat().st_mode
            if not stat.S_ISREG(mode):
                raise MachineError("ARTIFACT_SECURITY", f"downloaded artifact is not a regular file: {relative!r}")
            artifacts.append({
                "relative_path": pure.as_posix(),
                "size_bytes": size,
                "sha256": digest.hexdigest(),
            })
        if not artifacts:
            raise MachineError("NO_ARTIFACTS", f"job {job_id!r} has no canonical output artifacts")
        return {"job_id": job_id, "output_dir": str(root), "artifacts": artifacts}


def lookup_job(store: JobStorage, job_id: str) -> Job:
    return MachineFacade(store=store).lookup_job(job_id)


def _emit(payload: dict[str, Any]) -> None:
    click.echo(_canonical_json(payload))


def _invoke(operation: Callable[[], dict[str, Any]]) -> None:
    try:
        result = operation()
        _emit({"schema_version": SCHEMA_VERSION, "ok": True, "result": result})
    except MachineError as exc:
        _emit({
            "schema_version": SCHEMA_VERSION,
            "ok": False,
            "error": {"code": exc.code, "message": exc.message, "retryable": exc.retryable},
        })
        raise click.exceptions.Exit(1)
    except Exception as exc:
        _emit({
            "schema_version": SCHEMA_VERSION,
            "ok": False,
            "error": {"code": "INTERNAL", "message": str(exc), "retryable": False},
        })
        raise click.exceptions.Exit(1)


@click.group()
def machine() -> None:
    """Stable JSON machine interface."""


@machine.command("submit")
@click.option("--request-file", type=click.Path(dir_okay=False, path_type=Path), required=True)
def machine_submit(request_file: Path) -> None:
    """Submit one idempotent request from a JSON file."""
    def operation() -> dict[str, Any]:
        try:
            request = json.loads(request_file.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, json.JSONDecodeError) as exc:
            raise MachineError("INVALID_REQUEST", f"cannot read request JSON: {exc}") from exc
        return MachineFacade().submit_request(request)
    _invoke(operation)


@machine.command("status")
@click.argument("job_id")
def machine_status(job_id: str) -> None:
    """Read one job directly by ID."""
    _invoke(lambda: MachineFacade().status(job_id))


@machine.command("logs")
@click.argument("job_id")
@click.option("--cursor", type=int, default=0, show_default=True)
@click.option("--limit", type=int, default=65536, show_default=True)
def machine_logs(job_id: str, cursor: int, limit: int) -> None:
    """Read a byte-cursor page from the canonical command log."""
    _invoke(lambda: MachineFacade().read_logs(job_id, cursor, limit))


@machine.command("cancel")
@click.argument("job_id")
def machine_cancel(job_id: str) -> None:
    """Durably and idempotently cancel one job."""
    _invoke(lambda: MachineFacade().cancel_job(job_id))


@machine.command("artifacts")
@click.argument("job_id")
@click.option("--output-dir", type=click.Path(file_okay=False, path_type=Path), required=True)
def machine_artifacts(job_id: str, output_dir: Path) -> None:
    """Download and verify canonical artifacts for a terminal job."""
    _invoke(lambda: MachineFacade().download_artifacts(job_id, output_dir))
