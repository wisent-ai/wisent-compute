"""Bounded command, prompt, and artifact output helpers."""
from __future__ import annotations

import base64
import json
import shlex
from pathlib import PurePosixPath
from typing import Any, Callable

from ....models import Job
from ....providers.box import BoxProvider, BoxTransportError
from ....providers.local.helpers.execution import build_job_command, verify_command
from ....queue.leases import ProviderLease

LOG_BYTES = int("57344")
ARTIFACT_BYTES = int("16777216")
ARTIFACT_COUNT = int("16")
EVENT_PAGES = int("10")
EVENT_LIMIT = int("100")


def runtime_paths(job_id: str) -> dict[str, str]:
    root = f".stado/{job_id}"
    return {
        "root": root, "script": f"{root}/run.sh", "stdout": f"{root}/stdout.log",
        "stderr": f"{root}/stderr.log", "exit": f"{root}/exit_code",
        "pid": f"{root}/pid", "launch": f"{root}/launch_intent",
    }


def command_wrapper(job: Job, paths: dict[str, str]) -> str:
    command = shlex.quote(build_job_command(job))
    verification = verify_command(job)
    stdout = shlex.quote(paths["stdout"])
    stderr = shlex.quote(paths["stderr"])
    artifact_file = f"{paths['root']}/artifacts.json"
    artifact_payload = base64.b64encode(
        json.dumps(
            getattr(job, "resolved_input_artifacts", {}) or {},
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).decode("ascii")
    lines = [
        "#!/bin/bash",
        "set +e",
        "umask 077",
        f"mkdir -p {shlex.quote(paths['root'])}",
        f"printf '%s' {shlex.quote(artifact_payload)} | base64 --decode > "
        f"{shlex.quote(artifact_file)}",
        f"export WC_ARTIFACT_INPUTS_FILE={shlex.quote(artifact_file)}",
        f"export WC_ARTIFACT_INPUTS_JSON=\"$(cat {shlex.quote(artifact_file)})\"",
        f"bash -lc {command} > >(tail -c {LOG_BYTES} >{stdout}) "
        f"2> >(tail -c {LOG_BYTES} >{stderr})",
        "rc=$?",
        "wait",
    ]
    if verification:
        lines.extend([
            "if [ \"$rc\" -eq 0 ]; then",
            f"  bash -lc {shlex.quote(verification)} "
            f"> >(tail -c {LOG_BYTES} >>{stdout}) 2> >(tail -c {LOG_BYTES} >>{stderr})",
            "  rc=$?", "  wait", "fi",
        ])
    for key in ("stdout", "stderr"):
        path = shlex.quote(paths[key])
        lines.append(
            f"test ! -f {path} || (tail -c {LOG_BYTES} {path} >{path}.tmp && mv {path}.tmp {path})"
        )
    lines.extend([f"printf '%s' \"$rc\" >{shlex.quote(paths['exit'])}", "exit \"$rc\""])
    return "\n".join(lines) + "\n"


def file_content(value: dict[str, Any]) -> str:
    nested = value.get("file") if isinstance(value.get("file"), dict) else value
    content = nested.get("content")
    if not isinstance(content, str):
        raise BoxTransportError("Box file response omitted string content")
    return content


def recover_prompt_id(provider: BoxProvider, lease: ProviderLease, marker: str,
                      keepalive: Callable[[], None]) -> str:
    cursor = ""
    for _unused in range(EVENT_PAGES):
        page = provider.client.list_events(
            lease.provider_resource_id, cursor=cursor, limit=EVENT_LIMIT,
            sort="asc", event_type="prompt",
        )
        keepalive()
        for event in page.events:
            data = event.get("data") if isinstance(event.get("data"), dict) else {}
            if event.get("type") == "prompt" and str(data.get("prompt") or "").startswith(marker):
                return str(event.get("taskId") or event.get("id") or "")
        if not page.has_more or not page.next_cursor:
            break
        cursor = page.next_cursor
    return ""


def prompt_output(provider: BoxProvider, lease: ProviderLease,
                  keepalive: Callable[[], None]) -> str:
    cursor = ""
    parts: list[str] = []
    size = int()
    for _unused in range(EVENT_PAGES):
        page = provider.client.list_events(
            lease.provider_resource_id, cursor=cursor, limit=EVENT_LIMIT,
            sort="asc", event_type="response",
        )
        keepalive()
        for event in page.events:
            data = event.get("data") if isinstance(event.get("data"), dict) else {}
            content = data.get("content")
            if (event.get("type") != "response" or event.get("taskId") != lease.prompt_id
                    or data.get("is_streaming") or data.get("is_reverted")
                    or not isinstance(content, str)):
                continue
            encoded = content.encode("utf-8")
            remaining = LOG_BYTES - size
            if remaining <= int():
                return "\n".join(parts)
            parts.append(encoded[:remaining].decode("utf-8", errors="replace"))
            size += min(len(encoded), remaining)
        if not page.has_more or not page.next_cursor:
            break
        cursor = page.next_cursor
    return "\n".join(parts)


def _safe_artifact_path(value: object) -> str:
    path = str(value).strip()
    pure = PurePosixPath(path)
    if not path or pure.is_absolute() or ".." in pure.parts:
        raise ValueError("Box artifact path must be relative and contained")
    return path


def upload_artifacts(store: Any, provider: BoxProvider, job: Job,
                     lease: ProviderLease, keepalive: Callable[[], None]) -> None:
    if len(job.artifact_paths) > ARTIFACT_COUNT:
        raise ValueError("too many Box artifacts requested")
    remaining = ARTIFACT_BYTES
    for source in job.artifact_paths:
        keepalive()
        path = _safe_artifact_path(source)
        if remaining <= int():
            raise ValueError("Box artifact aggregate byte bound exceeded")
        content = provider.client.download_artifact(lease.provider_resource_id, path, remaining)
        remaining -= len(content)
        destination = f"status/{job.job_id}/output/artifacts/{path.replace('/', '_')}"
        azure = getattr(store, "_azure_backend", None)
        bucket = getattr(store, "_sdk_bucket", None)
        if azure is not None:
            azure._container.upload_blob(name=destination, data=content, overwrite=True)
        elif bucket is not None:
            bucket.blob(destination).upload_from_string(content)
        else:
            raise RuntimeError("Box artifact collection requires an SDK storage backend")
        keepalive()
