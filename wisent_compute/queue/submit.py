"""Job submission: via compute.wisent.com API or direct GCS."""
from __future__ import annotations

import json
import os
import re
import urllib.request
import urllib.error
from pathlib import Path

from ..models import Job, JobState
from ..config import estimate_gpu_memory, lookup_instance_type, BUCKET


TEMPLATE_DIR = Path(__file__).parent.parent / "templates"
COMPUTE_API = os.environ.get("COMPUTE_API_URL", "https://compute.wisent.com")


def _generate_job_id() -> str:
    return os.urandom(4).hex()


def _render_template(template_name: str, variables: dict) -> str:
    template_path = TEMPLATE_DIR / template_name
    content = template_path.read_text()
    for key, value in variables.items():
        content = content.replace(f"${{{key}}}", str(value))
    return content


def _render_repo_block(repo: str, workdir: str, extras: str) -> str:
    """Bash that clones repo into $WORK/{workdir} and pip-installs its extras
    so the user's command can `cd {workdir} && python -m foo` directly.
    Returns empty string when no repo was requested."""
    if not repo:
        return ""
    if not workdir:
        # Default workdir = repo basename without .git
        workdir = repo.rstrip("/").rsplit("/", 1)[-1].removesuffix(".git")
    install = ""
    if extras:
        install = f"pip install -e '.[{extras}]'"
    return (
        f"git clone --depth 1 {repo} {workdir}\n"
        f"cd {workdir}\n"
        f"{install}\n"
        f"cd $WORK\n"
    )


def submit_batch(commands: list[str], **kwargs) -> int:
    """Submit many commands concurrently. Returns the count submitted.

    Falls through to per-line submit_job for the actual GCS writes; the
    concurrency just hides GCS round-trip latency. ThreadPoolExecutor is
    correct here: each worker is I/O-bound on GCS, not CPU.
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed
    workers = 1 if len(commands) <= 4 else 64
    if workers == 1:
        for cmd in commands:
            submit_job(cmd, **kwargs)
        return len(commands)
    done = 0
    errors = 0
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = [pool.submit(submit_job, cmd, **kwargs) for cmd in commands]
        for fut in as_completed(futures):
            done += 1
            try:
                fut.result()
            except Exception:
                errors += 1
    return done - errors


def submit_job(
    command: str,
    provider: str = "gcp",
    batch_id: str = "",
    bucket: str = "",
    *,
    preemptible: bool = False,
    max_cost_per_hour_usd: float = 0.0,
    pin_to_provider: bool = False,
    priority: int = 0,
    repo: str = "",
    repo_workdir: str = "",
    repo_extras: str = "train",
) -> Job:
    """Submit a job. Uses compute.wisent.com API if available, GCS otherwise."""
    api_key = os.environ.get("COMPUTE_API_KEY", "").strip()
    if api_key:
        return _submit_via_api(command, api_key, provider)
    return _submit_via_gcs(
        command, provider, batch_id, bucket,
        preemptible=preemptible,
        max_cost_per_hour_usd=max_cost_per_hour_usd,
        pin_to_provider=pin_to_provider,
        priority=priority,
        repo=repo, repo_workdir=repo_workdir, repo_extras=repo_extras,
    )


def _submit_via_api(command: str, api_key: str, provider: str) -> Job:
    """Submit through compute.wisent.com API."""
    gpu_mem = estimate_gpu_memory(command)
    env_vars = {}
    hf_token = os.environ.get("HF_TOKEN", "")
    if hf_token:
        env_vars["HF_TOKEN"] = hf_token
        env_vars["HUGGING_FACE_HUB_TOKEN"] = hf_token

    payload = json.dumps({
        "docker_image": "pytorch/pytorch:2.1.0-cuda12.1-cudnn8-runtime",
        "docker_cmd": command,
        "docker_env": env_vars,
        "disk_gb": 50,
        "ssh_public_key": "",
        "label": f"wc-{_generate_job_id()}",
    }).encode()

    req = urllib.request.Request(
        f"{COMPUTE_API}/api/v1/instances",
        data=payload,
        headers={
            "Content-Type": "application/json",
            "X-API-Key": api_key,
        },
        method="POST",
    )
    try:
        resp = urllib.request.urlopen(req)
        data = json.loads(resp.read())
        return Job(
            job_id=data.get("id", _generate_job_id()),
            command=command,
            gpu_mem_gb=gpu_mem,
            provider=provider,
            state="running",
            instance_ref=data.get("id", ""),
        )
    except urllib.error.HTTPError as e:
        body = e.read().decode()
        raise RuntimeError(f"API error {e.code}: {body}")


def _submit_via_gcs(
    command: str, provider: str, batch_id: str, bucket: str,
    *,
    preemptible: bool = False,
    max_cost_per_hour_usd: float = 0.0,
    pin_to_provider: bool = False,
    priority: int = 0,
    repo: str = "",
    repo_workdir: str = "",
    repo_extras: str = "train",
) -> Job:
    """Submit directly to GCS queue (no API server needed)."""
    from .storage import JobStorage
    bucket = bucket or BUCKET
    job_id = _generate_job_id()
    gpu_mem = estimate_gpu_memory(command)
    machine_type, accel_type = lookup_instance_type(provider, gpu_mem)

    if gpu_mem == 0:
        machine_type = "e2-standard-8"
        accel_type = ""

    hf_token = os.environ.get("HF_TOKEN", "")
    gh_token = os.environ.get("GH_TOKEN", "")

    template = "startup_gpu.sh" if gpu_mem > 0 else "startup_cpu.sh"
    script = _render_template(template, {
        "JOB_ID": job_id,
        "COMMAND": command,
        "HF_TOKEN": hf_token,
        "GH_TOKEN": gh_token,
        "WISENT_VERSION": os.environ.get("WISENT_VERSION", "latest"),
        "REPO_BLOCK": _render_repo_block(repo, repo_workdir, repo_extras),
    })

    submitter = os.environ.get("USER", "") or os.environ.get("LOGNAME", "")
    try:
        host = os.uname().nodename
    except AttributeError:
        host = ""

    job = Job(
        job_id=job_id,
        command=command,
        gpu_mem_gb=gpu_mem,
        gpu_type=accel_type,
        machine_type=machine_type,
        provider=provider,
        batch_id=batch_id,
        state=JobState.QUEUED.value,
        startup_script_uri=f"gs://{bucket}/scripts/{job_id}.sh",
        preemptible=preemptible,
        max_cost_per_hour_usd=max_cost_per_hour_usd,
        pin_to_provider=pin_to_provider,
        priority=priority,
        submitted_by=submitter,
        submitted_from=host,
        submitted_via="cli",
        repo=repo, repo_workdir=repo_workdir, repo_extras=repo_extras,
    )

    store = JobStorage(bucket)
    store.upload_script(job_id, script)
    store.write_job("queue", job)
    return job
