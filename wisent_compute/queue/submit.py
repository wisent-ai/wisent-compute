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
    )

    store = JobStorage(bucket)
    store.upload_script(job_id, script)
    store.write_job("queue", job)
    return job
