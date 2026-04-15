"""Job submission: build job JSON, render startup script, upload to GCS."""
from __future__ import annotations

import os
import re
from pathlib import Path

from ..models import Job, JobState
from ..config import estimate_gpu_memory, lookup_instance_type, BUCKET
from .storage import JobStorage


TEMPLATE_DIR = Path(__file__).parent.parent / "templates"


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
) -> Job:
    """Submit a job to the queue. Returns the Job object with ID."""
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
    )

    store = JobStorage(bucket)
    store.upload_script(job_id, script)
    store.write_job("queue", job)

    return job
