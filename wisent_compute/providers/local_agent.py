"""Local GPU agent: runs on the workstation, polls GCS queue, respects Vast.ai.

Usage: wc agent --gpu-type nvidia-rtx-4090
Runs as a long-lived daemon. Picks up jobs when Vast.ai has no active renter.
"""
from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

from ..config import BUCKET
from ..models import Job, JobState, GPU_HOURLY_RATE_USD, SPOT_DISCOUNT
from ..queue.storage import JobStorage


def _accel_hourly_rate(accel_type: str, preemptible: bool) -> float:
    """$/hour for one accelerator at the given pricing model.

    Mirrors scheduler._accel_hourly_rate so both consumers apply the same
    cost-cap rule. Local agents are typically free hardware, but any job
    with max_cost_per_hour_usd set still respects that cap — it expresses
    intent ("don't run this on anything pricier than X") regardless of
    which consumer claims it.
    """
    base = GPU_HOURLY_RATE_USD.get(accel_type, 0.0)
    if not preemptible:
        return base
    return base * SPOT_DISCOUNT.get(accel_type, 0.5)

POLL_INTERVAL = 60
HEARTBEAT_INTERVAL = 300
VAST_API = "https://console.vast.ai/api/v0"


def _gsutil_bin() -> str:
    import shutil
    found = shutil.which("gsutil")
    if found:
        return found
    for p in [
        os.path.expanduser("~/google-cloud-sdk/bin/gsutil"),
        "/opt/google-cloud-sdk/bin/gsutil",
    ]:
        if os.path.isfile(p):
            return p
    return "gsutil"


def _log(msg):
    ts = datetime.now().strftime("%H:%M:%S")
    sys.stderr.write(f"[{ts}] [agent] {msg}\n")
    sys.stderr.flush()


def _vast_has_renter() -> bool:
    """Check if any Vast.ai instance is currently rented on this machine."""
    api_key = os.environ.get("VAST_API_KEY", "").strip()
    if not api_key:
        return False
    try:
        req = urllib.request.Request(
            f"{VAST_API}/instances?owner=me",
            headers={"Authorization": f"Bearer {api_key}"},
        )
        resp = urllib.request.urlopen(req)
        instances = json.loads(resp.read())
        return any(i.get("actual_status") == "running" for i in instances.get("instances", []))
    except Exception:
        return False


def _detect_gpu_type() -> str:
    """Detect GPU type from nvidia-smi or Apple Silicon."""
    try:
        r = subprocess.run(
            ["nvidia-smi", "--query-gpu=name", "--format=csv,noheader"],
            capture_output=True, text=True,
        )
        if r.returncode == 0:
            name = r.stdout.strip().split("\n")[0]
            return name.lower().replace(" ", "-").replace("geforce-", "nvidia-")
    except FileNotFoundError:
        pass
    # Check for Apple Silicon MPS
    try:
        r = subprocess.run(
            ["sysctl", "-n", "machdep.cpu.brand_string"],
            capture_output=True, text=True,
        )
        if "Apple" in r.stdout:
            return "apple-mps"
    except Exception:
        pass
    return "cpu"


def _write_heartbeat(store: JobStorage, job_id: str):
    ts = datetime.now(timezone.utc).isoformat()
    with open("/tmp/wc_heartbeat.txt", "w") as f:
        f.write(f"RUNNING {ts}")
    subprocess.run(
        [_gsutil_bin(), "cp", "/tmp/wc_heartbeat.txt", f"gs://{store.bucket_name}/status/{job_id}/heartbeat"],
        capture_output=True,
    )


def _write_status(store: JobStorage, job_id: str, status: str):
    with open("/tmp/wc_status.txt", "w") as f:
        f.write(status)
    subprocess.run(
        [_gsutil_bin(), "cp", "/tmp/wc_status.txt", f"gs://{store.bucket_name}/status/{job_id}/status"],
        capture_output=True,
    )


def _upload_output(store: JobStorage, job_id: str, output_dir: str):
    subprocess.run(
        [_gsutil_bin(), "-m", "cp", "-r", f"{output_dir}/*", f"gs://{store.bucket_name}/status/{job_id}/output/"],
        capture_output=True,
    )


def run_agent(gpu_type: str = ""):
    """Main agent loop. Polls queue, runs jobs when Vast.ai is idle."""
    if not gpu_type:
        gpu_type = _detect_gpu_type()
    _log(f"Agent started. GPU: {gpu_type}")

    store = JobStorage(BUCKET)
    current_proc = None
    current_job = None
    paused = False
    last_heartbeat = 0

    while True:
        now = time.time()

        # If running a job, manage it
        if current_proc is not None:
            # Check if Vast.ai renter appeared
            if not paused and _vast_has_renter():
                _log(f"Renter detected, pausing job {current_job.job_id}")
                os.kill(current_proc.pid, signal.SIGSTOP)
                paused = True

            # Check if renter left
            if paused and not _vast_has_renter():
                _log(f"Renter gone, resuming job {current_job.job_id}")
                os.kill(current_proc.pid, signal.SIGCONT)
                paused = False

            # Check if process finished
            ret = current_proc.poll()
            if ret is not None:
                status = "COMPLETED" if ret == 0 else f"FAILED exit={ret}"
                _write_status(store, current_job.job_id, status)
                output_dir = f"/tmp/wc-{current_job.job_id}/output"
                if Path(output_dir).exists():
                    _upload_output(store, current_job.job_id, output_dir)

                state = JobState.COMPLETED if ret == 0 else JobState.FAILED
                current_job.state = state.value
                ts = datetime.now(timezone.utc).isoformat()
                if ret == 0:
                    current_job.completed_at = ts
                else:
                    current_job.failed_at = ts
                store.move_job(current_job, "running", state.value)
                _log(f"Job {current_job.job_id} {state.value}")
                current_proc = None
                current_job = None
                paused = False
                continue

            # Heartbeat
            if not paused and now - last_heartbeat > HEARTBEAT_INTERVAL:
                _write_heartbeat(store, current_job.job_id)
                last_heartbeat = now

            time.sleep(10)
            continue

        # No job running — check if we can pick one up
        if _vast_has_renter():
            _log("Vast renter active, waiting...")
            time.sleep(POLL_INTERVAL)
            continue

        # Look for a queued job matching our GPU
        _log("Polling queue...")
        queued = store.list_jobs("queue")
        _log(f"Found {len(queued)} queued job(s)")
        # Higher priority first; older first within same priority.
        queued.sort(key=lambda j: (-getattr(j, "priority", 0), j.created_at))
        picked = None
        for job in queued:
            # Local agent claim rule:
            # - if pinned to a non-local provider, never claim
            # - if pinned to local, always claim
            # - otherwise (default): claim if our GPU satisfies the job
            #   (CPU job, no gpu_type, or matching gpu_type)
            pinned = getattr(job, "pin_to_provider", False)
            if pinned and job.provider != "local":
                continue
            matches = (
                job.provider == "local"
                or not job.gpu_type
                or job.gpu_type == gpu_type
            )
            if not matches:
                continue
            # Cost cap mirrors scheduler.py: refuse jobs where the local
            # agent's GPU exceeds the per-job $/hour budget.
            cap = getattr(job, "max_cost_per_hour_usd", 0.0) or 0.0
            if cap > 0 and job.gpu_type:
                preemptible = getattr(job, "preemptible", False)
                rate = _accel_hourly_rate(job.gpu_type, preemptible)
                if rate > 0 and rate > cap:
                    _log(f"Skip {job.job_id}: ${rate:.2f}/hr > cap ${cap:.2f}/hr")
                    continue
            picked = job
            break

        if not picked:
            time.sleep(POLL_INTERVAL)
            continue

        # Start the job
        _log(f"Starting job {picked.job_id}: {picked.command[:60]}")
        work_dir = f"/tmp/wc-{picked.job_id}"
        os.makedirs(f"{work_dir}/output", exist_ok=True)

        _write_status(store, picked.job_id, f"RUNNING {datetime.now(timezone.utc).isoformat()}")

        picked.state = JobState.RUNNING.value
        picked.started_at = datetime.now(timezone.utc).isoformat()
        picked.instance_ref = f"local@{os.uname().nodename}"
        store.move_job(picked, "queue", "running")

        log_file = open(f"{work_dir}/output/command_output.log", "w")
        current_proc = subprocess.Popen(
            picked.command,
            shell=True,
            stdout=log_file,
            stderr=subprocess.STDOUT,
            cwd=work_dir,
            env={**os.environ, "WISENT_DTYPE": "auto", "PYTHONUNBUFFERED": "1"},
        )
        current_job = picked
        last_heartbeat = time.time()
        _write_heartbeat(store, picked.job_id)
