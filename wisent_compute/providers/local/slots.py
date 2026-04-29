"""Per-slot helpers for the local GPU agent.

Splits the single-job lifecycle (start, heartbeat, vast pause/resume,
completion, status upload) out of local_agent.run_agent so the agent
can manage N concurrent slots without ballooning the main loop past
the 300-line cap.

A "slot" is a dict with keys:
  proc          subprocess.Popen running the job
  job           Job object
  log_file      open file handle for stdout/stderr capture
  last_hb       last heartbeat timestamp (monotonic seconds)
  paused        bool - currently SIGSTOPed because a Vast renter appeared
"""
from __future__ import annotations

import os
import signal
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path

from ...models import JobState
from ...queue.storage import JobStorage

HEARTBEAT_INTERVAL = 300


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


def _write_status(store: JobStorage, job_id: str, status: str) -> None:
    with open("/tmp/wc_status.txt", "w") as f:
        f.write(status)
    subprocess.run(
        [_gsutil_bin(), "cp", "/tmp/wc_status.txt",
         f"gs://{store.bucket_name}/status/{job_id}/status"],
        capture_output=True,
    )


def _write_heartbeat(store: JobStorage, job_id: str) -> None:
    ts = datetime.now(timezone.utc).isoformat()
    with open("/tmp/wc_heartbeat.txt", "w") as f:
        f.write(f"RUNNING {ts}")
    subprocess.run(
        [_gsutil_bin(), "cp", "/tmp/wc_heartbeat.txt",
         f"gs://{store.bucket_name}/status/{job_id}/heartbeat"],
        capture_output=True,
    )


def _upload_output(store: JobStorage, job_id: str, output_dir: str) -> None:
    subprocess.run(
        [_gsutil_bin(), "-m", "cp", "-r", f"{output_dir}/*",
         f"gs://{store.bucket_name}/status/{job_id}/output/"],
        capture_output=True,
    )


def start_slot(store: JobStorage, job, hostname: str, log_fn) -> dict:
    """Spawn a subprocess for `job`, register it in 'running' state, return slot."""
    work_dir = f"/tmp/wc-{job.job_id}"
    os.makedirs(f"{work_dir}/output", exist_ok=True)
    _write_status(store, job.job_id, f"RUNNING {datetime.now(timezone.utc).isoformat()}")
    job.state = JobState.RUNNING.value
    job.started_at = datetime.now(timezone.utc).isoformat()
    job.instance_ref = f"local@{hostname}"
    store.move_job(job, "queue", "running")
    log_file = open(f"{work_dir}/output/command_output.log", "w")
    proc = subprocess.Popen(
        job.command, shell=True, stdout=log_file, stderr=subprocess.STDOUT,
        cwd=work_dir,
        env={**os.environ, "WISENT_DTYPE": "auto", "PYTHONUNBUFFERED": "1"},
    )
    log_fn(f"Started job {job.job_id}: {job.command[:60]}")
    _write_heartbeat(store, job.job_id)
    return {"proc": proc, "job": job, "log_file": log_file,
            "last_hb": time.time(), "paused": False}


def _tail_log(path: str, max_bytes: int = 4096) -> str:
    """Last max_bytes of the per-job log; '' if missing/empty."""
    try:
        with open(path, "rb") as f:
            f.seek(0, 2)
            size = f.tell()
            f.seek(max(0, size - max_bytes))
            data = f.read()
        return data.decode("utf-8", errors="replace").strip()
    except Exception:
        return ""


def advance_slot(slot: dict, store: JobStorage, vast_active: bool, log_fn) -> bool:
    """Advance one slot. Returns True if still running, False if completed/failed."""
    proc = slot["proc"]
    job = slot["job"]
    if not slot["paused"] and vast_active:
        log_fn(f"Renter detected, pausing job {job.job_id}")
        os.kill(proc.pid, signal.SIGSTOP)
        slot["paused"] = True
    elif slot["paused"] and not vast_active:
        log_fn(f"Renter gone, resuming job {job.job_id}")
        os.kill(proc.pid, signal.SIGCONT)
        slot["paused"] = False
    ret = proc.poll()
    if ret is not None:
        status = "COMPLETED" if ret == 0 else f"FAILED exit={ret}"
        _write_status(store, job.job_id, status)
        output_dir = f"/tmp/wc-{job.job_id}/output"
        log_path = f"{output_dir}/command_output.log"
        if Path(output_dir).exists():
            _upload_output(store, job.job_id, output_dir)
        state = JobState.COMPLETED if ret == 0 else JobState.FAILED
        job.state = state.value
        ts = datetime.now(timezone.utc).isoformat()
        if ret == 0:
            job.completed_at = ts
        else:
            job.failed_at = ts
            tail = _tail_log(log_path)
            job.error = tail or f"exit={ret} (no stdout/stderr captured)"
        store.move_job(job, "running", state.value)
        try:
            slot["log_file"].close()
        except Exception:
            pass
        log_fn(f"Job {job.job_id} {state.value}")
        return False
    now = time.time()
    if not slot["paused"] and now - slot["last_hb"] > HEARTBEAT_INTERVAL:
        _write_heartbeat(store, job.job_id)
        slot["last_hb"] = now
    return True
