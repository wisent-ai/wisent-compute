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

HEARTBEAT_INTERVAL = 60  # write a fresh heartbeat every 60s; HEARTBEAT_STALE_MINUTES=15 leaves 15 missed-write tolerance


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
    """Stamp a fresh `status/<job_id>/heartbeat` blob so the CF monitor sees
    the workstation slot is alive.

    Earlier this used `subprocess.run([gsutil, cp, ...], capture_output=True)`
    which silently swallowed any failure. When gsutil hit a transient auth
    glitch, network blip, or concurrent-fork ENOMEM, the heartbeat write
    vanished into the void; the CF monitor saw an old/missing blob, and
    requeued every workstation job at the 15-minute staleness threshold.
    Confirmed live on 2026-05-06 (job 01d79e28 had no heartbeat blob despite
    the slot being live; jobs 4724ae6d/3f16d8b4/24dee60d were yanked from
    running/ for 'stale heartbeat (local consumer)' in a single 4-second
    monitor window).

    Uses the GCS SDK from `store._sdk_bucket` directly (no fork, no
    swallowed error). The agent always runs with an SDK-backed JobStorage,
    so we require it explicitly rather than degrading silently.
    """
    if store._sdk_bucket is None:
        raise RuntimeError(
            f"_write_heartbeat({job_id}): JobStorage has no _sdk_bucket "
            "— refusing to silently skip the heartbeat write"
        )
    ts = datetime.now(timezone.utc).isoformat()
    blob = store._sdk_bucket.blob(f"status/{job_id}/heartbeat")
    blob.upload_from_string(f"RUNNING {ts}")



def _start_heartbeat_thread(store: JobStorage, job_id: str, proc):
    """Stamp status/<job>/heartbeat every HEARTBEAT_INTERVAL for as long
    as the training subprocess is alive — independent of the agent main
    loop. The loop-coupled write (slots tick, only fires when the agent
    reaches it) let a loop blocked >1800s on another slot's checkpoint
    pull / drift check / HF download starve a HEALTHY job's heartbeat,
    so the CF monitor orphan-requeued it: Llama 3ef705b2 + Qwen3
    724084db were both requeued in one monitor pass at
    2026-05-15T16:18:42 ('local agent live but job heartbeat stale
    (orphan)') while training was actively progressing. A daemon thread
    keyed on proc.poll() makes the heartbeat mean 'training process
    alive', not 'agent loop ran recently'.
    """
    import threading

    def _run():
        while proc.poll() is None:
            time.sleep(HEARTBEAT_INTERVAL)
            try:
                _write_heartbeat(store, job_id)
            except Exception:
                pass

    th = threading.Thread(target=_run, name=f"hb-{job_id}", daemon=True)
    th.start()
    return th


def _upload_output(store: JobStorage, job_id: str, output_dir: str) -> None:
    """Upload every regular file under output_dir to GCS via the SDK.

    Earlier this used `subprocess.run([gsutil, -m, cp, -r, ..., capture_output=True])`
    which silently swallowed failures (same fire-and-forget gsutil pattern as
    the heartbeat bug). On the workstation, this resulted in 7/7 sampled
    `local@ubuntu-server` completions on 2026-05-07 having NO log file at all
    in GCS (`gsutil cat` returned `CommandException: No URLs matched`).
    SDK-based upload raises on failure; caller logs but does not crash the
    slot.
    """
    if store._sdk_bucket is None:
        raise RuntimeError(
            f"_upload_output({job_id}): JobStorage has no _sdk_bucket"
        )
    base = Path(output_dir)
    if not base.exists():
        return
    for p in base.rglob("*"):
        if not p.is_file():
            continue
        rel = p.relative_to(base).as_posix()
        blob = store._sdk_bucket.blob(f"status/{job_id}/output/{rel}")
        blob.upload_from_filename(str(p))


def _repo_prelude(job) -> str:
    """Bash that clones job.repo into a fresh subdir and pip-installs its
    extras. Returns '' when no repo was requested. Local agents reuse the
    same /tmp/wc-{job_id} workdir per restart, so we rm -rf the target dir
    first to keep retries idempotent (otherwise git clone errors with
    'destination path X already exists')."""
    repo = getattr(job, "repo", "") or ""
    if not repo:
        return ""
    workdir = (getattr(job, "repo_workdir", "") or "").strip()
    if not workdir:
        workdir = repo.rstrip("/").rsplit("/", 1)[-1].removesuffix(".git")
    extras = getattr(job, "repo_extras", "") or ""
    install = (
        f" && pip install --break-system-packages --upgrade pip setuptools wheel"
        f" && pip install --break-system-packages --no-build-isolation '.[{extras}]'"
    ) if extras else ""
    return (f"rm -rf {workdir} && git clone --depth 1 {repo} {workdir}"
            f" && cd {workdir}{install} && cd .. && ")


def _pre_command_prelude(job) -> str:
    """Caller-declared shell snippet placed before job.command. Runs in
    the SAME bash shell as the command (joined with `&&` so a non-zero
    exit in the prelude fails fast), so `export FOO=bar` reaches the
    subprocess. Returns '' when no prelude was requested."""
    pre = (getattr(job, "pre_command", "") or "").strip()
    if not pre:
        return ""
    # Strip trailing semicolons so the user's snippet composes cleanly
    # with the `&&` join. Multi-statement preludes (`a; b; c`) still work.
    return pre.rstrip(";").rstrip() + " && "


def _install_apt_packages(job, kind: str, log_fn) -> bool:
    """Install job.apt_packages via sudo apt-get on cloud-kind agents.

    Returns True on success (or no-op when no packages were requested),
    False on failure. Caller should refuse to start the slot when this
    returns False so the job stays queued for retry instead of running
    against missing system deps and failing with a confusing error.

    Refuses on kind='local' (physical operator workstation) — the
    operator owns what's installed on their box, and silent
    sudo-apt-installs from queued jobs are a footgun. Cloud VMs
    (kind='gcp'/'azure'/'aws') run with passwordless sudo by default
    on the deeplearning-platform image, so apt-install Just Works.
    """
    pkgs = list(getattr(job, "apt_packages", []) or [])
    if not pkgs:
        return True
    if kind == "local":
        log_fn(
            f"refuse {job.job_id}: apt_packages={pkgs} requested but agent "
            f"kind=local; install manually or submit to a cloud-kind agent"
        )
        return False
    cmd = ["sudo", "-n", "apt-get", "install", "-y", "--no-install-recommends", *pkgs]
    log_fn(f"apt-install for {job.job_id}: {' '.join(pkgs)}")
    res = subprocess.run(cmd, capture_output=True, text=True)
    if res.returncode != 0:
        log_fn(
            f"apt-install FAILED for {job.job_id}: rc={res.returncode} "
            f"err={(res.stderr or res.stdout)[:200]}"
        )
        return False
    return True


def _mirror_to_output_uri(store: JobStorage, job, log_fn) -> None:
    """Copy /tmp/wc-<job_id>/output/* to job.output_uri (e.g.
    gs://other-bucket/path/). Additive — the canonical
    status/<id>/output/ upload via _upload_output already ran.

    Uses gsutil for cross-bucket portability (job.output_uri may point
    at a bucket the SDK-cached client wasn't constructed for). Failures
    are logged but do NOT reverse the COMPLETED state — the canonical
    output is already in GCS and the caller can re-mirror manually.
    """
    uri = (getattr(job, "output_uri", "") or "").strip()
    if not uri:
        return
    output_dir = f"/tmp/wc-{job.job_id}/output"
    if not Path(output_dir).exists():
        return
    res = subprocess.run(
        [_gsutil_bin(), "-m", "cp", "-r", f"{output_dir}/.", uri],
        capture_output=True, text=True,
    )
    if res.returncode != 0:
        log_fn(
            f"output_uri mirror failed for {job.job_id} -> {uri}: "
            f"rc={res.returncode} err={(res.stderr or res.stdout)[:200]}"
        )
    else:
        log_fn(f"output_uri mirror ok: {job.job_id} -> {uri}")


def start_slot(store: JobStorage, job, hostname: str, log_fn,
               kind: str = "local") -> dict | None:
    """Spawn a subprocess for `job`, register it in 'running' state, return slot.

    Returns None when apt-install (job.apt_packages) refuses or fails —
    the caller should leave the job in queue/ for another agent to claim.
    """
    if not _install_apt_packages(job, kind, log_fn):
        return None
    work_dir = f"/tmp/wc-{job.job_id}"
    os.makedirs(f"{work_dir}/output", exist_ok=True)
    _write_status(store, job.job_id, f"RUNNING {datetime.now(timezone.utc).isoformat()}")
    job.state = JobState.RUNNING.value
    job.started_at = datetime.now(timezone.utc).isoformat()
    job.instance_ref = f"local@{hostname}"
    store.move_job(job, "queue", "running")
    log_file = open(f"{work_dir}/output/command_output.log", "w")
    full_command = _repo_prelude(job) + _pre_command_prelude(job) + job.command
    # WISENT_FLEET_STAGING_DIR points at a persistent agent-owned staging
    # dir. wisent's upload_extracted_activations writes shards there and
    # SKIPS the per-job flush. The agent flushes the whole dir periodically
    # across ALL jobs as one HF commit — reduces 429 risk drastically.
    fleet_staging = os.environ.get("WISENT_FLEET_STAGING_DIR",
                                    "/tmp/wisent_fleet_staging")
    os.makedirs(fleet_staging, exist_ok=True)
    proc = subprocess.Popen(
        full_command, shell=True, stdout=log_file, stderr=subprocess.STDOUT,
        cwd=work_dir,
        env={**os.environ, "WISENT_DTYPE": "auto", "PYTHONUNBUFFERED": "1",
             "WISENT_FLEET_STAGING_DIR": fleet_staging},
    )
    log_fn(f"Started job {job.job_id}: {job.command[:60]}")
    _write_heartbeat(store, job.job_id)
    _start_heartbeat_thread(store, job.job_id, proc)
    last_hb = time.time()
    return {"proc": proc, "job": job, "log_file": log_file,
            "last_hb": last_hb, "paused": False}


def _tail_log(path: str, max_bytes: int = 4096) -> str:
    """Last max_bytes of the per-job log; '' if missing."""
    if not Path(path).exists():
        return ""
    with open(path, "rb") as f:
        f.seek(0, 2)
        size = f.tell()
        f.seek(max(0, size - max_bytes))
        data = f.read()
    return data.decode("utf-8", errors="replace").strip()


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
        verify_err = ""
        if ret == 0 and (getattr(job, "verify_command", "") or "").strip():
            # Verification hook — see Job.verify_command docstring. Runs in
            # the same workdir as the original command. Non-zero exit
            # reverses the COMPLETED→FAILED. The verify command must define
            # its own clear failure conditions; the runner does not impose
            # a wall-clock cap.
            try:
                vres = subprocess.run(
                    job.verify_command, shell=True,
                    cwd=f"/tmp/wc-{job.job_id}", capture_output=True, text=True,
                )
                if vres.returncode != 0:
                    ret = 1000 + vres.returncode
                    verify_err = (vres.stderr or vres.stdout or "")[:500]
                    log_fn(f"verify_command failed for {job.job_id}: rc={vres.returncode} err={verify_err[:120]}")
            except Exception as e:
                ret = 1999
                verify_err = f"verify_command raised: {e}"
                log_fn(f"verify_command exception for {job.job_id}: {e}")
        # Close the log file BEFORE uploading. Earlier this was deferred
        # until the bottom of the branch, after _upload_output ran — so
        # buffered writes from the subprocess weren't flushed to disk
        # when gsutil cp captured the file, producing empty/truncated
        # command_output.log uploads. Confirmed live on 2026-05-06: 3
        # gpt-oss-20b "completions" had zero-byte logs despite the
        # subprocess running.
        slot["log_file"].flush()
        slot["log_file"].close()
        status = "COMPLETED" if ret == 0 else f"FAILED exit={ret}"
        _write_status(store, job.job_id, status)
        state = JobState.COMPLETED if ret == 0 else JobState.FAILED
        job.state = state.value
        output_dir = f"/tmp/wc-{job.job_id}/output"
        log_path = f"{output_dir}/command_output.log"
        ts = datetime.now(timezone.utc).isoformat()
        if ret == 0:
            job.completed_at = ts
        else:
            job.failed_at = ts
            tail = _tail_log(log_path)
            job.error = verify_err or tail or f"exit={ret} (no stdout/stderr captured)"
        # Durable state transition FIRST so a subsequent upload failure
        # doesn't leave the slot orphaned in running/. _upload_output now
        # raises on real SDK errors; a missing output_dir is a normal
        # happy-path case (job wrote nothing).
        store.move_job(job, "running", state.value)
        if Path(output_dir).exists():
            _upload_output(store, job.job_id, output_dir)
        # Mirror to job.output_uri if set. Runs for both COMPLETED and
        # FAILED so debugging logs and partial artifacts also land at
        # the caller's project URI. Failure here is logged, not raised
        # — canonical status/<id>/output/ is already written.
        _mirror_to_output_uri(store, job, log_fn)
        # log_file already flushed+closed above before _upload_output
        log_fn(f"Job {job.job_id} {state.value}")
        return False
    now = time.time()
    if not slot["paused"] and now - slot["last_hb"] > HEARTBEAT_INTERVAL:
        _write_heartbeat(store, job.job_id)
        # Stream the in-progress command_output.log to GCS on each heartbeat.
        # Without this, a job killed mid-run leaves its log on the workstation
        # /tmp dir and the operator has zero crash evidence in GCS.
        _log_path = f"/tmp/wc-{job.job_id}/output/command_output.log"
        if store._sdk_bucket is not None and Path(_log_path).exists():
            _blob = store._sdk_bucket.blob(f"status/{job.job_id}/output/command_output.log")
            _blob.upload_from_filename(_log_path)
        slot["last_hb"] = now
    return True
