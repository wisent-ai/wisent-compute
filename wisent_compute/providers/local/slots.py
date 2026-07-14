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
import shutil
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path

from ... import constants as _wc
from ...models import JobState, activation_extraction_must_share_gpu, deprecated_activation_command_reason
from ...queue.storage import JobStorage
from .helpers.gpu_probe import smi_job_used_gb, smi_pids_used_gb
from .resource_scope import (
    ScopeProcess,
    active_scopes,
    cgroup_v2_available,
    control_group,
    scope_command,
    scope_pids,
    scope_stats,
    terminate_scope,
    unit_name,
)

_HF_WRITE_TOK: dict = {}


def _hf_write_token(store) -> str:
    """Central write-scoped HF token from gs://<bucket>/config/hf_token,
    injected into every job env so uploads do not 403 when the agent ambient
    HF_TOKEN is read-only. Cached per process; empty if unset."""
    if "t" not in _HF_WRITE_TOK:
        _HF_WRITE_TOK["t"] = (store._download_text("config/hf_token") or "").strip()
    return _HF_WRITE_TOK["t"]

HEARTBEAT_INTERVAL = _wc.SLOT_HEARTBEAT_INTERVAL_S  # write a fresh heartbeat every 60s; HEARTBEAT_STALE_MINUTES=15 leaves 15 missed-write tolerance


def _raw_active_disk_refusal(command: str) -> str:
    if not activation_extraction_must_share_gpu(command):
        return ""
    root = os.path.join(os.environ.get("TMPDIR", "/tmp"), "wisent_raw_pending")
    try:
        os.makedirs(root, exist_ok=True)
        free_gb = shutil.disk_usage(root).free / (1024 ** 3)
    except OSError as exc:
        return f"raw active root unavailable: {root}: {exc}"
    reserve = float(os.environ.get("WISENT_RAW_CLAIM_RESERVE_GB", "180") or 180)
    min_free = float(os.environ.get(
        "WISENT_RAW_CLAIM_MIN_FREE_GB",
        os.environ.get("WISENT_RAW_HOT_FREE_TARGET_GB", "270"),
    ) or 270)
    if free_gb - reserve < min_free:
        return (
            f"raw active staging low: {root} free={free_gb:.1f}GB "
            f"reserve={reserve:.1f}GB min_free={min_free:.1f}GB"
        )
    return ""


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
    if store._sdk_bucket is None:
        raise RuntimeError(
            f"_write_status({job_id}): JobStorage has no _sdk_bucket "
            "— refusing to silently skip the status write"
        )
    blob = store._sdk_bucket.blob(f"status/{job_id}/status")
    blob.upload_from_string(status)


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
            except Exception as exc:
                # The coordinator requeues local jobs when their heartbeat
                # goes stale. Silent heartbeat failures leave live jobs looking
                # dead, so make the next failure visible in the agent log.
                print(
                    f"[heartbeat] write failed for {job_id}: "
                    f"{type(exc).__name__}: {exc}",
                    flush=True,
                )

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


def _start_scope_sampler(slot: dict) -> None:
    """Continuously retain cgroup peak/OOM evidence until the scope drains."""
    import threading

    cgroup = slot.get("cgroup", "")
    if not cgroup:
        return

    def _run() -> None:
        while True:
            stats = scope_stats(cgroup)
            slot["peak_host_ram_gb"] = max(
                float(slot.get("peak_host_ram_gb", 0.0) or 0.0),
                stats.peak_gb,
            )
            slot["memory_current_gb"] = stats.current_gb
            slot["oom_events"] = max(int(slot.get("oom_events", 0)), stats.oom_events)
            slot["oom_kill_events"] = max(
                int(slot.get("oom_kill_events", 0)), stats.oom_kill_events,
            )
            slot["scope_pids"] = stats.pids
            if slot["proc"].poll() is not None and not stats.pids:
                return
            time.sleep(0.25)

    threading.Thread(
        target=_run, name=f"scope-{slot['job'].job_id}", daemon=True,
    ).start()


def start_slot(store: JobStorage, job, hostname: str, log_fn,
               kind: str = "local") -> dict | None:
    """Spawn a subprocess in an owned cgroup and register it as running."""
    cmd = getattr(job, "command", "") or ""
    if activation_extraction_must_share_gpu(cmd):
        job.exclusive = False
    for terminal_prefix in ("uploaded", "completed"):
        if store.read_job(terminal_prefix, job.job_id):
            store.delete_job("queue", job.job_id)
            log_fn(f"drop duplicate queued {job.job_id}: already in {terminal_prefix}/")
            return None
    reason = deprecated_activation_command_reason(cmd)
    if reason:
        job.state = JobState.FAILED.value
        job.failed_at = datetime.now(timezone.utc).isoformat()
        job.error = reason
        store.move_job(job, "queue", "failed")
        log_fn(f"refuse {job.job_id}: {reason}")
        return None
    if not _install_apt_packages(job, kind, log_fn):
        return None
    raw_refusal = _raw_active_disk_refusal(cmd)
    if raw_refusal:
        log_fn(f"refuse {job.job_id}: {raw_refusal}")
        return None

    work_dir = f"/tmp/wc-{job.job_id}"
    os.makedirs(f"{work_dir}/output", exist_ok=True)
    full_command = _repo_prelude(job) + _pre_command_prelude(job) + job.command
    use_scope = cgroup_v2_available()
    scope_required = (
        os.environ.get("WC_ADMISSION_POLICY_V2", "legacy").strip().lower()
        == "enforce"
    )
    if scope_required and not use_scope:
        log_fn(f"refuse {job.job_id}: enforce mode requires cgroup v2 ownership")
        return None
    unit = unit_name(job.job_id) if use_scope else ""
    job.resource_scope = unit

    fleet_staging = (
        os.environ.get("WISENT_FLEET_STAGING_DIR")
        or os.path.join(os.environ.get("TMPDIR", "/tmp"), "wisent_fleet_staging")
    )
    os.makedirs(fleet_staging, exist_ok=True)
    job_env = {
        **os.environ,
        "WISENT_DTYPE": "auto",
        "PYTHONUNBUFFERED": "1",
        "HF_HUB_DISABLE_XET": "1",
        "WISENT_FLEET_STAGING_DIR": fleet_staging,
        "WC_JOB_ID": job.job_id,
    }
    hf_token = _hf_write_token(store)
    if hf_token:
        job_env["HF_TOKEN"] = job_env["HUGGING_FACE_HUB_TOKEN"] = hf_token

    if use_scope:
        launch, systemd_env, unit = scope_command(job.job_id, full_command)
        launch_env = {**systemd_env, **job_env}
    else:
        launch = full_command
        launch_env = job_env

    _write_status(store, job.job_id, f"RUNNING {datetime.now(timezone.utc).isoformat()}")
    job.state = JobState.RUNNING.value
    job.started_at = datetime.now(timezone.utc).isoformat()
    job.instance_ref = f"local@{hostname}"
    store.move_job(job, "queue", "running")
    log_file = open(f"{work_dir}/output/command_output.log", "w")
    try:
        proc = subprocess.Popen(
            launch,
            shell=not use_scope,
            stdout=log_file,
            stderr=subprocess.STDOUT,
            cwd=work_dir,
            env=launch_env,
            start_new_session=True,
        )
        cgroup = control_group(unit) if use_scope else ""
    except BaseException as exc:
        if use_scope:
            terminate_scope(unit)
        if use_scope and not scope_required:
            log_fn(
                f"scope launch unavailable for {job.job_id}; "
                f"legacy process-group fallback: {type(exc).__name__}"
            )
            job.resource_scope = ""
            store.write_job("running", job)
            try:
                proc = subprocess.Popen(
                    full_command,
                    shell=True,
                    stdout=log_file,
                    stderr=subprocess.STDOUT,
                    cwd=work_dir,
                    env=job_env,
                    start_new_session=True,
                )
            except BaseException as fallback_exc:
                exc = fallback_exc
            else:
                use_scope = False
                unit = ""
                cgroup = ""
        if use_scope:
            try:
                log_file.close()
            except Exception:
                pass
            job.state = JobState.QUEUED.value
            job.started_at = None
            job.instance_ref = None
            job.resource_scope = ""
            store.move_job(job, "running", "queue")
            log_fn(
                f"refuse {job.job_id}: resource scope launch failed: "
                f"{type(exc).__name__}"
            )
            return None

    slot = {
        "proc": proc,
        "job": job,
        "log_file": log_file,
        "last_hb": time.time(),
        "paused": False,
        "peak_vram_gb": 0,
        "peak_host_ram_gb": 0.0,
        "memory_current_gb": 0.0,
        "oom_events": 0,
        "oom_kill_events": 0,
        "started_mono": time.monotonic(),
        "scope_unit": unit,
        "cgroup": cgroup,
        "scope_pids": (),
    }
    log_fn(
        f"Started job {job.job_id} in "
        f"{unit or 'legacy-process-group'}: {job.command[:60]}"
    )
    _write_heartbeat(store, job.job_id)
    _start_heartbeat_thread(store, job.job_id, proc)
    _start_scope_sampler(slot)
    return slot
def reconstruct_slots(store: JobStorage, log_fn) -> list[dict]:
    """Recover active scope ownership after an agent process restart."""
    scopes = active_scopes()
    if not scopes:
        return []
    recovered: list[dict] = []
    for job in store.list_jobs("running"):
        unit = str(getattr(job, "resource_scope", "") or "")
        cgroup = scopes.get(unit)
        if not cgroup:
            continue
        work_dir = f"/tmp/wc-{job.job_id}"
        os.makedirs(f"{work_dir}/output", exist_ok=True)
        log_file = open(f"{work_dir}/output/command_output.log", "a")
        proc = ScopeProcess(unit, cgroup)
        stats = scope_stats(cgroup)
        slot = {
            "proc": proc,
            "job": job,
            "log_file": log_file,
            "last_hb": time.time(),
            "paused": False,
            "peak_vram_gb": 0,
            "peak_host_ram_gb": stats.peak_gb,
            "memory_current_gb": stats.current_gb,
            "oom_events": stats.oom_events,
            "oom_kill_events": stats.oom_kill_events,
            "started_mono": time.monotonic(),
            "scope_unit": unit,
            "cgroup": cgroup,
            "scope_pids": stats.pids,
            "reconstructed": True,
        }
        _write_heartbeat(store, job.job_id)
        _start_heartbeat_thread(store, job.job_id, proc)
        _start_scope_sampler(slot)
        recovered.append(slot)
        log_fn(f"reconstructed job {job.job_id} from {unit}")
    known = {slot["scope_unit"] for slot in recovered}
    for orphan in sorted(set(scopes) - known):
        log_fn(f"orphaned Stado scope left untouched for audit: {orphan}")
    return recovered




def request_yield(slot: dict, store: JobStorage, log_fn) -> bool:
    """Cooperatively yield a running slot to free its VRAM for higher-priority
    work. Returns True once the job has been requeued.

    Sequence (total bounded by job.yield_grace_seconds):
      1. Run the job's yield_command (the save-and-sync hook) in the job
         workdir with WC_JOB_PID set to the process-group leader, so the hook
         can signal the job, persist state + artifacts externally, and let it
         exit.
      2. Wait for the process to exit on its own within the remaining grace.
      3. SIGKILL the whole process group only if the grace is blown (logged
         loudly — a timed-out yield means the hook didn't actually stop it).
      4. Requeue: running -> queue, state QUEUED, yield_count++, clear
         instance_ref/started_at. NOT marked FAILED — resume is the job's own
         business (checkpoint pull, server-side state, ...).

    The slot's process was started with start_new_session=True, so proc.pid
    is the process-group id.
    """
    proc = slot["proc"]
    job = slot["job"]
    pgid = proc.pid
    grace = int(getattr(job, "yield_grace_seconds", 120) or 120)
    hook = (getattr(job, "yield_command", "") or "").strip()
    work_dir = f"/tmp/wc-{job.job_id}"
    deadline = time.time() + grace
    log_fn(f"yield: requesting yield of {job.job_id} (grace={grace}s, pgid={pgid})")

    if hook:
        hook_env = {**os.environ, "WC_JOB_ID": job.job_id, "WC_JOB_PID": str(pgid)}
        remaining = max(1, int(deadline - time.time()))
        try:
            hres = subprocess.run(
                hook, shell=True,
                cwd=work_dir if Path(work_dir).exists() else None,
                env=hook_env, capture_output=True, text=True, timeout=remaining,
            )
            if hres.returncode != 0:
                log_fn(f"yield: on-yield hook {job.job_id} rc={hres.returncode}: "
                       f"{(hres.stderr or hres.stdout or '')[:200]}")
        except subprocess.TimeoutExpired:
            log_fn(f"yield: on-yield hook {job.job_id} exceeded grace; terminating")
        except Exception as e:
            log_fn(f"yield: on-yield hook {job.job_id} raised: {e}")

    while proc.poll() is None and time.time() < deadline:
        time.sleep(1)
    if proc.poll() is None:
        log_fn(f"yield: {job.job_id} still alive after grace — terminating owned scope")
        _terminate_owned_descendants(slot)
        try:
            proc.wait(timeout=10)
        except Exception:
            pass

    try:
        slot["log_file"].flush()
        slot["log_file"].close()
    except Exception:
        pass

    job.yield_count = int(getattr(job, "yield_count", 0) or 0) + 1
    job.state = JobState.QUEUED.value
    job.instance_ref = None
    job.started_at = None
    _write_status(store, job.job_id, f"YIELDED {datetime.now(timezone.utc).isoformat()}")
    output_dir = f"/tmp/wc-{job.job_id}/output"
    try:
        if Path(output_dir).exists():
            _upload_output(store, job.job_id, output_dir)
    except Exception as e:
        log_fn(f"yield: output upload {job.job_id} failed (non-fatal): {e}")
    # running -> queue (NOT a terminal state, so the tracking tombstone hook
    # is a no-op and the CF monitor leaves it alone once out of running/).
    store.move_job(job, "running", "queue")
    log_fn(f"yield: {job.job_id} requeued (yield_count={job.yield_count})")
    return True


def _refresh_scope_stats(slot: dict) -> tuple[int, ...]:
    cgroup = slot.get("cgroup", "")
    if not cgroup:
        return ()
    stats = scope_stats(cgroup)
    slot["peak_host_ram_gb"] = max(
        float(slot.get("peak_host_ram_gb", 0.0) or 0.0), stats.peak_gb,
    )
    slot["memory_current_gb"] = stats.current_gb
    slot["oom_events"] = max(int(slot.get("oom_events", 0)), stats.oom_events)
    slot["oom_kill_events"] = max(
        int(slot.get("oom_kill_events", 0)), stats.oom_kill_events,
    )
    slot["scope_pids"] = stats.pids
    return stats.pids


def _terminate_owned_descendants(slot: dict) -> None:
    unit = slot.get("scope_unit", "")
    if unit:
        terminate_scope(unit)
        slot["scope_pids"] = ()
        return
    proc = slot.get("proc")
    if proc is None:
        return
    try:
        os.killpg(proc.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass


def _signal_owned(slot: dict, sig: signal.Signals) -> None:
    pids = _refresh_scope_stats(slot)
    if pids:
        for pid in pids:
            try:
                os.kill(pid, sig)
            except ProcessLookupError:
                continue
        return
    proc = slot.get("proc")
    if proc is None or not proc.pid:
        return
    try:
        os.killpg(proc.pid, sig)
    except ProcessLookupError:
        pass


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
    for terminal_prefix in ("uploaded", "completed"):
        if store.read_job(terminal_prefix, job.job_id):
            _terminate_owned_descendants(slot)
            try:
                slot["log_file"].flush(); slot["log_file"].close()
            except Exception:
                pass
            try:
                store.delete_job("running", job.job_id)
            except Exception:
                pass
            log_fn(f"drop duplicate running {job.job_id}: already in {terminal_prefix}/")
            return False
    if not slot["paused"] and vast_active:
        log_fn(f"Renter detected, pausing job {job.job_id}")
        _signal_owned(slot, signal.SIGSTOP)
        slot["paused"] = True
    elif slot["paused"] and not vast_active:
        log_fn(f"Renter gone, resuming job {job.job_id}")
        _signal_owned(slot, signal.SIGCONT)
        slot["paused"] = False
    ret = proc.poll()
    owned_pids = _refresh_scope_stats(slot)
    if owned_pids:
        owned_vram = smi_pids_used_gb(owned_pids)
        if owned_vram > slot.get("peak_vram_gb", 0):
            slot["peak_vram_gb"] = owned_vram
    if ret is not None:
        descendant_err = ""
        if bool(getattr(job, "service_mode", False)) and owned_pids:
            now = time.time()
            if now - slot["last_hb"] > HEARTBEAT_INTERVAL:
                _write_heartbeat(store, job.job_id)
                slot["last_hb"] = now
            return True
        if owned_pids:
            descendant_err = (
                f"ordinary job left {len(owned_pids)} owned descendants "
                "after its parent exited"
            )
            log_fn(f"{descendant_err}; terminating scope before state transition")
            ret = 2001
        _terminate_owned_descendants(slot)
        verify_err = descendant_err
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
        job.peak_vram_gb = max(int(getattr(job, "peak_vram_gb", 0) or 0),
                               int(slot.get("peak_vram_gb", 0) or 0))
        # Stamp the per-GPU-probe marker: this agent is 0.4.241+,
        # so smi_job_used_gb measured the MAX single-GPU footprint
        # (grouped by gpu_uuid), not a cross-GPU sum. observed_vram_gb
        # trusts only flagged peaks, so legacy summed records can no
        # longer poison the model max().
        job.peak_vram_per_gpu = True
        job.peak_host_ram_gb = max(
            float(getattr(job, "peak_host_ram_gb", 0.0) or 0.0),
            float(slot.get("peak_host_ram_gb", 0.0) or 0.0),
        )
        job.peak_host_ram_source = (
            "cgroup_v2_memory_peak" if slot.get("cgroup") else "unavailable"
        )
        job.memory_oom_events = int(slot.get("oom_events", 0) or 0)
        job.memory_oom_kill_events = int(slot.get("oom_kill_events", 0) or 0)
        if state == JobState.FAILED:
            from ...sizing import escalate_on_oom
            if escalate_on_oom(store, job, job.error or ""):
                log_fn(f"Job {job.job_id} OOM-escalated to gpu_mem_gb={job.gpu_mem_gb}; requeued")
                return False
        store.move_job(job, "running", state.value)
        if Path(output_dir).exists():
            _upload_output(store, job.job_id, output_dir)
        # Mirror to job.output_uri if set. Runs for both COMPLETED and
        # FAILED so debugging logs and partial artifacts also land at
        # the caller's project URI. Failure here is logged, not raised
        # — canonical status/<id>/output/ is already written.
        _mirror_to_output_uri(store, job, log_fn)
        # log_file already flushed+closed above before _upload_output
        if state == JobState.FAILED:
            log_fn(
                f"Job {job.job_id} failed ret={ret} "
                f"error_tail={(job.error or '')[-500:]}"
            )
        else:
            log_fn(f"Job {job.job_id} {state.value}")
        return False
    now = time.time()
    _used = (
        smi_pids_used_gb(owned_pids)
        if slot.get("cgroup")
        else smi_job_used_gb(proc.pid)
    )
    if _used > slot.get("peak_vram_gb", 0):
        slot["peak_vram_gb"] = _used
    if not slot["paused"] and now - slot["last_hb"] > HEARTBEAT_INTERVAL:
        _write_heartbeat(store, job.job_id)
        # Stream the in-progress command_output.log to GCS on each heartbeat.
        # Without this, a job killed mid-run leaves its log on the workstation
        # /tmp dir and the operator has zero crash evidence in GCS.
        _log_path = f"/tmp/wc-{job.job_id}/output/command_output.log"
        if store._sdk_bucket is not None and Path(_log_path).exists():
            try:
                _blob = store._sdk_bucket.blob(
                    f"status/{job.job_id}/output/command_output.log"
                )
                _blob.upload_from_filename(_log_path, timeout=10)
            except Exception as e:
                log_fn(
                    f"heartbeat log upload failed for {job.job_id}: "
                    f"{type(e).__name__}: {str(e)[:160]}"
                )
        slot["last_hb"] = now
    return True
