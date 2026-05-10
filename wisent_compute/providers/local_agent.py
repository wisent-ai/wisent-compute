"""Local GPU agent: runs on the workstation, polls GCS queue, respects Vast.ai.

Usage: wc agent --gpu-type nvidia-rtx-4090
Runs as a long-lived daemon. Picks up jobs when Vast.ai has no active renter.
"""
from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import time
import urllib.request
from datetime import datetime, timezone

from ..config import BUCKET, estimate_gpu_memory
from ..models import GPU_HOURLY_RATE_USD, SPOT_DISCOUNT
from ..queue.capacity import publish_capacity
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

POLL_INTERVAL = 10
HEARTBEAT_INTERVAL = 300
# Time to sleep after a successful claim so nvidia-smi can reflect the
# freshly-spawned subprocess's CUDA allocation before the next iteration
# decides whether to claim again. Empirically a torch model load starts
# allocating GPU memory within ~5 seconds of subprocess start.
SETTLE_AFTER_CLAIM_SECONDS = 5
VAST_API = "https://console.vast.ai/api/v0"


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


def _job_eligible(job, gpu_type: str, vram_gb: int = 0, kind: str = "local") -> bool:
    """Local-agent claim rules. Matches if pinned-local; or job.gpu_type is empty;
    or job.gpu_type equals our own; or job.gpu_type is in the VRAM-compatibility
    list (we have at least its tier of VRAM). Cloud agents (kind != 'local')
    additionally refuse jobs whose model is in LOCAL_ONLY_MODELS so that
    workstation-pinned models stay off paid cloud quota."""
    from ..config import LOCAL_ONLY_MODELS
    import re as _re
    if kind != "local":
        m = _re.search(r"--model\s+(\S+)", getattr(job, "command", "") or "")
        if m and m.group(1).strip("'\"") in LOCAL_ONLY_MODELS:
            return False
    pinned = getattr(job, "pin_to_provider", False)
    if pinned and job.provider != kind:
        return False
    job_accel = job.gpu_type or ""
    matches = (
        job.provider == "local"
        or not job_accel
        or job_accel == gpu_type
        or (vram_gb > 0 and job_accel in _compat_accel_types(vram_gb))
    )
    if not matches:
        return False
    cap = getattr(job, "max_cost_per_hour_usd", 0.0) or 0.0
    if cap > 0 and job_accel:
        rate = _accel_hourly_rate(job_accel, getattr(job, "preemptible", False))
        if rate > 0 and rate > cap:
            return False
    return True


def _detect_local_vram_gb() -> int:
    """Return total VRAM in GB on the first detected GPU, 0 if none."""
    try:
        r = subprocess.run(
            ["nvidia-smi", "--query-gpu=memory.total", "--format=csv,noheader,nounits"],
            capture_output=True, text=True,
        )
        if r.returncode == 0:
            mib = int(r.stdout.strip().splitlines()[0])
            return mib // 1024
    except Exception:
        pass
    return 0


def _smi_free_vram_gb() -> int:
    """Live nvidia-smi free VRAM in GB on the first GPU, -1 if unreadable.

    Used as a sanity floor on the agent's bookkeeping: if any other GPU
    user (vLLM, jupyter, dev work) is consuming VRAM the agent doesn't
    track, the bookkeeping overestimates free VRAM and the agent claims
    jobs whose model-load OOMs immediately. Capping bookkeeping by the
    smi reading prevents over-commit on shared GPUs.
    """
    try:
        r = subprocess.run(
            ["nvidia-smi", "--query-gpu=memory.free", "--format=csv,noheader,nounits"],
            capture_output=True, text=True,
        )
        if r.returncode == 0:
            mib = int(r.stdout.strip().splitlines()[0])
            return mib // 1024
    except Exception:
        pass
    return -1


def _compat_accel_types(local_vram_gb: int) -> list[str]:
    """Every GCP gpu_type whose required VRAM tier ≤ local VRAM."""
    from ..models import GPU_SIZING
    accels: list[str] = []
    for tier, (_, accel) in sorted(GPU_SIZING.get("gcp", {}).items()):
        if local_vram_gb >= tier and accel and accel not in accels:
            accels.append(accel)
    return accels


def _build_capacity_dict(gpu_type: str, free_vram_gb: int, total_vram_gb: int) -> dict[str, int]:
    """Backward-compat slot-shaped broadcast: free_slots[<accel>] = number of
    that-tier-sized jobs that fit in our free VRAM. Lets older schedulers
    still see capacity even though the authoritative admission signal is the
    free_vram_gb field below."""
    from ..models import GPU_SIZING
    out: dict[str, int] = {}
    if not gpu_type or gpu_type == "cpu" or free_vram_gb <= 0:
        return out
    for tier, (_, accel) in GPU_SIZING.get("gcp", {}).items():
        if total_vram_gb >= tier and accel:
            n = max(0, free_vram_gb // max(1, tier))
            if n > 0:
                out[accel] = max(out.get(accel, 0), n)
    if gpu_type not in out and free_vram_gb > 0:
        out[gpu_type] = 1
    return out


def _slot_is_exclusive(slot: dict) -> bool:
    from ..config import EXCLUSIVE_MODELS
    import re
    m = re.search(r"--model\s+(\S+)", getattr(slot.get("job"), "command", "") or "")
    return bool(m and m.group(1).strip("'\"") in EXCLUSIVE_MODELS)
def _slot_vram(slot: dict) -> int:
    job = slot.get("job")
    return max(int(getattr(job, "gpu_mem_gb", 0) or 0), estimate_gpu_memory(getattr(job, "command", "") or ""))

def _no_eligible_in_queue(store: JobStorage, gpu_type: str, total_vram_gb: int,
                          free_vram_gb: int, kind: str = "local") -> bool:
    """True when no queued job could even hypothetically fit + be eligible.

    Pure condition check — no timer, no constant duration. Signal is "queue
    holds nothing this consumer can run", not "we've waited N seconds".
    """
    queued = store.list_jobs("queue")
    for job in queued:
        need = max(int(getattr(job, "gpu_mem_gb", 0) or 0), estimate_gpu_memory(getattr(job, "command", "") or ""))  # live estimator
        if need > free_vram_gb:
            continue
        if not _job_eligible(job, gpu_type, total_vram_gb, kind=kind):
            continue
        return False
    return True



def _staging_size_gb(d: str) -> float:
    """Total size of files under d in GB. 0 if dir missing."""
    import os as _o
    if not _o.path.isdir(d): return 0.0
    total = 0
    for root, _, files in _o.walk(d):
        for f in files:
            try: total += _o.path.getsize(_o.path.join(root, f))
            except OSError: pass
    return total / (1024**3)


def run_agent(gpu_type: str = "", idle_shutdown: bool = False, kind: str = "local"):
    """Main agent loop. Polls queue, runs jobs when Vast.ai is idle.

    idle_shutdown=True: exit cleanly (and self-delete the GCE VM if running on
    one) once both: (a) no slots active, and (b) no queued job is eligible to
    run on this consumer's free VRAM. Used for the cloud-VM agent path. The
    workstation/Vast.ai path leaves it False so the daemon stays up.

    kind: capacity-broadcast label distinguishing physical workstations
    (kind="local") from ephemeral cloud-agent VMs (kind="gcp", "aws", ...).
    Surfaces cloud-VM count to the dashboard. Local-vs-cloud yield rules in
    the scheduler key on this same field.
    """
    from .local.slots import advance_slot, start_slot
    from ..targets import lookup_self
    if not gpu_type:
        gpu_type = _detect_gpu_type()
    total_vram_gb = max(1, _detect_local_vram_gb())
    # WC_LOCAL_SLOTS retained as a HARD safety cap (defaults to no cap when 0).
    hard_slot_cap = int(os.environ.get("WC_LOCAL_SLOTS", "0") or 0)
    _log(f"Agent started. kind={kind}  GPU: {gpu_type}  vram_gb={total_vram_gb}  hard_slot_cap={hard_slot_cap}")

    # Reap orphan /tmp/wisent_act_* dirs at agent startup. Job subprocesses
    # rmtree their workdir on clean exit; if the agent is killed mid-job the
    # workdir leaks. /tmp on the workstation is tmpfs (62 GB cap) — one
    # leaked 43 GB workdir on 2026-05-07 filled it 100% and caused 700+
    # ENOSPC failures in 17 hours. BEFORE rmtree-ing, tar the workdir and
    # upload to gs://<bucket>/orphans/<hostname>/<dirname>.tar.gz so the
    # operator has crash evidence in GCS for postmortem.
    import glob, shutil, os as _os, tarfile, tempfile, socket
    _hn = socket.gethostname()
    try:
        _orphan_store = JobStorage(BUCKET)  # JobStorage already imported at top
    except Exception:
        _orphan_store = None
    for _d in glob.glob("/tmp/wisent_act_*"):
        try:
            _pid = int(_d.rsplit("_pid", 1)[-1].split("_")[0])
        except (ValueError, IndexError):
            continue
        try:
            _os.kill(_pid, 0)
        except OSError:
            try:
                if _orphan_store is not None and getattr(_orphan_store, "_sdk_bucket", None) is not None:
                    with tempfile.NamedTemporaryFile(suffix=".tar.gz", delete=False) as _tmp:
                        _tar_path = _tmp.name
                    with tarfile.open(_tar_path, "w:gz") as _tar:
                        _tar.add(_d, arcname=_os.path.basename(_d))
                    _key = f"orphans/{_hn}/{_os.path.basename(_d)}.tar.gz"
                    _orphan_store._sdk_bucket.blob(_key).upload_from_filename(_tar_path)
                    _os.unlink(_tar_path)
                    _log(f"archived orphan {_d} -> gs://{BUCKET}/{_key}")
            except Exception as _e:
                _log(f"failed to archive orphan {_d}: {_e}")
            shutil.rmtree(_d, ignore_errors=True)
            _log(f"reaped orphan workdir {_d} (pid {_pid} dead)")

    initial_env: dict[str, str] = dict(os.environ)
    initial_gpu = gpu_type

    store = JobStorage(BUCKET)
    hostname = os.uname().nodename
    consumer_id = f"{kind}-{hostname}"
    slots: list[dict] = []
    agent_diag: dict = {}
    fleet_staging = os.environ.get("WISENT_FLEET_STAGING_DIR", "/tmp/wisent_fleet_staging")
    last_fleet_flush = time.time()
    FLEET_FLUSH_INTERVAL = 180  # 3-rotation cadence: 1 HF commit per 3-rotation per agent
                                # = ~20 commits/hour/agent << 200/hour HF cap

    while True:
        # Periodic batched flush of accumulated activation shards across
        # ALL completed jobs on this agent. flush_staging_dir uploads the
        # entire dir as ONE HF commit (api.upload_folder = single commit).
        # Triggered every FLEET_FLUSH_INTERVAL seconds OR when staging dir
        # exceeds 5 GB (back-pressure to avoid /tmp fill).
        if time.time() - last_fleet_flush > FLEET_FLUSH_INTERVAL or _staging_size_gb(fleet_staging) > 5:
            try:
                from wisent.core.reading.modules.utilities.data.sources.hf.hf_writers import flush_staging_dir
                if os.path.isdir(fleet_staging) and any(os.scandir(fleet_staging)):
                    flush_staging_dir(fleet_staging)
                    import shutil as _sh
                    _sh.rmtree(fleet_staging, ignore_errors=True)
                    os.makedirs(fleet_staging, exist_ok=True)
                    _log(f"flushed fleet staging dir to HF (1 commit)")
            except Exception as _e:
                _log(f"fleet staging flush failed: {_e}")
            last_fleet_flush = time.time()
        t = lookup_self(hostname, source="auto")
        if t and t.kind == "local":
            registry_env = t.env_overrides or {}
            # Only trigger restart on keys the registry actually declares whose
            # values differ from what we were started with. Don't compare the
            # whole environment — that would always differ.
            env_delta = {
                k: str(v) for k, v in registry_env.items()
                if str(initial_env.get(k, "")) != str(v)
            }
            if env_delta and not slots:
                _log(f"Registry env override delta {env_delta}; exit for systemd restart")
                raise SystemExit(0)
            if t.gpu_type and t.gpu_type != initial_gpu and not slots:
                _log(f"Registry gpu_type {initial_gpu} -> {t.gpu_type}; exit for restart")
                raise SystemExit(0)
            if t.vram_gb and int(t.vram_gb) != total_vram_gb:
                _log(f"Registry vram_gb override {total_vram_gb} -> {t.vram_gb}")
                total_vram_gb = int(t.vram_gb)
        # Advance slots BEFORE drift check so a drained slots list triggers
        # the in-process upgrade. Earlier the drift continue ran BEFORE
        # advance_slot, leaving slots full forever.
        vast_active = _vast_has_renter()
        slots = [s for s in slots if advance_slot(s, store, vast_active, _log)]
        from .local.version_check import maybe_drain_or_upgrade as _drain
        if _drain(slots, _log):
            time.sleep(POLL_INTERVAL); continue
        if vast_active:
            publish_capacity(store, consumer_id, kind, {}, free_vram_gb=0,
                             total_vram_gb=total_vram_gb, diag=dict(agent_diag))
            time.sleep(POLL_INTERVAL)
            continue

        used_vram = sum(_slot_vram(s) for s in slots)
        # Exclusive-model lockout: while any slot runs a model in
        # EXCLUSIVE_MODELS, force used = total so no co-schedule.
        if any(_slot_is_exclusive(s) for s in slots):
            used_vram = total_vram_gb
        free_vram_gb = max(0, total_vram_gb - used_vram)
        # Cap bookkeeping by live nvidia-smi free reading so other GPU users
        # (vLLM, jupyter, dev work) don't get over-committed. Without this
        # the agent claims 26-60GB jobs on a box where only 7GB is actually
        # free, the model-load OOM-fails in <10s, and the failure record
        # has no stdout because the subprocess was CUDA-killed.
        smi_free = _smi_free_vram_gb()
        if smi_free >= 0 and smi_free < free_vram_gb:
            free_vram_gb = smi_free
        # Disk-space gate. The agents pip-install + repo-clone + checkpoint
        # save each need tens of GB of free disk; when $HOME has < 30 GB free
        # every claimed job dies at install with ENOSPC before any work runs.
        # Refusing slots in that state lets jobs queue for a healthy agent.
        try:
            import shutil as _shutil
            _free_disk_gb = _shutil.disk_usage(os.path.expanduser("~")).free / (1024 ** 3)
        except Exception:
            _free_disk_gb = -1.0
        agent_diag["free_disk_gb"] = round(_free_disk_gb, 1)
        if 0 <= _free_disk_gb < 30:
            _log(f"disk low (~{_free_disk_gb:.1f} GB free in $HOME); refusing slots this tick")
            publish_capacity(store, consumer_id, kind, {},
                             free_vram_gb=0, total_vram_gb=total_vram_gb, diag=dict(agent_diag))
            time.sleep(10)
            continue
        free_slots = _build_capacity_dict(gpu_type, free_vram_gb, total_vram_gb)
        free_slots = _build_capacity_dict(gpu_type, free_vram_gb, total_vram_gb)
        publish_capacity(store, consumer_id, kind, free_slots,
                         free_vram_gb=free_vram_gb, total_vram_gb=total_vram_gb, diag=dict(agent_diag))

        if free_vram_gb <= 0 or (hard_slot_cap > 0 and len(slots) >= hard_slot_cap):
            time.sleep(10)
            continue

        queued = store.list_jobs_fitting("queue", max_gpu_mem_gb=free_vram_gb, cap=2000)  # filter on GCS metadata, do not download non-fitting jobs
        queued.sort(key=lambda j: (-getattr(j, "priority", 0), j.created_at))
        # Claim AT MOST one new job per loop iteration. The previous version
        # greedily claimed every job whose declared gpu_mem_gb fit in the
        # bookkeeping budget, in the same iteration. Because nvidia-smi takes
        # several seconds to reflect a freshly-spawned subprocess's CUDA
        # allocation, smi_free still showed 80+ GB while 3 model loads were
        # already underway — the cap-to-smi check was a no-op for the second
        # and third claim. Result: 4 simultaneous model loads on a 96 GB
        # workstation, total peak ~95 GB, last one OOMs mid-load (job ee060bb3
        # tail showed Process 584222 46.30 GiB + 587657 15.81 GiB + 588506
        # 8.54 GiB + 24.10 GiB this process = 94.75 GiB used, 0.21 GiB free).
        # Now: claim 1, sleep SETTLE_AFTER_CLAIM_SECONDS, re-read smi at top
        # of loop, decide whether the next claim still fits.
        started = 0
        diag_vram_rejected = 0
        diag_eligibility_rejected = 0
        diag_eligible = 0
        for job in queued:
            if hard_slot_cap > 0 and len(slots) >= hard_slot_cap:
                break
            need = max(int(getattr(job, "gpu_mem_gb", 0) or 0), estimate_gpu_memory(getattr(job, "command", "") or ""))  # live estimator
            if need > free_vram_gb:
                diag_vram_rejected += 1
                continue
            # Pre-flight /tmp disk check: a job at need GiB needs roughly that
            # much in workdir scratch (model load + safetensors). Refuse claim
            # if /tmp free < 2x need to avoid the ENOSPC cascade that hit the
            # workstation overnight (700+ failures from one filled tmpfs).
            try:
                _tmp_free_gb = shutil.disk_usage("/tmp").free / (1024**3)
            except Exception:
                _tmp_free_gb = 9999
            if _tmp_free_gb < 2 * need:
                diag_vram_rejected += 1
                _log(f"refuse {job.job_id}: /tmp free={_tmp_free_gb:.1f}G < 2*need={2*need}G")
                continue
            if not _job_eligible(job, gpu_type, total_vram_gb, kind=kind):
                diag_eligibility_rejected += 1
                continue
            diag_eligible += 1
            slots.append(start_slot(store, job, hostname, _log))
            free_vram_gb -= need
            started += 1
            agent_diag["last_started_job_id"] = job.job_id
            agent_diag["last_started_at"] = datetime.now(timezone.utc).isoformat()
            break
        agent_diag["queue_scanned"] = len(queued)
        agent_diag["vram_rejected"] = diag_vram_rejected
        agent_diag["eligibility_rejected"] = diag_eligibility_rejected
        agent_diag["eligible_count"] = diag_eligible
        agent_diag["claimed_this_loop"] = started
        agent_diag["last_claim_attempt_at"] = datetime.now(timezone.utc).isoformat()

        if started > 0:
            # Let nvidia-smi reflect the new subprocess's CUDA allocation
            # before the next iteration's smi_free read. Without this, the
            # agent re-enters the loop top with stale smi data and may claim
            # again before the previous load has consumed any VRAM.
            time.sleep(SETTLE_AFTER_CLAIM_SECONDS)
            continue

        if started == 0:
            if idle_shutdown and not slots and _no_eligible_in_queue(
                store, gpu_type, total_vram_gb, free_vram_gb, kind=kind,
            ):
                _log("idle_shutdown: no slots + no eligible queued jobs; exiting")
                from .local.gcp_self import self_terminate
                self_terminate(_log)
                return
            time.sleep(POLL_INTERVAL)
