"""Local GPU agent: runs on the workstation, polls GCS queue, respects Vast.ai.

Usage: wc agent --gpu-type nvidia-rtx-4090
Runs as a long-lived daemon. Picks up jobs when Vast.ai has no active renter.
"""
from __future__ import annotations

import glob
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import time
from datetime import datetime, timezone

from .. import constants as _wc
from ..config import BUCKET, estimate_gpu_memory
from ..models import activation_extraction_must_share_gpu
from ..queue.capacity import publish_capacity
from ..queue.storage import JobStorage
from .local.helpers import (
    _build_capacity_dict,
    _detect_gpu_type,
    _detect_local_vram_gb,
    _job_eligible,
    _no_eligible_in_queue,
    _slot_is_exclusive,
    _slot_vram,
    _slot_waiting_for_vram,
    _slot_rss, _free_ram_gb, _total_ram_gb,
    _static_ram_reserve_gb, _ram_safety_buffer_gb,
    _smi_free_vram_gb,
    _staging_size_gb,
    _vast_has_renter,
)
from .local.disk.staging import setup_agent_staging


POLL_INTERVAL = _wc.POLL_INTERVAL_S
HEARTBEAT_INTERVAL = _wc.CAPACITY_HEARTBEAT_INTERVAL_S
# Hard VRAM safety buffer at admission. The agent refuses to claim a
# job if accepting it would leave less than this margin between
# declared total VRAM use and the GPU's physical capacity. Catches the
# class of failure where neighbor processes' actual peak exceeds their
# declared gpu_mem_gb (estimate_gpu_memory has been observed to
# under-call by 5-10 GB on 7-8B activation extraction workloads). The
# buffer is independent of the per-job multipliers because it's the
# LAST line of defense — if the per-job estimate is wrong, this catches
# it before the n+1th job OOMs the entire VM.
# Derived from total VRAM instead of a flat constant.
def _vram_safety_buffer_gb(total_vram_gb: int) -> int:
    import math as _math
    return max(_wc.VRAM_SAFETY_BUFFER_MIN_GB,
               _math.ceil(total_vram_gb * _wc.VRAM_SAFETY_BUFFER_FRACTION))
# Cooperative-yield anti-thrash floor: never evict a yieldable slot that has
# run for less than this, so a just-(re)started background job gets real work
# done before it can be bumped again. Pairs with Job.max_yields_before_protected.
MIN_RUNTIME_BEFORE_YIELD_S = _wc.MIN_RUNTIME_BEFORE_YIELD_S
CUDA_PROBE_CACHE_S = _wc.CUDA_PROBE_CACHE_S
_CUDA_PROBE = {"checked_at": 0.0, "ok": False, "detail": "not checked"}


def _log(msg):
    ts = datetime.now().strftime("%H:%M:%S")
    sys.stderr.write(f"[{ts}] [agent] {msg}\n")
    sys.stderr.flush()


def _ensure_hf_token_from_cache() -> None:
    """Synchronize HF_TOKEN from standard local token files.

    The token file can be rotated while the agent is running. Do not keep an
    already-populated process env token if the cache file now contains a newer
    write token.
    """
    home = os.environ.get("HOME") or os.path.expanduser("~")
    candidates = [
        os.environ.get("HF_TOKEN_FILE", ""),
        os.path.join(home, ".cache", "huggingface", "token"),
        os.path.join(home, ".huggingface", "token"),
    ]
    for path in candidates:
        if not path:
            continue
        try:
            with open(path, encoding="utf-8") as fh:
                token = fh.read().strip()
        except OSError:
            continue
        if token:
            if os.environ.get("HF_TOKEN") != token:
                os.environ["HF_TOKEN"] = token
                os.environ["HUGGINGFACE_HUB_TOKEN"] = token
                os.environ["HUGGING_FACE_HUB_TOKEN"] = token
                _log(f"loaded HF_TOKEN from cached token file: {path}")
            return


def _cuda_child_available() -> tuple[bool, str]:
    """True only if a child Python in the agent's launch environment can
    initialize CUDA. nvidia-smi being healthy is not enough: the live failure
    mode was nvidia-smi reporting an RTX PRO 6000 while every claimed job's
    first Python process printed `CUDA available: False` and exited 66. Gate
    claims on the exact child-process condition that jobs require."""
    now = time.monotonic()
    if now - float(_CUDA_PROBE["checked_at"] or 0.0) < CUDA_PROBE_CACHE_S:
        return bool(_CUDA_PROBE["ok"]), str(_CUDA_PROBE["detail"])
    code = (
        "import torch, sys; "
        "ok=torch.cuda.is_available(); "
        "print(f'cuda_available={ok}', flush=True); "
        "sys.exit(0 if ok else 66)"
    )
    try:
        res = subprocess.run(
            [sys.executable, "-c", code],
            capture_output=True,
            text=True,
            timeout=30,
            env={**os.environ, "PYTHONUNBUFFERED": "1"},
        )
        detail = (res.stdout or res.stderr or f"rc={res.returncode}").strip()
        ok = res.returncode == 0
    except Exception as exc:
        ok = False
        detail = f"cuda probe raised: {exc}"
    _CUDA_PROBE.update({"checked_at": now, "ok": ok, "detail": detail[-300:]})
    return ok, str(_CUDA_PROBE["detail"])


def _reap_dead_pid_workdirs() -> int:
    """Reap dead-PID workdirs + unreferenced wisent_raw_stage_* across /tmp
    and the staging root (TMPDIR may be off-tmpfs on a disk volume; orphans
    there filled a 3.6T disk once). Skips the wisent_raw_pending pool."""
    reaped = 0
    import subprocess as _sp
    _cmd = _sp.run(["bash", "-c", "cat /proc/[0-9]*/cmdline 2>/dev/null"],
                   capture_output=True, text=True).stdout
    for base in {"/tmp", os.environ.get("TMPDIR", "/tmp")}:
        for d in glob.glob(f"{base}/wisent_act_*") + glob.glob(f"{base}/wisent_raw_*"):
            if "wisent_raw_pending" in d:
                continue
            if "_pid" in d:
                try:
                    pid = int(d.rsplit("_pid", 1)[-1].split("_")[0])
                except (ValueError, IndexError):
                    continue
                try:
                    os.kill(pid, 0); continue
                except OSError:
                    pass
                shutil.rmtree(d, ignore_errors=True); reaped += 1
            elif "wisent_raw_stage_" in d and d not in _cmd:
                shutil.rmtree(d, ignore_errors=True); reaped += 1
    return reaped


def _reap_orphan_workdirs(hostname: str) -> None:
    """Startup reap: PID-dead extraction workdirs + all /tmp/wc-* dirs.

    The wc-* wipe is unconditional and therefore startup-only — a fresh
    agent process owns no active job, so any leftover wc-* is stale. The
    periodic in-loop cleanup uses _reap_dead_pid_workdirs (PID-aware)."""
    _reap_dead_pid_workdirs()
    wc_dirs = glob.glob("/tmp/wc-*")
    for d in wc_dirs:
        shutil.rmtree(d, ignore_errors=True)
    if wc_dirs:
        _log(f"reaped {len(wc_dirs)} stale /tmp/wc-* dirs")


def _maybe_yield_for_priority(store, slots, gpu_type, total_vram_gb,
                              free_vram_gb, kind, consumer_id, log_fn) -> int:
    """If a strictly-higher-priority eligible queued job can't fit in the
    current free VRAM, cooperatively yield just enough lower-priority
    yieldable slots to make room. Returns the number of slots yielded
    (removed from `slots`); 0 means no action.

    Inert by construction: returns immediately unless a yieldable job is
    actually running, so existing (non-yieldable) prod workloads never enter
    the queue scan or any eviction logic.
    """
    if not any(getattr(s["job"], "yieldable", False) for s in slots):
        return 0
    from .local.slots import request_yield
    # Highest-priority queued job that needs MORE than current free VRAM but
    # could fit on the full GPU, and is eligible for THIS agent.
    candidates = store.list_jobs_fitting("queue", max_gpu_mem_gb=total_vram_gb, cap=200)
    candidates.sort(key=lambda j: (-int(getattr(j, "priority", 0) or 0), j.created_at))
    target = None
    for j in candidates:
        need_j = max(int(getattr(j, "gpu_mem_gb", 0) or 0),
                     estimate_gpu_memory(getattr(j, "command", "") or ""))
        if need_j <= free_vram_gb:
            continue  # already fits — not a VRAM-eviction case
        if not _job_eligible(j, gpu_type, total_vram_gb, kind=kind,
                             consumer_id=consumer_id, active_slot_count=len(slots)):
            continue
        target, need = j, need_j
        break
    if target is None:
        return 0
    target_prio = int(getattr(target, "priority", 0) or 0)

    now_mono = time.monotonic()
    evictable = []
    for s in slots:
        j = s["job"]
        if not getattr(j, "yieldable", False) or _slot_is_exclusive(s):
            continue
        if int(getattr(j, "priority", 0) or 0) >= target_prio:
            continue
        if int(getattr(j, "yield_count", 0) or 0) >= int(getattr(j, "max_yields_before_protected", 5) or 5):
            continue
        if now_mono - s.get("started_mono", now_mono) < MIN_RUNTIME_BEFORE_YIELD_S:
            continue
        evictable.append(s)
    if not evictable:
        return 0
    # Evict lowest-priority first; among equal priority, free the largest slot
    # first so we yield as few jobs as possible.
    evictable.sort(key=lambda s: (int(getattr(s["job"], "priority", 0) or 0), -_slot_vram(s)))
    freed, chosen = 0, []
    for s in evictable:
        chosen.append(s)
        freed += _slot_vram(s)
        if free_vram_gb + freed >= need:
            break
    if free_vram_gb + freed < need:
        return 0  # even yielding every candidate won't fit it — don't waste a yield
    n = 0
    for s in chosen:
        try:
            if request_yield(s, store, log_fn):
                slots.remove(s)
                n += 1
        except Exception as e:
            log_fn(f"yield: request_yield raised for {s['job'].job_id}: {e}")
    if n:
        log_fn(f"yield: freed ~{freed}G via {n} slot(s) for higher-priority "
               f"{target.job_id} (need={need}G prio={target_prio})")
    return n


def run_agent(gpu_type: str = "", idle_shutdown: bool = False, kind: str = "local"):
    """Main agent loop. Polls queue, runs jobs when Vast.ai is idle.

    idle_shutdown=True: exit cleanly (and self-delete the GCE VM if running
    on one) once both: (a) no slots active, and (b) no queued job is
    eligible to run on this consumer's free VRAM. Used for the cloud-VM
    agent path.

    kind: capacity-broadcast label distinguishing physical workstations
    (kind="local") from ephemeral cloud-agent VMs (kind="gcp", ...).
    No global error handler wraps the loop body: unexpected exceptions
    crash the agent visibly so the operator can diagnose.
    """
    from .local.slots import advance_slot, start_slot
    from ..targets import lookup_self
    if not gpu_type: gpu_type = _detect_gpu_type()
    total_vram_gb = max(1, _detect_local_vram_gb())
    hard_slot_cap = int(os.environ.get("WC_LOCAL_SLOTS", "0") or 0)
    # No default cap: local admission is governed by live VRAM/RAM/disk gates.
    _log(f"Agent started. kind={kind}  GPU: {gpu_type}  vram_gb={total_vram_gb}  hard_slot_cap={hard_slot_cap}")
    setup_agent_staging(_log)

    hostname = socket.gethostname()
    _log("init: pre-reap_orphan_workdirs")
    _reap_orphan_workdirs(hostname)
    _log("init: reap done; pre-JobStorage")

    initial_env: dict[str, str] = dict(os.environ)
    initial_gpu = gpu_type

    store = JobStorage(BUCKET)
    _log("init: JobStorage done")
    consumer_id = f"{kind}-{hostname}"
    slots: list[dict] = []
    agent_diag: dict = {}
    fleet_staging = os.environ.get("WISENT_FLEET_STAGING_DIR", "/tmp/wisent_fleet_staging")
    last_fleet_flush = time.time()
    FLEET_FLUSH_INTERVAL = _wc.FLEET_FLUSH_INTERVAL_S

    _last_cap = None
    while True:
        # Phase breadcrumbs for the 40GB a2-highgpu-1g first-iter hang.
        _log("loop: iter-start")
        try:
            from wisent.scripts.activations.raw.upload_worker import sweep as _upsweep
            _upsweep()  # keep the detached upload pool populated even
            # when extraction is gated and no live worker can chain-sweep
            # (else a restart leaves the pending pool orphaned).
        except Exception:
            pass
        _reap_dead_pid_workdirs()
        vast_active = _vast_has_renter()
        slots = [s for s in slots if advance_slot(s, store, vast_active, _log)]
        if _last_cap is not None:
            try:
                publish_capacity(
                    store, consumer_id, kind, _last_cap["free_slots"],
                    free_vram_gb=_last_cap["free_vram_gb"],
                    total_vram_gb=_last_cap["total_vram_gb"],
                    diag=_last_cap["diag"],
                )
            except Exception:
                pass
        if (
            time.time() - last_fleet_flush > FLEET_FLUSH_INTERVAL
            and not slots
        ):
            from .local.fleet_flush import spawn_fleet_flush
            if spawn_fleet_flush(fleet_staging, _log):
                _log("fleet staging flush running asynchronously")
            last_fleet_flush = time.time()
        t = lookup_self(hostname, source="auto")
        if t and t.kind == "local":
            registry_env = t.env_overrides or {}
            # Env overrides are now owned by systemd (/etc/wisent/wisent-agent.env).
            # Ignore registry env deltas so an external registry push cannot
            # trigger a pip reinstall loop or override local tuning.
            env_delta = {}
            if env_delta and not slots:
                for k, v in env_delta.items():
                    os.environ[k] = str(v)
                _log(f"Registry env override delta {env_delta}; pip_upgrade_and_exec for restart")
                from .local.version_check import pip_upgrade_and_exec as _upgrade_exec
                _upgrade_exec(_log)
            if t.gpu_type and t.gpu_type != initial_gpu and not slots:
                _log(f"Registry gpu_type {initial_gpu} -> {t.gpu_type}; pip_upgrade_and_exec for restart")
                from .local.version_check import pip_upgrade_and_exec as _upgrade_exec
                _upgrade_exec(_log)
            if t.vram_gb and int(t.vram_gb) != total_vram_gb:
                _log(f"Registry vram_gb override {total_vram_gb} -> {t.vram_gb}")
                total_vram_gb = int(t.vram_gb)
        # Disk eviction MUST run before pip_upgrade_and_exec. Workstation
        # crash-looped at disk_pct=89% (n_restarts=2380) because the
        # drift handler ran pip install which exhausted the remaining
        # 10 GB and crashed before stale_training_dirs eviction in
        # gate_and_maybe_evict could free disk. Running the gate first
        # gives pip room to work; the second gate call later in the
        # loop is a fast no-op when disk is already healthy.
        from .local.disk import gate_and_maybe_evict as _disk_gate_pre
        _pre_refuse, _pre_diag = _disk_gate_pre(_log)
        agent_diag.update(_pre_diag)
        _log("loop: pre-drain (detect_drift + import-smoketest subprocess)")
        from .local.version_check import maybe_drain_or_upgrade as _drain
        if _drain(slots, _log, kind=kind):
            time.sleep(POLL_INTERVAL); continue
        if vast_active:
            publish_capacity(store, consumer_id, kind, {}, free_vram_gb=0,
                             total_vram_gb=total_vram_gb, diag=dict(agent_diag))
            time.sleep(POLL_INTERVAL)
            continue

        used_vram = sum(_slot_vram(s) for s in slots)
        if any(_slot_is_exclusive(s) for s in slots):
            used_vram = total_vram_gb
        free_vram_gb = max(0, total_vram_gb - used_vram)
        smi_free = _smi_free_vram_gb()
        if smi_free >= 0 and smi_free < free_vram_gb:
            free_vram_gb = smi_free
        from .local.disk import gate_and_maybe_evict as _disk_gate
        _refuse_disk, _disk_diag = _disk_gate(_log)
        agent_diag.update(_disk_diag)
        if _refuse_disk:
            publish_capacity(store, consumer_id, kind, {},
                             free_vram_gb=0, total_vram_gb=total_vram_gb, diag=dict(agent_diag))
            time.sleep(10)
            continue
        vram_buffer_gb = _vram_safety_buffer_gb(total_vram_gb)
        settling_slots = [s for s in slots if _slot_waiting_for_vram(s)]
        if settling_slots:
            agent_diag["settling_slot_ids"] = [
                getattr(s.get("job"), "job_id", "") for s in settling_slots
            ]
            publish_capacity(store, consumer_id, kind, {},
                             free_vram_gb=0, total_vram_gb=total_vram_gb,
                             diag=dict(agent_diag))
            _last_cap = {"free_slots": {}, "free_vram_gb": 0,
                         "total_vram_gb": total_vram_gb,
                         "diag": dict(agent_diag)}
            time.sleep(POLL_INTERVAL)
            continue
        if free_vram_gb < vram_buffer_gb:
            agent_diag["vram_buffer_gb"] = vram_buffer_gb
            agent_diag["vram_buffer_free_gb"] = free_vram_gb
            publish_capacity(store, consumer_id, kind, {},
                             free_vram_gb=0, total_vram_gb=total_vram_gb,
                             diag=dict(agent_diag))
            _last_cap = {"free_slots": {}, "free_vram_gb": 0,
                         "total_vram_gb": total_vram_gb,
                         "diag": dict(agent_diag)}
            time.sleep(POLL_INTERVAL)
            continue
        if free_vram_gb > 0 and not slots:
            cuda_ok, cuda_detail = _cuda_child_available()
            agent_diag["cuda_child_ok"] = cuda_ok
            agent_diag["cuda_child_detail"] = cuda_detail
            agent_diag["cuda_child_checked_at"] = datetime.now(timezone.utc).isoformat()
            if not cuda_ok:
                _log(f"CUDA child probe failed; publishing zero capacity: {cuda_detail[:160]}")
                publish_capacity(store, consumer_id, kind, {},
                                 free_vram_gb=0, total_vram_gb=total_vram_gb,
                                 diag=dict(agent_diag))
                _last_cap = {"free_slots": {}, "free_vram_gb": 0,
                             "total_vram_gb": total_vram_gb,
                             "diag": dict(agent_diag)}
                time.sleep(POLL_INTERVAL)
                continue
        free_slots = _build_capacity_dict(gpu_type, free_vram_gb, total_vram_gb)
        publish_capacity(store, consumer_id, kind, free_slots,
                         free_vram_gb=free_vram_gb, total_vram_gb=total_vram_gb, diag=dict(agent_diag))
        _last_cap = {"free_slots": free_slots, "free_vram_gb": free_vram_gb, "total_vram_gb": total_vram_gb, "diag": dict(agent_diag)}

        # Cooperative yield: if a higher-priority queued job can't fit, evict
        # just enough lower-priority yieldable slots to make room. Runs BEFORE
        # the full-GPU early-return below because that is exactly when it's
        # needed. Inert (single any() over slots) unless a yieldable job runs.
        if _maybe_yield_for_priority(store, slots, gpu_type, total_vram_gb,
                                     free_vram_gb, kind, consumer_id, _log):
            continue  # re-loop: recompute free VRAM, then claim the freed room

        all_active_share_gpu = all(
            activation_extraction_must_share_gpu(
                getattr(s.get("job"), "command", "") or ""
            )
            for s in slots
        )
        slot_cap_reached = hard_slot_cap > 0 and len(slots) >= hard_slot_cap
        if free_vram_gb <= 0 or (slot_cap_reached and not all_active_share_gpu):
            time.sleep(10)
            continue
        # RAM gate: refuse new slots when MemAvailable drops below the
        # measured non-wisent baseline (ComfyUI, system daemons) plus a
        # dynamic safety buffer. This replaces the previous 30%-of-total
        # guess with a live reserve computed from /proc/*/status.
        _fr = _free_ram_gb()
        _ram_reserve = _static_ram_reserve_gb() + _ram_safety_buffer_gb()
        if 0 <= _fr < _ram_reserve:
            _log(f"RAM gate: {int(_fr)} GB free < {int(_ram_reserve)} GB reserve; skipping claims")
            time.sleep(10); continue

        # Centralized assignment writes job.assigned_to on the queue blob;
        # _job_eligible(consumer_id=...) below filters to ONLY the jobs this
        # agent owns. The coordinator's makespan matcher already made the
        # choice; this loop executes it.
        queued = store.list_jobs_fitting("queue", max_gpu_mem_gb=free_vram_gb, cap=2000)
        queued.sort(key=lambda j: (-getattr(j, "priority", 0), j.created_at))
        started = 0
        diag_vram_rejected = 0
        diag_eligibility_rejected = 0
        diag_eligible = 0
        max_claims = int(os.environ.get("WC_LOCAL_MAX_CLAIMS_PER_TICK", "0") or 0)
        raw_reserve = float(os.environ.get("WISENT_RAW_CLAIM_RESERVE_GB", "180") or 180)
        raw_min_free = float(os.environ.get(
            "WISENT_RAW_CLAIM_MIN_FREE_GB",
            os.environ.get("WISENT_RAW_HOT_FREE_TARGET_GB", "270"),
        ) or 270)
        raw_root = os.path.join(os.environ.get("TMPDIR", "/tmp"), "wisent_raw_pending")
        try:
            raw_free = shutil.disk_usage(raw_root).free / (1024 ** 3)
        except OSError:
            raw_free = -1.0
        raw_reserved = raw_reserve * sum(
            1 for s in slots
            if activation_extraction_must_share_gpu(getattr(s.get("job"), "command", "") or "")
        )
        diag_raw_disk_rejected = 0
        for job in queued:
            cmd = getattr(job, "command", "") or ""
            is_raw_share = activation_extraction_must_share_gpu(cmd)
            if is_raw_share and raw_free >= 0 and raw_free - raw_reserved - raw_reserve < raw_min_free:
                diag_raw_disk_rejected += 1
                agent_diag["raw_claim_free_gb"] = round(raw_free, 1)
                agent_diag["raw_claim_reserved_gb"] = round(raw_reserved, 1)
                continue
            all_active_share_gpu = all(
                activation_extraction_must_share_gpu(
                    getattr(s.get("job"), "command", "") or ""
                )
                for s in slots
            )
            slot_cap_reached = hard_slot_cap > 0 and len(slots) >= hard_slot_cap
            if slot_cap_reached and not (
                all_active_share_gpu and activation_extraction_must_share_gpu(cmd)
            ):
                continue
            need = max(
                int(getattr(job, "gpu_mem_gb", 0) or 0),
                estimate_gpu_memory(cmd),
            )
            # Hard VRAM safety buffer: refuse if declared use after admission
            # would leave less than the dynamic VRAM safety buffer. Use live
            # free VRAM, not only slot-declared usage, so external users such
            # as ComfyUI are included in the post-claim margin.
            claimable_vram_gb = max(0, free_vram_gb - _vram_safety_buffer_gb(total_vram_gb))
            if need > claimable_vram_gb:
                diag_vram_rejected += 1
                agent_diag["last_buffer_reject_job_id"] = job.job_id
                agent_diag["last_buffer_reject_at"] = datetime.now(timezone.utc).isoformat()
                agent_diag["last_buffer_reject_need_gb"] = need
                agent_diag["last_buffer_reject_claimable_gb"] = claimable_vram_gb
                continue
            # Also retain the slot-declared projection as a backstop for
            # cases where nvidia-smi temporarily under-reports a starting
            # child process.
            projected_used = sum(_slot_vram(s) for s in slots) + need
            if projected_used > total_vram_gb - _vram_safety_buffer_gb(total_vram_gb):
                diag_vram_rejected += 1
                agent_diag["last_buffer_reject_job_id"] = job.job_id
                agent_diag["last_buffer_reject_at"] = datetime.now(timezone.utc).isoformat()
                continue
            if not _job_eligible(job, gpu_type, total_vram_gb, kind=kind,
                                  consumer_id=consumer_id,
                                  active_slot_count=len(slots)):
                diag_eligibility_rejected += 1
                continue
            diag_eligible += 1
            new_slot = start_slot(store, job, hostname, _log, kind=kind)
            if new_slot is None:
                # apt-install refused or failed; job stays in queue/ for
                # another (cloud-kind or registry-fixed) agent to claim.
                continue
            slots.append(new_slot)
            free_vram_gb -= need
            if is_raw_share:
                raw_reserved += raw_reserve
            started += 1
            agent_diag["last_started_job_id"] = job.job_id
            agent_diag["last_started_at"] = datetime.now(timezone.utc).isoformat()
            if not is_raw_share:
                break
            if max_claims > 0 and started >= max_claims:
                break
            if free_vram_gb <= VRAM_SAFETY_BUFFER_GB:
                break
        agent_diag["queue_scanned"] = len(queued)
        agent_diag["vram_rejected"] = diag_vram_rejected
        agent_diag["raw_disk_rejected"] = diag_raw_disk_rejected
        agent_diag["eligibility_rejected"] = diag_eligibility_rejected
        agent_diag["eligible_count"] = diag_eligible
        agent_diag["claimed_this_loop"] = started
        agent_diag["last_claim_attempt_at"] = datetime.now(timezone.utc).isoformat()

        if started > 0:
            continue

        if started == 0:
            if idle_shutdown and not slots and _no_eligible_in_queue(
                store, gpu_type, total_vram_gb, free_vram_gb, kind=kind,
                consumer_id=consumer_id, active_slot_count=len(slots),
            ):
                _log("idle_shutdown: no slots + no eligible queued jobs; exiting")
                from .local.gcp_self import self_terminate
                self_terminate(_log)
                return
            time.sleep(POLL_INTERVAL)
