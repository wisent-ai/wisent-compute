"""Local GPU agent: runs on the workstation, polls GCS queue, respects Vast.ai.

Usage: wc agent --gpu-type nvidia-rtx-4090
Runs as a long-lived daemon. Picks up jobs when Vast.ai has no active renter.
"""
from __future__ import annotations

import glob
import os
import shutil
import socket
import sys
import tempfile
import time
from datetime import datetime, timezone

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
    _slot_rss, _free_ram_gb, _total_ram_gb,
    _smi_free_vram_gb,
    _staging_size_gb,
    _vast_has_renter,
)
from .local.disk.staging import setup_agent_staging


POLL_INTERVAL = 10
HEARTBEAT_INTERVAL = 300
# Time to sleep after a successful claim so nvidia-smi can reflect the
# freshly-spawned subprocess's CUDA allocation before the next iteration
# decides whether to claim again. Empirically a torch model load starts
# allocating GPU memory within ~5 seconds of subprocess start.
SETTLE_AFTER_CLAIM_SECONDS = 5
# Hard VRAM safety buffer at admission. The agent refuses to claim a
# job if accepting it would leave less than this margin between
# declared total VRAM use and the GPU's physical capacity. Catches the
# class of failure where neighbor processes' actual peak exceeds their
# declared gpu_mem_gb (estimate_gpu_memory has been observed to
# under-call by 5-10 GB on 7-8B activation extraction workloads). The
# buffer is independent of the per-job multipliers because it's the
# LAST line of defense — if the per-job estimate is wrong, this catches
# it before the n+1th job OOMs the entire VM.
VRAM_SAFETY_BUFFER_GB = 8
IDLE_DEVICE_OVERHEAD_GB = 1


def _effective_registry_vram(physical_vram_gb: int, registry_vram_gb: int) -> int:
    """Apply a registry override without exceeding measured GPU capacity."""
    return min(physical_vram_gb, registry_vram_gb)


def _claim_search_vram_limit(slots: list[dict], total_vram_gb: int,
                             free_vram_gb: int) -> int:
    """Include the nominal full-device tier despite small driver overhead."""
    if not slots and free_vram_gb >= total_vram_gb - IDLE_DEVICE_OVERHEAD_GB:
        return total_vram_gb
    return free_vram_gb


def _is_full_device_claim(slots: list[dict], need: int,
                          total_vram_gb: int, free_vram_gb: int) -> bool:
    return (not slots and need == total_vram_gb
            and free_vram_gb >= total_vram_gb - IDLE_DEVICE_OVERHEAD_GB)


def _exceeds_vram_admission_limit(slots: list[dict], need: int,
                                  total_vram_gb: int, free_vram_gb: int) -> bool:
    """Reject unsafe packing while permitting one exact-capacity idle claim."""
    projected_used = sum(_slot_vram(slot) for slot in slots) + need
    return (not _is_full_device_claim(slots, need, total_vram_gb, free_vram_gb)
            and projected_used > total_vram_gb - VRAM_SAFETY_BUFFER_GB)

# Cooperative-yield anti-thrash floor: never evict a yieldable slot that has
# run for less than this, so a just-(re)started background job gets real work
# done before it can be bumped again. Pairs with Job.max_yields_before_protected.
MIN_RUNTIME_BEFORE_YIELD_S = 300


def _log(msg):
    ts = datetime.now().strftime("%H:%M:%S")
    sys.stderr.write(f"[{ts}] [agent] {msg}\n")
    sys.stderr.flush()


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
    physical_vram_gb = max(1, _detect_local_vram_gb(gpu_type))
    total_vram_gb = physical_vram_gb
    hard_slot_cap = int(os.environ.get("WC_LOCAL_SLOTS", "0") or 0)
    if kind == "local" and hard_slot_cap <= 0: hard_slot_cap = 1
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
    FLEET_FLUSH_INTERVAL = 180  # ~20 commits/hour/agent << 200/hour HF cap

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
        if time.time() - last_fleet_flush > FLEET_FLUSH_INTERVAL or _staging_size_gb(fleet_staging) > 5:
            from wisent.core.reading.modules.utilities.data.sources.hf.hf_writers import flush_staging_dir
            if os.path.isdir(fleet_staging) and any(os.scandir(fleet_staging)):
                flush_staging_dir(fleet_staging)
                shutil.rmtree(fleet_staging)
                os.makedirs(fleet_staging, exist_ok=True)
                _log("flushed fleet staging dir to HF (1 commit)")
            last_fleet_flush = time.time()
        t = lookup_self(hostname, source="auto")
        if t and t.kind == "local":
            registry_env = {}
            for k, v in (t.env_overrides or {}).items():
                # Registry slots=0 means "do not advertise this target by
                # bundled registry defaults". It must not override an explicit
                # systemd/local operator cap such as WC_LOCAL_SLOTS=1, or the
                # env-drift restarter loops forever trying to restart into an
                # impossible state.
                if k == "WC_LOCAL_SLOTS" and str(v).strip() in ("", "0"):
                    if str(initial_env.get("WC_LOCAL_SLOTS", "")).strip():
                        continue
                registry_env[k] = v
            env_delta = {
                k: str(v) for k, v in registry_env.items()
                if str(initial_env.get(k, "")) != str(v)
            }
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
            if t.vram_gb:
                registry_vram_gb = int(t.vram_gb)
                effective_vram_gb = _effective_registry_vram(
                    physical_vram_gb, registry_vram_gb)
                if effective_vram_gb != total_vram_gb:
                    _log(f"Registry vram_gb override {total_vram_gb} -> "
                         f"{effective_vram_gb} (requested={registry_vram_gb}, "
                         f"physical_cap={physical_vram_gb})")
                    total_vram_gb = effective_vram_gb
        vast_active = _vast_has_renter()
        slots = [s for s in slots if advance_slot(s, store, vast_active, _log)]
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
        # RAM gate: refuse new slots when system free RAM (MemAvailable, which
        # captures the forked-worker procs + page-cache the per-slot RSS sum
        # missed) drops below a MemTotal reserve. Prevents the ~100G OOM.
        _fr = _free_ram_gb()
        if 0 <= _fr < _total_ram_gb() * 0.30:
            time.sleep(10); continue

        # Centralized assignment writes job.assigned_to on the queue blob;
        # _job_eligible(consumer_id=...) below filters to ONLY the jobs this
        # agent owns. The coordinator's makespan matcher already made the
        # choice; this loop executes it.
        queued = store.list_jobs_fitting(
            "queue",
            max_gpu_mem_gb=_claim_search_vram_limit(
                slots, total_vram_gb, free_vram_gb),
            cap=2000,
        )
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
            if (need > free_vram_gb
                    and not _is_full_device_claim(
                        slots, need, total_vram_gb, free_vram_gb)):
                diag_vram_rejected += 1
                continue
            # Keep the safety margin between packed jobs. A request for the
            # exact device capacity on an otherwise idle GPU is inherently a
            # single-slot claim: it consumes all free capacity and cannot
            # admit a neighbor, so reserving packing headroom would make the
            # device's own nominal resource tier impossible to run.
            if _exceeds_vram_admission_limit(
                    slots, need, total_vram_gb, free_vram_gb):
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
            time.sleep(SETTLE_AFTER_CLAIM_SECONDS)
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
