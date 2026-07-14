"""Local GPU agent: runs on the workstation, polls GCS queue, respects Vast.ai.

Usage: wc agent --gpu-type nvidia-rtx-4090
Runs as a long-lived daemon. Picks up jobs when Vast.ai has no active renter.
"""
from __future__ import annotations

import json
import os
import shutil
import stat
import socket
import subprocess
import sys
import time
from datetime import datetime, timezone

from .. import constants as _wc
from ..config import BUCKET, estimate_gpu_memory
from ..models import activation_extraction_must_share_gpu
from ..queue.capacity import publish_capacity
from ..queue.storage import JobStorage
from ..ram_sizing import effective_job_ram_request
from .local.admission import (
    AdmissionPolicy,
    AdmissionReason,
    JobRequest,
    ProjectedClaims,
    evaluate_admission,
)
from .local.telemetry import collect_host_snapshot
from .local.external import occupancy_diagnostics
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
from .local.disk import acquire_workload_lock, release_workload_lock, run_cleanup_once


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


def _persisted_disk_low_bytes() -> int | None:
    """Read the last canonical low watermark from janitor-owned safe state.

    Cleanup may be unable to reach the registry during agent startup.  Reuse a
    threshold only when it came from the janitor's owner-controlled, no-follow
    state file and the report identifies a validated canonical policy.
    """
    current = os.path.expanduser("~")
    state_path = os.path.join(current, ".cache", "wisent-compute", "disk-cleanup-state.json")
    try:
        for directory in (
            current,
            os.path.join(current, ".cache"),
            os.path.join(current, ".cache", "wisent-compute"),
        ):
            info = os.lstat(directory)
            if stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode):
                return None
            if info.st_uid != os.geteuid():
                return None
        fd = os.open(state_path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
        try:
            info = os.fstat(fd)
            if not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid():
                return None
            if info.st_size > 1024 * 1024:
                return None
            with os.fdopen(fd, "r", encoding="utf-8", closefd=False) as handle:
                state = json.load(handle)
        finally:
            os.close(fd)
    except (OSError, ValueError, TypeError):
        return None
    if not isinstance(state, dict) or state.get("version") != 1:
        return None
    report = state.get("report")
    if not isinstance(report, dict):
        return None
    digest = report.get("policy_digest")
    low_bytes = report.get("low_bytes")
    if not isinstance(digest, str) or len(digest) != 64:
        return None
    try:
        int(digest, 16)
    except ValueError:
        return None
    if isinstance(low_bytes, int) and not isinstance(low_bytes, bool) and low_bytes > 0:
        return low_bytes
    return None


def _validated_report_low_bytes(report: dict) -> int | None:
    """Return a threshold only from a successfully resolved policy report."""
    digest = report.get("policy_digest")
    low_bytes = report.get("low_bytes")
    if not isinstance(digest, str) or len(digest) != 64:
        return None
    try:
        int(digest, 16)
    except ValueError:
        return None
    if isinstance(low_bytes, int) and not isinstance(low_bytes, bool) and low_bytes > 0:
        return low_bytes
    return None


def _disk_pressure_unresolved(low_bytes: int | None, free_bytes: int | None) -> bool:
    """Fail admission closed until both policy threshold and free space are known."""
    return low_bytes is None or free_bytes is None or free_bytes < low_bytes


def _release_slot_workload_lock(slot: dict, log_fn) -> None:
    handle = slot.pop("disk_cleanup_lock", None)
    if handle is None:
        return
    try:
        release_workload_lock(handle)
    except OSError as exc:
        log_fn(f"disk cleanup workload lock release failed: {type(exc).__name__}")


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
                _release_slot_workload_lock(s, log_fn)
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
    from .local.slots import advance_slot, reconstruct_slots, start_slot
    from ..targets import lookup_self
    if not gpu_type: gpu_type = _detect_gpu_type()
    total_vram_gb = max(1, _detect_local_vram_gb())
    hard_slot_cap = int(os.environ.get("WC_LOCAL_SLOTS", "0") or 0)
    # No default cap: local admission is governed by live VRAM/RAM/disk gates.
    _log(f"Agent started. kind={kind}  GPU: {gpu_type}  vram_gb={total_vram_gb}  hard_slot_cap={hard_slot_cap}")
    setup_agent_staging(_log)

    hostname = socket.gethostname()
    _log("init: legacy workdir reaping disabled; cleanup is policy-owned")

    initial_env: dict[str, str] = dict(os.environ)
    initial_gpu = gpu_type

    store = JobStorage(BUCKET)
    _log("init: JobStorage done")
    consumer_id = f"{kind}-{hostname}"
    slots: list[dict] = reconstruct_slots(store, _log)
    agent_diag: dict = {}
    fleet_staging = os.environ.get("WISENT_FLEET_STAGING_DIR", "/tmp/wisent_fleet_staging")
    last_fleet_flush = time.time()
    FLEET_FLUSH_INTERVAL = _wc.FLEET_FLUSH_INTERVAL_S

    _last_cap = None
    disk_low_bytes = _persisted_disk_low_bytes()
    if disk_low_bytes is not None:
        _log("init: loaded validated disk low watermark from janitor state")
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
        vast_active = _vast_has_renter()
        advanced_slots = []
        for slot in slots:
            if advance_slot(slot, store, vast_active, _log):
                advanced_slots.append(slot)
            else:
                _release_slot_workload_lock(slot, _log)
        slots = advanced_slots
        try:
            cleanup_report = run_cleanup_once(
                active_slot_count=len(slots), log_fn=_log,
            )
        except BaseException as exc:
            _log(f"disk cleanup pass failed: {type(exc).__name__}")
            cleanup_report = {
                "outcome": "runtime_error",
                "active_slot_count": len(slots),
                "errors": [f"runtime:{type(exc).__name__}"],
            }
        agent_diag["disk_cleanup"] = cleanup_report
        reported_low = _validated_report_low_bytes(cleanup_report)
        if reported_low is not None:
            disk_low_bytes = reported_low
        try:
            current_free_bytes = shutil.disk_usage(os.path.expanduser("~")).free
        except OSError:
            current_free_bytes = None
        disk_policy_known = disk_low_bytes is not None
        disk_pressure_unresolved = _disk_pressure_unresolved(
            disk_low_bytes, current_free_bytes,
        )
        agent_diag["disk_cleanup_policy_known"] = disk_policy_known
        agent_diag["disk_pressure_unresolved"] = disk_pressure_unresolved
        if disk_pressure_unresolved:
            zero_cap = {
                "free_slots": {},
                "free_vram_gb": 0,
                "total_vram_gb": total_vram_gb,
                "diag": dict(agent_diag),
            }
            try:
                publish_capacity(
                    store, consumer_id, kind, {}, free_vram_gb=0,
                    total_vram_gb=total_vram_gb, diag=zero_cap["diag"],
                )
            except Exception:
                pass
            _last_cap = zero_cap
            time.sleep(POLL_INTERVAL)
            continue
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
        agent_diag["occupancy"] = occupancy_diagnostics(
            slots,
            policies=(t.external_workloads if t and t.kind == "local" else ()),
        )
        # Cleanup already ran before upgrade checks. This gate is now strictly
        # admission/diagnostics-only and has no destructive side effects.
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
        admission_mode = os.environ.get("WC_ADMISSION_POLICY_V2", "legacy").strip().lower()
        if admission_mode not in {"legacy", "shadow", "enforce"}:
            admission_mode = "legacy"
        snapshot = collect_host_snapshot(
            gpu_total_gb=total_vram_gb,
            gpu_free_gb=free_vram_gb if smi_free >= 0 else -1.0,
        )
        admission_policy = AdmissionPolicy(
            ram_safety_headroom_gb=_ram_safety_buffer_gb(),
            vram_safety_headroom_gb=vram_buffer_gb,
        )
        agent_diag["admission_v2_mode"] = admission_mode
        agent_diag["host"] = {
            "mem_total_gb": round(snapshot.mem_total_gb, 2),
            "mem_available_gb": round(snapshot.mem_available_gb, 2),
            "swap_free_gb": round(snapshot.swap_free_gb, 2),
            "gpu_total_gb": round(snapshot.gpu_total_gb, 2),
            "gpu_free_gb": round(snapshot.gpu_free_gb, 2),
            "timestamp": snapshot.timestamp,
            "telemetry_quality": snapshot.telemetry_quality,
        }
        agent_diag["policy"] = {
            "ram_safety_headroom_gb": round(admission_policy.ram_safety_headroom_gb, 2),
            "vram_safety_headroom_gb": round(admission_policy.vram_safety_headroom_gb, 2),
        }
        settling_slots = [s for s in slots if _slot_waiting_for_vram(s)]
        agent_diag["settling_timed_out_slot_ids"] = [
            getattr(slot.get("job"), "job_id", "")
            for slot in slots
            if slot.get("settling_timed_out")
        ]
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
            agent_diag["last_gate_reason"] = (
                AdmissionReason.HARD_SLOT_CAP.value
                if slot_cap_reached
                else AdmissionReason.EXCLUSIVE_CONFLICT.value
                if any(_slot_is_exclusive(slot) for slot in slots)
                else AdmissionReason.INSUFFICIENT_VRAM.value
            )
            time.sleep(10)
            continue
        # Legacy is retained only for rollback and shadow comparison. Enforce
        # mode exclusively uses the per-job projected equation below.
        legacy_ram_blocked = False
        if admission_mode != "enforce":
            _fr = _free_ram_gb()
            _ram_reserve = _static_ram_reserve_gb() + _ram_safety_buffer_gb()
            legacy_ram_blocked = 0 <= _fr < _ram_reserve
            if legacy_ram_blocked:
                agent_diag["legacy_ram_blocked"] = True
                agent_diag["legacy_ram_free_gb"] = round(_fr, 2)
                agent_diag["legacy_ram_reserve_gb"] = round(_ram_reserve, 2)
                if admission_mode == "legacy":
                    _log(
                        f"RAM gate: {int(_fr)} GB free < "
                        f"{int(_ram_reserve)} GB reserve; skipping claims"
                    )
                    time.sleep(10)
                    continue

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
        projected_claims = ProjectedClaims()
        admission_reasons: dict[str, int] = {}
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
                admission_reasons[AdmissionReason.DISK_GATE.value] = (
                    admission_reasons.get(AdmissionReason.DISK_GATE.value, 0) + 1
                )
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
                admission_reasons[AdmissionReason.HARD_SLOT_CAP.value] = (
                    admission_reasons.get(AdmissionReason.HARD_SLOT_CAP.value, 0) + 1
                )
                continue
            if not _job_eligible(
                job,
                gpu_type,
                total_vram_gb,
                kind=kind,
                consumer_id=consumer_id,
                active_slot_count=len(slots),
            ):
                diag_eligibility_rejected += 1
                assigned = getattr(job, "assigned_to", "") or ""
                reason = (
                    AdmissionReason.ASSIGNED_TO_OTHER_AGENT.value
                    if assigned and assigned != consumer_id
                    else AdmissionReason.EXCLUSIVE_CONFLICT.value
                    if bool(getattr(job, "exclusive", False)) and slots
                    else "INELIGIBLE_TARGET"
                )
                admission_reasons[reason] = admission_reasons.get(reason, 0) + 1
                agent_diag["last_gate_reason"] = reason
                continue
            declared_vram_gb = int(getattr(job, "gpu_mem_gb", 0) or 0)
            need = (
                declared_vram_gb
                if declared_vram_gb > 0
                else estimate_gpu_memory(cmd)
            )
            ram_request_gb, ram_source = effective_job_ram_request(
                job,
                store=store,
                mem_total_gb=snapshot.mem_total_gb,
                mem_available_gb=snapshot.mem_available_gb,
                ram_safety_headroom_gb=admission_policy.ram_safety_headroom_gb,
            )
            request = JobRequest(
                ram_request_gb=ram_request_gb,
                vram_request_gb=float(need),
                ram_estimation_source=ram_source,
                vram_estimation_source=(
                    "job.gpu_mem_gb"
                    if declared_vram_gb > 0
                    else "command_estimate"
                ),
                exclusive=bool(getattr(job, "exclusive", False)),
                priority=int(getattr(job, "priority", 0) or 0),
            )
            if admission_mode != "legacy":
                decision = evaluate_admission(
                    snapshot, request, projected_claims, admission_policy,
                )
                reason = decision.reason_code.value
                admission_reasons[reason] = admission_reasons.get(reason, 0) + 1
                agent_diag["last_admission_v2"] = {
                    "job_id": job.job_id,
                    **decision.as_dict(),
                    "ram_estimation_source": ram_source,
                }
                if admission_mode == "shadow":
                    agent_diag["last_admission_v2"]["legacy_ram_blocked"] = legacy_ram_blocked
                    if decision.accepted:
                        projected_claims.reserve(request)
                    if legacy_ram_blocked:
                        continue
                elif not decision.accepted:
                    if decision.reason_code is AdmissionReason.INSUFFICIENT_VRAM:
                        diag_vram_rejected += 1
                    continue
            # Hard VRAM safety buffer: refuse if declared use after admission
            # would leave less than the dynamic VRAM safety buffer. Use live
            # free VRAM, not only slot-declared usage, so external users such
            # as ComfyUI are included in the post-claim margin.
            claimable_vram_gb = max(0, free_vram_gb - _vram_safety_buffer_gb(total_vram_gb))
            if admission_mode != "enforce" and need > claimable_vram_gb:
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
            if (
                admission_mode != "enforce"
                and projected_used > total_vram_gb - _vram_safety_buffer_gb(total_vram_gb)
            ):
                diag_vram_rejected += 1
                agent_diag["last_buffer_reject_job_id"] = job.job_id
                agent_diag["last_buffer_reject_at"] = datetime.now(timezone.utc).isoformat()
                continue
            diag_eligible += 1
            try:
                workload_lock = acquire_workload_lock()
            except OSError as exc:
                _log(f"disk cleanup workload lock unavailable: {type(exc).__name__}")
                agent_diag["disk_cleanup_admission"] = "lock_error"
                break
            if workload_lock is None:
                agent_diag["disk_cleanup_admission"] = "cleanup_in_progress"
                break
            try:
                new_slot = start_slot(store, job, hostname, _log, kind=kind)
            except BaseException:
                release_workload_lock(workload_lock)
                raise
            if new_slot is None:
                # Admission failed before spawn; do not retain a workload lock.
                release_workload_lock(workload_lock)
                continue
            new_slot["disk_cleanup_lock"] = workload_lock
            slots.append(new_slot)
            if admission_mode == "enforce":
                projected_claims.reserve(request)
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
            if free_vram_gb <= vram_buffer_gb:
                break
        agent_diag["queue_scanned"] = len(queued)
        agent_diag["vram_rejected"] = diag_vram_rejected
        agent_diag["raw_disk_rejected"] = diag_raw_disk_rejected
        agent_diag["eligibility_rejected"] = diag_eligibility_rejected
        agent_diag["eligible_count"] = diag_eligible
        agent_diag["claimed_this_loop"] = started
        agent_diag["admission_v2_reasons"] = dict(sorted(admission_reasons.items()))
        agent_diag["projected_claims"] = {
            "ram_gb": round(projected_claims.ram_gb, 2),
            "vram_gb": round(projected_claims.vram_gb, 2),
        }
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
