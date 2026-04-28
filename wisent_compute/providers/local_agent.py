"""Local GPU agent: runs on the workstation, polls GCS queue, respects Vast.ai.

Usage: wc agent --gpu-type nvidia-rtx-4090
Runs as a long-lived daemon. Picks up jobs when Vast.ai has no active renter.
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import time
import urllib.request
from datetime import datetime

from ..config import BUCKET
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

POLL_INTERVAL = 60
HEARTBEAT_INTERVAL = 300
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


def _job_eligible(job, gpu_type: str, vram_gb: int = 0) -> bool:
    """Local-agent claim rules. Matches if pinned-local; or job.gpu_type is empty;
    or job.gpu_type equals our own; or job.gpu_type is in the VRAM-compatibility
    list (we have at least its tier of VRAM). The compatibility branch lets the
    box claim A100/L4 jobs it was yielded by the scheduler's compat broadcast."""
    pinned = getattr(job, "pin_to_provider", False)
    if pinned and job.provider != "local":
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


def _slot_vram(slot: dict) -> int:
    """Best-effort VRAM cost a running slot occupies."""
    job = slot.get("job")
    return int(getattr(job, "gpu_mem_gb", 0) or 0)


def _no_eligible_in_queue(store: JobStorage, gpu_type: str, total_vram_gb: int,
                          free_vram_gb: int) -> bool:
    """True when no queued job could even hypothetically fit + be eligible.

    Pure condition check — no timer, no constant duration. Signal is "queue
    holds nothing this consumer can run", not "we've waited N seconds".
    """
    queued = store.list_jobs("queue")
    for job in queued:
        need = int(getattr(job, "gpu_mem_gb", 0) or 0)
        if need > free_vram_gb:
            continue
        if not _job_eligible(job, gpu_type, total_vram_gb):
            continue
        return False
    return True


def run_agent(gpu_type: str = "", idle_shutdown: bool = False):
    """Main agent loop. Polls queue, runs jobs when Vast.ai is idle.

    idle_shutdown=True: exit cleanly (and self-delete the GCE VM if running on
    one) once both: (a) no slots active, and (b) no queued job is eligible to
    run on this consumer's free VRAM. Used for the cloud-VM agent path. The
    workstation/Vast.ai path leaves it False so the daemon stays up.
    """
    from .local.slots import advance_slot, start_slot
    from ..targets import lookup_self
    if not gpu_type:
        gpu_type = _detect_gpu_type()
    total_vram_gb = max(1, _detect_local_vram_gb())
    # WC_LOCAL_SLOTS retained as a HARD safety cap (defaults to no cap when 0).
    hard_slot_cap = int(os.environ.get("WC_LOCAL_SLOTS", "0") or 0)
    _log(f"Agent started. GPU: {gpu_type}  vram_gb={total_vram_gb}  hard_slot_cap={hard_slot_cap}")

    initial_env: dict[str, str] = dict(os.environ)
    initial_gpu = gpu_type

    store = JobStorage(BUCKET)
    hostname = os.uname().nodename
    consumer_id = f"local-{hostname}"
    slots: list[dict] = []

    while True:
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

        vast_active = _vast_has_renter()
        slots = [s for s in slots if advance_slot(s, store, vast_active, _log)]
        if vast_active:
            publish_capacity(store, consumer_id, "local", {}, free_vram_gb=0,
                             total_vram_gb=total_vram_gb)
            time.sleep(POLL_INTERVAL)
            continue

        used_vram = sum(_slot_vram(s) for s in slots)
        free_vram_gb = max(0, total_vram_gb - used_vram)
        free_slots = _build_capacity_dict(gpu_type, free_vram_gb, total_vram_gb)
        publish_capacity(store, consumer_id, "local", free_slots,
                         free_vram_gb=free_vram_gb, total_vram_gb=total_vram_gb)

        if free_vram_gb <= 0 or (hard_slot_cap > 0 and len(slots) >= hard_slot_cap):
            time.sleep(10)
            continue

        queued = store.list_jobs("queue")
        queued.sort(key=lambda j: (-getattr(j, "priority", 0), j.created_at))
        started = 0
        for job in queued:
            if hard_slot_cap > 0 and len(slots) >= hard_slot_cap:
                break
            need = int(getattr(job, "gpu_mem_gb", 0) or 0)
            if need > free_vram_gb:
                continue
            if not _job_eligible(job, gpu_type, total_vram_gb):
                continue
            slots.append(start_slot(store, job, hostname, _log))
            free_vram_gb -= need
            started += 1

        if started == 0:
            if idle_shutdown and not slots and _no_eligible_in_queue(
                store, gpu_type, total_vram_gb, free_vram_gb,
            ):
                _log("idle_shutdown: no slots + no eligible queued jobs; exiting")
                from .local.gcp_self import self_terminate
                self_terminate(_log)
                return
            time.sleep(POLL_INTERVAL)
