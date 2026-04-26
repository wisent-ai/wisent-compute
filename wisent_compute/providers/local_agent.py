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


def _job_eligible(job, gpu_type: str) -> bool:
    """Replicates the original local-agent claim rules: pinned-non-local rejects,
    pinned-local always claims, otherwise GPU type must match (or be unset)."""
    pinned = getattr(job, "pin_to_provider", False)
    if pinned and job.provider != "local":
        return False
    matches = job.provider == "local" or not job.gpu_type or job.gpu_type == gpu_type
    if not matches:
        return False
    cap = getattr(job, "max_cost_per_hour_usd", 0.0) or 0.0
    if cap > 0 and job.gpu_type:
        rate = _accel_hourly_rate(job.gpu_type, getattr(job, "preemptible", False))
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


def _build_capacity_dict(gpu_type: str, free: int, vram_gb: int) -> dict[str, int]:
    """Capacity broadcast: own gpu_type plus every compatible GCP type."""
    if not gpu_type or gpu_type == "cpu" or free <= 0:
        return {}
    out: dict[str, int] = {gpu_type: free}
    for compat in _compat_accel_types(vram_gb):
        out.setdefault(compat, free)
    return out


def run_agent(gpu_type: str = ""):
    """Main agent loop. Polls queue, runs jobs when Vast.ai is idle."""
    from .local.slots import advance_slot, start_slot
    from ..targets import lookup_self
    if not gpu_type:
        gpu_type = _detect_gpu_type()
    max_slots = max(1, int(os.environ.get("WC_LOCAL_SLOTS", "1")))
    vram_gb = _detect_local_vram_gb()
    _log(f"Agent started. GPU: {gpu_type}  max_slots={max_slots}  vram_gb={vram_gb}")

    # Snapshot the env vars and cli args we were started with — if the
    # registry asks for a different set, we exit so systemd restarts us
    # with the new state.
    initial_env_keys = {k for k, v in os.environ.items() if k.startswith(("HF_", "WISENT_", "WC_"))}
    initial_env = {k: os.environ[k] for k in initial_env_keys}
    initial_args = (gpu_type, max_slots)

    store = JobStorage(BUCKET)
    hostname = os.uname().nodename
    consumer_id = f"local-{hostname}"
    slots: list[dict] = []

    while True:
        t = lookup_self(hostname, source="gcs")
        if t and t.kind == "local":
            registry_env = t.env_overrides or {}
            registry_extra = (registry_env.items() ^ initial_env.items())
            registry_args = (t.gpu_type or gpu_type, max(1, int(t.slots)))
            if registry_extra and not slots:
                _log(f"Registry env override delta detected; exiting for systemd restart")
                raise SystemExit(0)
            if registry_args != initial_args and not slots:
                _log(f"Registry args delta {initial_args} -> {registry_args}; exit for restart")
                raise SystemExit(0)
            if t.vram_gb and int(t.vram_gb) != vram_gb:
                _log(f"Registry vram_gb update {vram_gb} -> {t.vram_gb}")
                vram_gb = int(t.vram_gb)

        vast_active = _vast_has_renter()
        slots = [s for s in slots if advance_slot(s, store, vast_active, _log)]
        if vast_active:
            publish_capacity(store, consumer_id, "local", {})
            time.sleep(POLL_INTERVAL)
            continue

        free = max_slots - len(slots)
        free_slots = _build_capacity_dict(gpu_type, free, vram_gb)
        publish_capacity(store, consumer_id, "local", free_slots)

        if free <= 0:
            time.sleep(10)
            continue

        # Pull eligible jobs from the queue, fill up to `free` of them.
        queued = store.list_jobs("queue")
        queued.sort(key=lambda j: (-getattr(j, "priority", 0), j.created_at))
        started = 0
        for job in queued:
            if started >= free:
                break
            if not _job_eligible(job, gpu_type):
                continue
            slots.append(start_slot(store, job, hostname, _log))
            started += 1

        if started == 0:
            time.sleep(POLL_INTERVAL)
