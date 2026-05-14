"""GPU detection, eligibility, capacity helpers for the local agent loop.

Extracted from providers/local_agent.py to keep the parent file under
the 300-line cap. The 0.4.100 cut adds consumer_id + assigned_to
enforcement to _job_eligible so the coordinator's centralized matcher
(_assign_jobs_to_agents in coordinator.py) can pin queued jobs to
specific agents instead of every agent racing to claim from a global
FIFO. Without this enforcement, fleet-aware LPT scheduling collapses to
greedy first-come-first-served and the makespan grows.
"""
from __future__ import annotations

import json
import os
import subprocess
import urllib.request

from ....config import estimate_gpu_memory
from ....models import GPU_HOURLY_RATE_USD, SPOT_DISCOUNT
from ....queue.storage import JobStorage


VAST_API = "https://console.vast.ai/api/v0"


def _accel_hourly_rate(accel_type: str, preemptible: bool) -> float:
    """$/hour for one accelerator at the given pricing model.

    Mirrors scheduler._accel_hourly_rate so both consumers apply the same
    cost-cap rule. Local agents are typically free hardware, but any job
    with max_cost_per_hour_usd set still respects that cap.
    """
    base = GPU_HOURLY_RATE_USD.get(accel_type, 0.0)
    if not preemptible:
        return base
    return base * SPOT_DISCOUNT.get(accel_type, 0.5)


def _vast_has_renter() -> bool:
    """Check if any Vast.ai instance is currently rented on this machine.

    No exception swallow: a failed API call would otherwise be silently
    treated as 'no renter' and the agent would claim jobs on top of a
    paid Vast.ai renter, wasting both the renter's GPU time and ours.
    Caller must crash visibly so the operator notices Vast.ai outage.
    """
    api_key = os.environ.get("VAST_API_KEY", "").strip()
    if not api_key:
        return False
    req = urllib.request.Request(
        f"{VAST_API}/instances?owner=me",
        headers={"Authorization": f"Bearer {api_key}"},
    )
    resp = urllib.request.urlopen(req)
    instances = json.loads(resp.read())
    return any(
        i.get("actual_status") == "running"
        for i in instances.get("instances", [])
    )


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


def _detect_local_vram_gb() -> int:
    """Total VRAM in GB on the first detected GPU, 0 if none."""
    try:
        r = subprocess.run(
            ["nvidia-smi", "--query-gpu=memory.total",
             "--format=csv,noheader,nounits"],
            capture_output=True, text=True,
        )
        if r.returncode == 0:
            mib = int(r.stdout.strip().splitlines()[0])
            return mib // 1024
    except Exception:
        pass
    return 0


def _smi_free_vram_gb() -> int:
    """Live nvidia-smi free VRAM in GB on the first GPU, -1 if unreadable."""
    try:
        r = subprocess.run(
            ["nvidia-smi", "--query-gpu=memory.free",
             "--format=csv,noheader,nounits"],
            capture_output=True, text=True,
        )
        if r.returncode == 0:
            mib = int(r.stdout.strip().splitlines()[0])
            return mib // 1024
    except Exception:
        pass
    return -1


def _compat_accel_types(local_vram_gb: int) -> list[str]:
    """Every GCP gpu_type whose required VRAM tier <= local VRAM."""
    from ....models import GPU_SIZING
    accels: list[str] = []
    for tier, (_, accel) in sorted(GPU_SIZING.get("gcp", {}).items()):
        if local_vram_gb >= tier and accel and accel not in accels:
            accels.append(accel)
    return accels


def _job_eligible(job, gpu_type: str, vram_gb: int = 0, kind: str = "local",
                   consumer_id: str = "") -> bool:
    """Local-agent claim rules.

    NEW (0.4.100): if job.assigned_to was set by the centralized
    coordinator matcher, only the agent whose consumer_id matches may
    claim. Empty assigned_to means unassigned and any-eligible-agent may
    claim (pre-0.4.100 back-compat).
    """
    assigned = getattr(job, "assigned_to", "") or ""
    if assigned and consumer_id and assigned != consumer_id:
        return False
    from ....config import LOCAL_ONLY_MODELS
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


def _build_capacity_dict(gpu_type: str, free_vram_gb: int,
                          total_vram_gb: int) -> dict[str, int]:
    """Slot-shaped capacity broadcast for back-compat schedulers."""
    from ....models import GPU_SIZING
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
    from ....config import EXCLUSIVE_MODELS
    import re
    m = re.search(r"--model\s+(\S+)",
                   getattr(slot.get("job"), "command", "") or "")
    return bool(m and m.group(1).strip("'\"") in EXCLUSIVE_MODELS)


def _slot_vram(slot: dict) -> int:
    job = slot.get("job")
    return max(
        int(getattr(job, "gpu_mem_gb", 0) or 0),
        estimate_gpu_memory(getattr(job, "command", "") or ""),
    )


def _no_eligible_in_queue(store: JobStorage, gpu_type: str,
                           total_vram_gb: int, free_vram_gb: int,
                           kind: str = "local",
                           consumer_id: str = "") -> bool:
    """True when no queued job fits + is eligible for this consumer."""
    queued = store.list_jobs("queue")
    for job in queued:
        need = max(
            int(getattr(job, "gpu_mem_gb", 0) or 0),
            estimate_gpu_memory(getattr(job, "command", "") or ""),
        )
        if need > free_vram_gb:
            continue
        if not _job_eligible(job, gpu_type, total_vram_gb, kind=kind,
                              consumer_id=consumer_id):
            continue
        return False
    return True


def _staging_size_gb(d: str) -> float:
    """Total size of files under d in GB. 0 if dir missing."""
    if not os.path.isdir(d):
        return 0.0
    total = 0
    for root, _, files in os.walk(d):
        for f in files:
            try:
                total += os.path.getsize(os.path.join(root, f))
            except OSError:
                pass
    return total / (1024 ** 3)
