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
PROVIDER_GCP = "gcp"
PROVIDER_LOCAL = "local"
GPU_TYPE_CPU = "cpu"
VAST_INSTANCES_KEY = "instances"
VAST_ACTUAL_STATUS_KEY = "actual_status"
VAST_STATUS_RUNNING = "running"
GEFORCE_PREFIX = "geforce-"
NVIDIA_PREFIX = "nvidia-"
EXCLUSIVE_ATTR = "exclusive"
SLOT_JOB_KEY = "job"
SLOT_PROC_KEY = "proc"
MEM_AVAILABLE_LABEL = "MemAvailable:"
MEM_TOTAL_LABEL = "MemTotal:"
VM_RSS_LABEL = "VmRSS:"
KIB_PER_GIB = 1024 ** 2
BYTES_PER_GIB = 1024 ** 3


def _dict_value(data: dict, key, default):
    return data[key] if key in data else default


def _dict_number(data: dict, key, default=0) -> int:
    value = _dict_value(data, key, default)
    return int(value if value is not None else default)


def _accel_hourly_rate(accel_type: str, preemptible: bool) -> float:
    """$/hour for one accelerator at the given pricing model.

    Mirrors scheduler._accel_hourly_rate so both consumers apply the same
    cost-cap rule. Local agents are typically free hardware, but any job
    with max_cost_per_hour_usd set still respects that cap.
    """
    if accel_type not in GPU_HOURLY_RATE_USD:
        raise KeyError(f"missing hourly GPU rate for {accel_type}")
    base = GPU_HOURLY_RATE_USD[accel_type]
    if not preemptible:
        return base
    if accel_type not in SPOT_DISCOUNT:
        raise KeyError(f"missing spot discount for {accel_type}")
    return base * SPOT_DISCOUNT[accel_type]


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
        _dict_value(i, VAST_ACTUAL_STATUS_KEY, None) == VAST_STATUS_RUNNING
        for i in _dict_value(instances, VAST_INSTANCES_KEY, [])
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
            return name.lower().replace(" ", "-").replace(GEFORCE_PREFIX, NVIDIA_PREFIX)
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
    for tier, (_, accel) in sorted(_dict_value(GPU_SIZING, PROVIDER_GCP, {}).items()):
        if local_vram_gb >= tier and accel and accel not in accels:
            accels.append(accel)
    return accels


def _job_eligible(job, gpu_type: str, vram_gb: int = 0, kind: str = PROVIDER_LOCAL,
                   consumer_id: str = "", active_slot_count: int = 0) -> bool:
    """Local-agent claim rules.

    NEW (0.4.100): if job.assigned_to was set by the centralized
    coordinator matcher, only the agent whose consumer_id matches may
    claim. Empty assigned_to means unassigned and any-eligible-agent may
    claim (pre-0.4.100 back-compat).

    NEW (0.4.131): job.exclusive=True is only eligible on an agent
    with zero active slots. Caller passes its current slot count via
    active_slot_count so this filter runs at the agent-side claim loop
    without needing the slot dict here.
    """
    assigned = getattr(job, "assigned_to", "") or ""
    if assigned and consumer_id and assigned != consumer_id:
        return False
    if bool(getattr(job, EXCLUSIVE_ATTR, False)) and active_slot_count > 0:
        return False
    from ....config import is_local_only_model
    import re as _re
    if kind != PROVIDER_LOCAL:
        m = _re.search(r"--model\s+(\S+)", getattr(job, "command", "") or "")
        if m and is_local_only_model(m.group(1).strip("'\"")):
            return False
    pinned = getattr(job, "pin_to_provider", False)
    if pinned and job.provider != kind:
        return False
    job_accel = job.gpu_type or ""
    matches = (
        job.provider == PROVIDER_LOCAL
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
    if not gpu_type or gpu_type == GPU_TYPE_CPU or free_vram_gb <= 0:
        return out
    for tier, (_, accel) in _dict_value(GPU_SIZING, PROVIDER_GCP, {}).items():
        if total_vram_gb >= tier and accel:
            n = max(0, free_vram_gb // max(1, tier))
            if n > 0:
                out[accel] = max(_dict_number(out, accel), n)
    if gpu_type not in out and free_vram_gb > 0:
        out[gpu_type] = 1
    return out


def _slot_is_exclusive(slot: dict) -> bool:
    job = _dict_value(slot, SLOT_JOB_KEY, None)
    # Per-job opt-in: Job.exclusive=True takes precedence over the
    # regex-on-command path. Used for workloads (e.g. Z-Image LoRA
    # training, SDXL full finetune) whose peak VRAM is hard to bound
    # from the command string alone, but which the submitter has
    # tagged exclusive at submit time.
    if bool(getattr(job, EXCLUSIVE_ATTR, False)):
        return True
    from ....config import is_exclusive_model
    import re
    m = re.search(r"--model\s+(\S+)",
                   getattr(job, "command", "") or "")
    return bool(m and is_exclusive_model(m.group(1).strip("'\"")))


def _slot_vram(slot: dict) -> int:
    job = _dict_value(slot, SLOT_JOB_KEY, None)
    return max(
        int(getattr(job, "gpu_mem_gb", 0) or 0),
        estimate_gpu_memory(getattr(job, "command", "") or ""),
    )


def _free_ram_gb() -> float:
    """Free host RAM (GB) from /proc/meminfo MemAvailable; -1.0 if unreadable
    (e.g. non-Linux) — caller treats <0 as 'unknown, do not gate'."""
    try:
        with open("/proc/meminfo") as f:
            for line in f:
                if line.startswith(MEM_AVAILABLE_LABEL):
                    return int(line.split()[1]) / KIB_PER_GIB
    except Exception:
        pass
    return -1.0


def _total_ram_gb() -> float:
    """Total host RAM (GB) from /proc/meminfo MemTotal; -1.0 if unreadable.
    The sum-based admission gate bounds anonymous slot RSS against THIS, not
    MemAvailable — MemAvailable counts reclaimable staging page-cache as free,
    so it masked the real footprint and the agent over-admitted to a status=1
    OOM at ~100G on a 123G box."""
    try:
        with open("/proc/meminfo") as f:
            for line in f:
                if line.startswith(MEM_TOTAL_LABEL):
                    return int(line.split()[1]) / KIB_PER_GIB
    except Exception:
        pass
    return -1.0


def _slot_rss(slot: dict) -> float:
    """Measured resident host RAM (GB) of a running slot's whole process tree
    (bash + python + upload workers), summed from /proc/<pid>/status VmRSS.
    This is the OBSERVED per-job footprint used to decide if another job fits
    — no hardcoded estimate."""
    pid = getattr(_dict_value(slot, SLOT_PROC_KEY, None), "pid", None)
    if not pid:
        return 0.0
    from .gpu_probe import _proc_tree_pids
    total_kb = 0
    for p in _proc_tree_pids(pid):
        try:
            with open(f"/proc/{p}/status") as f:
                for line in f:
                    if line.startswith(VM_RSS_LABEL):
                        total_kb += int(line.split()[1])
                        break
        except Exception:
            continue
    return total_kb / KIB_PER_GIB


def _no_eligible_in_queue(store: JobStorage, gpu_type: str,
                           total_vram_gb: int, free_vram_gb: int,
                           kind: str = PROVIDER_LOCAL,
                           consumer_id: str = "",
                           active_slot_count: int = 0) -> bool:
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
                              consumer_id=consumer_id,
                              active_slot_count=active_slot_count):
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
    return total / BYTES_PER_GIB
