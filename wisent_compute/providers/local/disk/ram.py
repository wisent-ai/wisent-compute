"""Host-RAM admission gate for the local agent.

The agent refuses new job slots when free VRAM or free disk is low, but had
no host-RAM check, so it over-admitted jobs and the host OOM-killed the large
raw-activation uploads mid-transfer (each job holds big CPU-side buffers).

ram_refuse() refuses admitting NEW jobs when free RAM drops below
max(MIN_FREE_RAM_GB, FREE_RAM_FRACTION * total). Running jobs keep going and
free RAM as their uploads finish, and the next tick re-checks — so concurrency
self-regulates to actual memory pressure rather than a static job-count cap.
If RAM is unreadable (non-Linux) the gate disables itself rather than guessing.
"""
from typing import Callable

MIN_FREE_RAM_GB = 16.0
FREE_RAM_FRACTION = 0.20


def _ram_gb() -> tuple[float, float]:
    """(MemAvailable, MemTotal) in GB from /proc/meminfo; (-1, -1) if unknown."""
    avail = total = -1.0
    try:
        with open("/proc/meminfo") as f:
            for line in f:
                if line.startswith("MemAvailable:"):
                    avail = int(line.split()[1]) / (1024 ** 2)
                elif line.startswith("MemTotal:"):
                    total = int(line.split()[1]) / (1024 ** 2)
    except Exception:
        return -1.0, -1.0
    return avail, total


def ram_refuse(log_fn: Callable[[str], None]) -> tuple[bool, dict]:
    """(refuse_new_slots, diag). Refuse only when free RAM is KNOWN and below
    the floor scaled to total RAM. Diag carries free_ram_gb + ram_floor_gb so
    the operator can see memory pressure without SSHing in."""
    avail, total = _ram_gb()
    if avail < 0 or total <= 0:
        return False, {"free_ram_gb": None}
    floor = max(MIN_FREE_RAM_GB, FREE_RAM_FRACTION * total)
    refuse = avail < floor
    if refuse:
        log_fn(
            f"host RAM low (~{avail:.1f} GB free < {floor:.1f} GB floor of "
            f"{total:.0f} GB total); refusing NEW slots this tick "
            f"(running jobs continue)"
        )
    return refuse, {"free_ram_gb": round(avail, 1),
                    "ram_floor_gb": round(floor, 1)}
