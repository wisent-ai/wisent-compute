"""Per-job ACTUAL GPU-memory measurement.

The sizing heuristic must learn from the real number a job used, not
from what it declared. nvidia-smi's whole-GPU memory.used is unusable
for that on a co-scheduled agent (a neighbour job inflates it), so this
module attributes GPU memory per process: it sums the compute-app
used_memory over exactly the PIDs in the job's subprocess tree. The
agent samples this every advance_slot tick and records the peak onto
the Job before it moves running -> completed, where the sizing module
reads it back.
"""
from __future__ import annotations

import subprocess


def _dict_value(data: dict, key, default):
    return data[key] if key in data else default


def _proc_tree_pids(root_pid: int) -> set[int]:
    """root_pid plus all descendants, from `ps -eo pid=,ppid=`.

    The job runs as `bash -c <cmd>` whose python child (and that
    child's data-loader / nccl workers) are what actually allocate GPU
    memory, so the whole subtree counts, not just root_pid. If ps is
    unreadable, only the root pid is returned: that under-counts rather
    than mis-attributing a neighbour's memory to this job.
    """
    try:
        r = subprocess.run(["ps", "-eo", "pid=,ppid="],
                            capture_output=True, text=True)
        if r.returncode != 0:
            return {root_pid}
    except Exception:
        return {root_pid}
    children: dict[int, list[int]] = {}
    for line in r.stdout.splitlines():
        parts = line.split()
        if len(parts) != 2:
            continue
        try:
            pid, ppid = int(parts[0]), int(parts[1])
        except ValueError:
            continue
        children.setdefault(ppid, []).append(pid)
    seen: set[int] = set()
    stack = [root_pid]
    while stack:
        p = stack.pop()
        if p in seen:
            continue
        seen.add(p)
        stack.extend(_dict_value(children, p, ()))
    return seen


def smi_job_used_gb(root_pid: int) -> int:
    """Peak single-GPU memory (GiB, ceil) used by root_pid's process tree.

    gpu_mem_gb means "VRAM this job needs on the GPU it runs on": the
    fleet is overwhelmingly single-GPU agents and a job loads onto one
    card. nvidia-smi --query-compute-apps emits one row per (process,
    GPU), so a process spread over N GPUs appears N times. Summing those
    rows yields the CROSS-GPU total, not the per-card footprint: on the
    multi-GPU lab box a sharded gpt-oss-20b summed to ~89 GiB while the
    same workload on a single-GPU 80 GB agent measured ~50-74 GiB, and
    that inflated 89 (propagated by observed_vram_gb's max()) pinned
    every gpt-oss-20b job above the entire single-GPU fleet.

    So attribute PER GPU: sum the tree's used_memory within each
    gpu_uuid, then return the MAX over GPUs — the largest footprint the
    job places on any one card, invariant to how many GPUs the host has.
    A model that genuinely cannot fit on one card is a separate
    "requires multi-GPU" concept and must not be conflated into this
    scalar by accidental summation.

    -1 if nvidia-smi is unreadable; 0 if the tree currently holds no
    GPU memory (not yet allocated, or a CPU-only job).
    """
    try:
        r = subprocess.run(
            ["nvidia-smi",
             "--query-compute-apps=gpu_uuid,pid,used_memory",
             "--format=csv,noheader,nounits"],
            capture_output=True, text=True,
        )
        if r.returncode != 0:
            return -1
    except Exception:
        return -1
    pids = _proc_tree_pids(root_pid)
    per_gpu_mib: dict[str, int] = {}
    for line in r.stdout.splitlines():
        parts = [x.strip() for x in line.split(",")]
        if len(parts) != 3:
            continue
        gpu_uuid = parts[0]
        try:
            pid, mib = int(parts[1]), int(parts[2])
        except ValueError:
            continue
        if pid in pids:
            per_gpu_mib[gpu_uuid] = per_gpu_mib.get(gpu_uuid, 0) + mib
    if not per_gpu_mib:
        return 0
    peak_mib = max(per_gpu_mib.values())
    return -(-peak_mib // 1024)  # ceil to GiB
