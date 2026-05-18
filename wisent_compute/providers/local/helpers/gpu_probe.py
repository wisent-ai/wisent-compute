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
        stack.extend(children.get(p, ()))
    return seen


def smi_job_used_gb(root_pid: int) -> int:
    """Actual GPU memory (GiB, ceil) used by root_pid's process tree.

    -1 if nvidia-smi is unreadable; 0 if the tree currently holds no
    GPU memory (not yet allocated, or a CPU-only job).
    """
    try:
        r = subprocess.run(
            ["nvidia-smi", "--query-compute-apps=pid,used_memory",
             "--format=csv,noheader,nounits"],
            capture_output=True, text=True,
        )
        if r.returncode != 0:
            return -1
    except Exception:
        return -1
    pids = _proc_tree_pids(root_pid)
    total_mib = 0
    for line in r.stdout.splitlines():
        parts = [x.strip() for x in line.split(",")]
        if len(parts) != 2:
            continue
        try:
            pid, mib = int(parts[0]), int(parts[1])
        except ValueError:
            continue
        if pid in pids:
            total_mib += mib
    return -(-total_mib // 1024)  # ceil to GiB
