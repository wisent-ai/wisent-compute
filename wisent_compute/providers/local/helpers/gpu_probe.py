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


def smi_pids_used_gb(pids: set[int] | tuple[int, ...]) -> int:
    """Peak per-GPU GiB attributed to an explicit ownership PID set."""
    try:
        result = subprocess.run(
            [
                "nvidia-smi",
                "--query-compute-apps=gpu_uuid,pid,used_memory",
                "--format=csv,noheader,nounits",
            ],
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            return -1
    except Exception:
        return -1
    owned = set(pids)
    per_gpu_mib: dict[str, int] = {}
    for line in result.stdout.splitlines():
        parts = [value.strip() for value in line.split(",")]
        if len(parts) != 3:
            continue
        gpu_uuid = parts[0]
        try:
            pid, mib = int(parts[1]), int(parts[2])
        except ValueError:
            continue
        if pid in owned:
            per_gpu_mib[gpu_uuid] = per_gpu_mib.get(gpu_uuid, 0) + mib
    if not per_gpu_mib:
        return 0
    return -(-max(per_gpu_mib.values()) // 1024)


def smi_job_used_gb(root_pid: int) -> int:
    """Legacy process-tree attribution for slots without a cgroup."""
    return smi_pids_used_gb(_proc_tree_pids(root_pid))
