"""Admission-only disk-space diagnostics for the local agent loop.

All cleanup is owned by the registry-authorized policy engine. This module
only probes the filesystems and refuses new slots while pressure is unresolved.
"""
from __future__ import annotations

import os
import shutil
import tempfile
from typing import Callable



def _free_gb(path: str) -> float:
    try:
        return shutil.disk_usage(path).free / (1024 ** 3)
    except OSError:
        return -1.0


def _write_probe_ok(path: str) -> bool:
    """True if the agent can create, flush, and remove a file on this FS."""
    try:
        with tempfile.NamedTemporaryFile(dir=path, prefix=".wc-disk-probe-", delete=True) as fh:
            fh.write(b"x")
            fh.flush()
            os.fsync(fh.fileno())
        return True
    except OSError:
        return False


def _dir_size_gb(path: str) -> float:
    total = 0
    if not os.path.isdir(path):
        return 0.0
    for root, _, files in os.walk(path):
        for name in files:
            p = os.path.join(root, name)
            try:
                total += os.path.getsize(p)
            except OSError:
                continue
    return total / (1024 ** 3)


def _largest_child_dir_gb(path: str) -> float:
    if not os.path.isdir(path):
        return 0.0
    largest = 0.0
    try:
        names = os.listdir(path)
    except OSError:
        return 0.0
    for name in names:
        child = os.path.join(path, name)
        if os.path.isdir(child):
            largest = max(largest, _dir_size_gb(child))
    return largest

def gate_and_maybe_evict(log_fn: Callable[[str], None]) -> tuple[bool, dict]:
    """Return admission refusal and path-free disk diagnostics.

    The historical name remains as the local-agent API, but this function
    never deletes data. The policy engine runs separately before admission.
    """
    home = os.path.expanduser("~")
    free_gb = _free_gb(home)
    home_writable = _write_probe_ok(home)
    diag: dict = {
        "free_disk_gb": round(free_gb, 1),
        "home_write_probe_ok": home_writable,
    }
    refuse = not home_writable
    if refuse:
        log_fn(
            f"$HOME write probe failed (~{free_gb:.1f} GB free); "
            "refusing slots this tick"
        )

    # Staging-pressure backpressure is also admission-only. Use aggregate
    # measurements in remote diagnostics; never publish local filesystem paths.
    staging_root = os.environ.get("TMPDIR", "/tmp")
    try:
        usage = shutil.disk_usage(staging_root)
        staging_free_gb = usage.free / (1024 ** 3)
    except OSError:
        staging_free_gb = -1.0
    largest_pending_gb = _largest_child_dir_gb(
        os.path.join(staging_root, "wisent_raw_pending")
    )
    diag["staging_free_gb"] = round(staging_free_gb, 1)
    diag["largest_pending_raw_dir_gb"] = round(largest_pending_gb, 1)
    if not refuse and 0 <= staging_free_gb < largest_pending_gb:
        log_fn(
            f"staging low (~{staging_free_gb:.0f}GB free < measured pending "
            f"dir {largest_pending_gb:.0f}GB); refusing slots"
        )
        refuse = True
    return refuse, diag
