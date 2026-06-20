"""Auto-redirect agent staging away from a RAM-backed /tmp.

When `/tmp` is a tmpfs (every byte staged there counts as RAM), the
agent's multi-GB raw-activation jobs accumulate in RAM, drive the
process to OOM territory, and exit `status=1` — losing every in-flight
job to requeue. This module runs at agent startup, detects that
condition, and points TMPDIR (and `tempfile.tempdir`) at the largest
disk-backed mount the agent user can actually traverse and write,
creating a `wisent-staging` subdir there. Children inherit the env, so
every job stages on disk for free.

Fully automatic, resource-linked. No hardcoded paths, no concurrency
cap. No-op when /tmp is already disk-backed or TMPDIR is already set
to a non-/tmp path. When running as root the agent can also chmod o+x
parent dirs to recover an otherwise-skipped large mount (e.g. a Vast
host where /var/lib/docker is 0710).
"""
from __future__ import annotations

import os
import pwd
import shutil
import subprocess
import tempfile

TMPFS_TYPE = "tmpfs"
PROC_MOUNTS_FIELD_COUNT = 4
BYTES_PER_GIB = 1024 ** 3
STAGING_MIN_FREE_MULTIPLIER = 1.5
STAGING_MIN_FREE_GB = 50.0


def _tmp_is_tmpfs() -> bool:
    try:
        out = subprocess.run(
            ["stat", "-f", "-c", "%T", "/tmp"],
            capture_output=True, text=True,
        ).stdout.strip()
    except Exception:
        return False
    return out == TMPFS_TYPE


_BAD_FS = {
    TMPFS_TYPE, "devtmpfs", "proc", "sysfs", "cgroup", "cgroup2",
    "fusectl", "configfs", "debugfs", "pstore", "bpf", "ramfs",
    "mqueue", "tracefs", "securityfs", "autofs", "nsfs",
    "binfmt_misc", "hugetlbfs", "rpc_pipefs", "fuse.gvfsd-fuse",
    "squashfs", "iso9660",
}


def _candidate_mounts() -> list[str]:
    """Disk-backed, read-write mount points (from /proc/mounts)."""
    out: list[str] = []
    try:
        with open("/proc/mounts") as f:
            for line in f:
                parts = line.split()
                if len(parts) < PROC_MOUNTS_FIELD_COUNT:
                    continue
                mnt, fstype, opts = parts[1], parts[2], parts[3]
                if fstype in _BAD_FS:
                    continue
                if "ro" in opts.split(","):
                    continue
                if mnt.startswith("/boot"):
                    continue
                out.append(mnt)
    except OSError:
        pass
    return out


def _free_gb(path: str) -> float:
    try:
        return shutil.disk_usage(path).free / BYTES_PER_GIB
    except OSError:
        return -1.0


def _writable_for_self(path: str) -> bool:
    if not os.path.isdir(path):
        return False
    probe = os.path.join(path, f".wc_staging_probe_{os.getpid()}")
    try:
        with open(probe, "w") as fh:
            fh.write("x")
        os.unlink(probe)
        return True
    except OSError:
        return False


def _add_other_exec(path: str) -> bool:
    try:
        st = os.stat(path)
        new_mode = st.st_mode | 0o001
        if new_mode != st.st_mode:
            os.chmod(path, new_mode)
        return bool(os.stat(path).st_mode & 0o001)
    except OSError:
        return False


def _try_repair_traversal(target: str, log_fn) -> None:
    p = os.path.dirname(target)
    while p and p != "/":
        if not _add_other_exec(p):
            log_fn(f"staging: cannot chmod o+x {p} (need root); admin should add o+x")
        p = os.path.dirname(p)


def _agent_user() -> str:
    explicit = os.environ.get("WISENT_STAGING_USER", "").strip()
    if explicit:
        return explicit
    try:
        return pwd.getpwuid(os.geteuid()).pw_name
    except KeyError:
        return str(os.geteuid())


def _chown_if_root(path: str, user: str) -> None:
    if os.geteuid() != 0:
        return
    try:
        info = pwd.getpwnam(user)
    except KeyError:
        return
    try:
        st = os.stat(path)
        if st.st_uid != info.pw_uid:
            os.chown(path, info.pw_uid, info.pw_gid)
    except OSError:
        pass


def setup_agent_staging(log_fn) -> str | None:
    """Detect a RAM-backed /tmp and redirect TMPDIR to disk. Returns the
    chosen path, or None when nothing was changed."""
    explicit = os.environ.get("TMPDIR", "").strip()
    if explicit and not explicit.startswith("/tmp"):
        log_fn(f"staging: TMPDIR already set to {explicit}; keeping it")
        return explicit
    if not _tmp_is_tmpfs():
        return None
    tmp_free = _free_gb("/tmp")
    user = _agent_user()
    best: tuple[str, float] | None = None
    for mnt in _candidate_mounts():
        target = os.path.join(mnt, "wisent-staging")
        try:
            os.makedirs(target, exist_ok=True)
        except OSError:
            continue
        _chown_if_root(target, user)
        if not _writable_for_self(target):
            if os.geteuid() == 0:
                _try_repair_traversal(target, log_fn)
                if not _writable_for_self(target):
                    continue
            else:
                log_fn(
                    f"staging: candidate {target} not writable by "
                    f"current user (parent perms?); skipping"
                )
                continue
        free = _free_gb(target)
        if free <= max(tmp_free * STAGING_MIN_FREE_MULTIPLIER, STAGING_MIN_FREE_GB):
            continue
        if best is None or free > best[1]:
            best = (target, free)
    if not best:
        log_fn(
            "staging: /tmp is tmpfs but no larger writable disk-backed "
            "mount found; staging stays on /tmp (RAM). Crashes possible."
        )
        return None
    target, free_gb = best
    os.environ["TMPDIR"] = target
    tempfile.tempdir = target
    log_fn(
        f"staging: redirected TMPDIR /tmp(tmpfs,{tmp_free:.0f}G) -> "
        f"{target} (disk-backed, {free_gb:.0f}G free)"
    )
    return target
