"""Registry-authorized, bounded disk cleanup for local compute hosts.

Only fixed roots and fixed cleaner implementations exist here.  Registry data can
select a cleaner and its retention, but can never supply a path or command.
"""
from __future__ import annotations

import ctypes
import errno
import fcntl
import hashlib
import json
import os
import shutil
import socket
import stat
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Optional

from ....targets import GCS_REGISTRY_URI, ComputeTarget, DiskCleanupPolicy, _from_dict
from ....targets.validation import normalize_hostname, ssh_hostname, validate_registry

_GIB = 1024 ** 3
_STATE_VERSION = 1
_STATE_DIR = (".cache", "wisent-compute")
_LOCK_NAME = "disk-cleanup.lock"
_STATE_NAME = "disk-cleanup-state.json"
_DEADLINE_SECONDS = 30.0
_MAX_ERRORS = 16
_HF_BARRIER_NAME = ".wisent-compute-lock-barrier"
_HF_BARRIER_MARKER = ".wisent-compute-barrier"


def _default_log(message: str) -> None:
    import sys
    sys.stderr.write(message + "\n")


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _error_code(exc: BaseException) -> str:
    """Return bounded diagnostics without paths, values, or credentials."""
    return type(exc).__name__[:80]


def _add_error(report: dict, area: str, exc: BaseException) -> None:
    if len(report["errors"]) < _MAX_ERRORS:
        report["errors"].append(area + ":" + _error_code(exc))


def _cleaner_report() -> dict:
    return {
        "scanned_items": 0,
        "eligible_items": 0,
        "deleted_items": 0,
        "expected_bytes": 0,
        "actual_free_delta_bytes": 0,
        "skipped": {},
    }


def _skip(report: dict, reason: str, count: int = 1) -> None:
    skipped = report["skipped"]
    skipped[reason] = skipped.get(reason, 0) + count


def _base_report(active_slot_count: int) -> dict:
    return {
        "version": _STATE_VERSION,
        "hostname": normalize_hostname(socket.gethostname()),
        "target_name": None,
        "policy_digest": None,
        "mode": None,
        "check_interval_seconds": None,
        "started_at": _utc_now(),
        "duration_ms": 0,
        "outcome": "invalid_or_unavailable_policy",
        "free_bytes_before": None,
        "free_bytes_after": None,
        "low_bytes": None,
        "target_bytes": None,
        "pressure_active": None,
        "cleaners": {
            "huggingface_cache": _cleaner_report(),
            "weles_recordings": _cleaner_report(),
        },
        "caps": {"bytes": False, "items": False, "scan": False, "deadline": False},
        "lock_busy": False,
        "active_slot_count": max(0, int(active_slot_count)),
        "last_success_at": None,
        "errors": [],
    }


def _fetch_canonical_registry() -> dict:
    """Fetch the canonical object directly; destructive checks never use fallback/cache."""
    from google.cloud import storage

    _, remainder = GCS_REGISTRY_URI.split("//", 1)
    bucket_name, blob_name = remainder.split("/", 1)
    text = storage.Client().bucket(bucket_name).blob(blob_name).download_as_text()
    value = json.loads(text)
    if not isinstance(value, dict):
        raise ValueError("canonical registry is not an object")
    return value


def _identities(target: dict) -> set[str]:
    values = {normalize_hostname(target["name"])}
    values.update(normalize_hostname(value) for value in target.get("hostnames", []))
    if target.get("ssh"):
        values.add(ssh_hostname(target["ssh"]))
    return values


def resolve_canonical_policy(hostname: Optional[str] = None) -> tuple[ComputeTarget, DiskCleanupPolicy, str]:
    """Resolve the unique local policy from validated canonical GCS data.

    No package registry fallback is permitted. Any fetch, schema, identity, or
    typing failure is propagated to the caller, which must fail closed.
    """
    data = _fetch_canonical_registry()
    validated = validate_registry(data)
    identity = normalize_hostname(hostname or socket.gethostname())
    matches = [raw for raw in validated["targets"] if identity in _identities(raw)]
    if len(matches) != 1:
        raise LookupError("canonical host identity did not match uniquely")
    raw = matches[0]
    if raw["kind"] != "local" or "disk_cleanup" not in raw:
        raise LookupError("matched target has no local cleanup policy")
    target = _from_dict(raw)
    if target.disk_cleanup is None:
        raise LookupError("cleanup policy could not be parsed")
    canonical = json.dumps(raw["disk_cleanup"], sort_keys=True, separators=(",", ":")).encode()
    digest = hashlib.sha256(canonical).hexdigest()
    return target, target.disk_cleanup, digest


def _secure_home() -> Path:
    home = Path.home()
    info = home.lstat()
    if stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode):
        raise OSError("unsafe home")
    if info.st_uid != os.geteuid():
        raise OSError("home owner mismatch")
    return home.resolve(strict=True)


def _ensure_state_dir(home: Path) -> Path:
    current = home
    for component in _STATE_DIR:
        current = current / component
        try:
            info = current.lstat()
        except FileNotFoundError:
            current.mkdir(mode=0o700)
            info = current.lstat()
        if stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode):
            raise OSError("unsafe state directory")
        if info.st_uid != os.geteuid():
            raise OSError("state directory owner mismatch")
    return current


def _open_lock(state_dir: Path) -> int:
    flags = os.O_RDWR | os.O_CREAT
    flags |= getattr(os, "O_NOFOLLOW", 0)
    fd = os.open(state_dir / _LOCK_NAME, flags, 0o600)
    try:
        info = os.fstat(fd)
        if not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid():
            raise OSError("unsafe cleanup lock")
        os.fchmod(fd, 0o600)
        return fd
    except BaseException:
        os.close(fd)
        raise


def _acquire_lock(state_dir: Path) -> Optional[int]:
    fd = _open_lock(state_dir)
    try:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            if exc.errno in (errno.EACCES, errno.EAGAIN):
                os.close(fd)
                return None
            raise
        return fd
    except BaseException:
        try:
            os.close(fd)
        except OSError:
            pass
        raise


def acquire_workload_lock() -> Optional[int]:
    """Acquire the cleanup lock in shared mode for one live workload.

    The returned opaque handle must be retained until the workload has fully
    left its slot, then passed to :func:`release_workload_lock`. ``None`` means
    a standalone cleanup currently owns the exclusive lock, so admission must
    be retried later.
    """
    state_dir = _ensure_state_dir(_secure_home())
    fd = _open_lock(state_dir)
    try:
        fcntl.flock(fd, fcntl.LOCK_SH | fcntl.LOCK_NB)
        return fd
    except OSError as exc:
        os.close(fd)
        if exc.errno in (errno.EACCES, errno.EAGAIN):
            return None
        raise


def release_workload_lock(handle: int) -> None:
    """Release a handle returned by :func:`acquire_workload_lock`."""
    try:
        fcntl.flock(handle, fcntl.LOCK_UN)
    finally:
        os.close(handle)


def _read_state(state_dir: Path) -> dict:
    path = state_dir / _STATE_NAME
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    try:
        fd = os.open(path, flags)
    except FileNotFoundError:
        return {}
    try:
        info = os.fstat(fd)
        if not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid():
            raise OSError("unsafe cleanup state")
        with os.fdopen(fd, "r", encoding="utf-8", closefd=False) as handle:
            value = json.load(handle)
        return value if isinstance(value, dict) else {}
    finally:
        os.close(fd)


def _write_state(state_dir: Path, report: dict, attempted_at: float) -> None:
    destination = state_dir / _STATE_NAME
    try:
        existing = destination.lstat()
    except FileNotFoundError:
        pass
    else:
        if stat.S_ISLNK(existing.st_mode) or not stat.S_ISREG(existing.st_mode) or existing.st_uid != os.geteuid():
            raise OSError("unsafe cleanup state")
    payload = json.dumps(
        {"version": _STATE_VERSION, "last_attempt_at": attempted_at, "report": report},
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    temp = state_dir / f".{_STATE_NAME}.{os.getpid()}.{time.monotonic_ns()}"
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    flags |= getattr(os, "O_NOFOLLOW", 0)
    fd = os.open(temp, flags, 0o600)
    try:
        with os.fdopen(fd, "wb", closefd=False) as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.close(fd)
        fd = -1
        os.replace(temp, destination)
        directory_fd = os.open(state_dir, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    finally:
        if fd >= 0:
            os.close(fd)
        try:
            temp.unlink()
        except FileNotFoundError:
            pass


def _fixed_root(home: Path, parts: tuple[str, ...], *, required: bool) -> Optional[Path]:
    current = home
    home_device = home.stat().st_dev
    for part in parts:
        current = current / part
        try:
            info = current.lstat()
        except FileNotFoundError:
            if required:
                raise
            return None
        if stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode):
            raise OSError("unsafe cleaner root")
        if info.st_uid != os.geteuid() or info.st_dev != home_device:
            raise OSError("cleaner root ownership or device mismatch")
    resolved = current.resolve(strict=True)
    if resolved == home or home not in resolved.parents:
        raise OSError("cleaner root is not beneath home")
    return resolved


def _free_bytes(home: Path) -> int:
    return int(shutil.disk_usage(home).free)


def _reserved_hf_path(path: Path, root: Path) -> bool:
    try:
        relative = path.relative_to(root)
    except ValueError:
        return True
    return any(part in {".locks", ".work"} or part.endswith(".incomplete") for part in relative.parts)


def _checked_hf_path(path: object, root: Path, root_info: os.stat_result, *, directory: bool) -> tuple[Path, os.stat_result]:
    if not isinstance(path, Path) or not path.is_absolute() or path == root:
        raise OSError("unsafe strategy path")
    try:
        relative = path.relative_to(root)
    except ValueError as exc:
        raise OSError("strategy path outside cache") from exc
    if _reserved_hf_path(path, root):
        raise OSError("reserved strategy path")
    current = root
    parts = relative.parts
    for index, part in enumerate(parts):
        current = current / part
        info = current.lstat()
        final = index == len(parts) - 1
        if info.st_uid != os.geteuid() or info.st_dev != root_info.st_dev:
            raise OSError("strategy ownership or device mismatch")
        if not final and (stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode)):
            raise OSError("unsafe strategy ancestor")
    if directory:
        if stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode):
            raise OSError("strategy directory type mismatch")
    elif stat.S_ISLNK(info.st_mode) or not stat.S_ISREG(info.st_mode):
        raise OSError("strategy file type mismatch")
    return path, info


def _hf_open_dir(parent_fd: int, name: str) -> int:
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
    return os.open(name, flags, dir_fd=parent_fd)


def _hf_open_path(root_fd: int, parts: tuple[str, ...]) -> int:
    descriptor = os.dup(root_fd)
    try:
        for part in parts:
            child = _hf_open_dir(descriptor, part)
            os.close(descriptor)
            descriptor = child
        return descriptor
    except BaseException:
        os.close(descriptor)
        raise


def _hf_identity(info: os.stat_result) -> tuple[int, int, int, int, int, int]:
    return (int(info.st_dev), int(info.st_ino), stat.S_IFMT(info.st_mode),
            int(info.st_size), int(info.st_mtime_ns), int(info.st_nlink))


def _hf_check_info(info: os.stat_result, root_info: os.stat_result) -> None:
    if info.st_uid != os.geteuid() or info.st_dev != root_info.st_dev:
        raise OSError("cache entry ownership or device mismatch")


def _hf_tick(budget: dict, deadline: float, report: dict) -> None:
    if time.monotonic() >= deadline:
        report["caps"]["deadline"] = True
        raise TimeoutError("cache scan deadline")
    if budget["remaining"] <= 0:
        report["caps"]["scan"] = True
        raise OSError("cache scan cap")
    budget["remaining"] -= 1
    report["cleaners"]["huggingface_cache"]["scanned_items"] += 1


def _hf_entries(directory_fd: int, budget: dict, deadline: float, report: dict):
    with os.scandir(directory_fd) as entries:
        for entry in entries:
            _hf_tick(budget, deadline, report)
            yield entry


def _hf_lock_state(
    root_fd: int,
    root_info: os.stat_result,
    budget: dict,
    deadline: float,
    report: dict,
    *,
    acquire: bool,
    lock_name: str = ".locks",
    already_held: frozenset[tuple[int, int, int]] = frozenset(),
) -> tuple[dict[tuple[str, ...], tuple[int, int, int, int, int, int]], list[int], bool]:
    state: dict[tuple[str, ...], tuple[int, int, int, int, int, int]] = {}
    held: list[int] = []
    try:
        locks_fd = _hf_open_dir(root_fd, lock_name)
    except FileNotFoundError:
        return state, held, False
    locks_info = os.fstat(locks_fd)
    _hf_check_info(locks_info, root_info)
    state[()] = _hf_identity(locks_info)
    stack: list[tuple[tuple[str, ...], int]] = [((), locks_fd)]
    try:
        while stack:
            prefix, directory_fd = stack.pop()
            try:
                for entry in _hf_entries(directory_fd, budget, deadline, report):
                    info = entry.stat(follow_symlinks=False)
                    _hf_check_info(info, root_info)
                    relative = prefix + (entry.name,)
                    if stat.S_ISDIR(info.st_mode) and not stat.S_ISLNK(info.st_mode):
                        child = _hf_open_dir(directory_fd, entry.name)
                        if _hf_identity(os.fstat(child)) != _hf_identity(info):
                            os.close(child)
                            raise OSError("cache lock directory changed")
                        state[relative] = _hf_identity(info)
                        stack.append((relative, child))
                    elif stat.S_ISREG(info.st_mode) and not stat.S_ISLNK(info.st_mode):
                        flags = os.O_RDWR | getattr(os, "O_NOFOLLOW", 0)
                        descriptor = os.open(entry.name, flags, dir_fd=directory_fd)
                        opened = os.fstat(descriptor)
                        if _hf_identity(opened) != _hf_identity(info):
                            os.close(descriptor)
                            raise OSError("cache lock changed")
                        state[relative] = _hf_identity(opened)
                        if acquire and _hf_stable_identity(opened) not in already_held:
                            try:
                                fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
                            except OSError as exc:
                                os.close(descriptor)
                                if exc.errno in {errno.EACCES, errno.EAGAIN}:
                                    raise BlockingIOError("cache lock held") from exc
                                raise
                            held.append(descriptor)
                        else:
                            os.close(descriptor)
                    else:
                        raise OSError("unsafe cache lock")
            finally:
                os.close(directory_fd)
        return state, held, True
    except BaseException:
        for descriptor in held:
            os.close(descriptor)
        for _, descriptor in stack:
            os.close(descriptor)
        raise


def _hf_stable_identity(info: os.stat_result) -> tuple[int, int, int]:
    return int(info.st_dev), int(info.st_ino), stat.S_IFMT(info.st_mode)


def _hf_exchange(root_fd: int, first: str, second: str) -> None:
    """Atomically exchange two names beneath the already-validated cache root."""
    libc = ctypes.CDLL(None, use_errno=True)
    first_bytes = os.fsencode(first)
    second_bytes = os.fsencode(second)
    if os.uname().sysname == "Darwin":
        operation = getattr(libc, "renameatx_np", None)
        if operation is None:
            raise OSError(errno.ENOTSUP, "atomic directory exchange unavailable")
        operation.argtypes = [ctypes.c_int, ctypes.c_char_p, ctypes.c_int, ctypes.c_char_p, ctypes.c_uint]
        operation.restype = ctypes.c_int
        result = operation(root_fd, first_bytes, root_fd, second_bytes, 0x00000002)
    else:
        operation = getattr(libc, "renameat2", None)
        if operation is None:
            raise OSError(errno.ENOTSUP, "atomic directory exchange unavailable")
        operation.argtypes = [ctypes.c_int, ctypes.c_char_p, ctypes.c_int, ctypes.c_char_p, ctypes.c_uint]
        operation.restype = ctypes.c_int
        result = operation(root_fd, first_bytes, root_fd, second_bytes, 0x00000002)
    if result != 0:
        error = ctypes.get_errno()
        raise OSError(error, os.strerror(error))


def _hf_has_barrier_marker(directory_fd: int, root_info: os.stat_result) -> bool:
    try:
        info = os.stat(_HF_BARRIER_MARKER, dir_fd=directory_fd, follow_symlinks=False)
    except FileNotFoundError:
        return False
    _hf_check_info(info, root_info)
    if not stat.S_ISREG(info.st_mode) or stat.S_ISLNK(info.st_mode):
        raise OSError("unsafe cache lock barrier marker")
    return True


def _hf_remove_barrier_tree(directory_fd: int, root_info: os.stat_result) -> None:
    os.fchmod(directory_fd, 0o700)
    with os.scandir(directory_fd) as entries:
        names = [entry.name for entry in entries]
    for name in names:
        info = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
        _hf_check_info(info, root_info)
        if stat.S_ISDIR(info.st_mode) and not stat.S_ISLNK(info.st_mode):
            child = _hf_open_dir(directory_fd, name)
            try:
                if _hf_stable_identity(os.fstat(child)) != _hf_stable_identity(info):
                    raise OSError("cache lock barrier directory changed")
                _hf_remove_barrier_tree(child, root_info)
            finally:
                os.close(child)
            os.rmdir(name, dir_fd=directory_fd)
        elif stat.S_ISREG(info.st_mode) and not stat.S_ISLNK(info.st_mode):
            os.unlink(name, dir_fd=directory_fd)
        else:
            raise OSError("unsafe cache lock barrier entry")


def _hf_discard_barrier(root_fd: int, root_info: os.stat_result) -> None:
    barrier_fd = _hf_open_dir(root_fd, _HF_BARRIER_NAME)
    try:
        _hf_remove_barrier_tree(barrier_fd, root_info)
    finally:
        os.close(barrier_fd)
    os.rmdir(_HF_BARRIER_NAME, dir_fd=root_fd)


def _hf_recover_lock_barrier(root_fd: int, root_info: os.stat_result) -> None:
    """Restore an atomic exchange interrupted by process termination."""
    try:
        barrier_fd = _hf_open_dir(root_fd, _HF_BARRIER_NAME)
    except FileNotFoundError:
        return
    try:
        _hf_check_info(os.fstat(barrier_fd), root_info)
        private_marked = _hf_has_barrier_marker(barrier_fd, root_info)
    finally:
        os.close(barrier_fd)
    locks_fd = _hf_open_dir(root_fd, ".locks")
    try:
        _hf_check_info(os.fstat(locks_fd), root_info)
        canonical_marked = _hf_has_barrier_marker(locks_fd, root_info)
    finally:
        os.close(locks_fd)
    if canonical_marked and not private_marked:
        _hf_exchange(root_fd, ".locks", _HF_BARRIER_NAME)
        _hf_discard_barrier(root_fd, root_info)
    elif private_marked and not canonical_marked:
        _hf_discard_barrier(root_fd, root_info)
    else:
        raise OSError("ambiguous cache lock barrier residue")


def _hf_prepare_lock_barrier(
    root_fd: int,
    root_info: os.stat_result,
    lock_state: dict[tuple[str, ...], tuple[int, int, int, int, int, int]],
) -> tuple[int, int, int]:
    os.mkdir(_HF_BARRIER_NAME, 0o700, dir_fd=root_fd)
    barrier_fd = _hf_open_dir(root_fd, _HF_BARRIER_NAME)
    try:
        marker = os.open(
            _HF_BARRIER_MARKER,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
            0o400,
            dir_fd=barrier_fd,
        )
        os.close(marker)
        directories = sorted(
            (parts for parts, identity in lock_state.items()
             if parts and identity[2] == stat.S_IFDIR),
            key=len,
        )
        for parts in directories:
            parent = _hf_open_path(barrier_fd, parts[:-1])
            try:
                os.mkdir(parts[-1], 0o700, dir_fd=parent)
            finally:
                os.close(parent)
        for parts, identity in lock_state.items():
            if not parts or identity[2] != stat.S_IFREG:
                continue
            source_parent = _hf_open_path(root_fd, (".locks",) + parts[:-1])
            destination_parent = _hf_open_path(barrier_fd, parts[:-1])
            try:
                expected_parent = lock_state[parts[:-1]]
                if _hf_stable_identity(os.fstat(source_parent)) != (
                    expected_parent[0], expected_parent[1], expected_parent[2],
                ):
                    raise OSError("cache lock parent changed while building barrier")
                os.link(
                    parts[-1],
                    parts[-1],
                    src_dir_fd=source_parent,
                    dst_dir_fd=destination_parent,
                    follow_symlinks=False,
                )
                linked = os.stat(parts[-1], dir_fd=destination_parent, follow_symlinks=False)
                if _hf_stable_identity(linked) != (identity[0], identity[1], identity[2]):
                    raise OSError("cache lock changed while building barrier")
            finally:
                os.close(source_parent)
                os.close(destination_parent)
        for parts in sorted(directories, key=len, reverse=True):
            descriptor = _hf_open_path(barrier_fd, parts)
            try:
                os.fchmod(descriptor, 0o555)
            finally:
                os.close(descriptor)
        os.fchmod(barrier_fd, 0o555)
        return _hf_stable_identity(os.fstat(barrier_fd))
    except BaseException:
        os.close(barrier_fd)
        _hf_discard_barrier(root_fd, root_info)
        raise
    finally:
        if barrier_fd >= 0:
            try:
                os.close(barrier_fd)
            except OSError:
                pass


def _hf_barrier_lock_state_matches(
    expected: dict[tuple[str, ...], tuple[int, int, int, int, int, int]],
    current: dict[tuple[str, ...], tuple[int, int, int, int, int, int]],
) -> bool:
    if expected.keys() != current.keys():
        return False
    for path, identity in expected.items():
        observed = current[path]
        if identity[2] == stat.S_IFREG:
            if observed[:5] != identity[:5] or observed[5] != identity[5] + 1:
                return False
        elif observed != identity:
            return False
    return True


def _hf_enter_lock_barrier(
    root_fd: int,
    root_info: os.stat_result,
    lock_state: dict[tuple[str, ...], tuple[int, int, int, int, int, int]],
    lock_fds: list[int],
    budget: dict,
    deadline: float,
    report: dict,
) -> tuple[tuple[int, int, int], tuple[int, int, int]]:
    original = (lock_state[()][0], lock_state[()][1], lock_state[()][2])
    barrier = _hf_prepare_lock_barrier(root_fd, root_info, lock_state)
    exchanged = False
    try:
        _hf_exchange(root_fd, ".locks", _HF_BARRIER_NAME)
        exchanged = True
        canonical_fd = _hf_open_dir(root_fd, ".locks")
        private_fd = _hf_open_dir(root_fd, _HF_BARRIER_NAME)
        try:
            if _hf_stable_identity(os.fstat(canonical_fd)) != barrier:
                raise OSError("cache lock barrier exchange changed")
            if _hf_stable_identity(os.fstat(private_fd)) != original:
                raise OSError("cache lock namespace changed during exchange")
        finally:
            os.close(canonical_fd)
            os.close(private_fd)
        held_identities = frozenset(_hf_stable_identity(os.fstat(fd)) for fd in lock_fds)
        expected_held = frozenset(
            (identity[0], identity[1], identity[2])
            for identity in lock_state.values()
            if identity[2] == stat.S_IFREG
        )
        current, new_fds, present = _hf_lock_state(
            root_fd,
            root_info,
            budget,
            deadline,
            report,
            acquire=True,
            lock_name=_HF_BARRIER_NAME,
            already_held=held_identities,
        )
        try:
            if not present or not _hf_barrier_lock_state_matches(lock_state, current):
                raise OSError("cache lock set changed during barrier exchange")
            if held_identities != expected_held:
                raise OSError("held cache lock changed during barrier exchange")
        finally:
            for descriptor in new_fds:
                os.close(descriptor)
        return original, barrier
    except BaseException:
        if exchanged:
            _hf_exchange(root_fd, ".locks", _HF_BARRIER_NAME)
        _hf_discard_barrier(root_fd, root_info)
        raise


def _hf_leave_lock_barrier(
    root_fd: int,
    root_info: os.stat_result,
    identities: tuple[tuple[int, int, int], tuple[int, int, int]],
) -> None:
    original, barrier = identities
    canonical_fd = _hf_open_dir(root_fd, ".locks")
    private_fd = _hf_open_dir(root_fd, _HF_BARRIER_NAME)
    try:
        if _hf_stable_identity(os.fstat(canonical_fd)) != barrier:
            raise OSError("cache lock barrier changed before restoration")
        if _hf_stable_identity(os.fstat(private_fd)) != original:
            raise OSError("held cache lock namespace changed before restoration")
    finally:
        os.close(canonical_fd)
        os.close(private_fd)
    _hf_exchange(root_fd, ".locks", _HF_BARRIER_NAME)
    restored_fd = _hf_open_dir(root_fd, ".locks")
    try:
        if _hf_stable_identity(os.fstat(restored_fd)) != original:
            raise OSError("cache lock namespace restoration failed")
    finally:
        os.close(restored_fd)
    _hf_discard_barrier(root_fd, root_info)


def _hf_normalize_link(repo_parts: tuple[str, ...], snapshot_parts: tuple[str, ...], target: str) -> tuple[str, ...]:
    if not target or os.path.isabs(target):
        raise OSError("unsafe snapshot link")
    parts = list(snapshot_parts[:-1])
    for part in target.split("/"):
        if part in {"", "."}:
            continue
        if part == "..":
            if not parts:
                raise OSError("snapshot link escapes cache")
            parts.pop()
        else:
            parts.append(part)
    normalized = tuple(parts)
    if normalized[:len(repo_parts) + 1] != repo_parts + ("blobs",):
        raise OSError("snapshot link does not target repository blob")
    return normalized


def _hf_snapshot_state(
    root_fd: int,
    root_info: os.stat_result,
    repo_parts: tuple[str, ...],
    commit: str,
    blobs: dict[tuple[str, ...], tuple[int, int, int, int, int, int]],
    budget: dict,
    deadline: float,
    report: dict,
) -> tuple[dict[tuple[str, ...], tuple[int, int, int, int, int, int]], float, int, frozenset[tuple[str, ...]]]:
    snapshot_parts = repo_parts + ("snapshots", commit)
    snapshot_fd = _hf_open_path(root_fd, snapshot_parts)
    snapshot_info = os.fstat(snapshot_fd)
    _hf_check_info(snapshot_info, root_info)
    state = {(): _hf_identity(snapshot_info)}
    modified = float(snapshot_info.st_mtime)
    expected = int(getattr(snapshot_info, "st_blocks", 0)) * 512
    referenced_blobs: set[tuple[str, ...]] = set()
    stack: list[tuple[tuple[str, ...], int]] = [((), snapshot_fd)]
    try:
        while stack:
            prefix, directory_fd = stack.pop()
            try:
                for entry in _hf_entries(directory_fd, budget, deadline, report):
                    if entry.name == ".work" or entry.name.endswith(".incomplete"):
                        raise OSError("reserved snapshot data")
                    info = entry.stat(follow_symlinks=False)
                    _hf_check_info(info, root_info)
                    relative = prefix + (entry.name,)
                    identity = _hf_identity(info)
                    state[relative] = identity
                    modified = max(modified, float(info.st_mtime))
                    if stat.S_ISDIR(info.st_mode) and not stat.S_ISLNK(info.st_mode):
                        child = _hf_open_dir(directory_fd, entry.name)
                        if _hf_identity(os.fstat(child)) != identity:
                            os.close(child)
                            raise OSError("snapshot directory changed")
                        expected += int(getattr(info, "st_blocks", 0)) * 512
                        stack.append((relative, child))
                        continue
                    if stat.S_ISLNK(info.st_mode):
                        target = os.readlink(entry.name, dir_fd=directory_fd)
                        blob_parts = _hf_normalize_link(repo_parts, snapshot_parts + relative, target)
                        blob_identity = blobs.get(blob_parts)
                        if blob_identity is None:
                            raise OSError("snapshot references unknown blob")
                        current_blob = _hf_leaf_info(root_fd, blob_parts)
                        if _hf_identity(current_blob) != blob_identity:
                            raise OSError("snapshot blob changed")
                        referenced_blobs.add(blob_parts)
                        modified = max(modified, blob_identity[4] / 1_000_000_000)
                        expected += int(getattr(info, "st_blocks", 0)) * 512
                        continue
                    if stat.S_ISREG(info.st_mode):
                        matches = [path for path, blob_identity in blobs.items()
                                   if identity[:2] == blob_identity[:2]]
                        if len(matches) != 1:
                            raise OSError("untracked or ambiguous snapshot data")
                        referenced_blobs.add(matches[0])
                        continue
                    raise OSError("unsupported snapshot entry")
            finally:
                os.close(directory_fd)
    except BaseException:
        for _, descriptor in stack:
            os.close(descriptor)
        raise
    if len(state) == 1:
        raise OSError("empty snapshot")
    return state, modified, expected, frozenset(referenced_blobs)


def _hf_scan_refs(
    root_fd: int,
    root_info: os.stat_result,
    repo_parts: tuple[str, ...],
    budget: dict,
    deadline: float,
    report: dict,
) -> dict[str, list[tuple[tuple[str, ...], tuple[int, int, int, int, int, int]]]]:
    by_commit: dict[str, list[tuple[tuple[str, ...], tuple[int, int, int, int, int, int]]]] = {}
    try:
        refs_fd = _hf_open_path(root_fd, repo_parts + ("refs",))
    except FileNotFoundError:
        return by_commit
    stack: list[tuple[tuple[str, ...], int]] = [((), refs_fd)]
    try:
        while stack:
            prefix, directory_fd = stack.pop()
            try:
                for entry in _hf_entries(directory_fd, budget, deadline, report):
                    info = entry.stat(follow_symlinks=False)
                    _hf_check_info(info, root_info)
                    relative = prefix + (entry.name,)
                    if stat.S_ISDIR(info.st_mode) and not stat.S_ISLNK(info.st_mode):
                        child = _hf_open_dir(directory_fd, entry.name)
                        if _hf_identity(os.fstat(child)) != _hf_identity(info):
                            os.close(child)
                            raise OSError("reference directory changed")
                        stack.append((relative, child))
                    elif stat.S_ISREG(info.st_mode) and not stat.S_ISLNK(info.st_mode):
                        flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
                        descriptor = os.open(entry.name, flags, dir_fd=directory_fd)
                        try:
                            opened = os.fstat(descriptor)
                            if _hf_identity(opened) != _hf_identity(info):
                                raise OSError("reference changed")
                            payload = os.read(descriptor, 257)
                        finally:
                            os.close(descriptor)
                        if len(payload) > 256:
                            raise OSError("oversized cache reference")
                        try:
                            commit = payload.decode("ascii").strip()
                        except UnicodeDecodeError as exc:
                            raise OSError("invalid cache reference") from exc
                        if not commit or any(character not in "0123456789abcdef" for character in commit.lower()):
                            raise OSError("invalid cache reference")
                        path_parts = repo_parts + ("refs",) + relative
                        by_commit.setdefault(commit, []).append((path_parts, _hf_identity(info)))
                    else:
                        raise OSError("unsafe cache reference")
            finally:
                os.close(directory_fd)
        return by_commit
    except BaseException:
        for _, descriptor in stack:
            os.close(descriptor)
        raise


def _hf_scan_reserved_metadata(
    root_fd: int,
    root_info: os.stat_result,
    parts: tuple[str, ...],
    budget: dict,
    deadline: float,
    report: dict,
) -> None:
    metadata_fd = _hf_open_path(root_fd, parts)
    stack: list[int] = [metadata_fd]
    try:
        while stack:
            directory_fd = stack.pop()
            try:
                for entry in _hf_entries(directory_fd, budget, deadline, report):
                    if entry.name == ".work" or entry.name.endswith(".incomplete"):
                        raise OSError("incomplete reserved cache metadata")
                    info = entry.stat(follow_symlinks=False)
                    _hf_check_info(info, root_info)
                    if stat.S_ISDIR(info.st_mode) and not stat.S_ISLNK(info.st_mode):
                        child = _hf_open_dir(directory_fd, entry.name)
                        if _hf_identity(os.fstat(child)) != _hf_identity(info):
                            os.close(child)
                            raise OSError("reserved cache metadata changed")
                        stack.append(child)
                    elif not stat.S_ISREG(info.st_mode) or stat.S_ISLNK(info.st_mode):
                        raise OSError("unsafe reserved cache metadata")
            finally:
                os.close(directory_fd)
    except BaseException:
        for descriptor in stack:
            os.close(descriptor)
        raise


def _hf_scan_repo(
    root_fd: int,
    root_info: os.stat_result,
    repo_parts: tuple[str, ...],
    budget: dict,
    deadline: float,
    report: dict,
) -> list[dict]:
    repo_fd = _hf_open_path(root_fd, repo_parts) if repo_parts else os.dup(root_fd)
    try:
        names: set[str] = set()
        has_no_exist = False
        for entry in _hf_entries(repo_fd, budget, deadline, report):
            info = entry.stat(follow_symlinks=False)
            _hf_check_info(info, root_info)
            if entry.name == ".work" or entry.name.endswith(".incomplete"):
                raise OSError("reserved repository data")
            if not repo_parts and entry.name in {".locks", "version.txt"}:
                continue
            if entry.name == ".no_exist":
                if not stat.S_ISDIR(info.st_mode) or stat.S_ISLNK(info.st_mode):
                    raise OSError("unsafe reserved repository metadata")
                has_no_exist = True
                continue
            names.add(entry.name)
            if entry.name not in {"blobs", "refs", "snapshots"}:
                raise OSError("unknown repository data")
            if not stat.S_ISDIR(info.st_mode) or stat.S_ISLNK(info.st_mode):
                raise OSError("unsafe repository layout")
    finally:
        os.close(repo_fd)
    if has_no_exist:
        _hf_scan_reserved_metadata(
            root_fd, root_info, repo_parts + (".no_exist",), budget, deadline, report,
        )
    if not {"blobs", "snapshots"}.issubset(names):
        raise OSError("incomplete repository layout")
    blobs_fd = _hf_open_path(root_fd, repo_parts + ("blobs",))
    blobs: dict[tuple[str, ...], tuple[int, int, int, int, int, int]] = {}
    blob_sizes: dict[tuple[str, ...], int] = {}
    try:
        for entry in _hf_entries(blobs_fd, budget, deadline, report):
            if entry.name.endswith(".incomplete") or entry.name == ".work":
                raise OSError("incomplete blob data")
            info = entry.stat(follow_symlinks=False)
            _hf_check_info(info, root_info)
            if not stat.S_ISREG(info.st_mode) or stat.S_ISLNK(info.st_mode):
                raise OSError("unsafe blob data")
            blobs[repo_parts + ("blobs", entry.name)] = _hf_identity(info)
            blob_sizes[repo_parts + ("blobs", entry.name)] = max(
                int(info.st_size), int(getattr(info, "st_blocks", 0)) * 512,
            )
    finally:
        os.close(blobs_fd)
    refs = _hf_scan_refs(root_fd, root_info, repo_parts, budget, deadline, report)
    snapshots_fd = _hf_open_path(root_fd, repo_parts + ("snapshots",))
    candidates: list[dict] = []
    try:
        for entry in _hf_entries(snapshots_fd, budget, deadline, report):
            info = entry.stat(follow_symlinks=False)
            _hf_check_info(info, root_info)
            if not stat.S_ISDIR(info.st_mode) or stat.S_ISLNK(info.st_mode):
                raise OSError("unsafe snapshot revision")
            state, modified, snapshot_expected, referenced_blobs = _hf_snapshot_state(
                root_fd, root_info, repo_parts, entry.name, blobs, budget, deadline, report,
            )
            candidates.append({
                "repo": repo_parts,
                "commit": entry.name,
                "snapshot": state,
                "modified": modified,
                "snapshot_expected": snapshot_expected,
                "expected": snapshot_expected,
                "referenced_blobs": referenced_blobs,
                "delete_blobs": (),
                "refs": refs.get(entry.name, []),
                "blobs": blobs,
                "deleted": False,
            })
    finally:
        os.close(snapshots_fd)
    for candidate in candidates:
        retained_references = set().union(*(
            other["referenced_blobs"] for other in candidates if other is not candidate
        )) if len(candidates) > 1 else set()
        exclusive = candidate["referenced_blobs"] - retained_references
        unique = {path for path in exclusive if blobs[path][5] == 1}
        if len(unique) != len(exclusive):
            _skip(report["cleaners"]["huggingface_cache"], "blob_link_count_uncertain",
                  len(exclusive) - len(unique))
        candidate["delete_blobs"] = tuple((path, blobs[path]) for path in sorted(unique))
        candidate["expected"] += sum(blob_sizes[path] for path in unique)
        candidate["repo_candidates"] = candidates
    return candidates


def _hf_scan_cache(
    root_fd: int,
    root_info: os.stat_result,
    budget: dict,
    deadline: float,
    report: dict,
) -> list[dict]:
    repositories: list[tuple[str, ...]] = []
    direct_layout = False
    for entry in _hf_entries(root_fd, budget, deadline, report):
        info = entry.stat(follow_symlinks=False)
        _hf_check_info(info, root_info)
        if entry.name == ".locks":
            if not stat.S_ISDIR(info.st_mode) or stat.S_ISLNK(info.st_mode):
                raise OSError("unsafe lock root")
        elif entry.name in {"blobs", "refs", "snapshots"}:
            direct_layout = True
        elif entry.name == "version.txt":
            if not stat.S_ISREG(info.st_mode) or stat.S_ISLNK(info.st_mode):
                raise OSError("unsafe cache version")
        elif entry.name.startswith(("models--", "datasets--", "spaces--")):
            if not stat.S_ISDIR(info.st_mode) or stat.S_ISLNK(info.st_mode):
                raise OSError("unsafe repository root")
            repositories.append((entry.name,))
        else:
            raise OSError("unknown cache root data")
    if direct_layout:
        if repositories:
            raise OSError("ambiguous cache layout")
        repositories.append(())
    candidates: list[dict] = []
    for repo_parts in repositories:
        candidates.extend(_hf_scan_repo(root_fd, root_info, repo_parts, budget, deadline, report))
    return candidates


def _strategy_paths(strategy: object, name: str) -> frozenset[Path]:
    value = getattr(strategy, name, None)
    if not isinstance(value, (set, frozenset)) or any(not isinstance(path, Path) for path in value):
        raise OSError("unknown strategy semantics")
    return frozenset(value)


def _validate_hf_strategy(strategy: object, revision: object, root: Path, deadline: float, scan_limit: int, report: dict) -> int:
    """Prove the SDK plan removes only this revision's tracked cache data."""
    root_info = root.stat()
    repos = _strategy_paths(strategy, "repos")
    if repos:
        raise OSError("whole repository strategy")
    blobs = _strategy_paths(strategy, "blobs")
    refs = _strategy_paths(strategy, "refs")
    snapshots = _strategy_paths(strategy, "snapshots")
    if not snapshots:
        raise OSError("strategy has no snapshot")
    files = getattr(revision, "files", None)
    if not isinstance(files, (set, frozenset)):
        raise OSError("unknown revision semantics")
    tracked_files: set[Path] = set()
    tracked_blobs: set[Path] = set()
    for cached_file in files:
        file_path = getattr(cached_file, "file_path", None)
        blob_path = getattr(cached_file, "blob_path", None)
        if not isinstance(file_path, Path) or not isinstance(blob_path, Path):
            raise OSError("unknown cached file semantics")
        tracked_files.add(file_path)
        tracked_blobs.add(blob_path)
    if not blobs.issubset(tracked_blobs):
        raise OSError("strategy contains untracked blob")
    snapshot_path = getattr(revision, "snapshot_path", None)
    revision_refs = getattr(revision, "refs", None)
    if not isinstance(snapshot_path, Path) or not isinstance(revision_refs, (set, frozenset)):
        raise OSError("unknown revision paths")
    expected_refs = frozenset(snapshot_path.parent.parent / "refs" / ref for ref in revision_refs
                              if isinstance(ref, str))
    if len(expected_refs) != len(revision_refs) or snapshots != frozenset({snapshot_path}):
        raise OSError("strategy targets another revision")
    if not refs.issubset(expected_refs):
        raise OSError("strategy contains untracked ref")

    planned: dict[Path, os.stat_result] = {}
    scanned = 0
    for path in blobs | refs:
        checked, info = _checked_hf_path(path, root, root_info, directory=False)
        planned[checked] = info
    for snapshot in snapshots:
        checked, info = _checked_hf_path(snapshot, root, root_info, directory=True)
        planned[checked] = info
        stack = [checked]
        while stack:
            if time.monotonic() >= deadline:
                report["caps"]["deadline"] = True
                raise TimeoutError("strategy validation deadline")
            directory = stack.pop()
            with os.scandir(directory) as entries:
                for entry in entries:
                    scanned += 1
                    if scanned >= scan_limit:
                        report["caps"]["scan"] = True
                        raise OSError("strategy validation scan cap")
                    path = Path(entry.path)
                    if _reserved_hf_path(path, root):
                        raise OSError("reserved snapshot data")
                    child_info = entry.stat(follow_symlinks=False)
                    if child_info.st_uid != os.geteuid() or child_info.st_dev != root_info.st_dev:
                        raise OSError("snapshot ownership or device mismatch")
                    planned[path] = child_info
                    if stat.S_ISDIR(child_info.st_mode) and not stat.S_ISLNK(child_info.st_mode):
                        stack.append(path)
                    elif not (stat.S_ISREG(child_info.st_mode) or stat.S_ISLNK(child_info.st_mode)):
                        raise OSError("unsupported snapshot entry")
                    elif path not in tracked_files:
                        raise OSError("untracked snapshot data")
    if not tracked_files or not tracked_files.issubset(planned):
        raise OSError("incomplete strategy coverage")
    return sum(int(info.st_size) for info in planned.values())


def _hf_leaf_info(root_fd: int, parts: tuple[str, ...]) -> os.stat_result:
    if not parts:
        return os.fstat(root_fd)
    parent_fd = _hf_open_path(root_fd, parts[:-1])
    try:
        return os.stat(parts[-1], dir_fd=parent_fd, follow_symlinks=False)
    finally:
        os.close(parent_fd)


def _hf_recheck_ref(root_fd: int, path_parts: tuple[str, ...], identity: tuple[int, int, int, int, int, int], commit: str) -> None:
    parent_fd = _hf_open_path(root_fd, path_parts[:-1])
    try:
        info = os.stat(path_parts[-1], dir_fd=parent_fd, follow_symlinks=False)
        if _hf_identity(info) != identity or not stat.S_ISREG(info.st_mode):
            raise OSError("cache reference changed")
        descriptor = os.open(path_parts[-1], os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0), dir_fd=parent_fd)
        try:
            if _hf_identity(os.fstat(descriptor)) != identity:
                raise OSError("cache reference changed")
            payload = os.read(descriptor, 257)
        finally:
            os.close(descriptor)
        if len(payload) > 256 or payload.decode("ascii").strip() != commit:
            raise OSError("cache reference retargeted")
    finally:
        os.close(parent_fd)


def _hf_recheck_repository_snapshots(
    root_fd: int,
    root_info: os.stat_result,
    candidate: dict,
    budget: dict,
    deadline: float,
    report: dict,
) -> None:
    live = [item for item in candidate["repo_candidates"] if not item["deleted"]]
    snapshots_fd = _hf_open_path(root_fd, candidate["repo"] + ("snapshots",))
    try:
        names: set[str] = set()
        for entry in _hf_entries(snapshots_fd, budget, deadline, report):
            info = entry.stat(follow_symlinks=False)
            _hf_check_info(info, root_info)
            if not stat.S_ISDIR(info.st_mode) or stat.S_ISLNK(info.st_mode):
                raise OSError("snapshot set changed")
            names.add(entry.name)
    finally:
        os.close(snapshots_fd)
    if names != {item["commit"] for item in live}:
        raise OSError("snapshot set changed")
    for item in live:
        state, modified, expected, referenced = _hf_snapshot_state(
            root_fd, root_info, item["repo"], item["commit"], item["blobs"],
            budget, deadline, report,
        )
        if (state != item["snapshot"] or modified != item["modified"]
                or expected != item["snapshot_expected"]
                or referenced != item["referenced_blobs"]):
            raise OSError("snapshot changed before deletion")


def _hf_unlink_checked(root_fd: int, path_parts: tuple[str, ...], identity: tuple[int, int, int, int, int, int], *, directory: bool) -> None:
    parent_fd = _hf_open_path(root_fd, path_parts[:-1])
    try:
        info = os.stat(path_parts[-1], dir_fd=parent_fd, follow_symlinks=False)
        current_identity = _hf_identity(info)
        if current_identity != identity and not (
                directory and current_identity[:3] == identity[:3]
        ):
            raise OSError("cache entry changed before deletion")
        if directory:
            if not stat.S_ISDIR(info.st_mode) or stat.S_ISLNK(info.st_mode):
                raise OSError("cache directory changed type")
            os.rmdir(path_parts[-1], dir_fd=parent_fd)
        else:
            if stat.S_ISDIR(info.st_mode) and not stat.S_ISLNK(info.st_mode):
                raise OSError("cache file changed type")
            os.unlink(path_parts[-1], dir_fd=parent_fd)
    finally:
        os.close(parent_fd)


def _hf_execute_candidate(root_fd: int, candidate: dict) -> None:
    repo_parts = candidate["repo"]
    commit = candidate["commit"]
    for path_parts, identity in candidate["refs"]:
        _hf_unlink_checked(root_fd, path_parts, identity, directory=False)
    snapshot_root = repo_parts + ("snapshots", commit)
    state = candidate["snapshot"]
    for relative, identity in sorted(state.items(), key=lambda item: len(item[0]), reverse=True):
        path_parts = snapshot_root + relative
        _hf_unlink_checked(
            root_fd,
            path_parts,
            identity,
            directory=identity[2] == stat.S_IFDIR,
        )
    for path_parts, identity in candidate["delete_blobs"]:
        _hf_unlink_checked(root_fd, path_parts, identity, directory=False)


def _run_hf(
    home: Path,
    policy: DiskCleanupPolicy,
    active_slot_count: int,
    now: float,
    deadline: float,
    report: dict,
) -> tuple[int, int]:
    cleaner = report["cleaners"]["huggingface_cache"]
    configured = policy.cleaners.get("huggingface_cache")
    if configured is None:
        return 0, 0
    if active_slot_count > 0:
        _skip(cleaner, "active_slots")
        return 0, 0

    root_fd = -1
    lock_fds: list[int] = []
    try:
        root = _fixed_root(home, (".cache", "huggingface", "hub"), required=False)
        if root is None:
            _skip(cleaner, "root_absent")
            return 0, 0
        root_fd = os.open(
            root,
            os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0),
        )
        root_info = os.fstat(root_fd)
        _hf_check_info(root_info, root_info)
        path_info = root.stat()
        if _hf_identity(path_info) != _hf_identity(root_info):
            raise OSError("cache root changed while opening")
        if policy.mode == "enforce":
            _hf_recover_lock_barrier(root_fd, root_info)
            root_info = os.fstat(root_fd)
            path_info = root.stat()
            if _hf_identity(path_info) != _hf_identity(root_info):
                raise OSError("cache root changed during lock barrier recovery")
        budget = {"remaining": policy.max_scan_items}
        try:
            lock_state, lock_fds, locks_present = _hf_lock_state(
                root_fd, root_info, budget, deadline, report, acquire=True,
            )
        except BlockingIOError:
            _skip(cleaner, "cache_locked")
            os.close(root_fd)
            root_fd = -1
            return 0, 0
        candidates = _hf_scan_cache(root_fd, root_info, budget, deadline, report)
    except BaseException as exc:
        if report["caps"]["deadline"]:
            _skip(cleaner, "scan_deadline")
        elif report["caps"]["scan"]:
            _skip(cleaner, "scan_cap")
        else:
            _add_error(report, "huggingface_cache", exc)
        for descriptor in lock_fds:
            os.close(descriptor)
        if root_fd >= 0:
            os.close(root_fd)
        return 0, 0

    young = sum(candidate["modified"] > now - configured.min_age_seconds
                for candidate in candidates)
    if young:
        _skip(cleaner, "too_young", young)
    candidates = [candidate for candidate in candidates
                  if candidate["modified"] <= now - configured.min_age_seconds]
    cleaner["eligible_items"] = len(candidates)
    candidates.sort(key=lambda candidate: (candidate["modified"], candidate["repo"], candidate["commit"]))
    deleted = 0
    selected = 0
    expected_total = 0
    try:
        for candidate in candidates:
            if time.monotonic() >= deadline:
                report["caps"]["deadline"] = True
                break
            if selected >= policy.max_items_per_pass:
                report["caps"]["items"] = True
                break
            if _free_bytes(home) >= policy.target_free_gb * _GIB:
                break
            expected = int(candidate["expected"])
            remaining = policy.max_bytes_per_pass - expected_total
            if expected > remaining:
                _skip(cleaner, "byte_cap")
                report["caps"]["bytes"] = True
                continue
            cleaner["expected_bytes"] += expected
            selected += 1
            if policy.mode != "enforce":
                expected_total += expected
                continue
            if not locks_present:
                _skip(cleaner, "lock_root_absent")
                break
            current_root = _fixed_root(home, (".cache", "huggingface", "hub"), required=True)
            if current_root is None or _hf_identity(current_root.stat()) != _hf_identity(root_info):
                _skip(cleaner, "root_changed")
                break
            try:
                _hf_recheck_repository_snapshots(
                    root_fd, root_info, candidate, budget, deadline, report,
                )
                for path_parts, identity in candidate["refs"]:
                    _hf_tick(budget, deadline, report)
                    _hf_recheck_ref(root_fd, path_parts, identity, candidate["commit"])
                current_locks, temporary_fds, current_locks_present = _hf_lock_state(
                    root_fd, root_info, budget, deadline, report, acquire=False,
                )
                for descriptor in temporary_fds:
                    os.close(descriptor)
                if not current_locks_present or current_locks != lock_state:
                    raise OSError("cache lock set changed")
                if any(_hf_identity(os.fstat(descriptor)) not in lock_state.values()
                       for descriptor in lock_fds):
                    raise OSError("held cache lock changed")
            except BaseException as exc:
                _add_error(report, "huggingface_recheck", exc)
                break
            before = _free_bytes(home)
            try:
                barrier_identities = _hf_enter_lock_barrier(
                    root_fd, root_info, lock_state, lock_fds, budget, deadline, report,
                )
                try:
                    _hf_execute_candidate(root_fd, candidate)
                finally:
                    _hf_leave_lock_barrier(root_fd, root_info, barrier_identities)
            except BaseException as exc:
                _add_error(report, "huggingface_delete", exc)
                break
            root_info = os.fstat(root_fd)
            candidate["deleted"] = True
            after = _free_bytes(home)
            actual = max(0, after - before)
            deleted += 1
            expected_total += expected
            cleaner["deleted_items"] += 1
            cleaner["actual_free_delta_bytes"] += actual
        return deleted, expected_total
    finally:
        for descriptor in lock_fds:
            os.close(descriptor)
        if root_fd >= 0:
            os.close(root_fd)


def _scan_weles(home: Path, policy: DiskCleanupPolicy, now: float, remaining_scan: int, report: dict) -> None:
    cleaner = report["cleaners"]["weles_recordings"]
    configured = policy.cleaners.get("weles_recordings")
    if configured is None or remaining_scan <= 0:
        return
    try:
        root = _fixed_root(home, ("weles", "recordings"), required=False)
        if root is None:
            _skip(cleaner, "root_absent")
            return
        ordered = []
        with os.scandir(root) as entries:
            for entry in entries:
                ordered.append(entry)
                if len(ordered) >= remaining_scan:
                    break
        ordered.sort(key=lambda entry: entry.name)
        for entry in ordered:
            cleaner["scanned_items"] += 1
            if entry.name == "local" or entry.name.startswith("."):
                _skip(cleaner, "reserved_or_hidden")
                continue
            try:
                info = entry.stat(follow_symlinks=False)
            except OSError:
                _skip(cleaner, "stat_failed")
                continue
            if not stat.S_ISDIR(info.st_mode) or stat.S_ISLNK(info.st_mode):
                _skip(cleaner, "not_run_directory")
                continue
            if info.st_uid != os.geteuid() or info.st_dev != home.stat().st_dev:
                _skip(cleaner, "unsafe_owner_or_device")
                continue
            if info.st_mtime > now - configured.min_age_seconds:
                _skip(cleaner, "too_young")
                continue
            # Current Weles uploads provide no durable whole-run proof.  Age is
            # reportable but never sufficient authorization to delete.
            _skip(cleaner, "upload_proof_unavailable_v1")
    except BaseException as exc:
        _add_error(report, "weles_recordings", exc)


def _finish(report: dict, started: float, home: Optional[Path], state_dir: Optional[Path], attempted_at: float, log_fn: Callable[[str], None]) -> dict:
    if home is not None:
        try:
            report["free_bytes_after"] = _free_bytes(home)
        except OSError:
            pass
    report["duration_ms"] = max(0, int((time.monotonic() - started) * 1000))
    if state_dir is not None:
        try:
            _write_state(state_dir, report, attempted_at)
        except BaseException as exc:
            _add_error(report, "state_write", exc)
            if report["outcome"] not in {"lock_busy", "invalid_or_unavailable_policy"}:
                report["outcome"] = "partial_error"
    report["errors"] = report["errors"][:_MAX_ERRORS]
    try:
        log_fn(json.dumps(report, sort_keys=True, separators=(",", ":")))
    except BaseException:
        pass
    return report


def run_cleanup_once(
    active_slot_count: int,
    force: bool = False,
    log_fn: Callable[[str], None] = _default_log,
) -> dict:
    """Resolve canonical policy and execute at most one bounded cleanup pass."""
    started = time.monotonic()
    attempted_at = time.time()
    report = _base_report(active_slot_count)
    home: Optional[Path] = None
    state_dir: Optional[Path] = None
    lock_fd: Optional[int] = None
    try:
        home = _secure_home()
        state_dir = _ensure_state_dir(home)
        lock_fd = _acquire_lock(state_dir)
        if lock_fd is None:
            report["lock_busy"] = True
            report["outcome"] = "lock_busy"
            return _finish(report, started, home, None, attempted_at, log_fn)

        try:
            target, policy, digest = resolve_canonical_policy(report["hostname"])
        except BaseException as exc:
            _add_error(report, "policy", exc)
            return _finish(report, started, home, state_dir, attempted_at, log_fn)

        report["target_name"] = target.name
        report["policy_digest"] = digest
        report["mode"] = policy.mode
        report["check_interval_seconds"] = policy.check_interval_seconds
        report["low_bytes"] = policy.low_free_gb * _GIB
        report["target_bytes"] = policy.target_free_gb * _GIB

        try:
            previous = _read_state(state_dir)
        except BaseException as exc:
            _add_error(report, "state_read", exc)
            return _finish(report, started, home, state_dir, attempted_at, log_fn)
        previous_report = previous.get("report") if isinstance(previous.get("report"), dict) else {}
        report["last_success_at"] = previous_report.get("last_success_at")
        last_attempt = previous.get("last_attempt_at")
        if not force and isinstance(last_attempt, (int, float)) and attempted_at - last_attempt < policy.check_interval_seconds:
            report["outcome"] = "interval_noop"
            return _finish(report, started, home, state_dir, float(last_attempt), log_fn)

        before = _free_bytes(home)
        report["free_bytes_before"] = before
        report["free_bytes_after"] = before
        report["pressure_active"] = before < policy.low_free_gb * _GIB
        if policy.mode == "off" or not report["pressure_active"]:
            report["outcome"] = "healthy_noop"
            report["last_success_at"] = _utc_now()
            return _finish(report, started, home, state_dir, attempted_at, log_fn)

        deadline = time.monotonic() + _DEADLINE_SECONDS
        _run_hf(home, policy, report["active_slot_count"], attempted_at, deadline, report)
        scanned = report["cleaners"]["huggingface_cache"]["scanned_items"]
        remaining_scan = max(0, policy.max_scan_items - scanned)
        if remaining_scan == 0 and "weles_recordings" in policy.cleaners:
            report["caps"]["scan"] = True
        _scan_weles(home, policy, attempted_at, remaining_scan, report)
        if sum(item["scanned_items"] for item in report["cleaners"].values()) >= policy.max_scan_items:
            report["caps"]["scan"] = True

        after = _free_bytes(home)
        report["free_bytes_after"] = after
        deleted = report["cleaners"]["huggingface_cache"]["deleted_items"]
        if policy.mode != "enforce":
            report["outcome"] = "report_only"
        elif report["active_slot_count"] > 0 and "huggingface_cache" in policy.cleaners:
            report["outcome"] = "blocked_active"
        elif after >= policy.target_free_gb * _GIB:
            report["outcome"] = "reclaimed_target"
        elif any(report["caps"].values()):
            report["outcome"] = "cap_reached"
        elif report["errors"]:
            report["outcome"] = "partial_error"
        elif deleted == 0:
            report["outcome"] = "no_eligible_items"
        else:
            report["outcome"] = "partial_error"
        if not report["errors"]:
            report["last_success_at"] = _utc_now()
        return _finish(report, started, home, state_dir, attempted_at, log_fn)
    except BaseException as exc:
        _add_error(report, "runtime", exc)
        report["outcome"] = "invalid_or_unavailable_policy"
        return _finish(report, started, home, state_dir, attempted_at, log_fn)
    finally:
        if lock_fd is not None:
            try:
                fcntl.flock(lock_fd, fcntl.LOCK_UN)
            finally:
                os.close(lock_fd)
