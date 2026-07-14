"""systemd/cgroup-v2 ownership for local Stado job trees."""
from __future__ import annotations

import os
import re
import shutil
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path

_CGROUP_ROOT = Path("/sys/fs/cgroup")
_UNIT_SAFE = re.compile(r"[^A-Za-z0-9_.-]+")


@dataclass(frozen=True)
class ScopeStats:
    current_gb: float
    peak_gb: float
    oom_events: int
    oom_kill_events: int
    pids: tuple[int, ...]


def _user_systemd_env() -> dict[str, str]:
    uid = os.getuid()
    runtime = os.environ.get("XDG_RUNTIME_DIR", f"/run/user/{uid}")
    return {
        **os.environ,
        "XDG_RUNTIME_DIR": runtime,
        "DBUS_SESSION_BUS_ADDRESS": os.environ.get(
            "DBUS_SESSION_BUS_ADDRESS", f"unix:path={runtime}/bus",
        ),
    }


def cgroup_v2_available() -> bool:
    if not (
        (_CGROUP_ROOT / "cgroup.controllers").is_file()
        and shutil.which("systemd-run") is not None
        and shutil.which("systemctl") is not None
    ):
        return False
    result = subprocess.run(
        [shutil.which("systemctl") or "systemctl", "--user", "is-active", "default.target"],
        env=_user_systemd_env(),
        capture_output=True,
        text=True,
    )
    return result.returncode == 0 and result.stdout.strip() == "active"
class ScopeProcess:
    """Minimal Popen-compatible handle for a scope recovered after restart."""

    def __init__(self, unit: str, cgroup: str):
        self.unit = unit
        self.cgroup = cgroup

    @property
    def pid(self) -> int:
        pids = scope_pids(self.cgroup)
        return pids[0] if pids else 0

    def poll(self) -> int | None:
        return None if scope_pids(self.cgroup) else 0

    def terminate(self) -> None:
        terminate_scope(self.unit)

    def wait(self, timeout: float | None = None) -> int:
        deadline = None if timeout is None else time.monotonic() + timeout
        while self.poll() is None:
            if deadline is not None and time.monotonic() >= deadline:
                raise subprocess.TimeoutExpired(self.unit, timeout)
            time.sleep(0.05)
        return 0


def active_scopes() -> dict[str, str]:
    """Return active Stado scope units mapped to cgroup paths."""
    if not cgroup_v2_available():
        return {}
    systemctl = shutil.which("systemctl") or "systemctl"
    result = subprocess.run(
        [
            systemctl,
            "--user",
            "list-units",
            "--type=scope",
            "--state=running",
            "--no-legend",
            "--plain",
            "stado-job-*.scope",
        ],
        env=_user_systemd_env(),
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        return {}
    found: dict[str, str] = {}
    for line in result.stdout.splitlines():
        fields = line.split()
        if not fields:
            continue
        unit = fields[0]
        try:
            found[unit] = control_group(unit, retries=1)
        except RuntimeError:
            continue
    return found




def unit_name(job_id: str) -> str:
    safe = _UNIT_SAFE.sub("-", job_id).strip("-.") or "unknown"
    return f"stado-job-{safe}.scope"


def scope_command(job_id: str, command: str) -> tuple[list[str], dict[str, str], str]:
    """Wrap a command in a transient user scope; fail if ownership is absent."""
    if not cgroup_v2_available():
        raise RuntimeError("cgroup v2 and systemd-run are required for resource ownership")
    unit = unit_name(job_id)
    argv = [
        shutil.which("systemd-run") or "systemd-run",
        "--user",
        "--scope",
        "--collect",
        "--quiet",
        f"--unit={unit.removesuffix('.scope')}",
        "--",
        "/bin/bash",
        "-lc",
        command,
    ]
    return argv, _user_systemd_env(), unit


def control_group(unit: str, *, retries: int = 20) -> str:
    """Resolve a transient scope's absolute cgroup path."""
    command = [
        shutil.which("systemctl") or "systemctl",
        "--user",
        "show",
        unit,
        "--property=ControlGroup",
        "--value",
    ]
    for _ in range(max(1, retries)):
        result = subprocess.run(
            command, env=_user_systemd_env(), capture_output=True, text=True,
        )
        value = result.stdout.strip()
        if result.returncode == 0 and value:
            return value
        time.sleep(0.05)
    raise RuntimeError(f"systemd scope {unit} has no ControlGroup")


def _read_number(path: Path) -> int:
    try:
        raw = path.read_text(encoding="utf-8").strip()
    except OSError:
        return 0
    if raw == "max":
        return 0
    try:
        return int(raw)
    except ValueError:
        return 0


def scope_pids(cgroup: str) -> tuple[int, ...]:
    root = _CGROUP_ROOT / cgroup.lstrip("/")
    pids: set[int] = set()
    if not root.exists():
        return ()
    for procs in root.rglob("cgroup.procs"):
        try:
            values = procs.read_text(encoding="utf-8").split()
        except OSError:
            continue
        for value in values:
            try:
                pids.add(int(value))
            except ValueError:
                continue
    return tuple(sorted(pids))


def scope_stats(cgroup: str) -> ScopeStats:
    root = _CGROUP_ROOT / cgroup.lstrip("/")
    events: dict[str, int] = {}
    try:
        for line in (root / "memory.events").read_text(encoding="utf-8").splitlines():
            key, raw = line.split(maxsplit=1)
            events[key] = int(raw)
    except (OSError, ValueError):
        pass
    gib = float(1024 ** 3)
    return ScopeStats(
        current_gb=_read_number(root / "memory.current") / gib,
        peak_gb=_read_number(root / "memory.peak") / gib,
        oom_events=events.get("oom", 0),
        oom_kill_events=events.get("oom_kill", 0),
        pids=scope_pids(cgroup),
    )


def terminate_scope(unit: str) -> None:
    """Kill every descendant owned by a scope, then stop the transient unit."""
    env = _user_systemd_env()
    systemctl = shutil.which("systemctl") or "systemctl"
    subprocess.run(
        [systemctl, "--user", "kill", "--kill-whom=all", "--signal=KILL", unit],
        env=env, capture_output=True, text=True,
    )
    subprocess.run(
        [systemctl, "--user", "stop", unit],
        env=env, capture_output=True, text=True,
    )
