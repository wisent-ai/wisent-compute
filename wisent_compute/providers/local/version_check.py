"""PyPI version-drift check used by the agent main loop to self-recycle
when newer wisent-compute / wisent / wisent-tools releases ship.

Without this, an agent that pip-installed at boot time keeps running its
original version forever — newer code on PyPI never reaches the running
fleet until GCE preempts the VM, idle_shutdown fires (which only
triggers when the queue stops yielding work), or an operator manually
deletes the VM. The class of bugs this surfaces is "fixes shipped to
PyPI but not running": e.g. wisent-compute 0.4.59's verify_command
hook, wisent 0.11.23's batched HF commits, wisent-tools 0.1.16's
exit-non-zero-on-strategy-failure — all on PyPI but ignored by 32
already-running pre-fix VMs on 2026-05-06 producing fake-COMPLETED
jobs at ~80/hour.
"""
from __future__ import annotations

import json
import time
import urllib.error
import urllib.request
from importlib.metadata import PackageNotFoundError, version as _local_version

_PACKAGES = ("wisent-compute", "wisent", "wisent-tools")
_CACHE: dict[str, tuple[float, str]] = {}
_IMPORT_CACHE: tuple[float, bool, str] | None = None
_IMPORT_OK_TTL_SECONDS = 300
_IMPORT_BAD_TTL_SECONDS = 30
# Lower than the previous 300s so a freshly-published wheel reaches
# running agents within a single agent-loop iteration's network round
# trip rather than after a 5-minute cache miss. PyPI's CDN handles
# the per-loop request rate trivially — 1 GET per package per loop
# iteration over ~50s loop time = 0.06 RPS per agent, 3 agents in
# the fleet = 0.18 RPS — well under any rate limit.
_CACHE_TTL_SECONDS = 30


def _version_tuple(v: str) -> tuple:
    parts = []
    for token in v.replace("-", ".").split("."):
        try:
            parts.append((0, int(token)))
        except ValueError:
            parts.append((1, token))
    return tuple(parts)


def _pypi_latest(pkg: str) -> str | None:
    cached = _CACHE.get(pkg)
    if cached and (time.time() - cached[0]) < _CACHE_TTL_SECONDS:
        return cached[1]
    try:
        with urllib.request.urlopen(
            f"https://pypi.org/pypi/{pkg}/json",
            timeout=10,
        ) as resp:
            data = json.loads(resp.read())
    except (OSError, TimeoutError, urllib.error.URLError, json.JSONDecodeError):
        return cached[1] if cached else None
    releases = data.get("releases") or {}
    if not releases:
        return None
    latest = max(releases.keys(), key=_version_tuple)
    _CACHE[pkg] = (time.time(), latest)
    return latest


def detect_drift() -> dict[str, tuple[str, str]]:
    """{pkg: (installed, pypi_latest)} for every package whose PyPI
    release is newer than the installed version. Empty = no drift."""
    out: dict[str, tuple[str, str]] = {}
    for pkg in _PACKAGES:
        try:
            installed = _local_version(pkg)
        except PackageNotFoundError:
            continue
        latest = _pypi_latest(pkg)
        if latest is None:
            continue
        if _version_tuple(installed) < _version_tuple(latest):
            out[pkg] = (installed, latest)
    return out


def wisent_import_ok() -> tuple[bool, str]:
    """Smoke-test `import wisent` in a subprocess of the agent's own
    interpreter. Returns (ok, error_message). Run before claiming a job
    so a broken venv (e.g. PyPI ships a wheel whose __init__.py imports
    a name that the same release forgot to re-export, as wisent 0.11.36
    did with ImageAdapter) triggers a self-repair upgrade rather than
    claiming jobs that will fail their first `python -m wisent...` line.
    """
    global _IMPORT_CACHE
    now = time.time()
    if _IMPORT_CACHE is not None:
        ts, ok, err = _IMPORT_CACHE
        ttl = _IMPORT_OK_TTL_SECONDS if ok else _IMPORT_BAD_TTL_SECONDS
        if now - ts < ttl:
            return ok, err
    import subprocess, sys
    try:
        res = subprocess.run(
            [sys.executable, "-c", "import wisent; import wisent_compute"],
            capture_output=True, text=True, timeout=20,
        )
    except subprocess.TimeoutExpired:
        _IMPORT_CACHE = (now, False, "import smoke test timed out")
        return False, _IMPORT_CACHE[2]
    if res.returncode == 0:
        _IMPORT_CACHE = (now, True, "")
        return True, ""
    err = (res.stderr or res.stdout or "(no output)").strip()[:400]
    _IMPORT_CACHE = (now, False, err)
    return False, err


def maybe_drain_or_upgrade(slots: list, log_fn, kind: str = "local") -> bool:
    """Combined drift + venv-integrity handling for the agent main loop.
    Returns True if the caller should `continue` (skip claim path); False
    if no remediation needed.

    Two triggers for upgrade:
      1. PyPI release newer than the installed version (`detect_drift`).
      2. `import wisent` raises in the venv (broken wheel published to
         PyPI — happened with wisent 0.11.36's missing ImageAdapter
         re-export; agents booted at exactly the wrong moment cached the
         bad install and accepted+failed 5+ jobs in a row before this
         check existed).

    Cloud agents (kind != "local") DO NOT pip-upgrade-and-exec on drift.
    Reason: `pip install --upgrade` overwrites wisent_compute/*.py while
    the running interpreter has them mapped; `os.execv` then loads a
    half-rewritten module tree and aborts with exit 1. The cloud VM
    stays alive but the agent is dead — exactly the failure mode that
    produced the zombie 1778695548-{2,3,5} VMs on 2026-05-13 (capacity
    blob published once at boot, then agent silent, VM still RUNNING).
    Instead, cloud agents self-terminate on drift; the dispatcher will
    create a fresh VM whose startup-script pip-installs the new version
    BEFORE the agent starts — no in-process file race possible.

    Local workstations (kind="local") still pip_upgrade_and_exec
    because they are long-lived hardware and re-installing is the only
    way to pick up new releases without a reboot.

    Caller MUST advance slots BEFORE calling this so a drained slots
    list triggers the remediation path.
    """
    # If a job is active, drift cannot be applied yet. Avoid making PyPI a
    # liveness dependency for running work; a transient PyPI reset used to
    # raise out of detect_drift() and crash the agent while a slot was active.
    if slots:
        ok, err = wisent_import_ok()
        if ok:
            return False
        log_fn(f"venv broken while slots active: {err}")
        return True
    drift = detect_drift()
    ok, err = wisent_import_ok()
    if not drift and ok:
        return False
    if not slots:
        if kind != "local":
            log_fn(f"cloud agent {kind} drift={drift} ok={ok}; self-terminate "
                   f"so dispatcher creates a fresh VM with new version baked in")
            from .gcp_self import self_terminate
            self_terminate(log_fn)
            raise SystemExit(0)
        if not ok:
            log_fn(f"venv broken: {err}; pip_upgrade_and_exec")
        else:
            log_fn(f"drift {drift}; pip_upgrade_and_exec")
        pip_upgrade_and_exec(log_fn)
    return True


def pip_upgrade_and_exec(log_fn) -> None:
    """In-process upgrade + re-exec. Run when drift detected and slots
    have drained.

    Earlier the drift handler in local_agent.py just `raise SystemExit(0)`,
    expecting systemd to restart with a fresh pip install. That's correct
    for cloud VMs whose startup_gpu_agent.sh re-runs pip install --upgrade
    on every boot, but BROKEN for the workstation: its
    /etc/systemd/system/wisent-agent.service has Restart=on-failure (clean
    exit 0 doesn't trigger restart) AND no ExecStartPre=pip install. The
    workstation went silently dark on the first drift event.

    This function: pip install --upgrade the wisent stack, then os.execv
    the freshly-installed entry point. No systemd dependency, no restart
    cycle.
    """
    import os, subprocess, sys
    log_fn(f"pip_upgrade_and_exec: starting upgrade of {_PACKAGES}")

    # venv-aware --user selection.
    in_venv = sys.prefix != getattr(sys, "base_prefix", sys.prefix)
    in_venv = in_venv or hasattr(sys, "real_prefix")
    # --force-reinstall + --no-cache-dir overrides editable (-e) installs.
    # Without these, an agent running from `pip install -e /path/to/clone`
    # gets a no-op upgrade because pip considers the editable install
    # already-satisfying the requirement, even when pypi has a newer
    # version. The editable install keeps the same code on disk forever.
    # --no-cache-dir avoids serving the same stale wheel from local cache.
    # --no-deps is required on the workstation agent: the heavy dependency
    # graph is already installed, and reinstalling it pulls CUDA wheels into
    # TMPDIR during drift. Drift upgrades should replace Wisent packages only.
    pip_args = [sys.executable, "-m", "pip", "install", "--upgrade",
                "--force-reinstall", "--no-cache-dir", "--no-deps", *_PACKAGES]
    if os.geteuid() != 0 and not in_venv:
        pip_args.insert(4, "--user")
    pip_args.append("--break-system-packages")
    log_fn(f"pip_upgrade_and_exec: cmd={' '.join(pip_args)}")
    res = subprocess.run(pip_args, capture_output=True, text=True)
    log_fn(f"pip_upgrade_and_exec: rc={res.returncode} "
           f"stdout_tail={(res.stdout or '')[-300:]} "
           f"stderr_tail={(res.stderr or '')[-300:]}")
    if res.returncode != 0:
        raise RuntimeError(f"pip upgrade failed: rc={res.returncode}")
    log_fn(f"pip_upgrade_and_exec: pip install ok; re-execing {sys.argv}")
    os.execv(sys.executable, [sys.executable, *sys.argv])
