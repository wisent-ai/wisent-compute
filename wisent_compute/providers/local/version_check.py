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
import urllib.request
from importlib.metadata import PackageNotFoundError, version as _local_version

_PACKAGES = ("wisent-compute", "wisent", "wisent-tools")
_CACHE: dict[str, tuple[float, str]] = {}
_CACHE_TTL_SECONDS = 300


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
        ) as resp:
            data = json.loads(resp.read())
    except Exception:
        return None
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
    import subprocess, sys
    try:
        res = subprocess.run(
            [sys.executable, "-c", "import wisent; import wisent_compute"],
            capture_output=True, text=True,
        )
    except Exception as e:
        return False, f"smoke subprocess error: {e}"
    if res.returncode == 0:
        return True, ""
    return False, (res.stderr or res.stdout or "(no output)").strip()[:400]


def maybe_drain_or_upgrade(slots: list, log_fn) -> bool:
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

    Caller MUST advance slots BEFORE calling this so a drained slots list
    triggers the upgrade path. The earlier inline version in local_agent.py
    accidentally ran continue BEFORE advance_slot, leaving slots full
    forever and the agent permanently stuck in drain mode.
    """
    drift = detect_drift()
    ok, err = wisent_import_ok()
    if not drift and ok:
        return False
    if not slots:
        if not ok:
            log_fn(f"venv broken: {err}; pip_upgrade_and_exec")
        else:
            log_fn(f"drift {drift}; pip_upgrade_and_exec")
        try:
            pip_upgrade_and_exec(log_fn)
        except Exception as e:
            log_fn(f"upgrade failed: {e}")
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

    # venv-aware --user selection. pip rejects --user inside a virtualenv
    # with "Can not perform a '--user' install. User site-packages are not
    # visible in this virtualenv." Detect venv via sys.prefix vs
    # sys.base_prefix (real_prefix is legacy virtualenv); skip --user in
    # that case so pip installs into the active venv's site-packages.
    in_venv = sys.prefix != getattr(sys, "base_prefix", sys.prefix)
    in_venv = in_venv or hasattr(sys, "real_prefix")
    pip_args = [sys.executable, "-m", "pip", "install", "--upgrade", *_PACKAGES]
    if os.geteuid() != 0 and not in_venv:
        # User-mode pip: --user only when not root AND not in a venv.
        pip_args.insert(4, "--user")
    pip_args.append("--break-system-packages")
    res = subprocess.run(pip_args, capture_output=True, text=True)
    # First-failure self-heal: if pip rejected --user (e.g. a venv detection
    # gap), retry once WITHOUT --user. Without this hatch the coordinator
    # gets stuck on a buggy version forever and only operator intervention
    # can unstick it.
    if res.returncode != 0 and "--user" in pip_args and "--user" in (res.stderr or ""):
        log_fn("pip_upgrade_and_exec: --user rejected; retrying without it")
        pip_args.remove("--user")
        res = subprocess.run(pip_args, capture_output=True, text=True)
    if res.returncode != 0:
        log_fn(f"pip_upgrade_and_exec: pip install failed rc={res.returncode} "
               f"stderr={(res.stderr or '')[:300]}")
        raise RuntimeError(f"pip upgrade failed: rc={res.returncode}")
    log_fn(f"pip_upgrade_and_exec: pip install ok; re-execing {sys.argv}")
    os.execv(sys.executable, [sys.executable, *sys.argv])
