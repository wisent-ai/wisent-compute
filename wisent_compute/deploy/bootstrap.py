"""`wc bootstrap` implementation: provision the agent on remote boxes.

For each kind=local registry entry with an ssh field, installs/upgrades
wisent-compute, writes a systemd unit that runs `wc agent` with the
configured WC_LOCAL_SLOTS, and enables it so the agent comes back up
on reboot. Targets with ssh=null are listed as unprovisioned.

Idempotent: re-running just refreshes the unit and re-enables it. The
existing capacity broadcast loop continues uninterrupted because the
unit's ExecStart is identical.
"""
from __future__ import annotations

import shlex
import subprocess
from typing import Callable

UNIT_TEMPLATE = """[Unit]
Description=Wisent Compute local GPU agent ({name})
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
Environment=WC_LOCAL_SLOTS={slots}
Environment=PYTHONUNBUFFERED=1
ExecStart={wc_bin} agent --target {name}
Restart=on-failure
RestartSec=30
User={user}

[Install]
WantedBy=multi-user.target
"""

WATCHDOG_UNIT_TEMPLATE = """[Unit]
Description=Wisent Compute diagnostics watchdog ({name})
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
Environment=PYTHONUNBUFFERED=1
Environment=GOOGLE_CLOUD_PROJECT=wisent-480400
Environment=GCP_PROJECT=wisent-480400
Environment=WC_BUCKET=wisent-compute
ExecStart={watchdog_bin}
Restart=on-failure
RestartSec=30
User={user}

[Install]
WantedBy=multi-user.target
"""

WC_BIN_SUFFIX = "/wc"
DEFAULT_REMOTE_WC_BIN = "$HOME/.local/bin/wc"
WATCHDOG_BIN_NAME = "wc-watchdog"
FAILURE_FIXER_TARGET = "failure-fixer"
WATCHDOG_TARGET = "watchdog"
LOCAL_KIND = "local"
AGENT_KIND = "agent"
COORDINATOR_KIND = "coordinator"
DAEMON_RUNTIME = "daemon"
CRON_RUNTIME = "cron"
LOCAL_SERVICE_RUNTIMES = (DAEMON_RUNTIME, CRON_RUNTIME)
GCP_CLOUD_FUNCTION_RUNTIME = "gcp_cloud_function"

REMOTE_INSTALL_SCRIPT = """set -euo pipefail
python3 -m pip install --upgrade --user wisent-compute >/tmp/wc_install.log 2>&1
WC_BIN="$(python3 -c 'import shutil,sys; sys.stdout.write(shutil.which(\"wc\") or \"\")')"
if [ -z "$WC_BIN" ]; then
  WC_BIN="$HOME/.local/bin/wc"
fi
echo "$WC_BIN"
"""


def _run_ssh(ssh_target: str, command: str, capture: bool = True):
    return subprocess.run(
        ["ssh", "-o", "StrictHostKeyChecking=accept-new", ssh_target, command],
        capture_output=capture, text=True, check=False,
    )


def _resolve_remote_wc(ssh_target: str) -> str:
    r = _run_ssh(ssh_target, REMOTE_INSTALL_SCRIPT)
    if r.returncode != 0:
        raise RuntimeError(f"install failed: {r.stderr or r.stdout}")
    out = (r.stdout or "").strip().splitlines()
    return out[-1] if out else ""


def _write_unit(ssh_target: str, unit_name: str, unit_text: str) -> None:
    payload = unit_text.replace("\\", "\\\\").replace("'", "'\\''")
    unit_path = shlex.quote(f"/etc/systemd/system/{unit_name}")
    unit_arg = shlex.quote(unit_name)
    cmd = (
        f"echo '{payload}' | sudo tee {unit_path} "
        f">/dev/null && sudo systemctl daemon-reload && "
        f"sudo systemctl enable --now {unit_arg}"
    )
    r = _run_ssh(ssh_target, cmd)
    if r.returncode != 0:
        raise RuntimeError(f"unit install failed: {r.stderr or r.stdout}")


def _sibling_bin(wc_bin: str, name: str) -> str:
    if wc_bin.endswith(WC_BIN_SUFFIX):
        return f"{wc_bin.removesuffix(WC_BIN_SUFFIX)}/{name}"
    return name


def _provision(target, dry_run: bool, echo: Callable[[str], None]) -> None:
    ssh_target = target.ssh
    if not ssh_target:
        echo(f"[skip] {target.name}: ssh=null (no host configured)")
        return
    user = ssh_target.split("@", 1)[0] if "@" in ssh_target else "root"

    if dry_run:
        wc_bin = DEFAULT_REMOTE_WC_BIN
    else:
        echo(f"[install] {target.name}: pip install --upgrade wisent-compute on {ssh_target}")
        wc_bin = _resolve_remote_wc(ssh_target) or DEFAULT_REMOTE_WC_BIN

    unit_text = UNIT_TEMPLATE.format(
        name=target.name,
        slots=target.slots,
        wc_bin=wc_bin,
        user=user,
    )
    watchdog_text = WATCHDOG_UNIT_TEMPLATE.format(
        name=target.name,
        watchdog_bin=_sibling_bin(wc_bin, WATCHDOG_BIN_NAME),
        user=user,
    )

    if dry_run:
        echo(f"--- {target.name} systemd unit ---")
        for line in unit_text.splitlines():
            echo(f"  {line}")
        echo(f"--- {target.name} watchdog systemd unit ---")
        for line in watchdog_text.splitlines():
            echo(f"  {line}")
        echo(f"--- ssh command (would run): ssh {shlex.quote(ssh_target)} 'install + enable' ---")
        return

    echo(f"[unit] {target.name}: writing /etc/systemd/system/wisent-compute-agent.service")
    _write_unit(ssh_target, "wisent-compute-agent.service", unit_text)
    echo(f"[unit] {target.name}: writing /etc/systemd/system/wisent-compute-watchdog.service")
    _write_unit(ssh_target, "wisent-compute-watchdog.service", watchdog_text)
    echo(f"[ok]   {target.name}: enabled, agent running with WC_LOCAL_SLOTS={target.slots}")


def run(targets, dry_run: bool, echo: Callable[[str], None]) -> None:
    for t in targets:
        try:
            _provision(t, dry_run, echo)
        except Exception as exc:
            echo(f"[err]  {t.name}: {exc}")


def run_bootstrap(target, dry_run: bool, local_install: bool,
                  echo: Callable[[str], None]) -> None:
    """Top-level dispatcher used by `wc bootstrap`. Decides between the
    SSH-based remote install and the local launchd/systemd --user install,
    and accepts either a kind=local target or a runtime=daemon coordinator.
    """
    from ..targets import load_targets, lookup, lookup_coordinator
    from .local_install import install_local

    if local_install:
        if not target:
            raise RuntimeError("--local requires --target NAME")
        # Special target: failure-fixer is a wisent-compute-internal
        # daemon, not a registry coordinator entry. Treated like the
        # local install path but with kind=failure-fixer so the
        # ExecArgs come from the bash-loop branch in
        # local_install._exec_args_for.
        if target == FAILURE_FIXER_TARGET:
            from types import SimpleNamespace
            install_local(SimpleNamespace(name=FAILURE_FIXER_TARGET),
                          FAILURE_FIXER_TARGET, dry_run, echo)
            return
        if target == WATCHDOG_TARGET:
            from types import SimpleNamespace
            install_local(SimpleNamespace(name=WATCHDOG_TARGET),
                          WATCHDOG_TARGET, dry_run, echo)
            return
        t = lookup(target)
        if t and t.kind == LOCAL_KIND:
            install_local(t, AGENT_KIND, dry_run, echo)
            return
        c = lookup_coordinator(target)
        if c and c.runtime in LOCAL_SERVICE_RUNTIMES:
            install_local(c, COORDINATOR_KIND, dry_run, echo)
            return
        if c and c.runtime == GCP_CLOUD_FUNCTION_RUNTIME:
            raise RuntimeError(
                f"coordinator '{target}' runtime=gcp_cloud_function: deployed via CI, "
                "not provisionable as a local service."
            )
        raise RuntimeError(f"'{target}' not found in registry (or wrong kind/runtime)")

    targets = [lookup(target)] if target else None
    if targets is not None and targets[0] is None:
        raise RuntimeError(f"target '{target}' not found in registry")
    if targets is None:
        targets = [t for t in load_targets() if t.kind == LOCAL_KIND]
    run(targets, dry_run=dry_run, echo=echo)
