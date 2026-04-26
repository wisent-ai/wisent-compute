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


def _write_unit(ssh_target: str, unit_text: str) -> None:
    payload = unit_text.replace("\\", "\\\\").replace("'", "'\\''")
    cmd = (
        f"echo '{payload}' | sudo tee /etc/systemd/system/wisent-compute-agent.service "
        f">/dev/null && sudo systemctl daemon-reload && "
        f"sudo systemctl enable --now wisent-compute-agent.service"
    )
    r = _run_ssh(ssh_target, cmd)
    if r.returncode != 0:
        raise RuntimeError(f"unit install failed: {r.stderr or r.stdout}")


def _provision(target, dry_run: bool, echo: Callable[[str], None]) -> None:
    ssh_target = target.ssh
    if not ssh_target:
        echo(f"[skip] {target.name}: ssh=null (no host configured)")
        return
    user = ssh_target.split("@", 1)[0] if "@" in ssh_target else "root"

    if dry_run:
        wc_bin = "$HOME/.local/bin/wc"
    else:
        echo(f"[install] {target.name}: pip install --upgrade wisent-compute on {ssh_target}")
        wc_bin = _resolve_remote_wc(ssh_target) or "$HOME/.local/bin/wc"

    unit_text = UNIT_TEMPLATE.format(
        name=target.name,
        slots=target.slots,
        wc_bin=wc_bin,
        user=user,
    )

    if dry_run:
        echo(f"--- {target.name} systemd unit ---")
        for line in unit_text.splitlines():
            echo(f"  {line}")
        echo(f"--- ssh command (would run): ssh {shlex.quote(ssh_target)} 'install + enable' ---")
        return

    echo(f"[unit] {target.name}: writing /etc/systemd/system/wisent-compute-agent.service")
    _write_unit(ssh_target, unit_text)
    echo(f"[ok]   {target.name}: enabled, agent running with WC_LOCAL_SLOTS={target.slots}")


def run(targets, dry_run: bool, echo: Callable[[str], None]) -> None:
    for t in targets:
        try:
            _provision(t, dry_run, echo)
        except Exception as exc:
            echo(f"[err]  {t.name}: {exc}")
