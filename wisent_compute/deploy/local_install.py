"""Local-machine install path for `wc bootstrap --local`.

Picks the right init system for the host OS and writes a per-user
service that runs `wc agent` (for kind=local targets) or
`wc coordinator` (for runtime=daemon coordinators) so it persists
across reboots without sudo or ssh.

Darwin: launchd plist at ~/Library/LaunchAgents/<label>.plist
        loaded with `launchctl bootstrap gui/<uid> <plist>`.
Linux : systemd --user unit at ~/.config/systemd/user/<name>.service
        enabled with `systemctl --user enable --now <name>`.
"""
from __future__ import annotations

import os
import platform
import shutil
import subprocess
from pathlib import Path
from typing import Callable

LABEL_PREFIX = "com.wisent.compute"


def _wc_bin() -> str:
    found = shutil.which("wc")
    if found and found != "/usr/bin/wc":
        return found
    user_bin = Path.home() / "Library" / "Python" / "3.12" / "bin" / "wc"
    if user_bin.is_file():
        return str(user_bin)
    user_bin2 = Path.home() / ".local" / "bin" / "wc"
    if user_bin2.is_file():
        return str(user_bin2)
    return "wc"


def _adc_path() -> str:
    """Return the ADC path the agent / coordinator should export, if any."""
    explicit = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "")
    if explicit and Path(explicit).is_file():
        return explicit
    candidates = list((Path.home() / ".config" / "gcloud" / "legacy_credentials").glob("*/adc.json"))
    return str(candidates[0]) if candidates else ""


def _plist_text(label: str, exec_args: list[str], env: dict[str, str]) -> str:
    args_xml = "".join(f"        <string>{a}</string>\n" for a in exec_args)
    env_xml = "".join(
        f"        <key>{k}</key>\n        <string>{v}</string>\n"
        for k, v in env.items() if v
    )
    log = f"/tmp/{label}.log"
    return f"""<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN"
  "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>{label}</string>
    <key>ProgramArguments</key>
    <array>
{args_xml}    </array>
    <key>RunAtLoad</key>
    <true/>
    <key>KeepAlive</key>
    <true/>
    <key>StandardOutPath</key>
    <string>{log}</string>
    <key>StandardErrorPath</key>
    <string>{log}</string>
    <key>EnvironmentVariables</key>
    <dict>
{env_xml}    </dict>
</dict>
</plist>
"""


def _systemd_user_unit(description: str, exec_args: list[str], env: dict[str, str]) -> str:
    env_lines = "".join(f"Environment={k}={v}\n" for k, v in env.items() if v)
    cmd = " ".join(exec_args)
    return f"""[Unit]
Description={description}
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
{env_lines}ExecStart={cmd}
Restart=on-failure
RestartSec=30

[Install]
WantedBy=default.target
"""


def _exec_args_for(entry, kind: str) -> list[str]:
    bin_path = _wc_bin()
    if kind == "agent":
        return [bin_path, "agent", "--auto"]
    if kind == "coordinator":
        return [bin_path, "coordinator", "--target", entry.name]
    raise ValueError(f"unknown install kind: {kind}")


def _install_darwin(label: str, exec_args: list[str], env: dict[str, str], echo: Callable[[str], None]) -> None:
    plist_dir = Path.home() / "Library" / "LaunchAgents"
    plist_dir.mkdir(parents=True, exist_ok=True)
    plist_path = plist_dir / f"{label}.plist"
    plist_path.write_text(_plist_text(label, exec_args, env))
    echo(f"[plist] wrote {plist_path}")
    uid = os.getuid()
    subprocess.run(["launchctl", "bootout", f"gui/{uid}/{label}"], capture_output=True)
    r = subprocess.run(["launchctl", "bootstrap", f"gui/{uid}", str(plist_path)],
                       capture_output=True, text=True)
    if r.returncode != 0:
        raise RuntimeError(f"launchctl bootstrap failed: {r.stderr or r.stdout}")
    subprocess.run(["launchctl", "kickstart", "-k", f"gui/{uid}/{label}"], capture_output=True)
    echo(f"[ok]   loaded launchd job {label} (logs: /tmp/{label}.log)")


def _install_linux(label: str, exec_args: list[str], env: dict[str, str],
                   description: str, echo: Callable[[str], None]) -> None:
    unit_dir = Path.home() / ".config" / "systemd" / "user"
    unit_dir.mkdir(parents=True, exist_ok=True)
    unit_path = unit_dir / f"{label}.service"
    unit_path.write_text(_systemd_user_unit(description, exec_args, env))
    echo(f"[unit] wrote {unit_path}")
    subprocess.run(["systemctl", "--user", "daemon-reload"], check=False)
    r = subprocess.run(["systemctl", "--user", "enable", "--now", f"{label}.service"],
                       capture_output=True, text=True)
    if r.returncode != 0:
        raise RuntimeError(f"systemctl enable failed: {r.stderr or r.stdout}")
    echo(f"[ok]   enabled systemd --user job {label}")


def install_local(entry, kind: str, dry_run: bool, echo: Callable[[str], None]) -> None:
    """Install agent/coordinator persistently on the current machine."""
    label = f"{LABEL_PREFIX}.{kind}.{entry.name}"
    exec_args = _exec_args_for(entry, kind)
    env = {"PYTHONUNBUFFERED": "1"}
    adc = _adc_path()
    if adc:
        env["GOOGLE_APPLICATION_CREDENTIALS"] = adc

    if dry_run:
        echo(f"[dry-run] {kind}={entry.name} on {platform.system()}")
        echo(f"  exec: {' '.join(exec_args)}")
        echo(f"  env:  {env}")
        return

    if platform.system() == "Darwin":
        _install_darwin(label, exec_args, env, echo)
    elif platform.system() == "Linux":
        _install_linux(label, exec_args, env, f"Wisent Compute {kind} ({entry.name})", echo)
    else:
        raise RuntimeError(f"unsupported OS for local install: {platform.system()}")
