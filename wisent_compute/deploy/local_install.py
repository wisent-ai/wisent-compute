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
import sys
from pathlib import Path
from typing import Callable

LABEL_PREFIX = "com.wisent.compute"


def _wc_bin() -> str:
    invoked = Path(sys.argv[0]).resolve()
    if invoked.name == "wc" and invoked.is_file() and str(invoked) != "/usr/bin/wc":
        return str(invoked)
    sibling = Path(sys.executable).resolve().with_name("wc")
    if sibling.is_file() and str(sibling) != "/usr/bin/wc":
        return str(sibling)
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
    standard = Path.home() / ".config" / "gcloud" / "application_default_credentials.json"
    if standard.is_file():
        return str(standard)
    candidates = list((Path.home() / ".config" / "gcloud" / "legacy_credentials").glob("*/adc.json"))
    return str(candidates[0]) if candidates else ""


def _hf_write_token() -> str:
    """Central write-scoped HF token from gs://<WC_BUCKET>/config/hf_token.
    Baked into the agent/coordinator/failure-fixer unit env so extraction
    jobs upload to wisent-ai/* with write perms instead of inheriting the
    box's read-only ambient token (the observed 403 'write token' failure)."""
    from google.cloud import storage
    from ..config import BUCKET
    project = os.environ.get("GOOGLE_CLOUD_PROJECT") or os.environ.get("GCP_PROJECT")
    client = storage.Client(project=project) if project else storage.Client()
    blob = client.bucket(BUCKET).blob("config/hf_token")
    return blob.download_as_text().strip() if blob.exists() else ""


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


def _wc_fix_bin() -> str:
    """Locate the wc-fix console script. Falls through the same paths
    as _wc_bin() but with the wc-fix label."""
    found = shutil.which("wc-fix")
    if found and found != "/usr/bin/wc-fix":
        return found
    for cand in (
        Path.home() / "Library" / "Python" / "3.12" / "bin" / "wc-fix",
        Path.home() / ".local" / "bin" / "wc-fix",
    ):
        if cand.is_file():
            return str(cand)
    return "wc-fix"


def _wc_watchdog_bin() -> str:
    found = shutil.which("wc-watchdog")
    if found:
        return found
    for cand in (
        Path.home() / "Library" / "Python" / "3.12" / "bin" / "wc-watchdog",
        Path.home() / ".local" / "bin" / "wc-watchdog",
    ):
        if cand.is_file():
            return str(cand)
    return "wc-watchdog"


def _exec_args_for(entry, kind: str) -> list[str]:
    bin_path = _wc_bin()
    if kind == "agent":
        return [bin_path, "agent", "--auto"]
    if kind == "coordinator":
        return [bin_path, "coordinator", "--target", entry.name]
    if kind == "disk-cleanup":
        return [bin_path, "disk-cleanup", "--watch"]
    if kind == "failure-fixer":
        # Run scan_and_dispatch every iteration. Loop in shell so a
        # single failure of scan_and_dispatch (transient GCS hiccup,
        # model-router 5xx) does not require launchd to restart the
        # whole job; the next iteration retries cleanly.
        from ..config import (
            FAILURE_FIXER_COMMAND_PATTERN as _pattern,
            FAILURE_FIXER_TICK_SECONDS as _tick,
        )
        wc_fix = _wc_fix_bin()
        # entry.name is the bootstrap target name. When target is
        # 'failure-fixer' the scope defaults to FAILURE_FIXER_COMMAND_PATTERN;
        # callers can pass a different target name to mean a different
        # workload (handled by the bootstrap dispatcher).
        pat_arg = f"--command-pattern '{_pattern}'" if _pattern else ""
        return [
            "/bin/bash", "-c",
            f"while true; do {wc_fix} scan-dispatch --execute {pat_arg}; sleep {_tick}; done",
        ]
    if kind == "watchdog":
        return [_wc_watchdog_bin()]
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
    """Install agent/coordinator/failure-fixer persistently on the current machine."""
    label = f"{LABEL_PREFIX}.{kind}.{entry.name}"
    exec_args = _exec_args_for(entry, kind)
    env = {"PYTHONUNBUFFERED": "1"}
    adc = _adc_path()
    if adc:
        env["GOOGLE_APPLICATION_CREDENTIALS"] = adc
    if kind == "disk-cleanup":
        from ..config import PROJECT
        env["GOOGLE_CLOUD_PROJECT"] = (
            os.environ.get("GOOGLE_CLOUD_PROJECT")
            or os.environ.get("GCP_PROJECT")
            or PROJECT
        )
    # Every kind needs the central write token: the agent spawns extraction
    # jobs that upload to wisent-ai/* (inherits this env), the coordinator
    # renders it into GCE agent startup, the failure-fixer's verify curl uses
    # it. Sourced from GCS config so it's correct regardless of the box's
    # ambient HF_TOKEN (which was read-only -> 403 on upload).
    if kind != "disk-cleanup":
        hf = _hf_write_token()
        if hf:
            env["HF_TOKEN"] = hf
            env["HUGGING_FACE_HUB_TOKEN"] = hf
    # failure-fixer authenticates via the local `claude` CLI's OAuth
    # session (maintained by wisent-claude-reauth on the mac mini), not
    # via env-var HMAC creds. PATH is forwarded so the LaunchAgent's
    # subshell can find `claude` in /opt/homebrew/bin or wherever the
    # CLI was installed. GCP project env vars are forwarded too,
    # otherwise google-cloud-storage `Client()` raises 'Project was not
    # passed and could not be determined from the environment.' and
    # the JobStorage SDK falls into a silent-skip code path that
    # returns 0 failed/ blobs every tick (confirmed 2026-05-22, 273
    # quota-burn ticks all emitted {"results": [], "count": 0}). HF
    # token is forwarded so the verify_command's curl HEAD works.
    if kind in ("failure-fixer", "watchdog"):
        env["PATH"] = os.environ.get(
            "PATH", "/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin"
        )
        for k in ("GOOGLE_CLOUD_PROJECT", "GCP_PROJECT"):
            v = os.environ.get(k, "")
            if v:
                env[k] = v
        env["WC_BUCKET"] = os.environ.get("WC_BUCKET", "wisent-compute")

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
