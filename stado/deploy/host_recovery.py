"""Registry-authorized recovery for a managed macOS host.

The remote program is fixed and deliberately narrow: run the canonical disk
cleanup, disable the obsolete local coordinator, and reload known LaunchAgents.
Registry data selects only the host; it cannot supply shell fragments.
"""
from __future__ import annotations

import json
import shlex
import subprocess
from typing import Any

from ..targets import ComputeTarget, lookup
from ..targets.validation import normalize_hostname, ssh_hostname

_TIMEOUT_SECONDS = 120
_WC_CANDIDATES = (
    "$HOME/.venvs/wisent-compute/bin/wc",
    "$HOME/.local/bin/wc",
    "/opt/homebrew/bin/wc",
)
_MANAGED_AGENTS = (
    ("com.wisent.compute.auto-deployer", "$HOME/Library/LaunchAgents/com.wisent.compute.auto-deployer.plist"),
    ("com.wisent.weles-auto-deploy", "$HOME/Library/LaunchAgents/com.wisent.weles-auto-deploy.plist"),
    ("com.wisent.weles-worker", "$HOME/Library/LaunchAgents/com.wisent.weles-worker.plist"),
    ("com.wisent.weles-keyword-planner-api", "$HOME/Library/LaunchAgents/com.wisent.weles-keyword-planner-api.plist"),
    ("com.wisent.host-health-beacon", "$HOME/Library/LaunchAgents/com.wisent.host-health-beacon.plist"),
)


def _target(target_name: str) -> ComputeTarget:
    target = lookup(target_name, source="gcs")
    if target is None:
        raise LookupError(f"target {target_name!r} is not in the canonical registry")
    if target.kind != "local":
        raise ValueError(f"target {target_name!r} is not a local host")
    if not target.ssh:
        raise ValueError(f"target {target_name!r} has no registry-managed ssh destination")
    return target


def _identity_values(target: ComputeTarget) -> list[str]:
    values = {normalize_hostname(target.name)}
    values.update(normalize_hostname(value) for value in target.hostnames)
    if target.ssh:
        values.add(ssh_hostname(target.ssh))
    values.discard("")
    return sorted(values)


def _remote_script(target: ComputeTarget) -> str:
    identity_words = " ".join(shlex.quote(value) for value in _identity_values(target))
    wc_words = " ".join(f'"{value}"' for value in _WC_CANDIDATES)
    agent_rows = "\n".join(
        f"recover_agent {shlex.quote(label)} \"{plist}\""
        for label, plist in _MANAGED_AGENTS
    )
    return f"""set -u
host=$(/bin/hostname -s 2>/dev/null | /usr/bin/tr '[:upper:]' '[:lower:]')
identity_ok=0
for expected in {identity_words}; do
  short="${{expected%.local}}"
  if [ "$host" = "$expected" ] || [ "$host" = "$short" ]; then identity_ok=1; fi
done
if [ "$identity_ok" -ne 1 ]; then
  printf 'STADO_RECOVER\\tidentity_mismatch\\t%s\\n' "$host"
  exit 64
fi
if [ "$(/usr/bin/uname -s)" != "Darwin" ]; then
  printf 'STADO_RECOVER\\tunsupported_os\\t%s\\n' "$(/usr/bin/uname -s)"
  exit 65
fi

disk_before=$(/bin/df -k / 2>/dev/null | /usr/bin/awk 'NR==2 {{print $4}}')
wc_bin=""
for candidate in {wc_words}; do
  if [ -x "$candidate" ]; then wc_bin="$candidate"; break; fi
done
cleanup_status="unavailable"
cleanup_json=""
if [ -n "$wc_bin" ]; then
  cleanup_json=$(GOOGLE_APPLICATION_CREDENTIALS="${{GOOGLE_APPLICATION_CREDENTIALS:-$HOME/.config/gcloud/application_default_credentials.json}}" "$wc_bin" disk-cleanup --once 2>/dev/null)
  cleanup_rc=$?
  if [ "$cleanup_rc" -eq 0 ]; then cleanup_status="ok"; else cleanup_status="failed:$cleanup_rc"; fi
fi

uid=$(/usr/bin/id -u)
gui="gui/$uid"
user_domain="user/$uid"
if /bin/launchctl print "$gui" >/dev/null 2>&1; then
  agent_domain="$gui"
  printf 'STADO_DOMAIN\t%s\tavailable\n' "$agent_domain"
elif /bin/launchctl print "$user_domain" >/dev/null 2>&1; then
  agent_domain="$user_domain"
  printf 'STADO_DOMAIN\t%s\tfallback\n' "$agent_domain"
else
  printf 'STADO_DOMAIN\t%s\tunavailable\n' "$gui"
  exit 66
fi
/bin/launchctl bootout "$gui/com.wisent.compute.coordinator" >/dev/null 2>&1 || true
/bin/launchctl bootout "$user_domain/com.wisent.compute.coordinator" >/dev/null 2>&1 || true
/bin/launchctl disable "$gui/com.wisent.compute.coordinator" >/dev/null 2>&1 || true
/bin/launchctl disable "$user_domain/com.wisent.compute.coordinator" >/dev/null 2>&1 || true

recover_agent() {{
  label="$1"
  plist="$2"
  if [ ! -f "$plist" ]; then
    printf 'STADO_AGENT\\t%s\\tmissing_plist\\n' "$label"
    return
  fi
  /bin/launchctl bootout "$gui/$label" >/dev/null 2>&1 || true
  /bin/launchctl bootout "$user_domain/$label" >/dev/null 2>&1 || true
  bootstrap_detail=$(/bin/launchctl bootstrap "$agent_domain" "$plist" 2>&1)
  bootstrap_rc=$?
  if [ "$bootstrap_rc" -eq 0 ]; then
    /bin/launchctl enable "$agent_domain/$label" >/dev/null 2>&1 || true
    /bin/launchctl kickstart -k "$agent_domain/$label" >/dev/null 2>&1 || true
    printf 'STADO_AGENT\t%s\trestarted\n' "$label"
  else
    bootstrap_detail=$(printf '%s' "$bootstrap_detail" | /usr/bin/tr '\t\r\n' ' ' | /usr/bin/cut -c1-160)
    printf 'STADO_AGENT\t%s\tbootstrap_failed:%s:%s\n' "$label" "$bootstrap_rc" "$bootstrap_detail"
  fi
}}

{agent_rows}
/bin/sleep 5
disk_after=$(/bin/df -k / 2>/dev/null | /usr/bin/awk 'NR==2 {{print $4}}')
printf 'STADO_RECOVER\\tok\\t%s\\t%s\\t%s\\t%s\\n' "$host" "${{disk_before:-0}}" "${{disk_after:-0}}" "$cleanup_status"
if [ -n "$cleanup_json" ]; then printf 'STADO_CLEANUP\\t%s\\n' "$cleanup_json"; fi
"""


def _parse_output(stdout: str, target: ComputeTarget) -> dict[str, Any]:
    report: dict[str, Any] = {
        "target": target.name,
        "ssh": target.ssh,
        "status": "failed",
        "agents": {},
    }
    for line in stdout.splitlines():
        fields = line.split("\t")
        if fields[:1] == ["STADO_AGENT"] and len(fields) == 3:
            report["agents"][fields[1]] = fields[2]
        elif fields[:1] == ["STADO_DOMAIN"] and len(fields) == 3:
            report["launchd_domain"] = {"name": fields[1], "status": fields[2]}
        elif fields[:2] == ["STADO_RECOVER", "ok"] and len(fields) == 6:
            report.update({
                "status": "ok",
                "host": fields[2],
                "disk_free_kb_before": int(fields[3]),
                "disk_free_kb_after": int(fields[4]),
                "cleanup_status": fields[5],
            })
        elif fields[:1] == ["STADO_CLEANUP"] and len(fields) == 2:
            try:
                report["cleanup"] = json.loads(fields[1])
            except json.JSONDecodeError:
                report["cleanup"] = {"outcome": "invalid_output"}
        elif fields[:1] == ["STADO_RECOVER"]:
            report["remote_error"] = fields[1:]
    return report


def recover_host(target_name: str) -> dict[str, Any]:
    """Run the fixed recovery procedure on one canonical registry host."""
    target = _target(target_name)
    process = subprocess.run(
        [
            "ssh",
            "-o", "BatchMode=yes",
            "-o", "ConnectTimeout=15",
            "-o", "StrictHostKeyChecking=accept-new",
            target.ssh,
            "/bin/bash", "-s",
        ],
        input=_remote_script(target),
        capture_output=True,
        text=True,
        check=False,
        timeout=_TIMEOUT_SECONDS,
    )
    report = _parse_output(process.stdout, target)
    report["exit_code"] = process.returncode
    if process.returncode != 0:
        detail = (process.stderr or process.stdout).strip().splitlines()
        report["error"] = detail[-1][:300] if detail else "remote recovery failed"
    return report
