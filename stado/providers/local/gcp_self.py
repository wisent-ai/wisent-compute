"""GCE self-awareness helpers for the agent.

Lets the agent detect that it is running inside a GCE VM and self-terminate
the VM via gcloud when it has no work left. Only used by the agent's
idle-shutdown branch — the workstation/Vast.ai mode never calls these.
"""
from __future__ import annotations

import subprocess
import urllib.request


_METADATA_BASE = "http://metadata.google.internal/computeMetadata/v1"


def _fetch_metadata(path: str, timeout: float = 2.0) -> str:
    req = urllib.request.Request(
        f"{_METADATA_BASE}/{path}",
        headers={"Metadata-Flavor": "Google"},
    )
    return urllib.request.urlopen(req, timeout=timeout).read().decode().strip()


def on_gcp() -> bool:
    """True iff this process is running on a GCE VM (metadata service responds)."""
    try:
        _fetch_metadata("instance/id")
        return True
    except Exception:
        return False


def self_metadata() -> tuple[str, str]:
    """Return (instance_name, zone) from the GCE metadata service."""
    name = _fetch_metadata("instance/name")
    zone = _fetch_metadata("instance/zone").rsplit("/", 1)[-1]
    return name, zone


def self_terminate(log_fn) -> None:
    """If on GCE, delete this VM via gcloud. Best-effort; failure is non-fatal.

    No-op outside GCE so a misconfigured idle-shutdown on the workstation can't
    accidentally power off the box.
    """
    if not on_gcp():
        return
    try:
        name, zone = self_metadata()
        log_fn(f"GCE self-terminate: instances delete {name} in {zone}")
        subprocess.Popen(
            ["gcloud", "compute", "instances", "delete", name,
             f"--zone={zone}", "--quiet"],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
    except Exception as exc:
        log_fn(f"GCE self-terminate failed: {exc}")
