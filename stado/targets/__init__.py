"""Compute-target registry loader.

Reads registry.json (sibling file) into a list of ComputeTarget objects.
The registry is the single source of truth for every box the queue can
route to: workstations, GCP zonal dispatchers, vast.ai pools.

Used by:
  wc agent --target NAME    -> reads its slot count + gpu_type from the
                               named entry instead of env vars.
  wc bootstrap              -> iterates entries with kind=local and
                               provisions an agent on each.
  cost estimator            -> sums per-target capacity to forecast the
                               local-vs-cloud split for a queued batch.
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional
from .validation import normalize_hostname, ssh_hostname


@dataclass(frozen=True)
class WelesPolicy:
    enabled: bool
    actions: list[str]


@dataclass(frozen=True)
class DiskCleanerPolicy:
    min_age_seconds: int


@dataclass(frozen=True)
class DiskCleanupPolicy:
    mode: str
    check_interval_seconds: int
    low_free_gb: int
    target_free_gb: int
    max_bytes_per_pass: int
    max_items_per_pass: int
    max_scan_items: int
    cleaners: dict[str, DiskCleanerPolicy]

REGISTRY_PATH = Path(__file__).parent / "registry.json"
GCS_REGISTRY_URI = "gs://wisent-compute/registry.json"
_GCS_CACHE: dict[str, object] = {"ts": 0.0, "data": None}
_GCS_TTL_SEC = 30  # re-fetch from GCS at most this often


@dataclass
class ComputeTarget:
    name: str
    kind: str  # "local" | "gcp" | "vast"
    gpu_type: Optional[str] = None
    slots: int = 1
    ssh: Optional[str] = None
    region: Optional[str] = None
    spot: bool = False
    max_concurrent: Optional[int] = None
    team_id: Optional[int] = None
    notes: str = ""
    hostnames: list[str] = field(default_factory=list)
    weles: Optional[WelesPolicy] = None
    disk_cleanup: Optional[DiskCleanupPolicy] = None
    # env_overrides and agent_args propagate via the GCS registry to running
    # agents — the agent compares them every poll and exits-for-restart when
    # they change, so systemd brings it back up with the new env / CLI flags.
    env_overrides: dict = field(default_factory=dict)
    agent_args: list = field(default_factory=list)
    # vram_gb is used by the agent to expand its capacity broadcast to every
    # GCP gpu_type whose required VRAM ≤ this value (compatibility-list
    # broadcast). Without it, the agent only advertises gpu_type as-is.
    vram_gb: Optional[int] = None
    # pinned_only=True: this host's agent claims ONLY jobs explicitly routed
    # to it (Job.pinned_host or coordinator assigned_to). Keeps shared
    # workstations from picking up stray queue backlog.
    pinned_only: bool = False
    extra: dict = field(default_factory=dict)


def _from_dict(d: dict) -> ComputeTarget:
    known = {
        "name", "kind", "gpu_type", "slots", "ssh", "region",
        "spot", "max_concurrent", "team_id", "notes", "hostnames", "weles",
        "disk_cleanup", "env_overrides", "agent_args", "vram_gb",
        "pinned_only",
    }
    extra = {k: v for k, v in d.items() if k not in known}
    weles_data = d.get("weles")
    weles = (
        WelesPolicy(
            enabled=weles_data["enabled"],
            actions=list(weles_data["actions"]),
        )
        if isinstance(weles_data, dict)
        else None
    )
    cleanup_data = d.get("disk_cleanup")
    disk_cleanup = (
        DiskCleanupPolicy(
            mode=cleanup_data["mode"],
            check_interval_seconds=cleanup_data["check_interval_seconds"],
            low_free_gb=cleanup_data["low_free_gb"],
            target_free_gb=cleanup_data["target_free_gb"],
            max_bytes_per_pass=cleanup_data["max_bytes_per_pass"],
            max_items_per_pass=cleanup_data["max_items_per_pass"],
            max_scan_items=cleanup_data["max_scan_items"],
            cleaners={
                name: DiskCleanerPolicy(min_age_seconds=value["min_age_seconds"])
                for name, value in cleanup_data["cleaners"].items()
            },
        )
        if isinstance(cleanup_data, dict)
        else None
    )
    return ComputeTarget(
        name=d["name"],
        kind=d["kind"],
        gpu_type=d.get("gpu_type"),
        slots=int(d.get("slots", 1)),
        ssh=d.get("ssh"),
        region=d.get("region"),
        spot=bool(d.get("spot", False)),
        max_concurrent=d.get("max_concurrent"),
        team_id=d.get("team_id"),
        notes=d.get("notes", ""),
        hostnames=list(d.get("hostnames") or []),
        weles=weles,
        disk_cleanup=disk_cleanup,
        env_overrides=dict(d.get("env_overrides") or {}),
        agent_args=list(d.get("agent_args") or []),
        vram_gb=d.get("vram_gb"),
        pinned_only=bool(d.get("pinned_only", False)),
        extra=extra,
    )


def _load_from_gcs() -> dict | None:
    """Best-effort GCS fetch of the canonical registry via the GCS Python SDK.

    Earlier this shelled out to `gsutil cat`. On systems with a broken
    gsutil install (e.g. cryptography/pyOpenSSL version mismatch breaking
    `module 'OpenSSL.crypto' has no attribute 'sign'`), gsutil exits
    non-zero with a stderr message and the agent crashes with
    'hostname X not in registry' even though the registry IS in GCS.
    Confirmed live on 2026-05-08: the workstation's gsutil was broken
    after a pip upgrade, knocking the agent offline. The GCS SDK is
    already a hard dependency (google-cloud-storage>=2.18.0); using it
    directly removes the gsutil binary as a single point of failure.
    """
    import time
    now = time.time()
    if _GCS_CACHE["data"] is not None and now - float(_GCS_CACHE["ts"]) < _GCS_TTL_SEC:
        return _GCS_CACHE["data"]  # type: ignore[return-value]
    try:
        from google.cloud import storage as _gcs
        _, rest = GCS_REGISTRY_URI.split("//", 1)
        bucket_name, blob_name = rest.split("/", 1)
        client = _gcs.Client()
        blob = client.bucket(bucket_name).blob(blob_name)
        blob.reload()
        if blob.generation is None:
            return None
        data = json.loads(
            blob.download_as_text(if_generation_match=int(blob.generation))
        )
        _GCS_CACHE["ts"] = now
        _GCS_CACHE["data"] = data
        return data
    except Exception as _gcs_exc:  # noqa: BLE001
        import sys as _sys
        _sys.stderr.write(f"[_load_from_gcs] failed: {_gcs_exc!r}\n")
        _sys.stderr.flush()
        return None


def load_targets(path: Path | None = None, source: str = "auto") -> list[ComputeTarget]:
    """Load every target from the registry JSON. Empty list if missing.

    source: 'gcs' = fetch from GCS only (errors -> empty list)
            'local' = read the file shipped with the package
            'auto' (default) = try GCS first, fall back to local
    """
    data: dict | None = None
    if source in ("gcs", "auto"):
        data = _load_from_gcs()
    if data is None and source in ("local", "auto"):
        p = path or REGISTRY_PATH
        if p.is_file():
            with p.open() as f:
                data = json.load(f)
    if data is None:
        return []
    raw = data.get("targets") if isinstance(data, dict) else data
    if not isinstance(raw, list):
        return []
    return [_from_dict(d) for d in raw if isinstance(d, dict) and d.get("name")]


def lookup(name: str, path: Path | None = None,
           source: str = "auto") -> Optional[ComputeTarget]:
    """Return the named target, or None if not in the registry."""
    for t in load_targets(path, source=source):
        if t.name == name:
            return t
    return None


def local_targets(path: Path | None = None, source: str = "auto") -> list[ComputeTarget]:
    """Subset of targets with kind='local'. Used by wc bootstrap."""
    return [t for t in load_targets(path, source=source) if t.kind == "local"]


@dataclass
class Coordinator:
    """Where the scheduling tick runs.

    runtime values:
      gcp_cloud_function   wisent-compute-tick CF + Cloud Scheduler (default).
      daemon               long-running `wc coordinator` process (any box).
      cron                 crontab entry that calls `wc coordinator --once`.
      aws_lambda           reserved.
    """
    name: str
    runtime: str
    host: Optional[str] = None  # ssh user@host for daemon/cron, None = local
    interval_seconds: int = 180
    state_uri: str = "gs://wisent-compute"
    active: bool = False
    notes: str = ""
    extra: dict = field(default_factory=dict)


def _coord_from_dict(d: dict) -> Coordinator:
    known = {"name", "runtime", "host", "interval_seconds", "state_uri", "active", "notes"}
    extra = {k: v for k, v in d.items() if k not in known}
    return Coordinator(
        name=d["name"],
        runtime=d.get("runtime", "daemon"),
        host=d.get("host"),
        interval_seconds=int(d.get("interval_seconds", 180)),
        state_uri=d.get("state_uri", "gs://wisent-compute"),
        active=bool(d.get("active", False)),
        notes=d.get("notes", ""),
        extra=extra,
    )


def load_coordinators(path: Path | None = None, source: str = "auto") -> list[Coordinator]:
    """Load every coordinator entry from the registry. Empty list if missing."""
    data: dict | None = None
    if source in ("gcs", "auto"):
        data = _load_from_gcs()
    if data is None and source in ("local", "auto"):
        p = path or REGISTRY_PATH
        if p.is_file():
            with p.open() as f:
                data = json.load(f)
    if data is None:
        return []
    raw = data.get("coordinators") if isinstance(data, dict) else None
    if not isinstance(raw, list):
        return []
    return [_coord_from_dict(d) for d in raw if isinstance(d, dict) and d.get("name")]


def lookup_coordinator(name: str, source: str = "auto") -> Optional[Coordinator]:
    for c in load_coordinators(source=source):
        if c.name == name:
            return c
    return None


def lookup_self(hostname: str, source: str = "gcs") -> Optional[ComputeTarget]:
    """Find the unique target declaring the normalized host identity.

    Names, explicit hostname aliases, and the host part of legacy SSH
    destinations are identities. Ambiguous registry data is rejected rather
    than allowing target order to decide which configuration a host receives.
    """
    identity = normalize_hostname(hostname)
    if not identity:
        return None

    matches: list[ComputeTarget] = []
    for target in load_targets(source=source):
        target_identities = {normalize_hostname(target.name)}
        target_identities.update(normalize_hostname(alias) for alias in target.hostnames)
        if target.ssh:
            target_identities.add(ssh_hostname(target.ssh))
        if identity in target_identities:
            matches.append(target)

    if len(matches) > 1:
        names = ", ".join(sorted(target.name for target in matches))
        raise ValueError(f"hostname {identity!r} matches multiple registry targets: {names}")
    return matches[0] if matches else None
