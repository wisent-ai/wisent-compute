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

REGISTRY_PATH = Path(__file__).parent / "registry.json"
GCS_REGISTRY_URI = "gs://wisent-compute/registry.json"
GCS_REGISTRY_SEPARATOR = "//"
REGISTRY_PATH_SEPARATOR = "/"
SSH_HOST_SEPARATOR = "@"
SOURCE_GCS = "gcs"
SOURCE_LOCAL = "local"
SOURCE_AUTO = "auto"
TARGET_KIND_LOCAL = "local"
TARGETS_KEY = "targets"
COORDINATORS_KEY = "coordinators"
NAME_KEY = "name"
KIND_KEY = "kind"
GPU_TYPE_KEY = "gpu_type"
SLOTS_KEY = "slots"
SSH_KEY = "ssh"
REGION_KEY = "region"
SPOT_KEY = "spot"
MAX_CONCURRENT_KEY = "max_concurrent"
TEAM_ID_KEY = "team_id"
NOTES_KEY = "notes"
ENV_OVERRIDES_KEY = "env_overrides"
AGENT_ARGS_KEY = "agent_args"
VRAM_GB_KEY = "vram_gb"
RUNTIME_KEY = "runtime"
HOST_KEY = "host"
INTERVAL_SECONDS_KEY = "interval_seconds"
STATE_URI_KEY = "state_uri"
ACTIVE_KEY = "active"
DEFAULT_TARGET_SLOTS = 1
DEFAULT_TARGET_SPOT = False
DEFAULT_TARGET_NOTES = ""
DEFAULT_COORDINATOR_RUNTIME = "daemon"
DEFAULT_COORDINATOR_INTERVAL_SECONDS = 180
DEFAULT_COORDINATOR_STATE_URI = "gs://wisent-compute"
DEFAULT_COORDINATOR_ACTIVE = False
DEFAULT_COORDINATOR_NOTES = ""
GCS_CACHE_TTL_SECONDS = 30
_GCS_CACHE: dict[str, object] = {"ts": 0.0, "data": None}


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
    # env_overrides and agent_args propagate via the GCS registry to running
    # agents — the agent compares them every poll and exits-for-restart when
    # they change, so systemd brings it back up with the new env / CLI flags.
    env_overrides: dict = field(default_factory=dict)
    agent_args: list = field(default_factory=list)
    # vram_gb is used by the agent to expand its capacity broadcast to every
    # GCP gpu_type whose required VRAM ≤ this value (compatibility-list
    # broadcast). Without it, the agent only advertises gpu_type as-is.
    vram_gb: Optional[int] = None
    extra: dict = field(default_factory=dict)


def _from_dict(d: dict) -> ComputeTarget:
    known = {
        NAME_KEY, KIND_KEY, GPU_TYPE_KEY, SLOTS_KEY, SSH_KEY, REGION_KEY,
        SPOT_KEY, MAX_CONCURRENT_KEY, TEAM_ID_KEY, NOTES_KEY,
        ENV_OVERRIDES_KEY, AGENT_ARGS_KEY, VRAM_GB_KEY,
    }
    extra = {k: v for k, v in d.items() if k not in known}
    return ComputeTarget(
        name=d[NAME_KEY],
        kind=d[KIND_KEY],
        gpu_type=_schema_value(d, GPU_TYPE_KEY, None),
        slots=int(_schema_value(d, SLOTS_KEY, DEFAULT_TARGET_SLOTS)),
        ssh=_schema_value(d, SSH_KEY, None),
        region=_schema_value(d, REGION_KEY, None),
        spot=bool(_schema_value(d, SPOT_KEY, DEFAULT_TARGET_SPOT)),
        max_concurrent=_schema_value(d, MAX_CONCURRENT_KEY, None),
        team_id=_schema_value(d, TEAM_ID_KEY, None),
        notes=_schema_value(d, NOTES_KEY, DEFAULT_TARGET_NOTES),
        env_overrides=dict(_schema_value(d, ENV_OVERRIDES_KEY, {}) or {}),
        agent_args=list(_schema_value(d, AGENT_ARGS_KEY, []) or []),
        vram_gb=_schema_value(d, VRAM_GB_KEY, None),
        extra=extra,
    )


def _schema_value(d: dict, key: str, default):
    return d[key] if key in d else default


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
    if _GCS_CACHE["data"] is not None and now - float(_GCS_CACHE["ts"]) < GCS_CACHE_TTL_SECONDS:
        return _GCS_CACHE["data"]  # type: ignore[return-value]
    from google.cloud import storage as _gcs
    _, rest = GCS_REGISTRY_URI.split(GCS_REGISTRY_SEPARATOR, 1)
    bucket_name, blob_name = rest.split(REGISTRY_PATH_SEPARATOR, 1)
    client = _gcs.Client()
    blob = client.bucket(bucket_name).blob(blob_name)
    if not blob.exists():
        return None
    data = json.loads(blob.download_as_text())
    _GCS_CACHE["ts"] = now
    _GCS_CACHE["data"] = data
    return data


def load_targets(path: Path | None = None, source: str = SOURCE_AUTO) -> list[ComputeTarget]:
    """Load every target from the registry JSON. Empty list if missing.

    source: 'gcs' = fetch from GCS only (errors -> empty list)
            'local' = read the file shipped with the package
            'auto' (default) = try GCS first, fall back to local
    """
    data: dict | None = None
    if source in (SOURCE_GCS, SOURCE_AUTO):
        data = _load_from_gcs()
    if data is None and source in (SOURCE_LOCAL, SOURCE_AUTO):
        p = path or REGISTRY_PATH
        if p.is_file():
            with p.open() as f:
                data = json.load(f)
    if data is None:
        return []
    raw = data[TARGETS_KEY] if isinstance(data, dict) and TARGETS_KEY in data else data
    if not isinstance(raw, list):
        return []
    return [_from_dict(d) for d in raw if isinstance(d, dict) and NAME_KEY in d]


def lookup(name: str, path: Path | None = None,
           source: str = SOURCE_AUTO) -> Optional[ComputeTarget]:
    """Return the named target, or None if not in the registry."""
    for t in load_targets(path, source=source):
        if t.name == name:
            return t
    return None


def local_targets(path: Path | None = None, source: str = SOURCE_AUTO) -> list[ComputeTarget]:
    """Subset of targets with kind='local'. Used by wc bootstrap."""
    return [t for t in load_targets(path, source=source) if t.kind == TARGET_KIND_LOCAL]


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
    interval_seconds: int = DEFAULT_COORDINATOR_INTERVAL_SECONDS
    state_uri: str = DEFAULT_COORDINATOR_STATE_URI
    active: bool = DEFAULT_COORDINATOR_ACTIVE
    notes: str = DEFAULT_COORDINATOR_NOTES
    extra: dict = field(default_factory=dict)


def _coord_from_dict(d: dict) -> Coordinator:
    known = {NAME_KEY, RUNTIME_KEY, HOST_KEY, INTERVAL_SECONDS_KEY, STATE_URI_KEY, ACTIVE_KEY, NOTES_KEY}
    extra = {k: v for k, v in d.items() if k not in known}
    return Coordinator(
        name=d[NAME_KEY],
        runtime=_schema_value(d, RUNTIME_KEY, DEFAULT_COORDINATOR_RUNTIME),
        host=_schema_value(d, HOST_KEY, None),
        interval_seconds=int(_schema_value(
            d, INTERVAL_SECONDS_KEY, DEFAULT_COORDINATOR_INTERVAL_SECONDS,
        )),
        state_uri=_schema_value(d, STATE_URI_KEY, DEFAULT_COORDINATOR_STATE_URI),
        active=bool(_schema_value(d, ACTIVE_KEY, DEFAULT_COORDINATOR_ACTIVE)),
        notes=_schema_value(d, NOTES_KEY, DEFAULT_COORDINATOR_NOTES),
        extra=extra,
    )


def load_coordinators(path: Path | None = None, source: str = SOURCE_AUTO) -> list[Coordinator]:
    """Load every coordinator entry from the registry. Empty list if missing."""
    data: dict | None = None
    if source in (SOURCE_GCS, SOURCE_AUTO):
        data = _load_from_gcs()
    if data is None and source in (SOURCE_LOCAL, SOURCE_AUTO):
        p = path or REGISTRY_PATH
        if p.is_file():
            with p.open() as f:
                data = json.load(f)
    if data is None:
        return []
    raw = data[COORDINATORS_KEY] if isinstance(data, dict) and COORDINATORS_KEY in data else None
    if not isinstance(raw, list):
        return []
    return [_coord_from_dict(d) for d in raw if isinstance(d, dict) and NAME_KEY in d]


def lookup_coordinator(name: str, source: str = SOURCE_AUTO) -> Optional[Coordinator]:
    for c in load_coordinators(source=source):
        if c.name == name:
            return c
    return None


def lookup_self(hostname: str, source: str = SOURCE_GCS) -> Optional[ComputeTarget]:
    """Find the registry entry whose ssh ends in @<hostname> or whose name == hostname.

    Used by `wc agent --auto`: the box knows its own hostname; the registry
    is the source of truth for slots/gpu_type/kind. GCS-first by default so
    a registry edit takes effect without re-installing the package on the box.
    """
    for t in load_targets(source=source):
        if t.name == hostname:
            return t
        if t.ssh and SSH_HOST_SEPARATOR in t.ssh and t.ssh.split(SSH_HOST_SEPARATOR, 1)[1] == hostname:
            return t
    return None
