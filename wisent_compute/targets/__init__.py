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
    extra: dict = field(default_factory=dict)


def _from_dict(d: dict) -> ComputeTarget:
    known = {
        "name", "kind", "gpu_type", "slots", "ssh", "region",
        "spot", "max_concurrent", "team_id", "notes",
    }
    extra = {k: v for k, v in d.items() if k not in known}
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
        extra=extra,
    )


def _load_from_gcs() -> dict | None:
    """Best-effort GCS fetch of the canonical registry. Returns parsed JSON or None."""
    import time
    now = time.time()
    if _GCS_CACHE["data"] is not None and now - float(_GCS_CACHE["ts"]) < _GCS_TTL_SEC:
        return _GCS_CACHE["data"]  # type: ignore[return-value]
    try:
        import shutil, subprocess
        gsutil = shutil.which("gsutil") or "gsutil"
        r = subprocess.run([gsutil, "cat", GCS_REGISTRY_URI],
                           capture_output=True, text=True)
        if r.returncode != 0:
            return None
        data = json.loads(r.stdout)
    except Exception:
        return None
    _GCS_CACHE["ts"] = now
    _GCS_CACHE["data"] = data
    return data


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


def lookup_self(hostname: str, source: str = "gcs") -> Optional[ComputeTarget]:
    """Find the registry entry whose ssh ends in @<hostname> or whose name == hostname.

    Used by `wc agent --auto`: the box knows its own hostname; the registry
    is the source of truth for slots/gpu_type/kind. GCS-first by default so
    a registry edit takes effect without re-installing the package on the box.
    """
    for t in load_targets(source=source):
        if t.name == hostname:
            return t
        if t.ssh and "@" in t.ssh and t.ssh.split("@", 1)[1] == hostname:
            return t
    return None
