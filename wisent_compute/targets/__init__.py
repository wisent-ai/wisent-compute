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


def load_targets(path: Path | None = None) -> list[ComputeTarget]:
    """Load every target from the registry JSON. Empty list if file missing."""
    p = path or REGISTRY_PATH
    if not p.is_file():
        return []
    with p.open() as f:
        data = json.load(f)
    raw = data.get("targets") if isinstance(data, dict) else data
    if not isinstance(raw, list):
        return []
    return [_from_dict(d) for d in raw if isinstance(d, dict) and d.get("name")]


def lookup(name: str, path: Path | None = None) -> Optional[ComputeTarget]:
    """Return the named target, or None if not in the registry."""
    for t in load_targets(path):
        if t.name == name:
            return t
    return None


def local_targets(path: Path | None = None) -> list[ComputeTarget]:
    """Subset of targets with kind='local'. Used by wc bootstrap."""
    return [t for t in load_targets(path) if t.kind == "local"]
