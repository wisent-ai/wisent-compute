"""Read registry-managed host health beacons through Stado."""
from __future__ import annotations

import json
from typing import Any

from ..targets import GCS_REGISTRY_URI, ComputeTarget, lookup, lookup_self
from ..targets.validation import normalize_hostname, ssh_hostname


HEALTH_PREFIX = "host_health"


def _registry_bucket() -> str:
    _, remainder = GCS_REGISTRY_URI.split("//", 1)
    return remainder.split("/", 1)[0]


def _resolve_target(identity: str) -> ComputeTarget:
    target = lookup(identity, source="gcs") or lookup_self(identity, source="gcs")
    if target is None:
        raise ValueError(f"target {identity!r} is not present in the GCS registry")
    if target.kind != "local":
        raise ValueError(f"target {target.name!r} is not a local registry host")
    return target


def _beacon_slugs(target: ComputeTarget, requested_identity: str) -> list[str]:
    identities = [*target.hostnames, target.name, requested_identity]
    if target.ssh:
        identities.append(ssh_hostname(target.ssh))

    slugs: list[str] = []
    for value in identities:
        normalized = normalize_hostname(value)
        for candidate in (normalized.split(".", 1)[0], normalized):
            if candidate and "/" not in candidate and candidate not in slugs:
                slugs.append(candidate)
    return slugs


def load_host_health(identity: str) -> dict[str, Any]:
    """Return one local target's beacon plus immutable object metadata."""
    from google.api_core.exceptions import NotFound
    from google.cloud import storage

    target = _resolve_target(identity)
    bucket_name = _registry_bucket()
    bucket = storage.Client().bucket(bucket_name)
    candidates = _beacon_slugs(target, identity)

    for slug in candidates:
        object_name = f"{HEALTH_PREFIX}/{slug}.json"
        blob = bucket.blob(object_name)
        try:
            blob.reload()
        except NotFound:
            continue

        generation = blob.generation
        if generation is None:
            raise OSError(f"host health generation is unavailable for gs://{bucket_name}/{object_name}")
        try:
            beacon = json.loads(blob.download_as_text(if_generation_match=int(generation)))
        except json.JSONDecodeError as exc:
            raise ValueError(f"host health beacon is not valid JSON: gs://{bucket_name}/{object_name}") from exc
        if not isinstance(beacon, dict):
            raise ValueError(f"host health beacon is not an object: gs://{bucket_name}/{object_name}")

        return {
            "target": {
                "name": target.name,
                "kind": target.kind,
                "hostnames": target.hostnames,
            },
            "object": {
                "uri": f"gs://{bucket_name}/{object_name}",
                "generation": str(generation),
                "updated_at": blob.updated.isoformat() if blob.updated else None,
                "created_at": blob.time_created.isoformat() if blob.time_created else None,
                "size_bytes": blob.size,
                "etag": blob.etag,
            },
            "beacon": beacon,
        }

    attempted = ", ".join(f"{HEALTH_PREFIX}/{slug}.json" for slug in candidates)
    raise FileNotFoundError(f"no host health beacon for {target.name!r}; checked {attempted}")


def format_host_health(report: dict[str, Any]) -> str:
    """Render the health report for an operator without discarding raw logs."""
    target = report["target"]
    metadata = report["object"]
    beacon = report["beacon"]
    lines = [
        f"target: {target['name']}",
        f"host: {beacon.get('host') or '-'}",
        f"reported_at: {beacon.get('reported_at') or '-'}",
        f"object_updated_at: {metadata.get('updated_at') or '-'}",
        f"object: {metadata['uri']}#{metadata['generation']}",
        f"disk: {beacon.get('disk_pct', '-')}% used; {beacon.get('disk_avail_gb', '-')} GiB available",
        "units:",
    ]
    units = beacon.get("units")
    if isinstance(units, dict) and units:
        for name, state in units.items():
            if isinstance(state, dict):
                lines.append(f"  {name}: {state.get('state') or 'unknown'}")
            else:
                lines.append(f"  {name}: {state}")
    else:
        lines.append("  -")
    lines.extend(("last_log:", str(beacon.get("last_log") or "-")))
    return "\n".join(lines)
