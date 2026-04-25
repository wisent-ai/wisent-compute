"""Capacity broadcast channel between wisent-compute consumers.

Each consumer (cloud function dispatcher, local agent, vast.ai worker, ...)
publishes its current free-slots-by-accelerator-type to GCS on every
poll/tick. Other consumers read all live (non-stale) publications to
decide whether to yield to a peer that has more capacity.

Scheme:
  gs://<bucket>/capacity/<consumer_id>.json
  {
    "consumer_id": "local-rtx-pro-6000-1",
    "kind": "local",                       # or "gcp", "aws", ...
    "free_slots": {"nvidia-tesla-a100": 1, "nvidia-l4": 0},
    "published_at": "2026-04-25T17:42:00.000Z"
  }

A publication older than CAPACITY_STALE_SECONDS is ignored.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone

from .storage import JobStorage

CAPACITY_PREFIX = "capacity/"
CAPACITY_STALE_SECONDS = 180


def publish_capacity(
    store: JobStorage,
    consumer_id: str,
    kind: str,
    free_slots: dict[str, int],
) -> None:
    """Write this consumer's current free-slot snapshot to GCS."""
    payload = {
        "consumer_id": consumer_id,
        "kind": kind,
        "free_slots": free_slots,
        "published_at": datetime.now(timezone.utc).isoformat(),
    }
    store._upload_text(f"{CAPACITY_PREFIX}{consumer_id}.json", json.dumps(payload))


def read_consumer_capacity(store: JobStorage) -> dict[str, dict]:
    """Return {consumer_id: payload} for every live (non-stale) consumer."""
    now = datetime.now(timezone.utc)
    out: dict[str, dict] = {}
    paths = store._list_paths(CAPACITY_PREFIX)
    for path in paths:
        if not path.endswith(".json"):
            continue
        raw = store._download_text(path)
        if not raw:
            continue
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            continue
        published = payload.get("published_at")
        if not published:
            continue
        try:
            ts = datetime.fromisoformat(published.replace("Z", "+00:00"))
        except ValueError:
            continue
        if (now - ts).total_seconds() > CAPACITY_STALE_SECONDS:
            continue
        cid = payload.get("consumer_id")
        if cid:
            out[cid] = payload
    return out


def total_free_by_accel(consumers: dict[str, dict], kinds: tuple[str, ...] | None = None) -> dict[str, int]:
    """Sum free_slots across consumers (optionally filtered by kind)."""
    totals: dict[str, int] = {}
    for payload in consumers.values():
        if kinds and payload.get("kind") not in kinds:
            continue
        for accel, n in (payload.get("free_slots") or {}).items():
            totals[accel] = totals.get(accel, 0) + int(n or 0)
    return totals
