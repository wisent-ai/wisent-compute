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
    free_vram_gb: int | None = None,
    total_vram_gb: int | None = None,
) -> None:
    """Write this consumer's current capacity snapshot to GCS.

    free_vram_gb is the authoritative admission signal for local consumers:
    the scheduler yields a queued job whose gpu_mem_gb fits in this number,
    instead of decrementing a flat per-accel slot counter that ignores the
    job's actual memory footprint. free_slots is kept for backward compat
    with consumers that haven't been upgraded yet.
    """
    payload = {
        "consumer_id": consumer_id,
        "kind": kind,
        "free_slots": free_slots,
        "published_at": datetime.now(timezone.utc).isoformat(),
    }
    if free_vram_gb is not None:
        payload["free_vram_gb"] = int(free_vram_gb)
    if total_vram_gb is not None:
        payload["total_vram_gb"] = int(total_vram_gb)
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


def consumers_by_free_vram(consumers: dict[str, dict], kinds: tuple[str, ...] | None = None) -> list[tuple[str, int]]:
    """[(consumer_id, free_vram_gb), ...] sorted descending. Empty if none publish vram."""
    rows: list[tuple[str, int]] = []
    for payload in consumers.values():
        if kinds and payload.get("kind") not in kinds:
            continue
        v = payload.get("free_vram_gb")
        if v is None:
            continue
        rows.append((payload["consumer_id"], int(v)))
    rows.sort(key=lambda r: -r[1])
    return rows
