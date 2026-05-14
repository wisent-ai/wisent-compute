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
    diag: dict | None = None,
) -> None:
    """Write this consumer's current capacity snapshot to GCS.

    free_vram_gb is the authoritative admission signal for local consumers:
    the scheduler yields a queued job whose gpu_mem_gb fits in this number,
    instead of decrementing a flat per-accel slot counter that ignores the
    job's actual memory footprint. free_slots is kept for backward compat
    with consumers that haven't been upgraded yet.

    diag carries per-tick claim-loop telemetry so a reaper or dashboard can
    see why a broadcasting agent isn't claiming. Keys:
      queue_scanned         # of queued jobs the agent inspected this loop
      vram_rejected         # rejected because need > free_vram_gb
      eligibility_rejected  # rejected by _job_eligible (incl. LOCAL_ONLY)
      eligible_count        # passed both gates
      claimed_this_loop     # actually start_slot()'d this iteration
      last_started_job_id   # most recent job_id this agent moved to running/
      last_started_at       # ISO ts of last successful start_slot
      last_claim_attempt_at # ISO ts of this loop iteration
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
    if diag is not None:
        payload["diag"] = diag
    store._upload_text(f"{CAPACITY_PREFIX}{consumer_id}.json", json.dumps(payload))


def read_consumer_capacity(store: JobStorage) -> dict[str, dict]:
    """Return {consumer_id: payload} for every live (non-stale) consumer.

    Filters on blob.updated metadata BEFORE downloading. Previously every
    tick downloaded all broadcast files (1900+ accumulated, most stale)
    just to read published_at — at ~30ms/blob this exceeded the 60s Cloud
    Function timeout, returned 504, and Cloud Scheduler auto-paused the
    cron. Filtering on server-side metadata first means the tick reads
    only the small number of fresh blobs.

    Also deletes long-stale blobs (older than 1h, well past
    CAPACITY_STALE_SECONDS=180s) so the bucket can't accumulate forever.
    Capped per tick so the Cloud Function never spends its budget on GC.

    Backend-agnostic: works against either the GCS SDK or AzureBlobBackend
    via JobStorage.list_blobs_with_meta. The gsutil-only fallback (no SDK
    on either side) is unsupported on the dispatcher path.
    """
    if store._sdk_bucket is None and store._azure_backend is None:
        raise RuntimeError(
            "read_consumer_capacity requires either the GCS SDK bucket or "
            "the Azure Blob backend. JobStorage was constructed without "
            "either (gsutil-only mode is not supported on the dispatcher path)."
        )

    now = datetime.now(timezone.utc)
    cutoff_fresh = now.timestamp() - CAPACITY_STALE_SECONDS
    cutoff_delete = now.timestamp() - 3600  # 1h

    out: dict[str, dict] = {}
    stale_blobs: list = []
    for blob in store.list_blobs_with_meta(CAPACITY_PREFIX):
        if not blob.name.endswith(".json"):
            continue
        if blob.updated is None:
            continue
        ts = blob.updated.timestamp()
        if ts < cutoff_delete:
            stale_blobs.append(blob)
            continue
        if ts < cutoff_fresh:
            continue
        # Race: an agent can self-delete its own broadcast (or another tick
        # can sweep stale broadcasts) between list above and download below.
        # Both backends translate the 404 into a None return so the
        # missing-blob case is the only one we drop; any other error
        # propagates to the caller so transient SDK/network failures stay
        # visible.
        raw = blob.download_text()
        if raw is None:
            continue
        payload = json.loads(raw)
        cid = payload.get("consumer_id")
        if cid:
            out[cid] = payload

    for blob in stale_blobs[:200]:
        blob.delete()
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
