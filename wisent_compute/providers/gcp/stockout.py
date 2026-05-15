"""Cross-instance cache of GCE zones currently returning
ZONE_RESOURCE_POOL_EXHAUSTED (stockout).

With Cloud Function maxScale=100 and ticks lasting longer than the 3-minute
cron, the function spawns multiple parallel instances, each with its own
Python module state. A process-local dict was insufficient: every fresh
instance re-discovered the same exhausted zones at ~30s per op.result()
call, blowing past the 540s tick timeout. Confirmed in production logs
01:46-01:48Z 2026-05-15: us-central1-c stocked out twice in 8 seconds
across two parallel function instances.

This module persists the stockout map to gs://<bucket>/state/stockout_zones.json
so every Cloud Function instance shares what's stocked out right now. The
blob is read at most every _LOCAL_CACHE_TTL_S seconds (in-process cache)
and written only on stockout detection (rare).
"""
from __future__ import annotations

import json
import time

STOCKOUT_TTL_S = 300
STOCKOUT_BLOB = "state/stockout_zones.json"
_LOCAL_CACHE_TTL_S = 10

_local_cache: dict[str, float] = {}
_local_cache_built_at: float = 0.0


def _stockout_blob():
    from ...queue.storage import JobStorage
    from ...config import BUCKET
    store = JobStorage(BUCKET)
    bucket = getattr(store, "_sdk_bucket", None)
    if bucket is None:
        return None
    return bucket.blob(STOCKOUT_BLOB)


def _load_stockouts() -> dict[str, float]:
    """Read the shared stockout-zone map from GCS, refreshing the in-process
    cache at most every _LOCAL_CACHE_TTL_S seconds.

    NotFound on the blob means the new-cluster state (no stockouts logged
    yet); return an empty dict. JSONDecodeError on a corrupt blob also
    returns empty — the recovery path is to overwrite the blob on the
    next stockout, and a corrupted state file should not crash the
    autoscaler. Any other GCS error propagates so the operator sees it.
    """
    global _local_cache_built_at, _local_cache
    if (time.time() - _local_cache_built_at) < _LOCAL_CACHE_TTL_S and _local_cache:
        return _local_cache
    blob = _stockout_blob()
    if blob is None:
        return {}
    from google.api_core.exceptions import NotFound
    try:
        text = blob.download_as_text()
    except NotFound:
        _local_cache = {}
        _local_cache_built_at = time.time()
        return {}
    try:
        data = json.loads(text)
    except json.JSONDecodeError:
        _local_cache = {}
        _local_cache_built_at = time.time()
        return {}
    if not isinstance(data, dict):
        _local_cache = {}
        _local_cache_built_at = time.time()
        return {}
    _local_cache = {k: float(v) for k, v in data.items()}
    _local_cache_built_at = time.time()
    return _local_cache


def _save_stockouts(stockouts: dict[str, float]) -> None:
    blob = _stockout_blob()
    if blob is None:
        return
    blob.upload_from_string(
        json.dumps(stockouts), content_type="application/json",
    )


def zone_recently_stocked_out(zone: str) -> bool:
    stockouts = _load_stockouts()
    ts = stockouts.get(zone)
    if ts is None:
        return False
    return (time.time() - ts) < STOCKOUT_TTL_S


def mark_zone_stockout(zone: str) -> None:
    stockouts = _load_stockouts()
    now = time.time()
    stockouts[zone] = now
    stockouts = {z: t for z, t in stockouts.items() if (now - t) < (2 * STOCKOUT_TTL_S)}
    _save_stockouts(stockouts)
    global _local_cache, _local_cache_built_at
    _local_cache = stockouts
    _local_cache_built_at = now


# Region-level quota cache: keys look like "us-central1:nvidia-tesla-a100"
# so different accel quotas in the same region cache independently. Quota
# resets when other VMs terminate, but within a tick window the same
# region's quota stays exhausted, so 60s is enough TTL to skip retries.
QUOTA_TTL_S = 60
QUOTA_BLOB = "state/quota_exceeded.json"
_quota_cache: dict[str, float] = {}
_quota_built_at: float = 0.0


def _quota_blob():
    from ...queue.storage import JobStorage
    from ...config import BUCKET
    store = JobStorage(BUCKET)
    bucket = getattr(store, "_sdk_bucket", None)
    if bucket is None:
        return None
    return bucket.blob(QUOTA_BLOB)


def _load_quota() -> dict[str, float]:
    global _quota_cache, _quota_built_at
    if (time.time() - _quota_built_at) < _LOCAL_CACHE_TTL_S and _quota_cache:
        return _quota_cache
    blob = _quota_blob()
    if blob is None:
        return {}
    from google.api_core.exceptions import NotFound
    try:
        text = blob.download_as_text()
    except NotFound:
        _quota_cache = {}
        _quota_built_at = time.time()
        return {}
    try:
        data = json.loads(text)
    except json.JSONDecodeError:
        _quota_cache = {}
        _quota_built_at = time.time()
        return {}
    if not isinstance(data, dict):
        _quota_cache = {}
        _quota_built_at = time.time()
        return {}
    _quota_cache = {k: float(v) for k, v in data.items()}
    _quota_built_at = time.time()
    return _quota_cache


def region_recently_quota_exceeded(region: str, accel: str) -> bool:
    key = f"{region}:{accel}"
    data = _load_quota()
    ts = data.get(key)
    if ts is None:
        return False
    return (time.time() - ts) < QUOTA_TTL_S


def mark_region_quota_exceeded(region: str, accel: str) -> None:
    key = f"{region}:{accel}"
    data = _load_quota()
    now = time.time()
    data[key] = now
    data = {k: t for k, t in data.items() if (now - t) < (2 * QUOTA_TTL_S)}
    blob = _quota_blob()
    if blob is not None:
        blob.upload_from_string(json.dumps(data), content_type="application/json")
    global _quota_cache, _quota_built_at
    _quota_cache = data
    _quota_built_at = now
