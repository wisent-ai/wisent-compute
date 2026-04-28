"""GPU quota tracking — live from GCP regions API, GCS file is reservation overlay only."""
from __future__ import annotations

import json
import os
from ..queue.storage import JobStorage
from ..providers.base import Provider


# Map GCP regional-quota metric names to the accel_type strings the scheduler
# uses internally. Spot/preemptible variants only — on-demand isn't dispatched
# by the wisent-compute scheduler today.
_GCP_METRIC_TO_ACCEL = {
    "PREEMPTIBLE_NVIDIA_T4_GPUS": "nvidia-tesla-t4",
    "PREEMPTIBLE_NVIDIA_L4_GPUS": "nvidia-l4",
    "PREEMPTIBLE_NVIDIA_A100_GPUS": "nvidia-tesla-a100",
    "PREEMPTIBLE_NVIDIA_A100_80GB_GPUS": "nvidia-a100-80gb",
}


def _fetch_gcp_quotas(project: str, region: str) -> dict[str, int]:
    """Live regional quota limits from GCP, keyed by internal accel_type names.

    Returns {} on any error so the scheduler can fall back to the GCS overlay
    if the API is unreachable. Uses google-cloud-compute (already a dep).
    """
    try:
        from google.cloud import compute_v1
        client = compute_v1.RegionsClient()
        region_obj = client.get(project=project, region=region)
    except Exception:
        return {}
    out: dict[str, int] = {}
    for q in region_obj.quotas:
        accel = _GCP_METRIC_TO_ACCEL.get(q.metric)
        if accel:
            out[accel] = int(q.limit)
    return out


def _load_overlay(store: JobStorage) -> dict:
    """Read the optional GCS reservations file. Format:
    {"gcp": {"nvidia-tesla-a100": {"reserved": 4}, ...}}.
    Reservations subtract from the live GCP limit so non-wisent workloads can
    keep some headroom without lowering the actual cloud quota.
    """
    if store.bucket is not None:
        blob = store.bucket.blob("config/quotas.json")
        if not blob.exists():
            return {}
        return json.loads(blob.download_as_text())
    raw = store._download_text("config/quotas.json")
    return json.loads(raw) if raw else {}


def load_quotas(store: JobStorage) -> dict:
    """Compose live GCP quota limits with the GCS reservation overlay.

    Source of truth for `total` is the GCP regions API — never the GCS file.
    The GCS file only contributes `reserved` slots per accel. Falls through
    to the GCS file's `total` if the live API call fails (offline / dev).
    """
    project = os.environ.get("GCP_PROJECT", "wisent-480400")
    region = os.environ.get("GCP_REGION", "us-central1")
    live = _fetch_gcp_quotas(project, region)
    overlay = _load_overlay(store)
    if not live:
        return overlay
    out: dict = {"gcp": {}}
    overlay_gcp = overlay.get("gcp", {})
    for accel, total in live.items():
        reserved = int(overlay_gcp.get(accel, {}).get("reserved", 0))
        out["gcp"][accel] = {"total": total, "reserved": reserved}
    return out


def get_available_slots(store: JobStorage, provider: Provider, provider_name: str) -> dict[str, int]:
    """Count available GPU slots: total - reserved - running."""
    quotas = load_quotas(store)
    provider_quotas = quotas.get(provider_name, {})
    running_counts = provider.list_running_instances()

    available = {}
    for accel_type, config in provider_quotas.items():
        total = config.get("total", 0)
        reserved = config.get("reserved", 0)
        used = running_counts.get(accel_type, 0)
        available[accel_type] = max(0, total - reserved - used)

    return available
