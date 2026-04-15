"""GPU quota tracking."""
from __future__ import annotations

import json
from ..queue.storage import JobStorage
from ..providers.base import Provider


def load_quotas(store: JobStorage) -> dict:
    """Load quota config from GCS config/quotas.json."""
    blob = store.bucket.blob("config/quotas.json")
    if not blob.exists():
        return {}
    return json.loads(blob.download_as_text())


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
