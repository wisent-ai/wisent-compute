"""GPU quota tracking — live from each provider's quota API, GCS file is
reservation overlay only.

GCP: read regional quotas via google-cloud-compute RegionsClient.
Azure: read per-location compute usages via azure-mgmt-compute usage.list.
"""
from __future__ import annotations

import json
import os
from ..models import AZURE_QUOTA_FAMILY_TO_ACCEL
from ..queue.storage import JobStorage
from ..providers.base import Provider


# Map GCP regional-quota metric names to the accel_type strings the scheduler
# uses internally. Tracks the ON-DEMAND quotas now that the dispatcher forces
# preemptible=False everywhere (per 0.4.56's no-preemptible policy). Earlier
# versions tracked PREEMPTIBLE_NVIDIA_*_GPUS, which became wrong as soon as
# the dispatcher stopped creating Spot VMs: 20 STANDARD T4s were running
# while the scheduler still believed it had 20 free PREEMPTIBLE T4 slots,
# so it would have dispatched into a saturated NVIDIA_T4_GPUS quota anyway
# and 504'd on the QUOTA_EXCEEDED retry path.
_GCP_METRIC_TO_ACCEL = {
    "NVIDIA_T4_GPUS": "nvidia-tesla-t4",
    "NVIDIA_L4_GPUS": "nvidia-l4",
    "NVIDIA_A100_GPUS": "nvidia-tesla-a100",
    "NVIDIA_A100_80GB_GPUS": "nvidia-a100-80gb",
}


def _fetch_quotas_gcp(project: str, regions: list[str]) -> dict[str, int]:
    """Live regional quota limits from GCP, summed across all dispatch regions,
    keyed by internal accel_type names.

    Returns {} on any error in the FIRST region; partial coverage across
    regions is preserved (a regional API hiccup just omits that region's
    contribution). Uses google-cloud-compute (already a dep).
    """
    try:
        from google.cloud import compute_v1
        client = compute_v1.RegionsClient()
    except Exception:
        return {}
    out: dict[str, int] = {}
    for region in regions:
        try:
            region_obj = client.get(project=project, region=region)
        except Exception:
            continue
        for q in region_obj.quotas:
            accel = _GCP_METRIC_TO_ACCEL.get(q.metric)
            if accel:
                out[accel] = out.get(accel, 0) + int(q.limit)
    return out


def _fetch_quotas_azure(subscription: str, locations: list[str]) -> dict[str, int]:
    """Live SKU-family quota limits from Azure compute usages, summed across
    locations and mapped to internal accel_type names.

    Azure exposes quota at the SKU-family level via
    `compute.usage.list(location)`; one row per family carries
    `current_value` and `limit`. We read `limit` (running count is tracked
    separately via provider.list_running_instances) so the scheduler's
    standard `total - reserved - running` math still applies.
    """
    try:
        from azure.identity import DefaultAzureCredential
        from azure.mgmt.compute import ComputeManagementClient
    except Exception:
        return {}
    if not subscription:
        return {}
    try:
        client = ComputeManagementClient(DefaultAzureCredential(), subscription)
    except Exception:
        return {}
    out: dict[str, int] = {}
    for loc in locations:
        try:
            usages = list(client.usage.list(loc))
        except Exception:
            continue
        for u in usages:
            family = (u.name.value if u.name else "") or ""
            accel = AZURE_QUOTA_FAMILY_TO_ACCEL.get(family)
            if not accel:
                continue
            # Each NC*_T4_v3 SKU consumes 4 vCPU per GPU minimum on Standard
            # NCasT4_v3 (NC4) up to 64 vCPU (NC64). The family quota counts
            # vCPU, not GPUs — divide by the smallest member's vCPU to get
            # the upper bound on parallel 1-GPU VMs of that family.
            vcpu_per_smallest = _AZURE_FAMILY_MIN_VCPU.get(family, 4)
            slots = int(u.limit) // max(1, vcpu_per_smallest)
            out[accel] = out.get(accel, 0) + slots
    return out


# Smallest member's vCPU count per Azure GPU family. Used to convert the
# vCPU-denominated quota limit into a GPU-slot count. NCasT4_v3 family's
# smallest is NC4as_T4_v3 at 4 vCPU; NC*ads_A100_v4 family's smallest is
# NC24ads at 24 vCPU. If you provision a larger SKU (NC8/NC16/etc) you'll
# get fewer concurrent VMs than this naive count predicts; the scheduler's
# per-region create_instance failure path (QuotaExceeded) catches that.
_AZURE_FAMILY_MIN_VCPU = {
    "standardNCASv3Family": 4,
    "standardNCASv4Family": 4,
    "standardNCADSA100v4Family": 24,
}


def _load_overlay(store: JobStorage) -> dict:
    """Read the optional reservations file from the queue's storage backend.
    Format: {"gcp": {"nvidia-tesla-a100": {"reserved": 4}, ...},
             "azure": {"nvidia-a100-80gb": {"reserved": 1}, ...}}.
    Reservations subtract from the live cloud limit so non-wisent workloads
    can keep some headroom without lowering the actual cloud quota.
    """
    raw = store._download_text("config/quotas.json")
    return json.loads(raw) if raw else {}


def load_quotas(store: JobStorage, provider_name: str = "gcp") -> dict:
    """Compose live cloud quota limits with the storage-backed reservation overlay.

    Dispatches per provider_name:
      gcp   -> _fetch_quotas_gcp(GCP_PROJECT, REGIONS)
      azure -> _fetch_quotas_azure(AZURE_SUBSCRIPTION_ID, AZURE_LOCATIONS)

    Source of truth for `total` is the live cloud API — never the storage
    file. The storage file only contributes `reserved` slots per accel.
    Falls through to the storage file's `total` if the live API call fails
    (offline / dev).
    """
    overlay = _load_overlay(store)
    if provider_name == "gcp":
        from ..config import REGIONS
        project = os.environ.get("GCP_PROJECT", "wisent-480400")
        live = _fetch_quotas_gcp(project, REGIONS)
    elif provider_name == "azure":
        from ..config import AZURE_LOCATIONS, AZURE_SUBSCRIPTION_ID
        live = _fetch_quotas_azure(AZURE_SUBSCRIPTION_ID, AZURE_LOCATIONS)
    else:
        live = {}
    if not live:
        return overlay
    out: dict = {provider_name: {}}
    overlay_p = overlay.get(provider_name, {})
    for accel, total in live.items():
        reserved = int(overlay_p.get(accel, {}).get("reserved", 0))
        out[provider_name][accel] = {"total": total, "reserved": reserved}
    return out


def get_available_slots(store: JobStorage, provider: Provider, provider_name: str) -> dict[str, int]:
    """Count available GPU slots: total - reserved - running."""
    quotas = load_quotas(store, provider_name)
    provider_quotas = quotas.get(provider_name, {})
    running_counts = provider.list_running_instances()

    available = {}
    for accel_type, config in provider_quotas.items():
        total = config.get("total", 0)
        reserved = config.get("reserved", 0)
        used = running_counts.get(accel_type, 0)
        available[accel_type] = max(0, total - reserved - used)

    return available
