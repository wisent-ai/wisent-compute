"""Configuration and constants."""
from __future__ import annotations

import os
import json
import re
import time

PROJECT = os.environ.get("GCP_PROJECT", "wisent-480400")
BUCKET = os.environ.get("WC_BUCKET", "wisent-compute")
REGION = os.environ.get("GCP_REGION", "us-central1")
ALERTS_TOPIC = os.environ.get("WC_ALERTS_TOPIC", f"projects/{PROJECT}/topics/wisent-compute-alerts")

# Multi-region dispatch. Every region listed here is queried for live quota
# AND iterated by the GCP provider when creating instances. Each region
# carries a default GCP-issued quota (16 preemptible A100, 4 preemptible
# A100-80GB, 8 preemptible L4, 8 preemptible T4) so spreading across these
# 5 regions lifts total parallel-VM ceiling from ~28 to ~140 without any
# quota-increase request. Override with GCP_REGIONS=us-central1,europe-west4
# (comma-separated) to narrow the dispatch surface for testing.
REGIONS = [r.strip() for r in os.environ.get(
    "GCP_REGIONS",
    "us-central1,europe-west4,us-east1,us-east4,us-east5",
).split(",") if r.strip()]

# Zones, ordered by preference. Primary region's zones first (lowest egress
# from existing infra in us-central1), then alternates. Provider iterates
# this list and falls through GCE 'does not exist' / 'no capacity' errors
# until one zone accepts the create_instance call.
ZONE_ROTATION = [
    f"{REGION}-b", f"{REGION}-a", f"{REGION}-c", f"{REGION}-f",
    "europe-west4-a", "europe-west4-b", "europe-west4-c",
    "us-east1-c", "us-east1-d",
    "us-east4-a", "us-east4-b", "us-east4-c",
    "us-east5-a", "us-east5-b", "us-east5-c",
]
# Per-machine-type zone rotation. Some SKUs don't exist in every zone, or
# have regional spot-capacity quirks. For those buckets, list the zones that
# actually carry the SKU first; the provider falls back to ZONE_ROTATION.
MACHINE_TYPE_ZONES = {
    "a2-ultragpu-1g": [
        f"{REGION}-c", f"{REGION}-a",
        "us-east5-a", "us-east5-b",
        "europe-west4-a",
        # Removed 2026-05-01: machine-type not present in europe-west4-b;
        # NVIDIA_A100_80GB_GPUS regional quota is 0 in us-east4, so
        # us-east4-c was generating "Quota exceeded" errors every tick.
    ],
    "a2-highgpu-1g": [
        f"{REGION}-b", f"{REGION}-a", f"{REGION}-c", f"{REGION}-f",
        "europe-west4-a", "europe-west4-b",
        "us-east1-b",
        # us-east1-c, us-east4-a, us-east4-b removed 2026-05-01: confirmed via
        # `gcloud compute machine-types describe a2-highgpu-1g --zone=...`
        # that the SKU is not present in those zones; the dispatcher was
        # logging "Machine type does not exist" on every attempt against them
        # which wasted Cloud Function ticks and slowed fleet ramp-up.
    ],
    "g2-standard-4": [  # nvidia-l4
        f"{REGION}-a", f"{REGION}-b", f"{REGION}-c",
        "europe-west4-a", "europe-west4-b",
        "us-east1-c", "us-east1-d",
        "us-east4-a", "us-east4-c",
        # Removed 2026-05-01: g2-standard-4 not present in us-east4-b,
        # us-east5-a, us-east5-b. Confirmed via gcloud compute machine-types
        # describe; the dispatcher was logging "Invalid machine type" each
        # tick for these zones.
    ],
}
HEARTBEAT_STALE_MINUTES = 15
MAX_SCHEDULE_PER_TICK = 4
INSTANCE_PREFIX = "wisent"

# Defaults for the smart-routing CLI flags. 0 means "no cap"; the scheduler
# only enforces a cost gate when this is positive.
DEFAULT_MAX_COST_PER_HOUR_USD = 0.0
DEFAULT_PRIORITY = 0
DEFAULT_PREEMPTIBLE = False
DEFAULT_ANY_PROVIDER = True

# Dashboard HTTP server bind address + port. Bind to all interfaces so a
# tailscale serve front-end can reach it; the host is firewalled to the
# tailnet anyway.
DASHBOARD_BIND = os.environ.get("WC_DASHBOARD_BIND", "127.0.0.1")
DASHBOARD_PORT = int(os.environ.get("WC_DASHBOARD_PORT", "8765"))
DASHBOARD_REFRESH_SECONDS = int(os.environ.get("WC_DASHBOARD_REFRESH_SECONDS", "10"))
# Capacity blob is "live" if its published_at is within this many seconds.
DASHBOARD_AGENT_FRESH_SECONDS = int(os.environ.get("WC_DASHBOARD_AGENT_FRESH_SECONDS", "180"))

DEFAULT_IMAGE = "pytorch-2-9-cu129-ubuntu-2204-nvidia-580-v20260408"
DEFAULT_IMAGE_PROJECT = "deeplearning-platform-release"
DEFAULT_CPU_IMAGE_FAMILY = "ubuntu-2204-lts"
DEFAULT_CPU_IMAGE_PROJECT = "ubuntu-os-cloud"
DEFAULT_BOOT_DISK_GB = 200

# Azure (parallel to GCP). All values resolved from env so the same
# wisent-compute install can target multiple subscriptions/resource groups
# without code changes. The provider does NOT create the vnet/subnet/NSG —
# it expects pre-provisioned infra named below.
AZURE_SUBSCRIPTION_ID = os.environ.get("AZURE_SUBSCRIPTION_ID", "")
AZURE_RESOURCE_GROUP = os.environ.get("AZURE_RESOURCE_GROUP", "wisent-compute")
AZURE_LOCATIONS = [r.strip() for r in os.environ.get(
    "AZURE_LOCATIONS",
    "eastus,westus3,westus2,northeurope",
).split(",") if r.strip()]
AZURE_VNET = os.environ.get("AZURE_VNET", "wisent-compute-vnet")
AZURE_SUBNET = os.environ.get("AZURE_SUBNET", "wisent-compute-subnet")
AZURE_NSG = os.environ.get("AZURE_NSG", "wisent-compute-nsg")
# microsoft-dsvm:ubuntu-hpc:2204:latest ships with NVIDIA driver + CUDA preinstalled,
# matching deeplearning-platform-release on GCP. Override via AZURE_IMAGE_URN
# (publisher:offer:sku:version) for a different base image.
AZURE_IMAGE_URN = os.environ.get(
    "AZURE_IMAGE_URN",
    "microsoft-dsvm:ubuntu-hpc:2204:latest",
)
AZURE_VM_USERNAME = os.environ.get("AZURE_VM_USERNAME", "wisent")
# SSH public key for the cloud-init admin user. Required by Azure VM create
# even when SSH is locked down via NSG; cloud-init will only accept the VM
# create call if either ssh keys or password auth is configured.
AZURE_SSH_PUBLIC_KEY = os.environ.get("AZURE_SSH_PUBLIC_KEY", "")

# Multi-provider dispatch. Coordinator and Cloud Function tick iterate this
# list, calling check_running_jobs / reap_dead_agents / schedule_queued_jobs
# per provider. A provider whose constructor throws (creds missing) is
# logged and skipped. Default keeps single-cloud GCP behavior.
WC_PROVIDERS = [p.strip() for p in os.environ.get("WC_PROVIDERS", "gcp").split(",") if p.strip()]

# Storage backend for the queue. "gcs" (default) keeps the existing
# gs://wisent-compute path; "azure" routes JobStorage at an Azure Blob
# container. The two are mutually exclusive — a single JobStorage instance
# binds to exactly one backend, decided at construction time.
WC_STORAGE_BACKEND = os.environ.get("WC_STORAGE_BACKEND", "gcs")
WC_AZURE_STORAGE_ACCOUNT = os.environ.get("WC_AZURE_STORAGE_ACCOUNT", "")
WC_AZURE_CONTAINER = os.environ.get("WC_AZURE_CONTAINER", "wisent-compute")

# Billing-credits collector. Each tick the Cloud Function reads the GCP
# BigQuery billing export (gross / credits-applied / net + per-credit
# cumulative + 7-day burn) and the Azure available-credit balance, then
# writes gs://<BUCKET>/billing_health/credits.json (same convention as
# host_health/<host>.json). The export table is account-specific; it is
# resolved from env so a different billing account only needs a redeploy
# env change, never a code edit. Dataset/table default to the live
# wisent-480400 export confirmed present 2026-05-16.
BILLING_DATASET = os.environ.get("WC_BILLING_DATASET", "billing_export")
BILLING_TABLE = os.environ.get(
    "WC_BILLING_TABLE", "gcp_billing_export_v1_017364_D3B657_F207B5"
)
# A day whose net_cost (gross + credits, credits are negative) exceeds this
# means the promotion credit no longer fully covers spend — i.e. it is
# exhausted or rate-capped. This is the depletion signal; it needs no
# knowledge of the original grant ceiling (which no GCP API exposes).
BILLING_NET_ALERT_USD = float(os.environ.get("WC_BILLING_NET_ALERT_USD", "100"))
# Secret Manager secret holding the Azure billing service principal as JSON
# {"tenant_id","client_id","client_secret", and one of
# "billing_account"/"billing_profile" or "subscription_id"}. Absent secret
# is reported as an explicit no_credentials status, never silently skipped.
AZURE_BILLING_SECRET = os.environ.get(
    "WC_AZURE_BILLING_SECRET", "wisent-azure-billing-sp"
)


# Co-schedule / cost POLICY flags, loaded from GCS so they change without
# a wisent-compute republish. These are operator decisions, NOT quantities
# measurable from GPU telemetry, so they legitimately live in config:
#   gs://<BUCKET>/config/model_overrides.json
#   {"exclusive": [<model>, ...], "local_only": [<model>, ...]}
# (Per-model VRAM sizing is NOT here — it is learned from measured peaks;
# see wisent_compute.sizing.observed_vram_gb.) Cached in-process 5 min.
_MODEL_POLICY_CACHE: dict = {"data": None, "fetched_at": 0.0}
_MODEL_POLICY_TTL_S = 300


def _load_model_policy() -> dict:
    now = time.time()
    if (_MODEL_POLICY_CACHE["data"] is not None
            and now - _MODEL_POLICY_CACHE["fetched_at"] < _MODEL_POLICY_TTL_S):
        return _MODEL_POLICY_CACHE["data"]
    try:
        from google.cloud import storage
        blob = storage.Client().bucket(BUCKET).blob("config/model_overrides.json")
        data = json.loads(blob.download_as_text())
        if not isinstance(data, dict):
            raise ValueError("model_overrides.json is not a JSON object")
    except Exception:
        data = {}
    data.setdefault("exclusive", [])
    data.setdefault("local_only", [])
    _MODEL_POLICY_CACHE["data"] = data
    _MODEL_POLICY_CACHE["fetched_at"] = now
    return data


def is_exclusive_model(model: str) -> bool:
    return model in _load_model_policy()["exclusive"]


def is_local_only_model(model: str) -> bool:
    return model in _load_model_policy()["local_only"]


def estimate_gpu_memory(command: str) -> int:
    """Estimate GPU memory needed from a command string."""
    model_match = re.search(r'--model\s+(\S+)', command)
    if not model_match:
        return 0
    model = model_match.group(1).strip("'\"")

    # Sizing is PURELY the measured peak. No params formula, no per-model
    # constant, no multiplier — those are all hardcoded guesses and are
    # forbidden. observed_vram_gb returns the max real nvidia-smi
    # peak_vram_gb the fleet has recorded for this model.
    from .sizing import observed_vram_gb
    measured = observed_vram_gb(model)
    if measured is not None:
        return measured

    # No measurement for this model yet: do NOT fabricate a number.
    # Return the smallest GPU tier's hardware capacity so the job is
    # dispatched to the smallest box. If it OOMs there, the failure path
    # (slots.advance_slot) escalates it one hardware tier at a time until
    # it runs; that successful run's nvidia-smi peak is recorded and
    # every later job of this model is then sized from the measurement.
    # min(GPU_SIZING[gcp]) is hardware fact, not a picked constant.
    from .models import GPU_SIZING
    return min(GPU_SIZING.get("gcp", {16: None}).keys())


def lookup_instance_type(provider: str, gpu_mem_gb: int) -> tuple[str, str]:
    """Return (machine_type, accel_type) for the given memory requirement.

    If gpu_mem_gb exceeds every tier in GPU_SIZING, returns the LARGEST
    available tier rather than ("", ""). The previous behavior produced an
    empty machine_type that the GCE create_instance call rejected with
    'Machine type with name "" does not exist', wedging the job. Sending
    it to the largest tier means the in-VM workload may still OOM, but
    that's a clearer failure mode than a malformed instance request.
    """
    from .models import GPU_SIZING
    sizing = GPU_SIZING.get(provider, {})
    if not sizing:
        return ("", "")
    best_mem, best_spec = 10**9, None
    largest_mem, largest_spec = 0, None
    for mem, spec in sizing.items():
        if mem > largest_mem:
            largest_mem, largest_spec = mem, spec
        if mem >= gpu_mem_gb and mem < best_mem:
            best_mem, best_spec = mem, spec
    return best_spec if best_spec is not None else largest_spec
