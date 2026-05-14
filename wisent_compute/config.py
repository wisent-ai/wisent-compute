"""Configuration and constants."""
from __future__ import annotations

import os
import json
import re

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


# Per-model peak-VRAM overrides for models whose actual peak diverges from
# the params-based heuristic below. Numbers are observed peak GiB during
# get-activations on an A100-80GB / RTX PRO 6000.
#
# openai/gpt-oss-20b: 78.6 GiB peak observed (job d3e0f18e tail, 2026-05-02).
#   Model ships in mxfp4 packed format which is dequantized to bf16 in
#   transformers' load path (mxfp4.py:115 convert_moe_packed_tensors), so
#   on-disk size ~= 12 GiB but on-GPU peak balloons to ~80 GiB. Without
#   this override the params-based heuristic predicts 72 GiB, leaving
#   ~22 GiB free in the agent's bookkeeping. The agent then claims a
#   second job that fits in 22 GiB (e.g. Llama-3.2-1B at 15 GiB) — once
#   gpt-oss-20b finishes dequant, total = 78 + 15 = 93 GiB and the GPU
#   OOMs. Reserving 88 GiB means no second job can be co-scheduled on a
#   95 GiB GPU, which is the desired behavior for this model.
MODEL_VRAM_OVERRIDES_GB = {
    # 80 GiB so a2-ultragpu-1g (80 GiB A100) can admit it (need=80 == free=80).
    # Previously 88 to forbid co-scheduling on the 96 GiB workstation, but that
    # locked the model to workstation-only. Workstation's lm-eval pt-task
    # loading bug then made gpt-oss-20b throughput effectively zero. Switching
    # to 80 + EXCLUSIVE_MODELS (below) keeps the no-co-schedule guarantee
    # WITHOUT blocking 80 GiB cloud VMs from admitting the job.
    "openai/gpt-oss-20b": 80,
}

# Models the agent must run with the entire GPU to itself. While a slot is
# running one of these, the agent treats remaining free VRAM as 0 so no
# second job is claimed. Reason: gpt-oss-20b's mxfp4 dequant balloons peak
# from ~40 GiB to ~78 GiB after admission; on a 96 GiB workstation the
# pre-balloon free reading lets the agent claim a co-scheduled small job
# that then OOMs once dequant completes (job d3e0f18e, 0e8f54b6).
EXCLUSIVE_MODELS = frozenset({
    "openai/gpt-oss-20b",
})

# Models that may ONLY be claimed by local (on-prem) agents, never cloud.
# Use case: gpt-oss-20b on a2-ultragpu-1g spot costs ~$2.40 per task
# (extracted 2026-05-05 from billing_export); the workstation runs them at
# $0 marginal. With ~6,166 gpt-oss-20b jobs in the current batch this is
# ~$14.8k of avoidable spend in exchange for ~8x slower drain on this
# subset only (other models keep flowing on cloud in parallel).
LOCAL_ONLY_MODELS = frozenset({
    "openai/gpt-oss-20b",
})


def estimate_gpu_memory(command: str) -> int:
    """Estimate GPU memory needed from a command string."""
    model_match = re.search(r'--model\s+(\S+)', command)
    if not model_match:
        return 0
    model = model_match.group(1).strip("'\"")

    if model in MODEL_VRAM_OVERRIDES_GB:
        return MODEL_VRAM_OVERRIDES_GB[model]

    params_b = 0
    m = re.search(r'(\d+\.?\d*)[Bb]', model)
    if m:
        params_b = float(m.group(1))
    else:
        m = re.search(r'(\d+)[Mm]', model)
        if m:
            params_b = int(m.group(1)) / 1000
    if params_b == 0:
        params_b = 7

    quant_factor = 1
    if re.search(r'GPTQ|AWQ|INT4|4bit|Q4', model):
        quant_factor = 4
    elif re.search(r'INT8|8bit|Q8', model):
        quant_factor = 2

    weights_gb = params_b * 2 / quant_factor
    kv_gb = weights_gb * 0.3
    # Bumped 8 -> 12 (2026-05-14): observed peak vs estimate gap of ~5-7 GB
    # on Qwen3-8B / Llama-3.1-8B activation-extraction runs. CUDA allocator
    # fragmentation + transformers' attention scratch + nccl staging all
    # eat headroom not captured by weights+kv. Under-estimating neighbors'
    # peak caused co-tenancy OOM on job 4b0411a1 (A100-80GB: 32+32+22 GB
    # actual, but agent's bookkeeping admitted as 26+26+22 = 74 / 80).
    overhead_gb = 12

    multiplier = 1.0
    if re.search(r'get-activations|generate-vector', command):
        # Bumped 1.2 -> 1.4 (2026-05-14): activation extraction layers
        # the entire residual stream through to disk, with hidden-state
        # buffers held in VRAM longer than the multiplier=1.2 assumed.
        # Observed peak/declared ratio ~1.32-1.38 on the workstation
        # fleet; 1.4 leaves 5% guard.
        multiplier = 1.4
    elif re.search(r'modify-weights|optimize-weights|training', command):
        multiplier = 1.5

    # GRPO / RL training memory profile is much heavier than inference or
    # activation extraction: per training step the trainer holds the model
    # (bf16 weights), the model gradients (bf16), the optimizer state
    # (8-bit AdamW from bitsandbytes saves ~10 GB vs fp32 AdamW), the
    # reference-model log-probs (another full bf16 forward), AND a KV
    # cache for num_generations × batch_size rollout sequences during the
    # reward computation. Tuned empirically:
    #   1B Llama with adamw_bnb_8bit + grad-checkpoint: OOMs on T4 (15 GB),
    #     succeeds on L4 (24 GB).
    #   4B Qwen with same: fits in 40 GB A100 (the RTX PRO 6000 advertises
    #     a 40 GB A100 slot; only 8 GB weights + ~25 GB rest).
    # Formula 3x weights + 16 GB overhead lands 1B on L4 (22 GB → 24 tier),
    # 4B on A100-40 (40 → 40 tier), 7B on A100-80 (37 → 40 or 80 tier).
    if re.search(r'(^|\s|-m\s+)train\.train\b', command):
        return round(weights_gb * 3 + 16)

    return round((weights_gb + kv_gb + overhead_gb) * multiplier)


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
