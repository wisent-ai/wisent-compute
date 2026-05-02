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

DEFAULT_IMAGE = "pytorch-2-9-cu129-ubuntu-2204-nvidia-580-v20260408"
DEFAULT_IMAGE_PROJECT = "deeplearning-platform-release"
DEFAULT_CPU_IMAGE_FAMILY = "ubuntu-2204-lts"
DEFAULT_CPU_IMAGE_PROJECT = "ubuntu-os-cloud"
DEFAULT_BOOT_DISK_GB = 200


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
    "openai/gpt-oss-20b": 88,
}


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
    overhead_gb = 8

    multiplier = 1.0
    if re.search(r'get-activations|generate-vector', command):
        multiplier = 1.2
    elif re.search(r'modify-weights|optimize-weights|training', command):
        multiplier = 1.5

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
