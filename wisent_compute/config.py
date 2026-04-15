"""Configuration and constants."""
from __future__ import annotations

import os
import json
import re

PROJECT = os.environ.get("GCP_PROJECT", "wisent-480400")
BUCKET = os.environ.get("WC_BUCKET", "wisent-compute")
REGION = os.environ.get("GCP_REGION", "us-central1")
ALERTS_TOPIC = os.environ.get("WC_ALERTS_TOPIC", f"projects/{PROJECT}/topics/wisent-compute-alerts")

ZONE_ROTATION = [f"{REGION}-b", f"{REGION}-a", f"{REGION}-c", f"{REGION}-f"]
HEARTBEAT_STALE_MINUTES = 15
MAX_SCHEDULE_PER_TICK = 4
INSTANCE_PREFIX = "wisent"

DEFAULT_IMAGE = "pytorch-2-9-cu129-ubuntu-2204-nvidia-580-v20260408"
DEFAULT_IMAGE_PROJECT = "deeplearning-platform-release"
DEFAULT_CPU_IMAGE_FAMILY = "ubuntu-2204-lts"
DEFAULT_CPU_IMAGE_PROJECT = "ubuntu-os-cloud"
DEFAULT_BOOT_DISK_GB = 200


def estimate_gpu_memory(command: str) -> int:
    """Estimate GPU memory needed from a command string."""
    model_match = re.search(r'--model\s+(\S+)', command)
    if not model_match:
        return 0
    model = model_match.group(1)

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
    """Return (machine_type, accel_type) for the given memory requirement."""
    from .models import GPU_SIZING
    sizing = GPU_SIZING.get(provider, {})
    best_mem, best_spec = 999999, ("", "")
    for mem, spec in sizing.items():
        if mem >= gpu_mem_gb and mem < best_mem:
            best_mem = mem
            best_spec = spec
    return best_spec
