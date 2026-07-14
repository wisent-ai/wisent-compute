"""Host-RAM request estimation for Stado jobs.

Precedence is explicit request, workload profile, measured successful history,
operator model catalog, then a live-host-derived legacy budget at admission.
"""
from __future__ import annotations

import json
import math
import os
import re
import shlex
import time
from typing import Iterable

from .models import Job

_MODEL_RE = re.compile(r"(?:^|\s)--model(?:=|\s+)([^\s]+)")
_MODULE_RE = re.compile(r"(?:^|\s)python(?:\d+(?:\.\d+)*)?\s+-m\s+([^\s]+)")
_HISTORY_TTL_SECONDS = 300
_HISTORY_SAMPLE_CAP = 2000
_cache: dict[str, object] = {"built_at": 0.0, "history": {}}


def command_family(command: str) -> str:
    """Return a stable, non-secret workload family key."""
    model = _MODEL_RE.search(command or "")
    if model:
        return f"model:{model.group(1).strip(chr(39) + chr(34))}"
    module = _MODULE_RE.search(command or "")
    if module:
        return f"module:{module.group(1).strip(chr(39) + chr(34))}"
    try:
        words = shlex.split(command or "")
    except ValueError:
        words = (command or "").split()
    return f"command:{words[0]}" if words else "command:unknown"


def _nearest_rank_percentile(values: Iterable[float], percentile: float) -> float:
    ordered = sorted(float(value) for value in values if float(value) > 0)
    if not ordered:
        return 0.0
    index = max(0, math.ceil(percentile * len(ordered)) - 1)
    return ordered[index]


def _history_map(store: object | None) -> dict[str, float]:
    if store is None:
        return {}
    now = time.time()
    cached = _cache.get("history")
    if (
        isinstance(cached, dict)
        and now - float(_cache["built_at"]) < _HISTORY_TTL_SECONDS
    ):
        return cached
    infos = list(store.list_blobs_with_meta("completed/"))
    infos.sort(
        key=lambda info: info.updated.timestamp() if info.updated else 0.0,
        reverse=True,
    )
    samples: dict[str, list[float]] = {}
    for info in infos[:_HISTORY_SAMPLE_CAP]:
        metadata = info.metadata or {}
        family = str(metadata.get("ram_family", "") or "")
        try:
            peak = float(metadata.get("peak_host_ram_gb", 0.0) or 0.0)
        except (TypeError, ValueError):
            continue
        if family and peak > 0:
            samples.setdefault(family, []).append(peak)
    history = {
        family: round(_nearest_rank_percentile(peaks, 0.95) * 1.15 + 2.0, 2)
        for family, peaks in samples.items()
    }
    _cache.update(built_at=now, history=history)
    return history


def _catalog_map() -> dict[str, float]:
    raw = os.environ.get("WC_MODEL_RAM_CATALOG_JSON", "").strip()
    if not raw:
        return {}
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        return {}
    if not isinstance(parsed, dict):
        return {}
    return {
        str(key): float(value)
        for key, value in parsed.items()
        if isinstance(value, (int, float)) and float(value) > 0
    }


def estimate_ram_request(
    command: str,
    *,
    explicit_ram_gb: float = 0.0,
    profile_ram_gb: float = 0.0,
    store: object | None = None,
) -> tuple[float, str]:
    """Return ``(GiB, source)`` using the measurable precedence chain.

    Zero with ``legacy_dynamic_at_admission`` deliberately leaves an unknown
    workload unsized until a local host can derive a conservative per-slot
    budget from its own current telemetry.
    """
    if explicit_ram_gb > 0:
        return float(explicit_ram_gb), "explicit"
    if profile_ram_gb > 0:
        return float(profile_ram_gb), "profile"
    family = command_family(command)
    history = _history_map(store)
    if family in history:
        return history[family], "measured_p95"
    catalog = _catalog_map()
    model_key = family.removeprefix("model:")
    if model_key in catalog:
        return catalog[model_key], "model_catalog"
    return 0.0, "legacy_dynamic_at_admission"


def effective_job_ram_request(
    job: Job,
    *,
    store: object | None = None,
    mem_total_gb: float = 0.0,
    mem_available_gb: float = 0.0,
    ram_safety_headroom_gb: float = 0.0,
) -> tuple[float, str]:
    """Resolve old queue records from measured history or live host policy."""
    declared = float(getattr(job, "ram_request_gb", 0.0) or 0.0)
    source = str(getattr(job, "ram_estimation_source", "") or "")
    if declared > 0:
        return declared, source or "explicit"
    estimated, estimated_source = estimate_ram_request(
        getattr(job, "command", "") or "", store=store,
    )
    if estimated > 0:
        return estimated, estimated_source
    if mem_total_gb <= 0 or mem_available_gb <= 0:
        return 0.0, "missing_live_ram_telemetry"
    # Legacy records without a measured family get at most one quarter of
    # this actual host. The available-minus-headroom cap prevents fabricating
    # capacity on an already pressured machine. Both terms are live-derived.
    dynamic = min(
        mem_total_gb * 0.25,
        max(0.0, mem_available_gb - ram_safety_headroom_gb),
    )
    return dynamic, "legacy_dynamic_host_quarter"
