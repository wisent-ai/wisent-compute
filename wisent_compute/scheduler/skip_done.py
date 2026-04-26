"""Pre-dispatch filter for queued jobs whose results are already on HF.

The wrapper short-circuits per-strategy on the box, but on GCP the VM
still pays for boot + pip install before discovering there is nothing
to do. This module catches that case at scheduler granularity: one HF
listing per tick, set-membership check per queued job, completed-jobs
get moved straight from queue to completed without ever spinning up.
"""
from __future__ import annotations

import re

# Strategies the activation wrapper runs by default. Keep in sync with
# wisent_tools/scripts/activations/extract_and_upload.VALIDATED_STRATEGIES.
DEFAULT_STRATEGIES = (
    "chat_last", "chat_mean", "chat_first", "chat_max_norm",
    "chat_weighted", "mc_balanced", "role_play",
)
DEFAULT_COMPONENT = "residual_stream"
HF_REPO_ID = "wisent-ai/activations"
HF_REPO_TYPE = "dataset"


def _model_to_safe_name(model: str) -> str:
    """Mirror wisent.core.reading.modules.utilities.data.sources.hf.hf_config."""
    return model.replace("/", "__").replace(":", "_")


_TASK_RE = re.compile(r"--task\s+(\S+)")
_MODEL_RE = re.compile(r"--model\s+'([^']+)'|--model\s+\"([^\"]+)\"|--model\s+(\S+)")


def _parse_command(cmd: str) -> tuple[str, str]:
    """Pull (model, task) out of an extract_and_upload command line."""
    task_m = _TASK_RE.search(cmd)
    model_m = _MODEL_RE.search(cmd)
    task = task_m.group(1) if task_m else ""
    model = ""
    if model_m:
        model = model_m.group(1) or model_m.group(2) or model_m.group(3) or ""
    return model, task


def fetch_hf_done_prefixes() -> set[str] | None:
    """Set of unique 'activations/<safe_model>/<task>/<strategy>/' prefixes.

    Returns None when the HF SDK is unreachable so the scheduler can carry on
    without the optimisation rather than dropping into a blocking failure.
    """
    try:
        from huggingface_hub import HfApi
    except Exception:
        return None
    try:
        files = list(HfApi().list_repo_files(repo_id=HF_REPO_ID, repo_type=HF_REPO_TYPE))
    except Exception:
        return None
    prefixes: set[str] = set()
    for f in files:
        parts = f.split("/")
        if len(parts) >= 4 and parts[0] == "activations":
            prefixes.add("/".join(parts[:4]) + "/")
    return prefixes


def is_job_already_done(command: str, prefixes: set[str]) -> bool:
    """True if every default strategy for this (model, task) has files in HF."""
    model, task = _parse_command(command)
    if not model or not task:
        return False
    safe = _model_to_safe_name(model)
    for strategy in DEFAULT_STRATEGIES:
        prefix = f"activations/{safe}/{task}/{strategy}/"
        if prefix not in prefixes:
            return False
    return True


def filter_already_done(queued, store, now_utc, log_fn):
    """Move every queued job whose results are already on HF straight to
    completed, returning the surviving still-to-run list. log_fn receives
    a single summary line. Errors during HF fetch are non-fatal — the
    scheduler keeps dispatching the unfiltered queue."""
    from ..models import JobState
    prefixes = fetch_hf_done_prefixes()
    if not prefixes:
        return queued
    survivors = []
    skipped = 0
    for j in queued:
        if is_job_already_done(j.command, prefixes):
            j.state = JobState.COMPLETED.value
            j.completed_at = now_utc.isoformat()
            j.error = "skipped: all strategies present on wisent-ai/activations"
            store.move_job(j, "queue", "completed")
            skipped += 1
        else:
            survivors.append(j)
    if skipped:
        log_fn(f"Skipped {skipped} jobs already complete on HF (no VM spawn)")
    return survivors
