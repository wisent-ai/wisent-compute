"""Cost reporting + projection. Backs `wc cost report` and `wc cost estimate`.

Reads every job from JobStorage (queue/running/completed/failed), computes
wall-time from started_at -> (completed_at or failed_at), looks up the
matching $/hour from models.GPU_HOURLY_RATE_USD with SPOT_DISCOUNT applied
when preemptible=True, attributes each job by instance_ref (local@host vs
gcp:zone:instance), and aggregates per (gpu_type, target_kind, model_id).

Replaces hand-waved cost ceilings with measured per-job distributions as
soon as any jobs have actually run.
"""
from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Iterable

from ..config import BUCKET
from ..models import (
    AZURE_VM_HOURLY_RATE_USD,
    GPU_HOURLY_RATE_USD, SPOT_DISCOUNT,
    VM_BUNDLE_HOURLY_RATE_USD, GPU_TYPE_TO_MACHINE_TYPE,
    Job,
)
from ..queue.storage import JobStorage

UNKNOWN_MODEL = "(unknown)"
FINISHED_JOB_STATES = ("completed", "failed")
SECONDS_PER_HOUR = 3600.0
STARTUP_SECONDS = 50.0
STRATEGY_COUNT = 7.0
BASE_SECONDS_PER_STRATEGY = 80.0
SECONDS_PER_GB_PER_STRATEGY = 5.0
TARGET_KIND_KEY = "target_kind"
MODEL_KEY = "model"
COST_ROW_FORMAT = "  {target:<10} jobs={jobs:<5} wall_h={wall_hours:>7.2f} cost=${cost_usd:.4f}"


def _parse_iso(ts: str | None) -> datetime | None:
    if not ts:
        return None
    return datetime.fromisoformat(ts.replace("Z", "+00:00"))


def _wall_seconds(job: Job) -> float | None:
    """started -> (completed or failed). None if either side missing."""
    start = _parse_iso(job.started_at)
    end = _parse_iso(job.completed_at) or _parse_iso(job.failed_at)
    if not start or not end:
        return None
    return max(0.0, (end - start).total_seconds())


def _hourly_rate_usd(gpu_type: str, preemptible: bool, machine_type: str = "") -> float:
    """Total $/hr the project actually pays for one VM.

    Azure: NC* SKUs bundle the GPU into a single line item, so we read
    AZURE_VM_HOURLY_RATE_USD directly and skip the GPU+bundle sum.

    GCP: bills the GPU SKU and the A2/N1/G2 Core+Ram SKUs separately, so
    summing both yields the line-item total a user sees in Cloud Billing.
    Falls back to GPU_TYPE_TO_MACHINE_TYPE to look up the bundle when
    machine_type wasn't recorded on the Job.
    """
    if machine_type and machine_type.startswith("Standard_"):
        if machine_type not in AZURE_VM_HOURLY_RATE_USD:
            raise KeyError(f"Azure machine type has no configured rate: {machine_type}")
        on_demand, spot = AZURE_VM_HOURLY_RATE_USD[machine_type]
        return spot if preemptible else on_demand
    if gpu_type not in GPU_HOURLY_RATE_USD:
        raise KeyError(f"GPU type has no configured rate: {gpu_type}")
    gpu = GPU_HOURLY_RATE_USD[gpu_type]
    if preemptible:
        if gpu_type not in SPOT_DISCOUNT:
            raise KeyError(f"GPU type has no configured spot discount: {gpu_type}")
        gpu *= SPOT_DISCOUNT[gpu_type]
    if machine_type:
        mt = machine_type
    else:
        if gpu_type not in GPU_TYPE_TO_MACHINE_TYPE:
            raise KeyError(f"GPU type has no configured machine type: {gpu_type}")
        mt = GPU_TYPE_TO_MACHINE_TYPE[gpu_type]
    if mt not in VM_BUNDLE_HOURLY_RATE_USD:
        raise KeyError(f"Machine type has no configured bundle rate: {mt}")
    bundle_pair = VM_BUNDLE_HOURLY_RATE_USD[mt]
    bundle = bundle_pair[1] if preemptible else bundle_pair[0]
    return gpu + bundle


def _target_kind(job: Job) -> str:
    """local | gcp | azure | aws | unknown.

    instance_ref shape (`name@zone-or-location`) doesn't disambiguate cloud
    providers, so we trust job.provider when set and fall back to the
    "anything-but-local@ -> gcp" heuristic for older records that predate
    multi-provider support.
    """
    ref = job.instance_ref or ""
    if ref.startswith("local@"):
        return "local"
    provider = (getattr(job, "provider", "") or "").strip()
    if provider in ("azure", "aws", "gcp"):
        return provider
    if ref:
        return "gcp"
    return "unknown"


def _model_from_command(cmd: str) -> str:
    """Best-effort: extract --model 'X' out of the command line."""
    if "--model" not in cmd:
        return ""
    parts = cmd.split("--model", 1)[1].lstrip()
    if parts.startswith(("'", '"')):
        quote = parts[0]
        parts = parts[1:]
        if quote in parts:
            return parts.split(quote, 1)[0]
    return parts.split()[0] if parts.split() else ""


def wall_time_table(rows: list[dict]) -> dict[tuple[str, str], float]:
    """Median observed wall_time_seconds keyed by (model, gpu_type)."""
    buckets: dict[tuple[str, str], list[float]] = {}
    for r in rows:
        key = (r["model"] or UNKNOWN_MODEL, r["gpu_type"])
        buckets.setdefault(key, []).append(float(r["wall_s"]))
    out: dict[tuple[str, str], float] = {}
    for key, walls in buckets.items():
        walls.sort()
        out[key] = walls[len(walls) // 2]
    return out


def heuristic_wall_time_seconds(gpu_mem_gb: int) -> float:
    """Used when no completed-job data exists for a (model, gpu_type) pair.

    Derived from cdacc255 phase data: 50s startup + 7 strategies, each strategy
    spending ~80s on layer upload plus extract time scaling with model size.
    """
    per_strategy = BASE_SECONDS_PER_STRATEGY + max(0.0, gpu_mem_gb * SECONDS_PER_GB_PER_STRATEGY)
    return STARTUP_SECONDS + STRATEGY_COUNT * per_strategy


def estimate_wall_time(job_command: str, gpu_type: str, gpu_mem_gb: int,
                       table: dict[tuple[str, str], float]) -> float:
    """Median observed wall-time for this (model, gpu_type) when available."""
    model = _model_from_command(job_command) or UNKNOWN_MODEL
    key = (model, gpu_type)
    val = table[key] if key in table else None
    if val and val > 0:
        return val
    return heuristic_wall_time_seconds(gpu_mem_gb)


def collect_completed(store: JobStorage) -> list[dict]:
    """One entry per finished job with wall-time + cost attribution."""
    rows: list[dict] = []
    for state in FINISHED_JOB_STATES:
        for job in store.list_jobs(state):
            wall = _wall_seconds(job)
            if wall is None:
                continue
            rate = _hourly_rate_usd(
                job.gpu_type,
                getattr(job, "preemptible", False),
                getattr(job, "machine_type", "") or "",
            )
            cost = (wall / SECONDS_PER_HOUR) * rate
            rows.append({
                "job_id": job.job_id,
                "state": state,
                "gpu_type": job.gpu_type or "cpu",
                "preemptible": bool(getattr(job, "preemptible", False)),
                "wall_s": wall,
                "rate_usd_hr": rate,
                "cost_usd": cost,
                "target_kind": _target_kind(job),
                "model": _model_from_command(job.command),
            })
    return rows


def report(store: JobStorage | None = None) -> dict:
    """Aggregate finished-job rows into per-bucket summaries."""
    store = store or JobStorage(BUCKET)
    rows = collect_completed(store)
    by_target: dict[str, dict] = {}
    by_model: dict[str, dict] = {}
    total_cost = 0.0
    total_wall = 0.0
    for r in rows:
        for table, key in ((by_target, r[TARGET_KIND_KEY]), (by_model, r[MODEL_KEY] or UNKNOWN_MODEL)):
            bucket = table.setdefault(key, {"jobs": 0, "wall_s": 0.0, "cost_usd": 0.0})
            bucket["jobs"] += 1
            bucket["wall_s"] += r["wall_s"]
            bucket["cost_usd"] += r["cost_usd"]
        total_cost += r["cost_usd"]
        total_wall += r["wall_s"]
    return {
        "rows": rows,
        "by_target": by_target,
        "by_model": by_model,
        "total_jobs": len(rows),
        "total_cost_usd": total_cost,
        "total_wall_s": total_wall,
    }


def project_batch(batch_path: Path, store: JobStorage | None = None) -> dict:
    """Project total cost for a batch file, using observed per-job cost."""
    store = store or JobStorage(BUCKET)
    rep = report(store)
    rows = rep["rows"]
    if not rows:
        return {"jobs_in_batch": 0, "samples": 0, "projected_cost_usd": None,
                "reason": "no completed jobs to base projection on"}
    avg = rep["total_cost_usd"] / len(rows)
    n = sum(1 for line in batch_path.read_text().splitlines()
            if line.strip() and not line.startswith("#"))
    return {
        "jobs_in_batch": n,
        "samples": len(rows),
        "avg_cost_usd_per_job": avg,
        "projected_cost_usd": avg * n,
        "by_model": rep["by_model"],
    }


def format_report(rep: dict) -> Iterable[str]:
    yield f"jobs_with_walltime: {rep['total_jobs']}"
    yield f"total_cost_usd:     ${rep['total_cost_usd']:.4f}"
    yield f"total_wall_hours:   {rep['total_wall_s']/SECONDS_PER_HOUR:.2f}"
    yield ""
    yield "by target_kind:"
    for k, v in sorted(rep["by_target"].items()):
        yield COST_ROW_FORMAT.format(
            target=k,
            jobs=v["jobs"],
            wall_hours=v["wall_s"] / SECONDS_PER_HOUR,
            cost_usd=v["cost_usd"],
        )
    yield ""
    yield "by model:"
    for k, v in sorted(rep["by_model"].items()):
        yield f"  {k:<48} jobs={v['jobs']:<5} cost=${v['cost_usd']:.4f}"
