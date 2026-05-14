"""Dashboard summary/parsing helpers extracted from dashboard.py.

dashboard.py kept a single 399-line module that mixed HTTP serving with
fleet summary generation. Splitting the summary helpers out lets the
parent file fit under the 300-line cap, which is what unblocks removing
the broad `except Exception: pass` blocks that previously absorbed
corrupt-blob JSON decodes and slot-count int parses. Each removed
silent-except now raises so the dashboard surfaces ingest errors
instead of quietly under-reporting fleet state.
"""
from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from typing import Any

from .. import config as _config
from ..queue.storage import JobStorage


_MODEL_RE = re.compile(r"--model\s+['\"]?([^'\"\s]+)")
_TASK_RE = re.compile(r"--task\s+(\S+)")


def _parse_iso(ts: str | None) -> datetime | None:
    if not ts:
        return None
    return datetime.fromisoformat(ts.replace("Z", "+00:00"))


def _wall_seconds(job) -> float | None:
    s = _parse_iso(getattr(job, "started_at", None))
    e = _parse_iso(getattr(job, "completed_at", None)) or \
        _parse_iso(getattr(job, "failed_at", None))
    if s is None or e is None:
        return None
    return max(0.0, (e - s).total_seconds())


def _model_of(cmd: str) -> str:
    m = _MODEL_RE.search(cmd or "")
    return m.group(1) if m else "(unknown)"


def _task_of(cmd: str) -> str:
    m = _TASK_RE.search(cmd or "")
    return m.group(1) if m else "(unknown)"


def _read_capacity_blobs(store: JobStorage) -> list[dict]:
    """Return parsed capacity/<consumer>.json blobs (most recent first)."""
    if store._sdk_bucket is None:
        return []
    blobs: list[dict] = []
    for blob in store._sdk_bucket.list_blobs(prefix="capacity/"):
        if not blob.name.endswith(".json"):
            continue
        data = json.loads(blob.download_as_text())
        data["_blob_name"] = blob.name
        data["_blob_updated"] = blob.updated.isoformat() if blob.updated else None
        blobs.append(data)
    blobs.sort(key=lambda d: d.get("published_at") or "", reverse=True)
    return blobs


def _fast_counts(store: JobStorage) -> dict[str, int]:
    """Count blobs per state prefix without downloading job JSONs.

    Used by the cheap-render path so the dashboard /api/state.json can
    return SOMETHING while the full per-job summary is still building.
    """
    out: dict[str, int] = {}
    for prefix in ("queue", "running", "completed", "failed"):
        paths = store._list_paths(f"{prefix}/")
        out[prefix] = sum(1 for p in paths if p.endswith(".json"))
    return out


def _summarize(store: JobStorage) -> dict[str, Any]:
    all_jobs = store.list_all_jobs()
    counts = {k: len(v) for k, v in all_jobs.items()}

    by_model_state: dict[str, dict[str, int]] = {}
    recent_failed: list[dict] = []
    completed_walls: list[float] = []
    completed_recent: list[dict] = []
    for state, jobs in all_jobs.items():
        for job in jobs:
            model = _model_of(job.command or "")
            row = by_model_state.setdefault(model,
                {"queue": 0, "running": 0, "completed": 0, "failed": 0})
            if state in row:
                row[state] += 1
            if state == "completed":
                w = _wall_seconds(job)
                if w is not None:
                    completed_walls.append(w)
                if len(completed_recent) < 200:
                    completed_recent.append({
                        "job_id": job.job_id,
                        "model": model,
                        "task": _task_of(job.command or ""),
                        "wall_seconds": w,
                        "completed_at": getattr(job, "completed_at", None),
                    })
            elif state == "failed" and len(recent_failed) < 30:
                recent_failed.append({
                    "job_id": job.job_id,
                    "model": model,
                    "task": _task_of(job.command or ""),
                    "error": (getattr(job, "error", None) or "")[:240],
                })

    completed_recent.sort(
        key=lambda r: r.get("completed_at") or "", reverse=True)

    capacity = _read_capacity_blobs(store)
    now = datetime.now(timezone.utc)
    fresh_cutoff_seconds = float(_config.DASHBOARD_AGENT_FRESH_SECONDS)
    live_agents: list[dict] = []
    stale_agents: list[dict] = []
    for c in capacity:
        published = _parse_iso(c.get("published_at"))
        age = (now - published).total_seconds() if published else None
        entry = {
            "consumer_id": c.get("consumer_id"),
            "kind": c.get("kind"),
            "free_slots": c.get("free_slots") or {},
            "free_vram_gb": c.get("free_vram_gb"),
            "total_vram_gb": c.get("total_vram_gb"),
            "published_at": c.get("published_at"),
            "age_seconds": age,
            "diag": c.get("diag") or {},
        }
        if age is not None and age <= fresh_cutoff_seconds:
            live_agents.append(entry)
        else:
            stale_agents.append(entry)

    # Throughput-based projection: median wall * queue_depth / live worker
    # parallelism. If we have no live agents, projection is None.
    avg_wall = (sum(completed_walls) / len(completed_walls)
                if completed_walls else None)
    live_slots = 0
    for a in live_agents:
        for n in (a.get("free_slots") or {}).values():
            live_slots += int(n)
    queue_depth = counts.get("queue", 0)
    projected_remaining_seconds: float | None = None
    if avg_wall and live_slots > 0:
        projected_remaining_seconds = avg_wall * queue_depth / live_slots

    return {
        "now": now.isoformat(),
        "bucket": store.bucket_name,
        "counts": counts,
        "by_model_state": by_model_state,
        "live_agents": live_agents,
        "stale_agents": stale_agents,
        "recent_failed": recent_failed[:30],
        "completed_recent": completed_recent[:30],
        "throughput": {
            "avg_wall_seconds_per_completed_job": avg_wall,
            "samples": len(completed_walls),
            "live_total_free_slots": live_slots,
            "projected_remaining_seconds": projected_remaining_seconds,
        },
    }
