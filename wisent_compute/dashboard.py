"""Read-only HTTP dashboard for the wisent-compute queue.

Renders one HTML page covering queue counts, per-model breakdown, live
agent capacity, recent failures, and a throughput-based completion
projection. Exposed via the wc dashboard CLI command; in production runs
as the com.wisent.compute.dashboard LaunchAgent on the mac mini.

GET /                  - HTML overview (auto-refresh)
GET /api/state.json    - same data as JSON
"""
from __future__ import annotations

import json
import re
import sys
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any

from .config import (
    BUCKET,
    DASHBOARD_AGENT_FRESH_SECONDS,
    DASHBOARD_BIND,
    DASHBOARD_PORT,
    DASHBOARD_REFRESH_SECONDS,
)
from .queue.storage import JobStorage


_MODEL_RE = re.compile(r"--model\s+['\"]?([^'\"\s]+)")
_TASK_RE = re.compile(r"--task\s+(\S+)")


def _parse_iso(ts: str | None) -> datetime | None:
    if not ts:
        return None
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except Exception:
        return None


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
    return m.group(1) if m else ""


def _read_capacity_blobs(store: JobStorage) -> list[dict]:
    """Return parsed capacity/<consumer>.json blobs (most recent first)."""
    if store._sdk_bucket is None:
        return []
    blobs: list[dict] = []
    for blob in store._sdk_bucket.list_blobs(prefix="capacity/"):
        if not blob.name.endswith(".json"):
            continue
        try:
            data = json.loads(blob.download_as_text())
        except Exception:
            continue
        data["_blob_name"] = blob.name
        data["_blob_updated"] = blob.updated.isoformat() if blob.updated else None
        blobs.append(data)
    blobs.sort(key=lambda d: d.get("published_at") or "", reverse=True)
    return blobs


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
    fresh_cutoff_seconds = float(DASHBOARD_AGENT_FRESH_SECONDS)
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
            try:
                live_slots += int(n)
            except Exception:
                pass
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


def _format_age(s: float | None) -> str:
    if s is None:
        return "?"
    if s < 60:
        return f"{int(s)}s"
    if s < 3600:
        return f"{int(s/60)}m{int(s%60)}s"
    return f"{int(s/3600)}h{int((s%3600)/60)}m"


def _render_html(state: dict[str, Any]) -> str:
    refresh = DASHBOARD_REFRESH_SECONDS
    counts = state["counts"]
    th = state["throughput"]
    avg = th.get("avg_wall_seconds_per_completed_job")
    proj = th.get("projected_remaining_seconds")

    rows_models = []
    for model, st in sorted(state["by_model_state"].items(),
                            key=lambda kv: -sum(kv[1].values())):
        rows_models.append(
            f"<tr><td>{model}</td><td>{st['queue']}</td>"
            f"<td>{st['running']}</td><td>{st['completed']}</td>"
            f"<td>{st['failed']}</td></tr>")
    rows_live_agents = []
    for a in state["live_agents"]:
        slots = ", ".join(f"{k}:{v}" for k, v in a["free_slots"].items())
        rows_live_agents.append(
            f"<tr><td>{a['consumer_id']}</td><td>{a['kind']}</td>"
            f"<td>{slots}</td>"
            f"<td>{a.get('free_vram_gb','?')}/"
            f"{a.get('total_vram_gb','?')}</td>"
            f"<td>{_format_age(a.get('age_seconds'))} ago</td></tr>")
    rows_recent_failed = []
    for r in state["recent_failed"]:
        rows_recent_failed.append(
            f"<tr><td>{r['job_id']}</td><td>{r['model']}</td>"
            f"<td>{r['task']}</td><td>{r['error']}</td></tr>")
    rows_recent_completed = []
    for r in state["completed_recent"]:
        wall = _format_age(r.get("wall_seconds"))
        rows_recent_completed.append(
            f"<tr><td>{r['job_id']}</td><td>{r['model']}</td>"
            f"<td>{r['task']}</td><td>{wall}</td>"
            f"<td>{r.get('completed_at') or '?'}</td></tr>")

    return f"""<!doctype html>
<html><head><meta charset="utf-8">
<meta http-equiv="refresh" content="{refresh}">
<title>wisent-compute dashboard</title>
<style>
body{{font-family:-apple-system,system-ui,sans-serif;margin:1em;color:#222}}
h2{{margin-top:1.2em;border-bottom:1px solid #ddd;padding-bottom:.3em}}
table{{border-collapse:collapse;font-size:13px;margin:.4em 0}}
th,td{{border:1px solid #ddd;padding:4px 8px;text-align:left;vertical-align:top}}
th{{background:#f5f5f5}}
.big{{font-size:24px;font-weight:600}}
.muted{{color:#666;font-size:12px}}
.warn{{color:#a00}}
</style></head><body>
<h1>wisent-compute</h1>
<div class="muted">bucket gs://{state['bucket']}/ &middot; refreshed every {refresh}s &middot; now {state['now']}</div>

<h2>queue</h2>
<div class="big">
queued <strong>{counts.get('queue',0)}</strong> &nbsp;
running <strong>{counts.get('running',0)}</strong> &nbsp;
completed <strong>{counts.get('completed',0)}</strong> &nbsp;
failed <strong>{counts.get('failed',0)}</strong>
</div>

<h2>throughput &amp; ETA</h2>
<div>avg wall per completed job: <strong>{_format_age(avg)}</strong>
({th.get('samples',0)} samples)</div>
<div>live free slots across all agents: <strong>{th.get('live_total_free_slots',0)}</strong></div>
<div>projected drain of current queue: <strong>{_format_age(proj)}</strong></div>

<h2>per model</h2>
<table><tr><th>model</th><th>queued</th><th>running</th>
<th>completed</th><th>failed</th></tr>
{''.join(rows_models) or '<tr><td colspan=5 class=muted>no jobs</td></tr>'}
</table>

<h2>live agents</h2>
<table><tr><th>consumer_id</th><th>kind</th><th>free_slots</th>
<th>free/total vram (GB)</th><th>last heartbeat</th></tr>
{''.join(rows_live_agents) or '<tr><td colspan=5 class=warn>no live agents</td></tr>'}
</table>
<div class="muted">stale agents (heartbeat older than threshold): {len(state['stale_agents'])}</div>

<h2>recent failed</h2>
<table><tr><th>job_id</th><th>model</th><th>task</th><th>error</th></tr>
{''.join(rows_recent_failed) or '<tr><td colspan=4 class=muted>none recent</td></tr>'}
</table>

<h2>recent completed</h2>
<table><tr><th>job_id</th><th>model</th><th>task</th>
<th>wall</th><th>completed_at</th></tr>
{''.join(rows_recent_completed) or '<tr><td colspan=5 class=muted>none recent</td></tr>'}
</table>
</body></html>"""


class _Handler(BaseHTTPRequestHandler):
    def _store(self) -> JobStorage:
        return JobStorage(BUCKET)

    def do_GET(self):  # noqa: N802 (stdlib API)
        try:
            if self.path == "/api/state.json":
                state = _summarize(self._store())
                body = json.dumps(state, default=str).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return
            if self.path in ("/", "/index.html"):
                state = _summarize(self._store())
                body = _render_html(state).encode()
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return
            self.send_response(404)
            self.end_headers()
        except Exception as exc:
            body = f"dashboard error: {exc!r}".encode()
            self.send_response(500)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    def log_message(self, fmt, *args):  # noqa: N802
        sys.stderr.write("[dashboard] " + (fmt % args) + "\n")


def serve(host: str | None = None, port: int | None = None) -> None:
    """Run the dashboard HTTP server. Blocks until killed."""
    h = host or DASHBOARD_BIND
    p = port if port is not None else DASHBOARD_PORT
    server = HTTPServer((h, p), _Handler)
    sys.stderr.write(f"[dashboard] listening on http://{h}:{p}\n")
    sys.stderr.flush()
    server.serve_forever()
