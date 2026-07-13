"""Read-only HTTP dashboard for the wisent-compute queue.

Renders one HTML page covering queue counts, per-model breakdown, live
agent capacity, recent failures, and a throughput-based completion
projection. Exposed via the wc dashboard CLI command; in production runs
as the com.wisent.compute.dashboard LaunchAgent on the mac mini.

GET /                  - HTML overview (auto-refresh)
GET /api/state.json    - same data as JSON

Summary/parsing helpers live in dashboard_summary/. This module holds
the HTTP plumbing, refresh loop, and HTML rendering only.
"""
from __future__ import annotations

import json
import sys
import threading
import time
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any

from .config import (
    BUCKET,
    DASHBOARD_BIND,
    DASHBOARD_PORT,
    DASHBOARD_REFRESH_SECONDS,
)
from .dashboard_summary import _fast_counts, _summarize
from .queue.storage import JobStorage


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


# Background refresh state — populated by a daemon thread and read by
# the request handlers. The slow path (_summarize) downloads every job
# blob, so it never runs inline with a request; we serve the last
# cached snapshot and refresh in the background.
_CACHE_LOCK = threading.Lock()
_CACHE_STATE: dict[str, Any] = {
    "ready": False,
    "now": None,
    "bucket": BUCKET,
    "counts": {"queue": 0, "running": 0, "completed": 0, "failed": 0},
    "by_model_state": {},
    "live_agents": [],
    "stale_agents": [],
    "recent_failed": [],
    "completed_recent": [],
    "throughput": {
        "avg_wall_seconds_per_completed_job": None,
        "samples": 0,
        "live_total_free_slots": 0,
        "projected_remaining_seconds": None,
    },
    "last_refresh_seconds": None,
}


def _refresh_loop(interval: int) -> None:
    """Daemon: refresh the cache every <interval> seconds.

    Refresh failures crash the loop; the daemon dies and the HTTP
    handler keeps serving the last good cached snapshot until the
    operator restarts the dashboard. Previously a broad exception
    handler logged-and-continued, silently producing a stale dashboard
    indefinitely.
    """
    while True:
        store = JobStorage(BUCKET)
        t0 = time.time()
        counts = _fast_counts(store)
        with _CACHE_LOCK:
            _CACHE_STATE["counts"].update(counts)
            _CACHE_STATE["now"] = datetime.now(timezone.utc).isoformat()
            _CACHE_STATE["ready"] = True
        full = _summarize(store)
        with _CACHE_LOCK:
            _CACHE_STATE.update(full)
            _CACHE_STATE["last_refresh_seconds"] = time.time() - t0
            _CACHE_STATE["ready"] = True
        time.sleep(interval)


class _Handler(BaseHTTPRequestHandler):
    def do_GET(self):  # noqa: N802 (stdlib API)
        # HTTP 500 on internal error is the documented error contract for
        # an HTTP handler; this except surfaces the failure to the client
        # rather than hanging the connection.
        try:
            with _CACHE_LOCK:
                state = dict(_CACHE_STATE)
            if self.path == "/api/state.json":
                body = json.dumps(state, default=str).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return
            if self.path in ("/", "/index.html"):
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
    refresher = threading.Thread(
        target=_refresh_loop, args=(DASHBOARD_REFRESH_SECONDS,),
        daemon=True, name="dashboard-refresh")
    refresher.start()
    server = HTTPServer((h, p), _Handler)
    sys.stderr.write(f"[dashboard] listening on http://{h}:{p}\n")
    sys.stderr.flush()
    server.serve_forever()
