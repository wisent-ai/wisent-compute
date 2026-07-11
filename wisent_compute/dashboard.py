"""HTTP operator dashboard for the wisent-compute queue and disk cleanup.

GET /                  - HTML overview (auto-refresh)
GET /api/state.json    - queue dashboard data as JSON
GET /api/cleanup.json  - sanitized current cleanup state
POST /api/cleanup/run  - one parameterless registry-controlled cleanup pass

Summary and rendering helpers live in dashboard_summary/. This module holds
only the HTTP plumbing and refresh loop.
"""
from __future__ import annotations

from ipaddress import ip_address
import json
import sys
import threading
import time
from datetime import datetime, timezone
from urllib.parse import urlsplit
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

from .config import (
    BUCKET,
    DASHBOARD_BIND,
    DASHBOARD_PORT,
    DASHBOARD_REFRESH_SECONDS,
)
from .dashboard_summary import _fast_counts, _summarize
from .dashboard_summary.web_view import cleanup_envelope, render_html
from .providers.local.disk import (
    read_cleanup_state,
    run_cleanup_once,
    sanitize_cleanup_report,
)
from .queue.storage import JobStorage




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




def _trusted_request_host(value: str | None) -> bool:
    """Require a direct IP Host header to prevent DNS rebinding."""
    if not value:
        return False
    try:
        parsed = urlsplit(f"//{value}")
        host = parsed.hostname
        _ = parsed.port
        if not host or parsed.username or parsed.password or parsed.path or parsed.query or parsed.fragment:
            return False
        ip_address(host)
    except ValueError:
        return False
    return True


class _Handler(BaseHTTPRequestHandler):
    def _send_json(self, status: int, payload: dict[str, Any]) -> None:
        body = json.dumps(payload, default=str, sort_keys=True, separators=(",", ":")).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _cleanup_failure(self, status: int = 500) -> None:
        report = sanitize_cleanup_report({"outcome": "invalid_or_unavailable_policy"})
        self._send_json(status, {"ok": False, "service": "error", "report": report})

    def do_GET(self):  # noqa: N802 (stdlib API)
        if not _trusted_request_host(self.headers.get("Host")):
            self._cleanup_failure(403)
            return
        try:
            with _CACHE_LOCK:
                state = dict(_CACHE_STATE)
            if self.path == "/api/state.json":
                self._send_json(200, state)
                return
            if self.path == "/api/cleanup.json":
                payload = cleanup_envelope(read_cleanup_state())
                self._send_json(409 if payload["service"] == "busy" else 200, payload)
                return
            if self.path in ("/", "/index.html"):
                cleanup = cleanup_envelope(read_cleanup_state())
                body = render_html(state, cleanup, DASHBOARD_REFRESH_SECONDS).encode()
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return
            self.send_response(404)
            self.end_headers()
        except Exception:
            if self.path == "/api/cleanup.json":
                self._cleanup_failure()
                return
            body = b"dashboard error"
            self.send_response(500)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    def do_POST(self):  # noqa: N802 (stdlib API)
        if not _trusted_request_host(self.headers.get("Host")):
            self._cleanup_failure(403)
            return
        if self.path != "/api/cleanup/run":
            if self.path.partition("?")[0] == "/api/cleanup/run":
                self._cleanup_failure(400)
            else:
                self.send_response(404)
                self.end_headers()
            return
        try:
            if self.headers.get("X-Stado-Action") != "cleanup":
                self._cleanup_failure(403)
                return
            try:
                content_length = int(self.headers.get("Content-Length", "0"))
            except (TypeError, ValueError):
                self._cleanup_failure(400)
                return
            if content_length != 0 or self.headers.get("Transfer-Encoding"):
                self._cleanup_failure(400)
                return
            report = sanitize_cleanup_report(run_cleanup_once(active_slot_count=0, force=True))
            payload = cleanup_envelope(report)
            self._send_json(409 if payload["service"] == "busy" else 200, payload)
        except Exception:
            self._cleanup_failure()

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
    server = ThreadingHTTPServer((h, p), _Handler)
    sys.stderr.write(f"[dashboard] listening on http://{h}:{p}\n")
    sys.stderr.flush()
    server.serve_forever()
