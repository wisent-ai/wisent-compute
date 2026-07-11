"""Escaped HTML rendering and cleanup API presentation for the dashboard."""
from __future__ import annotations

from html import escape
from typing import Any


def _e(value: object) -> str:
    return escape(str(value), quote=True)


def _format_age(seconds: object) -> str:
    if isinstance(seconds, bool) or not isinstance(seconds, (int, float)):
        return "?"
    if seconds < 60:
        return f"{max(0, int(seconds))}s"
    if seconds < 3600:
        return f"{int(seconds / 60)}m{int(seconds % 60)}s"
    return f"{int(seconds / 3600)}h{int((seconds % 3600) / 60)}m"


def _bytes(value: object) -> str:
    if isinstance(value, bool) or not isinstance(value, int):
        return "unknown"
    gib = value / (1024 ** 3)
    return f"{gib:.1f} GiB"


def cleanup_envelope(report: dict[str, Any]) -> dict[str, Any]:
    busy = report.get("lock_busy") is True or report.get("outcome") == "lock_busy"
    return {"ok": not busy, "service": "busy" if busy else "ready", "report": report}


def _cleanup_totals(report: dict[str, Any]) -> dict[str, int]:
    totals = {"eligible": 0, "deleted": 0, "reclaimed": 0, "skips": 0}
    cleaners = report.get("cleaners")
    if not isinstance(cleaners, dict):
        return totals
    for cleaner in cleaners.values():
        if not isinstance(cleaner, dict):
            continue
        totals["eligible"] += int(cleaner.get("eligible_items") or 0)
        totals["deleted"] += int(cleaner.get("deleted_items") or 0)
        totals["reclaimed"] += int(cleaner.get("actual_free_delta_bytes") or 0)
        skipped = cleaner.get("skipped")
        if isinstance(skipped, dict):
            totals["skips"] += sum(int(value or 0) for value in skipped.values())
    return totals


def _cleanup_card(cleanup: dict[str, Any]) -> str:
    report = cleanup.get("report") if isinstance(cleanup.get("report"), dict) else {}
    totals = _cleanup_totals(report)
    caps = report.get("caps") if isinstance(report.get("caps"), dict) else {}
    active_caps = ", ".join(name for name in ("bytes", "items", "scan", "deadline")
                            if caps.get(name) is True) or "none"
    pressure = report.get("pressure_active")
    pressure_text = "active" if pressure is True else "clear" if pressure is False else "unknown"
    error_count = len(report.get("errors")) if isinstance(report.get("errors"), list) else 0
    return f"""
<section class="cleanup-card" aria-labelledby="cleanup-title">
  <div class="cleanup-heading">
    <div><h2 id="cleanup-title">Stado Disk Cleanup</h2>
      <p class="muted">Registry-controlled local cleanup; no custom paths or parameters.</p></div>
    <div class="controls">
      <button type="button" id="cleanup-refresh">Refresh</button>
      <button type="button" id="cleanup-run">Run cleanup</button>
    </div>
  </div>
  <p id="cleanup-status" class="service-status" role="status" aria-live="polite">
    Service {_e(cleanup.get('service', 'unknown'))}; outcome {_e(report.get('outcome', 'never_run'))}.
  </p>
  <dl class="cleanup-grid">
    <div><dt>Mode / outcome</dt><dd>{_e(report.get('mode') or 'not configured')} / {_e(report.get('outcome') or 'never_run')}</dd></div>
    <div><dt>Free / low / target</dt><dd>{_e(_bytes(report.get('free_bytes_after')))} / {_e(_bytes(report.get('low_bytes')))} / {_e(_bytes(report.get('target_bytes')))}</dd></div>
    <div><dt>Pressure</dt><dd>{_e(pressure_text)}</dd></div>
    <div><dt>Last success</dt><dd>{_e(report.get('last_success_at') or 'none')}</dd></div>
    <div><dt>Caps reached</dt><dd>{_e(active_caps)}</dd></div>
    <div><dt>Eligible / deleted</dt><dd>{_e(totals['eligible'])} / {_e(totals['deleted'])}</dd></div>
    <div><dt>Reclaimed</dt><dd>{_e(_bytes(totals['reclaimed']))}</dd></div>
    <div><dt>Skips / errors</dt><dd>{_e(totals['skips'])} / {_e(error_count)}</dd></div>
  </dl>
</section>"""


def render_html(state: dict[str, Any], cleanup: dict[str, Any], refresh: int) -> str:
    counts = state.get("counts") if isinstance(state.get("counts"), dict) else {}
    throughput = state.get("throughput") if isinstance(state.get("throughput"), dict) else {}
    model_rows = []
    models = state.get("by_model_state") if isinstance(state.get("by_model_state"), dict) else {}
    for model, model_state in sorted(models.items(), key=lambda item: -sum(item[1].values())):
        model_rows.append("<tr>" + "".join(
            f"<td>{_e(value)}</td>" for value in (
                model, model_state.get("queue", 0), model_state.get("running", 0),
                model_state.get("completed", 0), model_state.get("failed", 0))) + "</tr>")

    agent_rows = []
    agents = state.get("live_agents") if isinstance(state.get("live_agents"), list) else []
    for agent in agents:
        slots = ", ".join(f"{key}:{value}" for key, value in agent.get("free_slots", {}).items())
        values = (agent.get("consumer_id"), agent.get("kind"), slots,
                  f"{agent.get('free_vram_gb', '?')}/{agent.get('total_vram_gb', '?')}",
                  f"{_format_age(agent.get('age_seconds'))} ago")
        agent_rows.append("<tr>" + "".join(f"<td>{_e(value)}</td>" for value in values) + "</tr>")

    failed_rows = []
    for row in state.get("recent_failed", []):
        failed_rows.append("<tr>" + "".join(f"<td>{_e(value)}</td>" for value in
            (row.get("job_id"), row.get("model"), row.get("task"), "details withheld")) + "</tr>")
    completed_rows = []
    for row in state.get("completed_recent", []):
        completed_rows.append("<tr>" + "".join(f"<td>{_e(value)}</td>" for value in
            (row.get("job_id"), row.get("model"), row.get("task"),
             _format_age(row.get("wall_seconds")), row.get("completed_at") or "?")) + "</tr>")

    return f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Stado Control Center</title>
<style>
:root{{color-scheme:light dark}}body{{font-family:-apple-system,system-ui,sans-serif;margin:1em;color:#222;background:#fff}}
h2{{margin-top:1.2em;border-bottom:1px solid #ddd;padding-bottom:.3em}}table{{border-collapse:collapse;font-size:13px;margin:.4em 0;max-width:100%;display:block;overflow-x:auto}}
th,td{{border:1px solid #ddd;padding:4px 8px;text-align:left;vertical-align:top}}th{{background:#f5f5f5}}
.big{{font-size:24px;font-weight:600}}.muted{{color:#666;font-size:12px}}.warn{{color:#a00}}
.cleanup-card{{margin:1.2em 0;padding:1em;border:1px solid #ccc;border-radius:12px;background:#fafafa}}.cleanup-heading{{display:flex;gap:1em;align-items:flex-start;justify-content:space-between}}.cleanup-heading h2{{margin:0;border:0}}.cleanup-heading p{{margin:.3em 0}}.controls{{display:flex;gap:.5em;flex-wrap:wrap}}button{{font:inherit;padding:.5em .8em;cursor:pointer}}button:disabled{{cursor:wait;opacity:.6}}.service-status{{font-weight:600}}.cleanup-grid{{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:.8em;margin:1em 0 0}}.cleanup-grid div{{min-width:0}}dt{{font-size:12px;color:#666}}dd{{margin:.2em 0 0;font-weight:600;overflow-wrap:anywhere}}
@media(max-width:800px){{.cleanup-grid{{grid-template-columns:repeat(2,minmax(0,1fr))}}}}@media(max-width:480px){{.cleanup-heading{{display:block}}.controls{{margin-top:.8em}}.cleanup-grid{{grid-template-columns:1fr}}}}
@media(prefers-color-scheme:dark){{body{{color:#eee;background:#111}}th{{background:#222}}.cleanup-card{{background:#181818;border-color:#444}}.muted,dt{{color:#aaa}}}}
</style></head><body>
<h1>Stado Control Center</h1><div class="muted">refreshed every {_e(refresh)}s &middot; now {_e(state.get('now') or '?')}</div>
{_cleanup_card(cleanup)}
<h2>queue</h2><div class="big">queued <strong>{_e(counts.get('queue', 0))}</strong> &nbsp; running <strong>{_e(counts.get('running', 0))}</strong> &nbsp; completed <strong>{_e(counts.get('completed', 0))}</strong> &nbsp; failed <strong>{_e(counts.get('failed', 0))}</strong></div>
<h2>throughput &amp; ETA</h2><div>avg wall per completed job: <strong>{_e(_format_age(throughput.get('avg_wall_seconds_per_completed_job')))}</strong> ({_e(throughput.get('samples', 0))} samples)</div><div>live free slots across all agents: <strong>{_e(throughput.get('live_total_free_slots', 0))}</strong></div><div>projected drain of current queue: <strong>{_e(_format_age(throughput.get('projected_remaining_seconds')))}</strong></div>
<h2>per model</h2><table><tr><th>model</th><th>queued</th><th>running</th><th>completed</th><th>failed</th></tr>{''.join(model_rows) or '<tr><td colspan="5" class="muted">no jobs</td></tr>'}</table>
<h2>live agents</h2><table><tr><th>consumer_id</th><th>kind</th><th>free_slots</th><th>free/total vram (GB)</th><th>last heartbeat</th></tr>{''.join(agent_rows) or '<tr><td colspan="5" class="warn">no live agents</td></tr>'}</table><div class="muted">stale agents (heartbeat older than threshold): {_e(len(state.get('stale_agents', [])))}</div>
<h2>recent failed</h2><table><tr><th>job_id</th><th>model</th><th>task</th><th>error</th></tr>{''.join(failed_rows) or '<tr><td colspan="4" class="muted">none recent</td></tr>'}</table>
<h2>recent completed</h2><table><tr><th>job_id</th><th>model</th><th>task</th><th>wall</th><th>completed_at</th></tr>{''.join(completed_rows) or '<tr><td colspan="5" class="muted">none recent</td></tr>'}</table>
<script>
(() => {{
  const status = document.getElementById('cleanup-status');
  const buttons = [document.getElementById('cleanup-refresh'), document.getElementById('cleanup-run')];
  let autoRefresh = window.setTimeout(() => window.location.reload(), {_e(max(1, refresh) * 1000)});
  async function request(url, options) {{
    window.clearTimeout(autoRefresh);
    buttons.forEach(button => button.disabled = true); status.textContent = options ? 'Cleanup requested; waiting for completion.' : 'Refreshing cleanup status.';
    try {{
      const response = await fetch(url, options); const payload = await response.json();
      const report = payload.report || {{}}; status.textContent = `Service ${{payload.service || 'unknown'}}; outcome ${{report.outcome || 'unknown'}}.`;
      if (response.ok) window.location.reload();
    }} catch (_) {{ status.textContent = 'Cleanup service request failed safely.'; }}
    finally {{
      buttons.forEach(button => button.disabled = false);
      autoRefresh = window.setTimeout(() => window.location.reload(), {_e(max(1, refresh) * 1000)});
    }}
  }}
  buttons[0].addEventListener('click', () => request('/api/cleanup.json'));
  buttons[1].addEventListener('click', () => request('/api/cleanup/run', {{method:'POST', headers:{{'X-Stado-Action':'cleanup'}}}}));
}})();
</script></body></html>"""
