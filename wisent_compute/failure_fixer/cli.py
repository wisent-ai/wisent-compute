"""`wc-fix` CLI: scan + dispatch one Claude Code session per failed job.

Subcommands:
  scan                          list recent failed jobs that have not been dispatched
  prompt <job_id>               emit the Claude Code fix prompt for one job to stdout
  dispatch <job_id> [--execute] HMAC-sign + POST one job's fix request
  scan-dispatch [--execute]     scan -> dispatch every undispatched job

`--execute` is the explicit gate before any model-router POST runs.
"""
from __future__ import annotations

import json
import sys

import click

from . import (
    dispatch_fix,
    format_fix_prompt,
    scan_and_dispatch,
    scan_new_failures,
    state_load,
)
from ..config import BUCKET
from ..queue.storage import JobStorage


@click.group()
def main() -> None:
    """Autonomous failure-fixer: failure -> Claude Code -> ship fix -> retry."""


@main.command("scan")
@click.option("--since", default=None, help="ISO-8601 lower bound on failed_at")
def cmd_scan(since: str | None) -> None:
    """List recent failed jobs and their dispatch state."""
    store = JobStorage(BUCKET)
    records = list(scan_new_failures(store, since_iso=since))
    summary = []
    for rec in records:
        st = state_load(store, rec.job_id)
        summary.append({
            "job_id": rec.job_id,
            "batch_id": rec.batch_id,
            "failed_at": rec.failed_at,
            "attempts": st.get("attempts", 0),
            "command_head": rec.command[:160],
        })
    click.echo(json.dumps({
        "total_failures_scanned": len(records),
        "undispatched": sum(1 for s in summary if s["attempts"] == 0),
        "already_dispatched": sum(1 for s in summary if s["attempts"] > 0),
        "jobs": summary,
    }, indent=2))


@main.command("prompt")
@click.argument("job_id")
@click.option("--since", default=None)
def cmd_prompt(job_id: str, since: str | None) -> None:
    """Emit the Claude Code fix prompt for one failed job to stdout."""
    store = JobStorage(BUCKET)
    for rec in scan_new_failures(store, since_iso=since):
        if rec.job_id == job_id:
            click.echo(format_fix_prompt(rec))
            return
    click.echo(f"no failed job {job_id!r} in current failed/", err=True)
    sys.exit(1)


@main.command("dispatch")
@click.argument("job_id")
@click.option("--since", default=None)
@click.option("--execute", is_flag=True, default=False,
              help="Actually HMAC-sign + POST to model-router; default dry-run.")
def cmd_dispatch(job_id: str, since: str | None, execute: bool) -> None:
    """Dispatch a fix request for ONE failed job to Claude Code via
    model-router. Writes per-job state to
    gs://<bucket>/failure_fixes/<job_id>.json."""
    store = JobStorage(BUCKET)
    for rec in scan_new_failures(store, since_iso=since):
        if rec.job_id == job_id:
            result = dispatch_fix(rec, store=store, execute=execute)
            click.echo(json.dumps(result, indent=2))
            return
    click.echo(f"no failed job {job_id!r} in current failed/", err=True)
    sys.exit(1)


@main.command("scan-dispatch")
@click.option("--since", default=None)
@click.option("--execute", is_flag=True, default=False,
              help="Actually dispatch each undispatched failed job; default dry-run.")
def cmd_scan_dispatch(since: str | None, execute: bool) -> None:
    """Scan failed/ and dispatch one Claude Code session per undispatched
    job. Per-job ATTEMPT_CAP stops re-dispatching after
    FAILURE_FIXER_ATTEMPT_CAP attempts."""
    results = scan_and_dispatch(since_iso=since, execute=execute)
    click.echo(json.dumps({"results": results, "count": len(results)}, indent=2))


if __name__ == "__main__":
    main()
