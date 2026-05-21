"""`wc-fix` CLI: scan + dispatch autonomous failure fixes.

Subcommands:
  scan                              cluster recent failures by fingerprint
  prompt <fingerprint>              emit the Claude Code fix prompt to stdout
  dispatch <fingerprint> [--execute] HMAC-sign + POST to model-router
  scan-dispatch [--execute]         scan -> dispatch every new fingerprint

`--execute` is the explicit gate before any model-router POST runs.
"""
from __future__ import annotations

import json
import sys

import click

from . import (
    dispatch_fix,
    format_fix_prompt,
    group_by_fingerprint,
    scan_and_dispatch,
    scan_new_failures,
)
from ..config import BUCKET
from ..queue.storage import JobStorage


@click.group()
def main() -> None:
    """Autonomous failure-fixer: failure -> Claude Code -> ship fix -> retry."""


@main.command("scan")
@click.option("--since", default=None, help="ISO-8601 lower bound on failed_at")
def cmd_scan(since: str | None) -> None:
    """Cluster recent failed/<jid>.json blobs by fingerprint and print
    a per-cluster summary (count + sample command + sample timestamps)."""
    store = JobStorage(BUCKET)
    records = list(scan_new_failures(store, since_iso=since))
    groups = group_by_fingerprint(records)
    summary = [
        {
            "fingerprint": g.fingerprint,
            "count": g.count,
            "latest_failed_at": g.latest.failed_at,
            "sample_job_id": g.latest.job_id,
            "sample_command_head": g.latest.command[:160],
        }
        for g in sorted(groups.values(), key=lambda x: -x.count)
    ]
    click.echo(json.dumps({
        "total_failures_scanned": len(records),
        "unique_fingerprints": len(groups),
        "clusters": summary,
    }, indent=2))


@main.command("prompt")
@click.argument("fingerprint")
@click.option("--since", default=None)
def cmd_prompt(fingerprint: str, since: str | None) -> None:
    """Emit the Claude Code fix prompt for one fingerprint to stdout.
    No model-router POST; useful for inspection + manual `claude-code`
    invocation if you want a human in the loop."""
    store = JobStorage(BUCKET)
    records = list(scan_new_failures(store, since_iso=since))
    groups = group_by_fingerprint(records)
    g = groups.get(fingerprint)
    if g is None:
        click.echo(f"no fingerprint {fingerprint!r} in current failed/", err=True)
        sys.exit(1)
    click.echo(format_fix_prompt(g))


@main.command("dispatch")
@click.argument("fingerprint")
@click.option("--since", default=None)
@click.option("--execute", is_flag=True, default=False,
              help="Actually HMAC-sign + POST to model-router; default dry-run.")
def cmd_dispatch(fingerprint: str, since: str | None, execute: bool) -> None:
    """Dispatch a fix request for one fingerprint to Claude Code via
    model-router. Writes per-fingerprint state to
    gs://<bucket>/failure_fixes/<fingerprint>.json."""
    store = JobStorage(BUCKET)
    records = list(scan_new_failures(store, since_iso=since))
    groups = group_by_fingerprint(records)
    g = groups.get(fingerprint)
    if g is None:
        click.echo(f"no fingerprint {fingerprint!r} in current failed/", err=True)
        sys.exit(1)
    result = dispatch_fix(g, store=store, execute=execute)
    click.echo(json.dumps(result, indent=2))


@main.command("scan-dispatch")
@click.option("--since", default=None)
@click.option("--execute", is_flag=True, default=False,
              help="Actually dispatch each fingerprint; default dry-run.")
def cmd_scan_dispatch(since: str | None, execute: bool) -> None:
    """Scan failed/ -> group by fingerprint -> dispatch each new
    fingerprint. ATTEMPT_CAP gates so a never-fixable fingerprint
    stops re-dispatching after FAILURE_FIXER_ATTEMPT_CAP attempts."""
    results = scan_and_dispatch(since_iso=since, execute=execute)
    click.echo(json.dumps({"results": results, "count": len(results)}, indent=2))


if __name__ == "__main__":
    main()
