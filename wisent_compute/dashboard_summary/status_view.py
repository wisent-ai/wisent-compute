"""GCS status view for the wc CLI.

Lives next to _fast_counts (in this package's __init__) so the cheap
counts path can reuse it without an extra hop. The dominant `wc status`
usage is "no filter, show counts" which previously triggered a download
of every JSON body in queue/running/completed/failed prefixes (~4000 GCS
GETs once failed/ accumulated thousands of historical entries). The
no-filter branch here calls _fast_counts (4 list calls, zero downloads).
"""
from __future__ import annotations

import re

import click

from wisent_compute.config import BUCKET
from wisent_compute.queue.storage import JobStorage


JOB_ID_RE = re.compile(r"^[0-9a-f]{8}$")
STATES = ("running", "queue", "completed", "failed")


def _print_job_row(job, state: str) -> None:
    cmd_one_line = " ".join(job.command.split())
    cmd = cmd_one_line[:42] + "..." if len(cmd_one_line) > 42 else cmd_one_line
    submitted_by = getattr(job, "submitted_by", "") or "?"
    submitted_from = (getattr(job, "submitted_from", "") or "")[:12]
    who = f"{submitted_by}@{submitted_from}"[:22]
    click.echo(
        f"{job.job_id:<12} {state:<10} {job.gpu_type or 'cpu':<18} {who:<22} {cmd}"
    )


def status_gcs(filter_id: str | None) -> None:
    """Render `wc status` via direct GCS reads (no API key path)."""
    store = JobStorage(BUCKET)
    click.echo(
        f"{'JOB ID':<12} {'STATE':<10} {'GPU':<18} {'SUBMITTED_BY':<22} {'COMMAND'}"
    )
    click.echo("-" * 110)

    # 8-char hex job_id: 4 parallel direct reads, no listing.
    if filter_id and JOB_ID_RE.match(filter_id):
        from concurrent.futures import ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=4) as pool:
            results = list(pool.map(
                lambda s: (s, store.read_job(s, filter_id)), STATES
            ))
        found = [(s, j) for s, j in results if j is not None]
        for state, job in found:
            _print_job_row(job, state)
        if not found:
            click.echo(f"(no job with id {filter_id})")
        return

    # No filter: counts-only via the dashboard's _fast_counts helper.
    # 4 GCS list calls, zero blob downloads. Replaces a ~4000-GET scan.
    if not filter_id:
        from wisent_compute.dashboard_summary import _fast_counts
        c = _fast_counts(store)
        click.echo(
            f"\n{c.get('running', 0)} running, "
            f"{c.get('queue', 0)} queued, "
            f"{c.get('completed', 0)} completed, "
            f"{c.get('failed', 0)} failed"
        )
        return

    # batch_id filter: batch_id lives inside the JSON body so we must scan.
    all_jobs = store.list_all_jobs()
    for state in STATES:
        for job in all_jobs[state]:
            if filter_id not in (job.job_id, job.batch_id):
                continue
            _print_job_row(job, state)
    counts = {k: len(v) for k, v in all_jobs.items()}
    click.echo(
        f"\n{counts['running']} running, {counts['queue']} queued, "
        f"{counts['completed']} completed, {counts['failed']} failed"
    )


if __name__ == "__main__":
    import sys
    status_gcs(sys.argv[1] if len(sys.argv) > 1 else None)
