"""CLI entry point: wc submit, wc status, wc results, wc cancel."""
from __future__ import annotations

import os
import time
import click

from .config import BUCKET
from .queue.submit import submit_job
from .queue.storage import JobStorage


@click.group()
def main():
    """Wisent Compute — GPU job queue management."""


@main.command()
@click.argument("command")
@click.option("--provider", default="gcp")
@click.option("--batch", "batch_file", default=None, help="File with one command per line")
def submit(command, provider, batch_file):
    """Submit a job (or batch) to the queue."""
    commands = []
    if batch_file:
        with open(batch_file) as f:
            commands = [l.strip() for l in f if l.strip() and not l.startswith("#")]
    else:
        commands = [command]

    batch_id = f"batch-{int(time.time())}"
    for cmd in commands:
        job = submit_job(cmd, provider=provider, batch_id=batch_id, bucket=BUCKET)
        click.echo(f"  {job.job_id}  {job.gpu_type or 'cpu':>20s}  {cmd[:60]}")
    click.echo(f"\nSubmitted {len(commands)} job(s). Batch: {batch_id}")


@main.command()
@click.argument("filter_id", required=False)
def status(filter_id):
    """Show job status across all states."""
    store = JobStorage(BUCKET)
    all_jobs = store.list_all_jobs()

    click.echo(f"{'JOB ID':<12} {'STATE':<12} {'GPU':<20} {'RESTARTS':<10} {'COMMAND'}")
    click.echo("-" * 90)
    for state in ("running", "queue", "completed", "failed"):
        for job in all_jobs[state]:
            if filter_id and filter_id not in (job.job_id, job.batch_id):
                continue
            cmd = job.command[:50] + "..." if len(job.command) > 50 else job.command
            click.echo(f"{job.job_id:<12} {state:<12} {job.gpu_type or 'cpu':<20} {job.restarts:<10} {cmd}")

    counts = {k: len(v) for k, v in all_jobs.items()}
    click.echo(f"\n{counts['running']} running, {counts['queue']} queued, "
               f"{counts['completed']} completed, {counts['failed']} failed")


@main.command()
@click.argument("job_id")
@click.argument("output_dir")
def results(job_id, output_dir):
    """Download job results from GCS."""
    os.makedirs(output_dir, exist_ok=True)
    bucket = BUCKET
    os.system(f"gsutil -m cp -r 'gs://{bucket}/status/{job_id}/output/*' '{output_dir}/'")
    click.echo(f"Results downloaded to {output_dir}")


@main.command()
@click.argument("job_id")
def cancel(job_id):
    """Cancel a queued or running job."""
    store = JobStorage(BUCKET)
    job = store.read_job("queue", job_id)
    if job:
        store.delete_job("queue", job_id)
        click.echo(f"Removed {job_id} from queue")
        return
    job = store.read_job("running", job_id)
    if job and job.instance_ref:
        from .providers import get_provider
        prov = get_provider(job.provider)
        prov.delete_instance(job.instance_ref)
        job.state = "failed"
        job.error = "cancelled"
        store.move_job(job, "running", "failed")
        click.echo(f"Cancelled {job_id}, instance terminated")
        return
    click.echo(f"Job {job_id} not found in queue or running")


@main.command()
@click.option("--gpu-type", default="", help="GPU type (auto-detected if empty)")
def agent(gpu_type):
    """Run local GPU agent. Polls queue, respects Vast.ai renters."""
    from .providers.local_agent import run_agent
    run_agent(gpu_type=gpu_type)
