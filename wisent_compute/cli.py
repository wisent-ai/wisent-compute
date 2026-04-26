"""CLI entry point: wc submit, wc status, wc results, wc cancel, wc agent."""
from __future__ import annotations

import json
import os
import time
import urllib.request
import urllib.error
from pathlib import Path

import click

from .config import (
    BUCKET,
    DEFAULT_ANY_PROVIDER,
    DEFAULT_MAX_COST_PER_HOUR_USD,
    DEFAULT_PREEMPTIBLE,
    DEFAULT_PRIORITY,
)
from .queue.submit import submit_job, COMPUTE_API
from .queue.storage import JobStorage


def _api_key():
    return os.environ.get("COMPUTE_API_KEY", "").strip()


def _api_get(path):
    req = urllib.request.Request(
        f"{COMPUTE_API}{path}",
        headers={"X-API-Key": _api_key()},
    )
    resp = urllib.request.urlopen(req)
    return json.loads(resp.read())


@click.group()
def main():
    """Wisent Compute — GPU job queue management."""


@main.command()
@click.argument("command")
@click.option("--provider", default="gcp",
              help="Preferred provider (gcp/local). With --any-provider this is just a hint.")
@click.option("--batch", "batch_file", default=None, help="File with commands")
@click.option("--spot/--no-spot", default=DEFAULT_PREEMPTIBLE,
              help="Dispatch on Spot/Preemptible GPUs (cheaper, can be preempted).")
@click.option("--max-cost-per-hour", "max_cost_per_hour", type=float,
              default=DEFAULT_MAX_COST_PER_HOUR_USD,
              help="Hard cap on $/hour for the chosen accelerator. 0 = no cap.")
@click.option("--any-provider/--pin-provider", "any_provider",
              default=DEFAULT_ANY_PROVIDER,
              help="If true (default), any consumer with capacity can claim. "
                   "If --pin-provider, only the named --provider is allowed.")
@click.option("--priority", type=int, default=DEFAULT_PRIORITY,
              help="Higher = scheduled first within FIFO bucket.")
def submit(command, provider, batch_file, spot, max_cost_per_hour, any_provider, priority):
    """Submit a job (or batch) to the queue."""
    commands = []
    if batch_file:
        with open(batch_file) as f:
            commands = [l.strip() for l in f if l.strip() and not l.startswith("#")]
    else:
        commands = [command]

    batch_id = f"batch-{int(time.time())}"
    for cmd in commands:
        job = submit_job(
            cmd, provider=provider, batch_id=batch_id, bucket=BUCKET,
            preemptible=spot,
            max_cost_per_hour_usd=max_cost_per_hour,
            pin_to_provider=not any_provider,
            priority=priority,
        )
        click.echo(f"  {job.job_id}  {job.gpu_type or 'cpu':>20s}  {cmd[:60]}")
    mode = "API" if _api_key() else "GCS"
    flags = []
    if spot: flags.append("spot")
    if max_cost_per_hour > 0: flags.append(f"cap=${max_cost_per_hour:.2f}/hr")
    if not any_provider: flags.append(f"pinned={provider}")
    if priority: flags.append(f"priority={priority}")
    flag_str = (" [" + ", ".join(flags) + "]") if flags else ""
    click.echo(f"\nSubmitted {len(commands)} job(s) via {mode}{flag_str}. Batch: {batch_id}")


@main.command()
@click.argument("filter_id", required=False)
def status(filter_id):
    """Show job status."""
    if _api_key():
        _status_api(filter_id)
    else:
        _status_gcs(filter_id)


def _status_api(filter_id):
    instances = _api_get("/api/v1/instances")
    click.echo(f"{'ID':<38} {'STATUS':<12} {'IMAGE':<30} {'COST'}")
    click.echo("-" * 95)
    for inst in instances:
        iid = inst.get("id", "")[:36]
        st = inst.get("status", "")
        img = inst.get("docker_image", "")[:28]
        cost = inst.get("total_cost_cents", 0) / 100
        if filter_id and filter_id not in iid:
            continue
        click.echo(f"{iid:<38} {st:<12} {img:<30} ${cost:.2f}")
    click.echo(f"\n{len(instances)} instance(s)")


def _status_gcs(filter_id):
    store = JobStorage(BUCKET)
    all_jobs = store.list_all_jobs()
    click.echo(f"{'JOB ID':<12} {'STATE':<10} {'GPU':<18} {'SUBMITTED_BY':<22} {'COMMAND'}")
    click.echo("-" * 110)
    for state in ("running", "queue", "completed", "failed"):
        for job in all_jobs[state]:
            if filter_id and filter_id not in (job.job_id, job.batch_id):
                continue
            cmd = job.command[:42] + "..." if len(job.command) > 42 else job.command
            who = (f"{getattr(job, 'submitted_by', '') or '?'}@{(getattr(job, 'submitted_from', '') or '')[:12]}")[:22]
            click.echo(f"{job.job_id:<12} {state:<10} {job.gpu_type or 'cpu':<18} {who:<22} {cmd}")
    counts = {k: len(v) for k, v in all_jobs.items()}
    click.echo(f"\n{counts['running']} running, {counts['queue']} queued, "
               f"{counts['completed']} completed, {counts['failed']} failed")


@main.command()
@click.argument("job_id")
@click.argument("output_dir")
def results(job_id, output_dir):
    """Download job results."""
    os.makedirs(output_dir, exist_ok=True)
    os.system(f"gsutil -m cp -r 'gs://{BUCKET}/status/{job_id}/output/*' '{output_dir}/'")
    click.echo(f"Results downloaded to {output_dir}")


@main.command()
@click.argument("job_id")
def cancel(job_id):
    """Cancel a queued or running job."""
    if _api_key():
        req = urllib.request.Request(
            f"{COMPUTE_API}/api/v1/instances/{job_id}",
            headers={"X-API-Key": _api_key()},
            method="DELETE",
        )
        try:
            urllib.request.urlopen(req)
            click.echo(f"Cancelled {job_id}")
        except urllib.error.HTTPError as e:
            click.echo(f"Failed: {e.code}")
        return

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
    click.echo(f"Job {job_id} not found")


@main.command()
@click.option("--gpu-type", default="", help="GPU type (auto-detected if --target/--auto absent)")
@click.option("--target", default=None,
              help="Pull gpu_type and slot count from registry by name.")
@click.option("--auto", is_flag=True, default=False,
              help="Look up self in the GCS-hosted registry by hostname; no manual config.")
def agent(gpu_type, target, auto):
    """Run local GPU agent. Polls queue, respects Vast.ai renters.

    --auto looks up the local hostname in gs://wisent-compute/registry.json
    and uses that entry's slots/gpu_type. Re-fetches periodically so registry
    edits propagate without restarting the agent.
    """
    import os as _os
    if auto:
        from .targets import lookup_self
        t = lookup_self(_os.uname().nodename, source="auto")
        if not t:
            raise click.ClickException(f"hostname '{_os.uname().nodename}' not in registry")
        gpu_type = gpu_type or (t.gpu_type or "")
        _os.environ["WC_LOCAL_SLOTS"] = str(t.slots)
        for k, v in (t.env_overrides or {}).items():
            _os.environ[k] = str(v)
        click.echo(f"agent --auto: target={t.name} gpu_type={gpu_type} slots={t.slots}")
    elif target:
        from .targets import lookup
        t = lookup(target)
        if not t:
            raise click.ClickException(f"target '{target}' not found in registry")
        if t.kind != "local":
            raise click.ClickException(f"target '{target}' kind={t.kind}, expected local")
        gpu_type = gpu_type or (t.gpu_type or "")
        _os.environ["WC_LOCAL_SLOTS"] = str(t.slots)
        click.echo(f"agent: target={t.name} gpu_type={gpu_type} slots={t.slots}")
    from .providers.local_agent import run_agent
    run_agent(gpu_type=gpu_type)


@main.command()
@click.option("--target", default=None,
              help="Coordinator name in registry (default: the one with active=true).")
@click.option("--once", is_flag=True, default=False,
              help="Run a single scheduling tick and exit (cron-friendly).")
def coordinator(target, once):
    """Run the scheduling tick locally instead of the GCP Cloud Function.

    Reads the named coordinator entry from the registry, loops on its
    interval_seconds, runs the same monitor_jobs/schedule_queued_jobs
    chain the Cloud Function does. State stays in the registry-declared
    state_uri so all consumers (cloud + local) keep seeing the same queue.
    """
    from .coordinator import run as run_coordinator
    raise SystemExit(run_coordinator(target=target, once=once))


@main.group()
def cost():
    """Per-job and per-batch cost reporting from observed wall-times."""


@cost.command("report")
def cost_report():
    """Summarize $ spent per target_kind and per model from completed jobs."""
    from .scheduler.cost import format_report, report
    from .queue.storage import JobStorage
    rep = report(JobStorage(BUCKET))
    for line in format_report(rep):
        click.echo(line)


@cost.command("estimate")
@click.argument("batch_file", type=click.Path(exists=True, dir_okay=False))
def cost_estimate(batch_file):
    """Project total $ for a batch file using observed per-job cost."""
    from .scheduler.cost import project_batch
    from .queue.storage import JobStorage
    proj = project_batch(Path(batch_file), JobStorage(BUCKET))
    if proj["projected_cost_usd"] is None:
        click.echo(f"cannot project: {proj.get('reason', 'no data')}")
        return
    click.echo(f"jobs_in_batch:        {proj['jobs_in_batch']}")
    click.echo(f"samples:              {proj['samples']} completed jobs in queue history")
    click.echo(f"avg_cost_usd_per_job: ${proj['avg_cost_usd_per_job']:.4f}")
    click.echo(f"projected_cost_usd:   ${proj['projected_cost_usd']:.2f}")


@main.group()
def registry():
    """Manage the canonical compute-target registry hosted in GCS."""


@registry.command("push")
@click.argument("path", type=click.Path(exists=True, dir_okay=False), required=False)
def registry_push(path):
    """Upload local registry.json to gs://wisent-compute/registry.json."""
    import shutil, subprocess
    from .targets import REGISTRY_PATH, GCS_REGISTRY_URI
    src = path or str(REGISTRY_PATH)
    gsutil = shutil.which("gsutil") or "gsutil"
    r = subprocess.run([gsutil, "cp", src, GCS_REGISTRY_URI], capture_output=True, text=True)
    if r.returncode != 0:
        raise click.ClickException(r.stderr or r.stdout or "gsutil cp failed")
    click.echo(f"pushed {src} -> {GCS_REGISTRY_URI}")


@registry.command("pull")
def registry_pull():
    """Print the GCS-hosted registry to stdout."""
    from .targets import _load_from_gcs
    data = _load_from_gcs()
    if data is None:
        raise click.ClickException("could not fetch registry from GCS")
    import json as _json
    click.echo(_json.dumps(data, indent=2))


@main.command()
@click.option("--target", default=None, help="Specific entry name (target or coordinator).")
@click.option("--dry-run", is_flag=True, default=False, help="Print unit/plist; do not enable.")
@click.option("--local", "local_install", is_flag=True, default=False,
              help="Install on THIS machine (launchd/systemd --user) instead of via SSH.")
def bootstrap(target, dry_run, local_install):
    """Provision wisent-compute services persistently across reboots."""
    from .deploy.bootstrap import run_bootstrap
    run_bootstrap(target=target, dry_run=dry_run, local_install=local_install,
                  echo=click.echo)
