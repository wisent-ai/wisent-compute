"""CLI entry point: wc submit, wc status, wc results, wc cancel, wc agent."""
from __future__ import annotations

import json
import os
import re
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
              help="Preferred provider (gcp/azure/aws/local). With --any-provider this is just a hint.")
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
@click.option("--repo", default="", help="Optional git URL to clone before running command (no auth).")
@click.option("--repo-workdir", default="", help="Override cloned-repo dir; default = repo basename.")
@click.option("--repo-extras", default="train", help="pip extras to install on the clone; empty skips install.")
@click.option("--gpu-type", default="",
              help="Pin the accelerator label (e.g. 'nvidia-l4', 'nvidia-a100-80gb'). "
                   "Skips the --model regex inference. Resolves machine_type from "
                   "GPU_SIZING unless --machine-type is also passed.")
@click.option("--vram-gb", type=int, default=0,
              help="Caller-declared VRAM (GB). Picks the smallest SKU whose tier >= this value. "
                   "Skips the --model regex inference.")
@click.option("--machine-type", default="",
              help="Pin the GCE/Azure machine type verbatim (e.g. 'g2-standard-8'). "
                   "Use for SKUs not in the wisent-compute catalog.")
@click.option("--pre-command", "pre_command", default="",
              help="Shell snippet placed before the command in the SAME bash shell. "
                   "Use to export env vars (LD_LIBRARY_PATH, CUDA_VISIBLE_DEVICES, etc.) "
                   "that the command will see.")
@click.option("--apt", "apt_packages", default="",
              help="Comma-separated apt package list. Installed via sudo apt-get on "
                   "cloud-kind agents only — local-kind agents refuse the job for safety.")
@click.option("--output-uri", "output_uri", default="",
              help="Additional gs:// destination for job output. Additive — canonical "
                   "status/<id>/output/ path is always written too.")
@click.option("--verify", "verify_command", default="",
              help="Shell command that must exit 0 after the job succeeds; non-zero "
                   "reverses COMPLETED->FAILED. Catches silent-success failure modes.")
@click.option("--exclusive", is_flag=True, default=False,
              help="Claim the WHOLE GPU. Agent only claims this job on an "
                   "empty slot and refuses to admit any other job while it runs. "
                   "Use for diffusion training / full-finetunes whose peak VRAM "
                   "can't be safely co-tenanted.")
@click.option("--profile", "profile_name", default="",
              help="Apply a named profile from wisent_compute/profiles/ (or "
                   "$WC_PROFILES_DIR). CLI flags override profile fields. "
                   "Run `wcomp profiles` to list available profiles.")
def submit(command, provider, batch_file, spot, max_cost_per_hour, any_provider, priority,
           repo, repo_workdir, repo_extras,
           gpu_type, vram_gb, machine_type,
           pre_command, apt_packages, output_uri, verify_command,
           exclusive,
           profile_name):
    """Submit a job (or batch) to the queue."""
    apt_list = [p.strip() for p in apt_packages.split(",") if p.strip()]

    # Profile merge — CLI args win on conflict. The submit_job kwargs
    # dict is built from the Click values (which all have known defaults),
    # then merge_into_kwargs adopts profile fields wherever the CLI
    # value matches the wisent-compute default.
    if profile_name:
        from .profiles import load_profile, merge_into_kwargs
        try:
            prof = load_profile(profile_name)
        except FileNotFoundError as e:
            raise click.ClickException(str(e))
        cli_kwargs = {
            "gpu_type": gpu_type, "vram_gb": vram_gb, "machine_type": machine_type,
            "apt_packages": apt_list, "pre_command": pre_command,
            "repo": repo, "repo_workdir": repo_workdir, "repo_extras": repo_extras,
            "output_uri": output_uri, "verify_command": verify_command,
            "exclusive": exclusive,
            "priority": priority, "preemptible": spot,
            "max_cost_per_hour_usd": max_cost_per_hour,
            "provider": provider, "pin_to_provider": not any_provider,
        }
        merged = merge_into_kwargs(prof, cli_kwargs)
        gpu_type = merged["gpu_type"]
        vram_gb = merged["vram_gb"]
        machine_type = merged["machine_type"]
        apt_list = merged["apt_packages"]
        pre_command = merged["pre_command"]
        repo = merged["repo"]
        repo_workdir = merged["repo_workdir"]
        repo_extras = merged["repo_extras"]
        output_uri = merged["output_uri"]
        verify_command = merged["verify_command"]
        exclusive = merged["exclusive"]
        priority = merged["priority"]
        spot = merged["preemptible"]
        max_cost_per_hour = merged["max_cost_per_hour_usd"]
        provider = merged["provider"]
        any_provider = not merged["pin_to_provider"]
        click.echo(f"Profile '{profile_name}' applied: {prof.get('description', '')[:80]}")
    commands = []
    if batch_file:
        with open(batch_file) as f:
            commands = [l.strip() for l in f if l.strip() and not l.startswith("#")]
    else:
        commands = [command]
    batch_id = f"batch-{int(time.time())}"
    from .queue.submit import submit_batch
    n = submit_batch(
        commands, provider=provider, batch_id=batch_id, bucket=BUCKET,
        preemptible=spot, max_cost_per_hour_usd=max_cost_per_hour,
        pin_to_provider=not any_provider, priority=priority,
        repo=repo, repo_workdir=repo_workdir, repo_extras=repo_extras,
        gpu_type=gpu_type, vram_gb=vram_gb, machine_type=machine_type,
        pre_command=pre_command, apt_packages=apt_list,
        output_uri=output_uri, verify_command=verify_command,
        exclusive=exclusive,
    )
    click.echo(f"  submitted {n}/{len(commands)} jobs")
    mode = "API" if _api_key() else "GCS"
    flags = []
    if spot: flags.append("spot")
    if max_cost_per_hour > 0: flags.append(f"cap=${max_cost_per_hour:.2f}/hr")
    if not any_provider: flags.append(f"pinned={provider}")
    if priority: flags.append(f"priority={priority}")
    if gpu_type: flags.append(f"gpu={gpu_type}")
    if vram_gb: flags.append(f"vram={vram_gb}G")
    if machine_type: flags.append(f"mt={machine_type}")
    if apt_list: flags.append(f"apt={','.join(apt_list)}")
    if pre_command: flags.append("pre_cmd")
    if output_uri: flags.append(f"out={output_uri}")
    if verify_command: flags.append("verify")
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


_JOB_ID_RE = re.compile(r"^[0-9a-f]{8}$")
_STATES = ("running", "queue", "completed", "failed")


def _print_job_row(job, state):
    cmd_one_line = " ".join(job.command.split())
    cmd = cmd_one_line[:42] + "..." if len(cmd_one_line) > 42 else cmd_one_line
    who = (f"{getattr(job, 'submitted_by', '') or '?'}@{(getattr(job, 'submitted_from', '') or '')[:12]}")[:22]
    click.echo(f"{job.job_id:<12} {state:<10} {job.gpu_type or 'cpu':<18} {who:<22} {cmd}")


def _status_gcs(filter_id):
    store = JobStorage(BUCKET)
    click.echo(f"{'JOB ID':<12} {'STATE':<10} {'GPU':<18} {'SUBMITTED_BY':<22} {'COMMAND'}")
    click.echo("-" * 110)

    # Fast path: filter looks like a job_id — 4 parallel direct reads, no listing.
    if filter_id and _JOB_ID_RE.match(filter_id):
        from concurrent.futures import ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=4) as pool:
            results = list(pool.map(lambda s: (s, store.read_job(s, filter_id)), _STATES))
        found = [(s, j) for s, j in results if j is not None]
        for state, job in found:
            _print_job_row(job, state)
        if not found:
            click.echo(f"(no job with id {filter_id})")
        return

    # Slow path: no filter, or filter is a batch_id — must scan all blobs.
    all_jobs = store.list_all_jobs()
    for state in _STATES:
        for job in all_jobs[state]:
            if filter_id and filter_id not in (job.job_id, job.batch_id):
                continue
            _print_job_row(job, state)
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
@click.option("--target", default=None, help="Pull gpu_type/slots from registry by name.")
@click.option("--auto", is_flag=True, default=False,
              help="Look up self in registry by hostname; no manual config.")
@click.option("--idle-shutdown", is_flag=True, default=False,
              help="Exit (and self-delete the GCE VM) when no slots active and no "
                   "queued job is eligible. Use on ephemeral cloud-VM agents.")
@click.option("--kind", default="local",
              help='Consumer label in capacity broadcasts: "local" (physical box, '
                   'default), "gcp" / "azure" / "aws" / "vast" (ephemeral cloud-agent VM).')
def agent(gpu_type, target, auto, idle_shutdown, kind):
    """Run local GPU agent. Polls queue, respects Vast.ai renters."""
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
    run_agent(gpu_type=gpu_type, idle_shutdown=idle_shutdown, kind=kind)


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


@main.command()
@click.option("--bind", default=None,
              help="Bind address. Default WC_DASHBOARD_BIND or 127.0.0.1.")
@click.option("--port", type=int, default=None,
              help="Port. Default WC_DASHBOARD_PORT or 8765.")
def dashboard(bind, port):
    """Run the read-only HTTP dashboard for the wisent-compute queue.

    Renders queue counts, per-model breakdown, live agent capacity, recent
    failures, and a throughput-based completion projection at GET / with
    auto-refresh, and the same data as JSON at GET /api/state.json.
    """
    from .dashboard import serve as serve_dashboard
    serve_dashboard(host=bind, port=port)



@main.group(invoke_without_command=True)
@click.option("--json", "as_json", is_flag=True, default=False,
              help="Emit machine-readable JSON instead of the table (show subcommand).")
@click.pass_context
def quota(ctx, as_json):
    """GPU quota inspection and increase requests across WC_PROVIDERS.

    Default (no subcommand) is equivalent to `wc quota show` — prints
    live cloud quota minus reservation minus running per provider.
    Subcommands: `show` for the table, `request` to submit a quota-
    increase request via the cloud provider's Quotas API.
    """
    if ctx.invoked_subcommand is None:
        ctx.invoke(quota_show, as_json=as_json)


@quota.command("show")
@click.option("--json", "as_json", is_flag=True, default=False,
              help="Emit machine-readable JSON instead of the table.")
def quota_show(as_json):
    """Show GPU quota totals across all providers in WC_PROVIDERS."""
    from .scheduler.quota import summarize_quotas
    from .queue.storage import JobStorage
    summary = summarize_quotas(JobStorage(BUCKET))
    if as_json:
        click.echo(json.dumps(summary, indent=2, sort_keys=True))
        return
    click.echo(f"{'PROVIDER':<10} {'ACCEL':<22} {'TOTAL':>6} {'RESERVED':>9} {'USED':>5} {'AVAIL':>6}")
    click.echo("-" * 70)
    grand_total: dict[str, int] = {}
    grand_avail: dict[str, int] = {}
    for provider_name, rows in summary.items():
        if not rows:
            click.echo(f"{provider_name:<10} (no quota visible — credentials missing or SDK not installed)")
            continue
        for accel in sorted(rows.keys()):
            r = rows[accel]
            click.echo(f"{provider_name:<10} {accel:<22} {r['total']:>6} {r['reserved']:>9} {r['used']:>5} {r['available']:>6}")
            grand_total[accel] = grand_total.get(accel, 0) + r["total"]
            grand_avail[accel] = grand_avail.get(accel, 0) + r["available"]
    if len(summary) > 1 and grand_total:
        click.echo("-" * 70)
        for accel in sorted(grand_total.keys()):
            click.echo(f"{'TOTAL':<10} {accel:<22} {grand_total[accel]:>6} {'':>9} {'':>5} {grand_avail[accel]:>6}")


@quota.command("request")
@click.argument("accel")
@click.option("--to", "new_limit", type=int, required=True,
              help="New per-region quota limit to request (e.g. 16).")
@click.option("--region", "regions", default="",
              help="Comma-separated regions/locations; default = every region "
                   "the provider dispatches into (REGIONS / AZURE_LOCATIONS).")
@click.option("--provider", "providers_arg", default="",
              help="Comma-separated provider list (gcp,azure); default = WC_PROVIDERS.")
@click.option("--justification",
              default="wisent-compute autoscaler queue depth requires more parallel GPU capacity",
              help="Reviewer-visible justification text.")
@click.option("--email", "contact_email", default="",
              help="Contact email for the Cloud Quotas reviewer (required for GCP). "
                   "Default: $WC_QUOTA_CONTACT_EMAIL.")
@click.option("--json", "as_json", is_flag=True, default=False,
              help="Emit machine-readable JSON result list.")
def quota_request(accel, new_limit, regions, providers_arg, justification,
                  contact_email, as_json):
    """Submit GPU quota-increase request(s) for ACCEL via the provider Quotas API.

    Fans out one request per (provider, region) across every provider in
    --provider (default WC_PROVIDERS) and every region in --region
    (default: the provider's configured region/location list). Approval
    is asynchronous on the cloud provider's side; this command only
    submits the preference and returns the per-target result table.
    """
    import os as _os
    from .config import WC_PROVIDERS
    from .scheduler.dispatch.quota_request import request_quota_increases
    if not contact_email:
        contact_email = _os.environ.get("WC_QUOTA_CONTACT_EMAIL", "").strip()
    if not contact_email:
        raise click.ClickException(
            "--email is required (or set WC_QUOTA_CONTACT_EMAIL); the GCP "
            "Cloud Quotas API requires a contact email on every preference."
        )
    providers = [p.strip() for p in providers_arg.split(",") if p.strip()] \
        or WC_PROVIDERS
    region_list = [r.strip() for r in regions.split(",") if r.strip()] or None
    results = request_quota_increases(
        accel=accel, new_limit=new_limit, providers=providers,
        regions=region_list, justification=justification,
        contact_email=contact_email,
    )
    if as_json:
        click.echo(json.dumps(results, indent=2, sort_keys=True))
        return
    click.echo(f"{'PROVIDER':<8} {'REGION/LOC':<18} {'OK':<3} {'DETAIL'}")
    click.echo("-" * 80)
    for r in results:
        rkey = r.get("region") or r.get("location") or "-"
        ok = "Y" if r.get("ok") else "N"
        detail = r.get("name") if r.get("ok") else r.get("error", "?")
        click.echo(f"{r.get('provider', '?'):<8} {rkey:<18} {ok:<3} {detail}")
    ok_count = sum(1 for r in results if r.get("ok"))
    click.echo(f"\n{ok_count}/{len(results)} succeeded")


@quota.command("azure-replies")
@click.option("--dry-run", is_flag=True, default=False,
              help="Print what would be sent without invoking az "
                   "support communication create.")
@click.option("--email", "contact_email", default="",
              help="Contact email shown in the response signature. "
                   "Default: $WC_QUOTA_CONTACT_EMAIL.")
def quota_azure_replies(dry_run, contact_email):
    """Respond to Open Azure quota support tickets awaiting customer info.

    Scans the configured Azure subscription for Open quota-classification
    tickets, identifies ones whose most-recent communication came from
    Microsoft (no customer reply yet), and posts a single canonical
    reply per ticket answering Azure Capacity CX's standard five
    questions. Region is parsed from the ticket title. Requires az CLI
    on PATH with an active Azure auth (same prerequisite as az support
    in-subscription tickets list).
    """
    import os as _os
    from .scheduler.dispatch.quota_replies import respond_to_open_quota_tickets
    if not contact_email:
        contact_email = _os.environ.get("WC_QUOTA_CONTACT_EMAIL", "").strip()
    if not contact_email:
        raise click.ClickException(
            "--email is required (or set WC_QUOTA_CONTACT_EMAIL); the "
            "reply body signs off with the customer contact email."
        )
    results = respond_to_open_quota_tickets(
        contact_email=contact_email, dry_run=dry_run,
    )
    if not results:
        click.echo("(no Open Azure quota tickets requiring reply)")
        return
    click.echo(f"{'TICKET':<46} {'REGION':<22} {'OK':<3} {'ACTION'}")
    click.echo("-" * 92)
    for r in results:
        click.echo(
            f"{r.get('name', '?')[:44]:<46} "
            f"{r.get('region', '-')[:20]:<22} "
            f"{'Y' if r.get('ok') else 'N':<3} "
            f"{r.get('action', '?')}{' — ' + r.get('error', '') if r.get('error') else ''}"
        )
    ok_count = sum(1 for r in results if r.get("ok"))
    click.echo(f"\n{ok_count}/{len(results)} tickets processed")


@quota.command("catalog")
@click.option("--provider", "providers_arg", default="",
              help="Comma-separated provider list (gcp,azure); default = WC_PROVIDERS.")
@click.option("--json", "as_json", is_flag=True, default=False,
              help="Emit machine-readable JSON.")
def quota_catalog(providers_arg, as_json):
    """List the full GPU catalog for each provider in WC_PROVIDERS.

    GCP: every GPU-related quota under compute.googleapis.com with its
    current per-region limit on file.
    Azure: every GPU VM family (NC/ND/NV/GPU) the subscription
    advertises with the locations each family is available in.
    """
    from .config import WC_PROVIDERS
    from .scheduler.dispatch.quota_skus import all_catalogs
    providers = [p.strip() for p in providers_arg.split(",") if p.strip()] \
        or WC_PROVIDERS
    cats = all_catalogs(providers)
    if as_json:
        click.echo(json.dumps(cats, indent=2, sort_keys=True, default=str))
        return
    for provider, rows in cats.items():
        click.echo(f"\n=== {provider} ({len(rows)} rows) ===")
        if not rows:
            click.echo("  (empty)")
            continue
        if any(r.get("ok") is False for r in rows):
            for r in rows:
                if r.get("ok") is False:
                    click.echo(f"  ERROR: {r.get('error', '?')}")
            continue
        if provider == "gcp":
            click.echo(f"  {'QUOTA_ID':<52} {'FAMILY':<20} {'REGION':<16} {'LIMIT':>6}")
            for r in sorted(rows, key=lambda x: (x.get("quota_id", ""), x.get("region", ""))):
                lim = r.get("limit")
                lim_str = str(lim) if lim is not None else "-"
                click.echo(
                    f"  {(r.get('quota_id', '?') or '?')[:50]:<52} "
                    f"{(r.get('gpu_family') or '-')[:18]:<20} "
                    f"{(r.get('region') or '-')[:14]:<16} "
                    f"{lim_str:>6}"
                )
        elif provider == "azure":
            seen_fam = {}
            for r in rows:
                fam = r.get("family", "")
                loc = r.get("location", "")
                seen_fam.setdefault(fam, set()).add(loc)
            click.echo(f"  {'FAMILY':<36} {'LOCATIONS'}")
            for fam in sorted(seen_fam):
                locs = sorted(seen_fam[fam])
                click.echo(f"  {fam[:34]:<36} {len(locs)} ({', '.join(locs[:5])}{', …' if len(locs) > 5 else ''})")


@quota.command("request-all")
@click.option("--to", "new_limit", type=int, required=True,
              help="New per-region quota limit to request for every GPU family.")
@click.option("--provider", "providers_arg", default="",
              help="Comma-separated provider list (gcp,azure); default = WC_PROVIDERS.")
@click.option("--region", "regions_arg", default="",
              help="Comma-separated regions/locations; default = the provider's "
                   "configured REGIONS / AZURE_LOCATIONS.")
@click.option("--justification",
              default="wisent-compute autoscaler bulk capacity request: provision GPU headroom "
                      "across every supported family in the dispatch regions so the "
                      "scheduler can fall through to whichever family Google/Azure can serve.",
              help="Reviewer-visible justification text.")
@click.option("--email", "contact_email", default="",
              help="Contact email for the GCP Cloud Quotas reviewer. "
                   "Default: $WC_QUOTA_CONTACT_EMAIL.")
@click.option("--json", "as_json", is_flag=True, default=False,
              help="Emit machine-readable JSON result list.")
def quota_request_all(new_limit, providers_arg, regions_arg, justification,
                      contact_email, as_json):
    """Submit quota-increase requests for EVERY known GPU family on each provider.

    For GCP this iterates the project's known cloudquotas gpu_family
    values (T4, L4, A100, A100_80GB, H100, H100_MEGA, H200, B200,
    V100, P100, P4, K80) across the configured REGIONS, firing one
    QuotaPreference per (region, family). For Azure it iterates every
    GPU VM family the subscription advertises (NC/ND/NV) across the
    configured AZURE_LOCATIONS.
    """
    import os as _os
    from .config import WC_PROVIDERS, REGIONS, AZURE_LOCATIONS
    from .scheduler.dispatch.quota_skus import (
        gcp_request_all_families, azure_request_all_families,
    )
    providers = [p.strip() for p in providers_arg.split(",") if p.strip()] \
        or WC_PROVIDERS
    explicit_regions = [r.strip() for r in regions_arg.split(",") if r.strip()]
    if not contact_email:
        contact_email = _os.environ.get("WC_QUOTA_CONTACT_EMAIL", "").strip()
    if not contact_email and "gcp" in providers:
        raise click.ClickException(
            "--email is required for GCP (or set WC_QUOTA_CONTACT_EMAIL); "
            "the Cloud Quotas API mandates a contact email on every preference."
        )
    results: list[dict] = []
    for p in providers:
        if p == "gcp":
            regions = explicit_regions or REGIONS
            results.extend(gcp_request_all_families(
                new_limit=new_limit, regions=regions,
                contact_email=contact_email, justification=justification,
            ))
        elif p == "azure":
            locs = explicit_regions or AZURE_LOCATIONS
            results.extend(azure_request_all_families(
                new_limit=new_limit, locations=locs,
            ))
        else:
            results.append({"provider": p, "ok": False,
                            "error": "no request-all impl for this provider"})
    if as_json:
        click.echo(json.dumps(results, indent=2, sort_keys=True))
        return
    click.echo(f"{'PROVIDER':<8} {'REGION/LOC':<18} {'FAMILY':<22} {'OK':<3} {'DETAIL'}")
    click.echo("-" * 100)
    for r in results:
        rkey = r.get("region") or r.get("location") or "-"
        fam = r.get("gpu_family") or r.get("family") or "-"
        ok = "Y" if r.get("ok") else "N"
        detail = r.get("name") if r.get("ok") else r.get("error", "?")
        click.echo(
            f"{r.get('provider', '?'):<8} {rkey[:16]:<18} {fam[:20]:<22} "
            f"{ok:<3} {str(detail)[:60]}"
        )
    ok_count = sum(1 for r in results if r.get("ok"))
    click.echo(f"\n{ok_count}/{len(results)} requests submitted")

@main.command()
@click.argument("name", required=False)
def profiles(name):
    """List available submit profiles, or show one profile's JSON."""
    from .profiles import list_profiles, load_profile
    if name:
        try:
            p = load_profile(name)
        except FileNotFoundError as e:
            raise click.ClickException(str(e))
        click.echo(json.dumps(p, indent=2))
        return
    names = list_profiles()
    if not names:
        click.echo("(no profiles found)")
        return
    for n in names:
        try:
            p = load_profile(n)
            desc = (p.get("description") or "").split(".")[0][:90]
            click.echo(f"{n:<24} {desc}")
        except Exception as e:
            click.echo(f"{n:<24} (load error: {e})")


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
