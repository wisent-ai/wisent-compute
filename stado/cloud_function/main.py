"""Cloud Function entry point. Triggered every 3 min by Cloud Scheduler."""
from __future__ import annotations

import os
import sys
from google.cloud import pubsub_v1, secretmanager_v1

from stado.config import PROJECT, BUCKET, ALERTS_TOPIC, WC_PROVIDERS
from stado.queue.storage import JobStorage
from stado.providers import get_provider
from stado.monitor import (
    check_running_jobs,
    reap_dead_agents,
    collect_billing,
)
from stado.scheduler import schedule_queued_jobs
from stado.scheduler.makespan import assign_jobs
from stado.sizing import normalize_queue_sizing

_publisher = None
_secrets = None


def _log(msg):
    sys.stderr.write(f"[tick] {msg}\n")
    sys.stderr.flush()


def _load_secrets():
    global _secrets
    if _secrets is not None:
        return _secrets
    client = secretmanager_v1.SecretManagerServiceClient()
    _secrets = {}
    for name in ("wisent-hf-token", "wisent-gh-token"):
        r = client.access_secret_version(request={
            "name": f"projects/{PROJECT}/secrets/{name}/versions/latest"
        })
        key = name.replace("wisent-", "").replace("-", "_").upper()
        _secrets[key] = r.payload.data.decode("utf-8")
    return _secrets


def monitor_jobs(request=None):
    """Main tick: assign queued jobs to agents (makespan-minimizing matcher),
    then per provider in WC_PROVIDERS check running + reap + schedule.

    The assign step writes job.assigned_to on every queue blob so the
    agent-side _job_eligible can refuse jobs pinned to a different agent.
    Runs FIRST so subsequent schedule_queued_jobs sees the up-to-date
    routing decisions. Empty live-agent set is a no-op.
    """
    global _publisher
    _log("Tick started")

    store = JobStorage(BUCKET)
    if _publisher is None:
        _publisher = pubsub_v1.PublisherClient()

    # Fire due recurring (cron) schedules FIRST so any job submitted this
    # tick flows through the sizing -> assign -> dispatch passes below
    # instead of waiting a full Cloud Scheduler interval. This is the
    # DEPLOYED prod path; coordinator._run_tick mirrors it for the
    # `wc coordinator` daemon.
    from stado.schedules import fire_due_schedules
    n_fired = fire_due_schedules(store, log_fn=_log)
    if n_fired:
        _log(f"schedules: fired {n_fired} due schedule(s)")

    # Coordinator-authoritative sizing, BEFORE assignment. A pre-0.4.237
    # agent that requeues a job writes the OLD hardcoded
    # estimate_gpu_memory value (gpt-oss-20b 64/12/80); makespan's
    # assigned_to-only write then preserves it, so the queue keeps
    # re-accumulating hardcoded sizes until fleet-wide drift. This pass
    # forces unmeasured-model queue blobs back to gpu_mem_gb=0 (measured
    # -> observed peak) every tick so the stale value is corrected within
    # one tick. This is the deployed GCP coordinator path; coordinator.
    # _run_tick (the `wc coordinator` daemon) calls the same function.
    n_sized = normalize_queue_sizing(store, log_fn=_log)
    if n_sized:
        _log(f"sizing: corrected {n_sized} stale queue gpu_mem_gb values")

    n_assigned = assign_jobs(store, log_fn=_log)
    if n_assigned:
        _log(f"makespan: rewrote assigned_to on {n_assigned} queue blobs")

    secrets = _load_secrets()
    total_reaped = 0
    total_scheduled = 0
    for name in WC_PROVIDERS:
        if name in {"box", "box-ascii"}:
            try:
                from stado.scheduler.dispatch.box import run_box_tick
                provider = get_provider(name)
                owner = os.environ.get("WC_COORDINATOR_ID", "gcp-cloud-function")
                total_scheduled += run_box_tick(store, provider, owner)
            except Exception as exc:
                _log(f"provider {name} tick failed: {type(exc).__name__}: {exc}")
            continue
        provider = get_provider(name)
        check_running_jobs(store, provider, _publisher)
        total_reaped += reap_dead_agents(store, provider, kind=name)
        total_scheduled += schedule_queued_jobs(store, provider, name, secrets)
    _log(
        f"Tick done: reaped {total_reaped} dead-agent VMs, "
        f"scheduled {total_scheduled} (providers={WC_PROVIDERS})"
    )

    # Billing-credits collector. Global (not per-provider), runs last and is
    # fully fault-isolated: collect_billing internally captures each source's
    # exact error into the JSON, and this guard ensures even a hard failure
    # in the collector path logs the precise cause WITHOUT aborting the
    # dispatch tick that the drain depends on.
    try:
        collect_billing(store)
    except Exception as e:
        import traceback
        _log(f"billing collector failed: {type(e).__name__}: {e}\n"
             f"{traceback.format_exc()}")

    return "OK"
