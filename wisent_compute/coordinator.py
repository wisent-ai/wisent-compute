"""Coordinator daemon - the same scheduling tick the GCP Cloud Function runs,
as a long-lived local process so the system can run without GCP CF / Cloud
Scheduler.

Reads the named coordinator entry from the registry (default: the one whose
active=true), constructs the same JobStorage + GCP provider + scheduler call
chain monitor_jobs uses, and loops on the configured interval_seconds.

--once runs a single tick and exits (cron-driven runtimes).

State stays in the registry-declared state_uri (currently always GCS), so
swapping coordinator from GCF to a daemon on the Mac doesn't change which
queue the agents see.
"""
from __future__ import annotations

import os
import sys
import time
import traceback
from typing import Optional

from .config import BUCKET, WC_PROVIDERS
from .monitor import check_running_jobs, reap_dead_agents
from .providers import get_provider
from .queue.storage import JobStorage
from .scheduler import schedule_queued_jobs
from .targets import Coordinator, load_coordinators, lookup_coordinator


def _log(msg: str) -> None:
    sys.stderr.write(f"[tick] {msg}\n")
    sys.stderr.flush()


def _resolve_coordinator(target: Optional[str]) -> Coordinator:
    """Pick the coordinator entry: explicit --target, or the active one."""
    if target:
        c = lookup_coordinator(target)
        if c is None:
            raise SystemExit(f"coordinator '{target}' not found in registry")
        return c
    active = [c for c in load_coordinators() if c.active]
    if not active:
        raise SystemExit(
            "no active coordinator in registry. Set active=true on one entry "
            "or pass --target NAME explicitly."
        )
    if len(active) > 1:
        names = ", ".join(c.name for c in active)
        raise SystemExit(f"multiple active coordinators ({names}); set active=true on exactly one")
    return active[0]


def _bucket_from_state_uri(state_uri: str) -> str:
    """Strip 'gs://' prefix to get the bucket name JobStorage expects."""
    if state_uri.startswith("gs://"):
        return state_uri[len("gs://"):].split("/", 1)[0]
    return state_uri


def _run_tick(store: JobStorage, secrets: dict) -> int:
    """One scheduling cycle across every provider in WC_PROVIDERS.

    Each provider gets its own check_running_jobs + schedule_queued_jobs
    pass; the queue is shared (state lives in JobStorage), so a
    `pin_to_provider` job lands wherever its provider field points and an
    unpinned job is offered to whichever provider claims first. A
    constructor failure (e.g. AzureProvider when AZURE_SUBSCRIPTION_ID is
    empty) is logged and skipped so a misconfigured fallback provider
    never blocks the primary one.
    """
    total = 0
    for name in WC_PROVIDERS:
        try:
            provider = get_provider(name)
        except Exception as exc:
            _log(f"skip {name}: {exc!r}")
            continue
        check_running_jobs(store, provider, publisher=None)
        # Reap orphan VMs (RUNNING in cloud, no fresh capacity broadcast,
        # no recent completions). cloud_function/main.py already does
        # this; the local-mac coordinator was missing it, which is why
        # 37 broken-startup-script VMs accumulated for 24h on
        # 2026-05-09 -> 2026-05-10. With this call, dead agents and
        # never-worked VMs get auto-deleted on every tick (60s).
        try:
            reaped = reap_dead_agents(store, provider, kind=name)
            if reaped:
                _log(f"{name}: reaped {reaped} dead-agent VM(s)")
        except Exception as exc:
            _log(f"{name}: reap_dead_agents failed: {exc!r}")
        total += schedule_queued_jobs(store, provider, name, secrets)
    return total


def run(target: Optional[str] = None, once: bool = False) -> int:
    """Coordinator daemon entry point. Used by `wc coordinator`."""
    coord = _resolve_coordinator(target)
    if coord.runtime == "gcp_cloud_function":
        _log(
            f"coordinator '{coord.name}' runtime=gcp_cloud_function: tick is "
            f"driven by Cloud Scheduler, this daemon is a no-op. Use --target "
            f"to point at a runtime=daemon entry instead."
        )
        return 0

    bucket = _bucket_from_state_uri(coord.state_uri) or BUCKET
    store = JobStorage(bucket)
    interval = max(15, int(coord.interval_seconds))
    _log(f"coordinator '{coord.name}' runtime={coord.runtime} interval={interval}s state={coord.state_uri}")

    # Populate the secrets dict that dispatch_agent_vms uses to fill
    # ${KEY} placeholders in startup_gpu_agent.sh. Without this, the
    # rendered script keeps a literal ${HF_TOKEN}; with `set -u` at the
    # top of the template, line 50 (`export HF_TOKEN="${HF_TOKEN}"`)
    # crashes on unbound variable, the agent never starts, and the VM
    # sits idle until manually deleted. We saw 37 such orphan VMs
    # accumulate over ~24h on 2026-05-09 -> 2026-05-10 because secrets
    # had been an empty dict here forever.
    secrets: dict = {}
    for key in ("HF_TOKEN", "HUGGING_FACE_HUB_TOKEN"):
        val = os.environ.get(key, "").strip()
        if val:
            secrets[key] = val
    if "HF_TOKEN" in secrets and "HUGGING_FACE_HUB_TOKEN" not in secrets:
        secrets["HUGGING_FACE_HUB_TOKEN"] = secrets["HF_TOKEN"]
    # Supabase Management API token goes to a distinct placeholder
    # (WC_SUPABASE_TOKEN) so the startup template can use bash empty-
    # default expansion without conflicting with python's literal-string
    # substitution. dispatched VMs export SUPABASE_ACCESS_TOKEN in their
    # env when this is populated.
    supa = os.environ.get("SUPABASE_ACCESS_TOKEN", "").strip()
    if supa:
        secrets["WC_SUPABASE_TOKEN"] = supa
    if not secrets.get("HF_TOKEN"):
        _log(
            "WARN: HF_TOKEN not in coordinator env; dispatched VMs will "
            "fail their startup script on `set -u` line 50. Set HF_TOKEN "
            "in the LaunchAgent's EnvironmentVariables and reload."
        )
    # Drift-detect + in-process pip-upgrade-and-exec for the coordinator
    # itself. Agents already have this via version_check.maybe_drain_or_upgrade
    # but the coordinator had no such loop, so new wisent-compute releases
    # never reached the running mac-mini coordinator without manual
    # LaunchAgent restart. Now: each tick, check PyPI versions; if drift
    # detected, pip install --upgrade and os.execv into the freshly
    # installed entry point. No systemd dependency.
    from .providers.local.version_check import detect_drift, pip_upgrade_and_exec
    while True:
        try:
            drift = detect_drift()
        except Exception as exc:
            _log(f"drift check failed: {exc!r}")
            drift = {}
        if drift:
            _log(f"coordinator drift detected {drift}; pip_upgrade_and_exec")
            try:
                pip_upgrade_and_exec(_log)
            except Exception as exc:
                _log(f"coordinator upgrade failed: {exc!r}")
        try:
            n = _run_tick(store, secrets)
            _log(f"tick scheduled={n}")
        except Exception as exc:
            _log(f"tick failed: {exc!r}")
            _log(traceback.format_exc())
        if once:
            return 0
        time.sleep(interval)
