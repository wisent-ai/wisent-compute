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

import sys
import time
import traceback
from typing import Optional

from .config import BUCKET, WC_PROVIDERS
from .monitor import check_running_jobs
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

    secrets: dict = {}
    while True:
        try:
            n = _run_tick(store, secrets)
            _log(f"tick scheduled={n}")
        except Exception as exc:
            _log(f"tick failed: {exc!r}")
            _log(traceback.format_exc())
        if once:
            return 0
        time.sleep(interval)
