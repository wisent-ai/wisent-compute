"""Fenced dispatch and exhaustive reconciliation for Box-backed jobs."""
from __future__ import annotations

import sys
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from ....models import Job, JobState
from ....providers.box import BoxAPIError, BoxProvider
from ....queue.leases import LeaseConflict, LeaseState, ProviderLease, ProviderLeaseStore
from .runtime import BoxRuntime

_OWNER_TTL_SECONDS = int("300")
_QUEUE_SCAN_CAP = int("25")
_START_RECOVERY_SECONDS = int("120")
_TERMINAL_BOX_STATES = frozenset({"archived", "error"})
_READY_BOX_STATES = frozenset({"ready", "idle", "running"})
_RENEWED_STATES = frozenset({
    LeaseState.PROVISIONING.value, LeaseState.READY.value,
    LeaseState.STARTING.value, LeaseState.RUNNING.value,
})


def _save(leases: ProviderLeaseStore, lease: ProviderLease) -> ProviderLease:
    return leases.save(lease, lease.version)


def _log_failure(job_id: str, exc: Exception) -> None:
    text = str(exc).replace("\r", " ").replace("\n", " ")[:int("512")]
    sys.stderr.write(f"[box] job={job_id} {type(exc).__name__}: {text}\n")


def _fail_queued(store: Any, job: Job, message: str) -> None:
    job.state = JobState.FAILED.value
    job.error = message[:int("512")]
    job.failed_at = datetime.now(timezone.utc).isoformat()
    store.move_job(job, "queue", "failed")


def _relinquish(leases: ProviderLeaseStore, lease: ProviderLease | None) -> None:
    if lease is None or lease.owner_expired():
        return
    try:
        lease.relinquish(lease.owner_id, lease.fence_token)
        _save(leases, lease)
    except Exception:
        return


def dispatch_box_jobs(store: Any, provider: BoxProvider, owner_id: str) -> int:
    """Admit pinned queued jobs and allocate available Box capacity."""
    leases = ProviderLeaseStore(store)
    scheduled = int()
    for job in store.list_jobs_priority_first("queue", cap=_QUEUE_SCAN_CAP):
        if job.provider not in {"box", "box-ascii"}:
            continue
        if not job.pin_to_provider:
            _fail_queued(store, job, "Box jobs must set pin_to_provider=true")
            continue
        decision = provider.admit(job)
        if not decision.accepted:
            _fail_queued(store, job, "; ".join(decision.reasons))
            continue
        resource_ttl = job.box_ttl_seconds or provider.ttl_seconds
        lease: ProviderLease | None = None
        resource_recorded = False
        try:
            lease = leases.acquire(
                job.job_id, "box", owner_id, _OWNER_TTL_SECONDS, resource_ttl
            )
            if lease.provider_resource_id:
                resource_recorded = True
                if lease.state in {LeaseState.FAILED.value, LeaseState.RELEASING.value,
                                   LeaseState.RELEASED.value}:
                    _fail_queued(store, job, f"Box lease is already {lease.state}")
                    continue
                job.instance_ref = lease.provider_resource_id
            else:
                box = provider.create_box(resource_ttl)
                lease.provider_resource_id = box.box_id
                lease.transition(LeaseState.PROVISIONING, owner_id, lease.fence_token)
                lease = _save(leases, lease)
                resource_recorded = True
                job.instance_ref = box.box_id
            job.state = JobState.RUNNING.value
            job.started_at = job.started_at or datetime.now(timezone.utc).isoformat()
            store.move_job(job, "queue", "running")
            scheduled += int("1")
        except LeaseConflict:
            continue
        except Exception as exc:
            _log_failure(job.job_id, exc)
            if not resource_recorded:
                if lease is not None and lease.state == LeaseState.ALLOCATING.value:
                    lease.last_error = "Box allocation outcome is unknown; resource TTL remains the bound"
                    lease.result_state = JobState.FAILED.value
                    lease.transition(LeaseState.FAILED, owner_id, lease.fence_token)
                    lease = _save(leases, lease)
                _fail_queued(store, job, str(exc))
        finally:
            _relinquish(leases, lease)
    return scheduled


def _box_state(provider: BoxProvider, lease: ProviderLease) -> str:
    try:
        return provider.client.get_box(lease.provider_resource_id).state
    except BoxAPIError as exc:
        if exc.status == int("404"):
            return "gone"
        raise


def _reconcile_one(store: Any, provider: BoxProvider, runtime: BoxRuntime,
                   leases: ProviderLeaseStore, job: Job, lease: ProviderLease) -> bool:
    state = LeaseState(lease.state)
    if state in {LeaseState.COLLECTING, LeaseState.FAILED,
                 LeaseState.RELEASING, LeaseState.RELEASED}:
        runtime.resume_terminal(job, lease)
        return True
    if not lease.provider_resource_id:
        runtime.fail(job, lease, "Box lease has no provider resource", resource_released=True)
        return True
    box_state = _box_state(provider, lease)
    if box_state == "gone" or box_state == "archived":
        runtime.fail(job, lease, f"Box became {box_state} before completion",
                     resource_released=True)
        return True
    if box_state == "error":
        runtime.fail(job, lease, "Box entered error state")
        return True
    ttl = job.box_ttl_seconds or provider.ttl_seconds
    if lease.state in _RENEWED_STATES:
        provider.renew_box(lease.provider_resource_id, ttl)
        lease.renew_resource(lease.owner_id, lease.fence_token, ttl)
        lease.renew_owner(lease.owner_id, lease.fence_token, _OWNER_TTL_SECONDS)
        lease = _save(leases, lease)
    if lease.state == LeaseState.PROVISIONING.value:
        if box_state not in _READY_BOX_STATES:
            return False
        lease.transition(LeaseState.READY, lease.owner_id, lease.fence_token)
        lease = _save(leases, lease)
    if lease.state in {LeaseState.READY.value, LeaseState.STARTING.value}:
        try:
            return runtime.start(job, lease)
        except Exception:
            if lease.operation_started_at:
                started = datetime.fromisoformat(
                    lease.operation_started_at.replace("Z", "+00:00")
                )
                age = (datetime.now(timezone.utc) - started).total_seconds()
                if age >= _START_RECOVERY_SECONDS:
                    runtime.fail(job, lease, "Box start did not recover before deadline")
                    return True
            raise
    if lease.state == LeaseState.RUNNING.value:
        return runtime.reconcile_running(job, lease)
    if lease.state == LeaseState.ALLOCATING.value:
        runtime.fail(job, lease, "Box allocation did not record a resource",
                     resource_released=True)
        return True
    raise RuntimeError(f"unhandled Box lease state {lease.state}")


def reconcile_box_jobs(store: Any, provider: BoxProvider, owner_id: str) -> int:
    """Advance every persisted lease state without duplicating mutations."""
    leases = ProviderLeaseStore(store)
    runtime = BoxRuntime(store, provider, leases)
    changed = int()
    for job in store.list_jobs("running"):
        if job.provider not in {"box", "box-ascii"}:
            continue
        lease: ProviderLease | None = None
        try:
            lease = leases.acquire(
                job.job_id, "box", owner_id, _OWNER_TTL_SECONDS,
                job.box_ttl_seconds or provider.ttl_seconds,
            )
            if _reconcile_one(store, provider, runtime, leases, job, lease):
                changed += int("1")
        except LeaseConflict:
            continue
        except Exception as exc:
            _log_failure(job.job_id, exc)
        finally:
            _relinquish(leases, lease)
    return changed


def cancel_box_job(store: Any, provider: BoxProvider, job: Job, owner_id: str) -> None:
    """Cancel the process or prompt, then release the fenced resource."""
    leases = ProviderLeaseStore(store)
    session_owner = f"{owner_id}:{uuid4().hex}"
    lease = leases.acquire(
        job.job_id, "box", session_owner, _OWNER_TTL_SECONDS,
        job.box_ttl_seconds or provider.ttl_seconds,
    )
    try:
        BoxRuntime(store, provider, leases).cancel(job, lease)
    finally:
        _relinquish(leases, lease)


def cancel_box_for_legacy_move(store: Any, provider: BoxProvider,
                               job: Job, owner_id: str) -> None:
    leases = ProviderLeaseStore(store)
    session_owner = f"{owner_id}:{uuid4().hex}"
    lease = leases.acquire(job.job_id, "box", session_owner, _OWNER_TTL_SECONDS,
                           job.box_ttl_seconds or provider.ttl_seconds)
    runtime = BoxRuntime(store, provider, leases)
    runtime.interrupt(job, lease)
    lease.result_state = JobState.FAILED.value
    lease.last_error = "cancelled"
    if lease.state not in {LeaseState.FAILED.value, LeaseState.RELEASING.value,
                           LeaseState.RELEASED.value}:
        lease.transition(LeaseState.FAILED, session_owner, lease.fence_token)
        lease = _save(leases, lease)
    if lease.state == LeaseState.FAILED.value:
        lease.transition(LeaseState.RELEASING, session_owner, lease.fence_token)
        lease = _save(leases, lease)
    if lease.state == LeaseState.RELEASING.value:
        provider.release_box(lease.provider_resource_id)
        lease.transition(LeaseState.RELEASED, session_owner, lease.fence_token)
        _save(leases, lease)


def run_box_tick(store: Any, provider: BoxProvider, owner_id: str) -> int:
    """Use a unique owner per invocation; reconcile before allocating."""
    session_owner = f"{owner_id}:{uuid4().hex}"
    return reconcile_box_jobs(store, provider, session_owner) + dispatch_box_jobs(
        store, provider, session_owner
    )
