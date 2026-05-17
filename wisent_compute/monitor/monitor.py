"""Monitor running jobs: check heartbeat, status, cleanup.

Handles four exit conditions for a running job:
  COMPLETED         -> finalize success path
  FAILED            -> finalize failure path + alert
  preempted (Spot)  -> instance is TERMINATED but the Job is otherwise healthy.
                      Delete the GCE instance, increment preempt_count, requeue.
                      preempt_count is separate from restarts so a Spot-heavy
                      job doesn't burn the restart budget on preemptions alone.
  instance gone OR
  stale heartbeat   -> requeue (counted against restarts).
"""
from __future__ import annotations

import sys
from datetime import datetime, timezone

from ..config import ALERTS_TOPIC
from ..models import JobState
from ..queue.storage import JobStorage
from ..providers.base import Provider
from .alerts import send_alert
from .reap.helpers import (
    _instance_refs_with_completions,
    _requeue,
    _requeue_jids_after_reap,
    _requeue_preempted,
)


def _log(msg):
    sys.stderr.write(f"[monitor] {msg}\n")
    sys.stderr.flush()


def check_running_jobs(store: JobStorage, provider: Provider, publisher=None):
    """Check all running jobs. Handle completion, failure, preemption, stale."""
    running = store.list_jobs("running")
    _log(f"Checking {len(running)} running jobs")

    _live_consumers_cache: dict | None = None
    _running_vm_names_cache: set | None = None
    for job in running:
        job_id = job.job_id
        ref = job.instance_ref
        if not ref:
            _requeue(store, job, "no instance ref")
            continue

        status = store.read_status(job_id)

        if status == "COMPLETED":
            job.state = JobState.COMPLETED.value
            job.completed_at = datetime.now(timezone.utc).isoformat()
            provider.delete_instance(ref)
            store.move_job(job, "running", "completed")
            store.cleanup_status(job_id)
            _log(f"{job_id}: COMPLETED")

        elif status == "FAILED":
            job.state = JobState.FAILED.value
            job.failed_at = datetime.now(timezone.utc).isoformat()
            provider.delete_instance(ref)
            store.move_job(job, "running", "failed")
            store.cleanup_status(job_id)
            send_alert(publisher, ALERTS_TOPIC, f"Job {job_id} FAILED: {job.command[:100]}")
            _log(f"{job_id}: FAILED")

        else:
            # Boot grace: a freshly (re)dispatched job has not yet
            # written its first heartbeat (agent claim -> apt/clone/pip/
            # multi-GB ckpt-pull preamble before slots.py Popen +
            # _write_heartbeat) while the previous run's heartbeat blob
            # is already aged, so the orphan / VM-gone staleness guards
            # below false-positive and requeue a healthy starting job
            # (synchronized 3ef705b2+724084db requeues 16:18/20:00/
            # 21:33/21:57 on RUNNING 0.4.228 VMs). Skip requeue logic
            # until the job has had BOOT_GRACE_SECONDS to heartbeat.
            _sa = getattr(job, "started_at", None)
            if _sa:
                try:
                    if (datetime.now(timezone.utc) - datetime.fromisoformat(_sa)).total_seconds() < 1800:
                        continue
                except (ValueError, TypeError):
                    pass
            if (ref or "").startswith("local@"):
                hostname = ref[len("local@"):]
                if _live_consumers_cache is None:
                    from ..queue.capacity import read_consumer_capacity
                    _live_consumers_cache = read_consumer_capacity(store)
                agent_live = any(f"{p}-{hostname}" in _live_consumers_cache
                                 for p in ("local", "gcp", "azure", "aws"))
                if agent_live:
                    # Agent up != this old job progresses (restarts
                    # orphan it). Heartbeat is proof; self-terminating
                    # cmds (pkill wc agent) -> kill IS success.
                    from . import heartbeat_guard as _hg
                    if _hg.any_job_heartbeat_fresh(store, [job_id], 1800):
                        continue
                    if _hg.finalize_if_self_terminating(store, job, _log):
                        continue
                    if not _hg.any_job_checkpoint_fresh(store, job, 5400):
                        _requeue(store, job, "local agent live but job heartbeat stale (orphan)")
                    continue
                is_cloud_agent_name = hostname.startswith("wisent-agent-")
                if is_cloud_agent_name:
                    if _running_vm_names_cache is None:
                        _running_vm_names_cache = {
                            r.split("@", 1)[0]
                            for r, _ in provider.list_running_instance_refs_with_age()
                        }
                    if hostname not in _running_vm_names_cache:
                        from . import heartbeat_guard as _hg_vm
                        if _hg_vm.any_job_heartbeat_fresh(store, [job_id], 1800):
                            continue  # fresh job heartbeat = VM+agent+training alive; aggregated_list missed a transient non-RUNNING (STAGING/REPAIRING/live-migration) snapshot
                        if getattr(job, "preemptible", False):
                            _requeue_preempted(store, job, "Spot preempted (cloud agent gone)")
                        else:
                            _requeue(store, job, "VM gone (cloud agent missing from fleet)")
                        continue
                continue

            alive = provider.instance_exists(ref)
            lifecycle = provider.instance_lifecycle_state(ref)

            if not alive and lifecycle == "TERMINATED" and getattr(job, "preemptible", False):
                _requeue_preempted(store, job, "Spot preempted")
                provider.delete_instance(ref)
            elif not alive:
                _requeue(store, job, f"instance gone (lifecycle={lifecycle})")
                provider.delete_instance(ref)


def reap_dead_agents(store: JobStorage, provider: Provider, kind: str = "gcp") -> int:
    """Delete RUNNING VMs whose `wc agent` has stopped doing useful work.

    The agent's main loop publishes a freshness-stamped JSON to
    gs://<bucket>/capacity/<kind>-<hostname>.json on every iteration. If the
    process crashes (OOM, segfault, uncaught exception) the GCE instance keeps
    running, holding GPU + disk quota with zero work output. read_consumer_capacity
    filters to broadcasts younger than CAPACITY_STALE_SECONDS. Any RUNNING VM
    whose corresponding consumer_id is missing from that filtered set has a
    dead agent and gets deleted here so the dispatcher can spawn a fresh
    replacement.
    """
    from ..queue.capacity import read_consumer_capacity
    live = read_consumer_capacity(store)  # consumer_id -> payload, fresh only
    refs = provider.list_running_instance_refs_with_age()
    deleted = 0
    # Three reap conditions, each age/liveness-guarded so a VM still in its
    # startup-script install phase (~10-14 min on baked images) is not killed
    # before it can work.
    #   Branch A (dead-agent): age > BOOT_GRACE AND no fresh capacity
    #     broadcast. Covers crashed agents + startup-script failures.
    #   Branch B (never-worked): age > IDLE_GRACE AND broadcasting AND zero
    #     completions in completed/ for this instance_ref.
    #   Branch C (wedged): broadcasting fresh capacity BUT free_vram_gb<=0
    #     AND free_slots={} AND last claim/start (diag) stale AND no job on
    #     this VM heartbeating. A hung claimed subprocess pins VRAM forever
    #     (advance_slot only retires on proc.poll() != None), free_vram_gb=0
    #     short-circuits the agent loop before the diag write, and the
    #     top-of-loop re-publish keeps capacity fresh — so the VM is invisible
    #     to Branch A (capacity fresh) AND Branch B (historical completions
    #     keep it in completed_refs). Confirmed live 2026-05-17
    #     (gcp-wisent-agent-80gb-1778921111-0: free_vram_gb=0, last_started_at
    #     frozen 2026-05-16T09:17:32, 127 gpt-oss-20b jobs dead-pinned hours).
    # BOOT/IDLE 1800s: 900s reaped real 14m boots (3ef705b2/931b865e/f3fd41fb
    # ricocheting dispatch<->reap, confirmed 2026-05-15 02:24Z).
    BOOT_GRACE_SECONDS = 1800
    IDLE_GRACE_SECONDS = 1800      # half-window grace for first completion
    # Build the completed-refs set ONLY if any VM is old enough to need it.
    # Iterating completed/ at fleet scale (~11k blobs) blows the 60s tick
    # budget every time, returning 504 and pausing Cloud Scheduler. Cheap
    # short-circuit: if no VM has crossed IDLE_GRACE_SECONDS, branch B
    # cannot fire anyway.
    needs_completions_scan = any(
        age_seconds > IDLE_GRACE_SECONDS for _, age_seconds in refs
    )
    completed_refs = (
        _instance_refs_with_completions(store, kind=kind)
        if needs_completions_scan else set()
    )
    # ALSO build the set of VMs that currently have a job in running/. A VM
    # mid-extraction on its FIRST big job (e.g. gpt-oss-20b 80GB shards)
    # legitimately exceeds IDLE_GRACE_SECONDS=1800 before producing its
    # first completion. Without this check, the never-worked reaper kills
    # healthy VMs and the parent jobs ricochet through restart cycles.
    # Confirmed live on 2026-05-07: reaper killed 23+ working VMs in one
    # hour, triggering the "never-worked reap (>5 in 1h)" alert email
    # storm.
    active_refs: set = set()
    if needs_completions_scan:
        for j in store.list_jobs("running"):
            r = getattr(j, "instance_ref", None)
            if r:
                active_refs.add(r)
    # Second signal: per-job heartbeat. Defers the reap when the agent's
    # capacity blob is stale BUT a running job assigned to its VM still
    # has a fresh heartbeat — agent is alive, just starved on its
    # broadcast tick by a training subprocess. Without this guard the
    # reaper destroys productive VMs (Llama-1B 5k run was reaped 3 times
    # mid-training on 2026-05-12 because rollout steps exceeded
    # CAPACITY_STALE_SECONDS).
    from .heartbeat_guard import any_job_heartbeat_fresh, build_ref_to_jids, any_job_checkpoint_fresh_jids, fresh_jids_pointing_to_ref
    _ref_to_jids = build_ref_to_jids(store)
    _hb_threshold = 1800
    for ref, age_seconds in refs:
        name = ref.split("@", 1)[0]
        consumer_id = f"{kind}-{name}"
        instance_ref = f"local@{name}"
        if consumer_id not in live:
            if age_seconds < BOOT_GRACE_SECONDS:
                continue  # still installing, give it time
            _jids = _ref_to_jids.get(ref, []) + _ref_to_jids.get(instance_ref, [])
            if any_job_heartbeat_fresh(store, _jids, _hb_threshold) or any_job_checkpoint_fresh_jids(store, _jids, 5400):
                _log(
                    f"defer reap of {ref}: capacity stale "
                    f"(age={age_seconds:.0f}s) but job heartbeat fresh for {_jids}"
                )
                continue
            if (_safety := fresh_jids_pointing_to_ref(store, instance_ref)):
                _log(f"defer dead-agent reap of {ref}: fresh running/ found {_safety} (active_refs race)")
                continue
            provider.delete_instance(ref)
            _log(
                f"reaped dead-agent VM {ref} (no fresh capacity broadcast, "
                f"age={age_seconds:.0f}s > boot grace {BOOT_GRACE_SECONDS}s, "
                f"no fresh job heartbeat either)"
            )
            deleted += 1
            _requeue_jids_after_reap(store, _jids, f"VM reaped (dead agent, age={age_seconds:.0f}s)")
            continue
        if (age_seconds > IDLE_GRACE_SECONDS
                and instance_ref not in completed_refs
                and instance_ref not in active_refs):
            # Branch A defers on a fresh job heartbeat; Branch B must
            # too. A long training run never appears in completed/ and
            # is protected only by the race-prone active_refs set, so a
            # working VM (Llama 3ef705b2 at step ~3533, heartbeat fresh
            # via the 0.4.224 daemon thread) was reaped here as
            # "never-worked" at 2026-05-15T23:14:01 (restart 8). A fresh
            # job heartbeat is proof the VM is productive — never reap.
            _jids_b = _ref_to_jids.get(ref, []) + _ref_to_jids.get(instance_ref, [])
            if any_job_heartbeat_fresh(store, _jids_b, _hb_threshold) or any_job_checkpoint_fresh_jids(store, _jids_b, 5400):
                _log(f"defer never-worked reap of {ref}: job heartbeat fresh for {_jids_b}")
                continue
            if (_safety := fresh_jids_pointing_to_ref(store, instance_ref)):
                _log(f"defer never-worked reap of {ref}: fresh running/ found {_safety} (active_refs race; root cause of 724084db restart 16 wedge 2026-05-17T21:26:07)")
                continue
            provider.delete_instance(ref)
            _log(
                f"reaped never-worked VM {ref} (broadcasting but 0 completions "
                f"AND no active running job in age={age_seconds:.0f}s, "
                f"> grace {IDLE_GRACE_SECONDS}s)"
            )
            deleted += 1
            _jids = _ref_to_jids.get(ref, []) + _ref_to_jids.get(instance_ref, [])
            _requeue_jids_after_reap(store, _jids, "VM reaped (never-worked)")
            continue
        # Branch C (wedged): fresh capacity but structurally stuck. ALL of:
        # free_vram_gb<=0 AND empty free_slots, diag last_started_at /
        # last_claim_attempt_at older than _hb_threshold, no fresh heartbeat
        # for any job on this VM. The heartbeat guard protects a healthy long
        # trainer whose broadcast tick is merely starved; the diag-stale
        # guard protects an agent that is actively claiming.
        payload = live.get(consumer_id) or {}
        if int(payload.get("free_vram_gb") or 0) <= 0 and not (payload.get("free_slots") or {}):
            _diag = payload.get("diag") or {}
            _last = _diag.get("last_started_at") or _diag.get("last_claim_attempt_at")
            _stale = False
            if _last:
                try:
                    _stale = (datetime.now(timezone.utc) - datetime.fromisoformat(
                        str(_last).replace("Z", "+00:00"))).total_seconds() > _hb_threshold
                except (ValueError, TypeError):
                    _stale = False
            if not _stale:
                continue
            _jids_c = _ref_to_jids.get(ref, []) + _ref_to_jids.get(instance_ref, [])
            if any_job_heartbeat_fresh(store, _jids_c, _hb_threshold) or any_job_checkpoint_fresh_jids(store, _jids_c, 5400):
                _log(f"defer wedged reap of {ref}: job heartbeat fresh for {_jids_c}")
                continue
            if (_safety := fresh_jids_pointing_to_ref(store, instance_ref)):
                _log(f"defer wedged reap of {ref}: fresh running/ found {_safety} (active_refs race)")
                continue
            provider.delete_instance(ref)
            _log(
                f"reaped wedged VM {ref} (capacity fresh but free_vram_gb<=0 "
                f"& no free_slots, last claim/start {_last} stale "
                f"> {_hb_threshold}s, no fresh job heartbeat)"
            )
            deleted += 1
            _requeue_jids_after_reap(store, _jids_c, "VM reaped (wedged agent)")
            continue
    if deleted:
        _log(f"reap_dead_agents: deleted {deleted} VM(s)")
    return deleted
