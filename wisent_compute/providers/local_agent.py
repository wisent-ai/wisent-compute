"""Local GPU agent: runs on the workstation, polls GCS queue, respects Vast.ai.

Usage: wc agent --gpu-type nvidia-rtx-4090
Runs as a long-lived daemon. Picks up jobs when Vast.ai has no active renter.
"""
from __future__ import annotations

import glob
import os
import shutil
import socket
import sys
import tarfile
import tempfile
import time
from datetime import datetime, timezone

from ..config import BUCKET, estimate_gpu_memory
from ..queue.capacity import publish_capacity
from ..queue.storage import JobStorage
from .local.helpers import (
    _build_capacity_dict,
    _detect_gpu_type,
    _detect_local_vram_gb,
    _job_eligible,
    _no_eligible_in_queue,
    _slot_is_exclusive,
    _slot_vram,
    _smi_free_vram_gb,
    _staging_size_gb,
    _vast_has_renter,
)


POLL_INTERVAL = 10
HEARTBEAT_INTERVAL = 300
# Time to sleep after a successful claim so nvidia-smi can reflect the
# freshly-spawned subprocess's CUDA allocation before the next iteration
# decides whether to claim again. Empirically a torch model load starts
# allocating GPU memory within ~5 seconds of subprocess start.
SETTLE_AFTER_CLAIM_SECONDS = 5
# Hard VRAM safety buffer at admission. The agent refuses to claim a
# job if accepting it would leave less than this margin between
# declared total VRAM use and the GPU's physical capacity. Catches the
# class of failure where neighbor processes' actual peak exceeds their
# declared gpu_mem_gb (estimate_gpu_memory has been observed to
# under-call by 5-10 GB on 7-8B activation extraction workloads). The
# buffer is independent of the per-job multipliers because it's the
# LAST line of defense — if the per-job estimate is wrong, this catches
# it before the n+1th job OOMs the entire VM.
VRAM_SAFETY_BUFFER_GB = 8


def _log(msg):
    ts = datetime.now().strftime("%H:%M:%S")
    sys.stderr.write(f"[{ts}] [agent] {msg}\n")
    sys.stderr.flush()


def _reap_orphan_workdirs(hostname: str) -> None:
    """Archive then delete orphan /tmp/wisent_act_* dirs at agent startup.

    Job subprocesses rmtree their workdir on clean exit; if the agent is
    killed mid-job the workdir leaks. /tmp on the workstation is tmpfs
    (62 GB cap) — one leaked 43 GB workdir on 2026-05-07 filled it 100%
    and caused 700+ ENOSPC failures in 17 hours. Before rmtree-ing, tar
    and upload to gs://<bucket>/orphans/<hostname>/<dirname>.tar.gz so
    the operator has crash evidence in GCS for postmortem.

    Also reap /tmp/wc-* (agent-owned per-job dirs from slots.start_slot).
    Any leftover at agent startup is stale since this fresh process owns
    no active job.
    """
    store = JobStorage(BUCKET)
    for d in glob.glob("/tmp/wisent_act_*"):
        pid = int(d.rsplit("_pid", 1)[-1].split("_")[0])
        try:
            os.kill(pid, 0)
            still_alive = True
        except OSError:
            still_alive = False
        if still_alive:
            continue
        with tempfile.NamedTemporaryFile(suffix=".tar.gz", delete=False) as tmp:
            tar_path = tmp.name
        with tarfile.open(tar_path, "w:gz") as tar:
            tar.add(d, arcname=os.path.basename(d))
        key = f"orphans/{hostname}/{os.path.basename(d)}.tar.gz"
        store._sdk_bucket.blob(key).upload_from_filename(tar_path)
        os.unlink(tar_path)
        _log(f"archived orphan {d} -> gs://{BUCKET}/{key}")
        shutil.rmtree(d)
        _log(f"reaped orphan workdir {d} (pid {pid} dead)")
    wc_dirs = glob.glob("/tmp/wc-*")
    for d in wc_dirs:
        shutil.rmtree(d)
    if wc_dirs:
        _log(f"reaped {len(wc_dirs)} stale /tmp/wc-* dirs")


def run_agent(gpu_type: str = "", idle_shutdown: bool = False, kind: str = "local"):
    """Main agent loop. Polls queue, runs jobs when Vast.ai is idle.

    idle_shutdown=True: exit cleanly (and self-delete the GCE VM if running
    on one) once both: (a) no slots active, and (b) no queued job is
    eligible to run on this consumer's free VRAM. Used for the cloud-VM
    agent path.

    kind: capacity-broadcast label distinguishing physical workstations
    (kind="local") from ephemeral cloud-agent VMs (kind="gcp", ...).
    No global error handler wraps the loop body: unexpected exceptions
    crash the agent visibly so the operator can diagnose.
    """
    from .local.slots import advance_slot, start_slot
    from ..targets import lookup_self
    if not gpu_type:
        gpu_type = _detect_gpu_type()
    total_vram_gb = max(1, _detect_local_vram_gb())
    hard_slot_cap = int(os.environ.get("WC_LOCAL_SLOTS", "0") or 0)
    WORKDIR_SCRATCH_GB = int(os.environ.get("WC_WORKDIR_SCRATCH_GB", "15"))
    _log(f"Agent started. kind={kind}  GPU: {gpu_type}  vram_gb={total_vram_gb}  hard_slot_cap={hard_slot_cap}")

    hostname = socket.gethostname()
    _reap_orphan_workdirs(hostname)

    initial_env: dict[str, str] = dict(os.environ)
    initial_gpu = gpu_type

    store = JobStorage(BUCKET)
    consumer_id = f"{kind}-{hostname}"
    slots: list[dict] = []
    agent_diag: dict = {}
    fleet_staging = os.environ.get("WISENT_FLEET_STAGING_DIR", "/tmp/wisent_fleet_staging")
    last_fleet_flush = time.time()
    FLEET_FLUSH_INTERVAL = 180  # ~20 commits/hour/agent << 200/hour HF cap

    while True:
        if time.time() - last_fleet_flush > FLEET_FLUSH_INTERVAL or _staging_size_gb(fleet_staging) > 5:
            from wisent.core.reading.modules.utilities.data.sources.hf.hf_writers import flush_staging_dir
            if os.path.isdir(fleet_staging) and any(os.scandir(fleet_staging)):
                flush_staging_dir(fleet_staging)
                shutil.rmtree(fleet_staging)
                os.makedirs(fleet_staging, exist_ok=True)
                _log("flushed fleet staging dir to HF (1 commit)")
            last_fleet_flush = time.time()
        t = lookup_self(hostname, source="auto")
        if t and t.kind == "local":
            registry_env = t.env_overrides or {}
            env_delta = {
                k: str(v) for k, v in registry_env.items()
                if str(initial_env.get(k, "")) != str(v)
            }
            if env_delta and not slots:
                _log(f"Registry env override delta {env_delta}; pip_upgrade_and_exec for restart")
                from .local.version_check import pip_upgrade_and_exec as _upgrade_exec
                _upgrade_exec(_log)
            if t.gpu_type and t.gpu_type != initial_gpu and not slots:
                _log(f"Registry gpu_type {initial_gpu} -> {t.gpu_type}; pip_upgrade_and_exec for restart")
                from .local.version_check import pip_upgrade_and_exec as _upgrade_exec
                _upgrade_exec(_log)
            if t.vram_gb and int(t.vram_gb) != total_vram_gb:
                _log(f"Registry vram_gb override {total_vram_gb} -> {t.vram_gb}")
                total_vram_gb = int(t.vram_gb)
        vast_active = _vast_has_renter()
        slots = [s for s in slots if advance_slot(s, store, vast_active, _log)]
        from .local.version_check import maybe_drain_or_upgrade as _drain
        if _drain(slots, _log, kind=kind):
            time.sleep(POLL_INTERVAL); continue
        if vast_active:
            publish_capacity(store, consumer_id, kind, {}, free_vram_gb=0,
                             total_vram_gb=total_vram_gb, diag=dict(agent_diag))
            time.sleep(POLL_INTERVAL)
            continue

        used_vram = sum(_slot_vram(s) for s in slots)
        if any(_slot_is_exclusive(s) for s in slots):
            used_vram = total_vram_gb
        free_vram_gb = max(0, total_vram_gb - used_vram)
        smi_free = _smi_free_vram_gb()
        if smi_free >= 0 and smi_free < free_vram_gb:
            free_vram_gb = smi_free
        from .local.disk import gate_and_maybe_evict as _disk_gate
        _refuse_disk, _disk_diag = _disk_gate(_log)
        agent_diag.update(_disk_diag)
        if _refuse_disk:
            publish_capacity(store, consumer_id, kind, {},
                             free_vram_gb=0, total_vram_gb=total_vram_gb, diag=dict(agent_diag))
            time.sleep(10)
            continue
        free_slots = _build_capacity_dict(gpu_type, free_vram_gb, total_vram_gb)
        publish_capacity(store, consumer_id, kind, free_slots,
                         free_vram_gb=free_vram_gb, total_vram_gb=total_vram_gb, diag=dict(agent_diag))

        if free_vram_gb <= 0 or (hard_slot_cap > 0 and len(slots) >= hard_slot_cap):
            time.sleep(10)
            continue

        # Centralized assignment writes job.assigned_to on the queue blob;
        # _job_eligible(consumer_id=...) below filters to ONLY the jobs this
        # agent owns. The coordinator's makespan matcher already made the
        # choice; this loop executes it.
        queued = store.list_jobs_fitting("queue", max_gpu_mem_gb=free_vram_gb, cap=2000)
        queued.sort(key=lambda j: (-getattr(j, "priority", 0), j.created_at))
        started = 0
        diag_vram_rejected = 0
        diag_eligibility_rejected = 0
        diag_eligible = 0
        for job in queued:
            if hard_slot_cap > 0 and len(slots) >= hard_slot_cap:
                break
            need = max(
                int(getattr(job, "gpu_mem_gb", 0) or 0),
                estimate_gpu_memory(getattr(job, "command", "") or ""),
            )
            if need > free_vram_gb:
                diag_vram_rejected += 1
                continue
            # Hard safety buffer: even if `need <= free_vram_gb` (bookkeeping
            # + smi_free check both passed), refuse to claim if total declared
            # use after admission would leave less than VRAM_SAFETY_BUFFER_GB
            # of GPU capacity. Catches under-estimated neighbor jobs whose
            # actual peak exceeds their declared gpu_mem_gb. Without this,
            # an agent with `slots=[26GB-decl-but-32GB-actual, 26GB-decl-but-
            # 32GB-actual]` looks like 28 GB free in bookkeeping (80-26-26),
            # admits a 22 GB job, and the 3rd CUDA load races into a wall.
            projected_used = sum(_slot_vram(s) for s in slots) + need
            if projected_used > total_vram_gb - VRAM_SAFETY_BUFFER_GB:
                diag_vram_rejected += 1
                agent_diag["last_buffer_reject_job_id"] = job.job_id
                agent_diag["last_buffer_reject_at"] = datetime.now(timezone.utc).isoformat()
                continue
            _tmp_free_gb = shutil.disk_usage("/tmp").free / (1024**3)
            if _tmp_free_gb < WORKDIR_SCRATCH_GB:
                diag_vram_rejected += 1
                _log(f"refuse {job.job_id}: /tmp free={_tmp_free_gb:.1f}G < workdir_scratch={WORKDIR_SCRATCH_GB}G")
                continue
            if not _job_eligible(job, gpu_type, total_vram_gb, kind=kind,
                                  consumer_id=consumer_id,
                                  active_slot_count=len(slots)):
                diag_eligibility_rejected += 1
                continue
            diag_eligible += 1
            new_slot = start_slot(store, job, hostname, _log, kind=kind)
            if new_slot is None:
                # apt-install refused or failed; job stays in queue/ for
                # another (cloud-kind or registry-fixed) agent to claim.
                continue
            slots.append(new_slot)
            free_vram_gb -= need
            started += 1
            agent_diag["last_started_job_id"] = job.job_id
            agent_diag["last_started_at"] = datetime.now(timezone.utc).isoformat()
            break
        agent_diag["queue_scanned"] = len(queued)
        agent_diag["vram_rejected"] = diag_vram_rejected
        agent_diag["eligibility_rejected"] = diag_eligibility_rejected
        agent_diag["eligible_count"] = diag_eligible
        agent_diag["claimed_this_loop"] = started
        agent_diag["last_claim_attempt_at"] = datetime.now(timezone.utc).isoformat()

        if started > 0:
            time.sleep(SETTLE_AFTER_CLAIM_SECONDS)
            continue

        if started == 0:
            if idle_shutdown and not slots and _no_eligible_in_queue(
                store, gpu_type, total_vram_gb, free_vram_gb, kind=kind,
                consumer_id=consumer_id, active_slot_count=len(slots),
            ):
                _log("idle_shutdown: no slots + no eligible queued jobs; exiting")
                from .local.gcp_self import self_terminate
                self_terminate(_log)
                return
            time.sleep(POLL_INTERVAL)
