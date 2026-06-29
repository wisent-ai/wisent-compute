"""Job data model and state definitions."""
from __future__ import annotations

import os
import json
from dataclasses import dataclass, field, asdict
from enum import Enum
from datetime import datetime, timezone


class JobState(str, Enum):
    QUEUED = "queued"
    RUNNING = "running"
    # COMPLETED = extraction finished + handed off to the detached upload
    # worker; activations are written to local staging but NOT yet on HF.
    # Kept named "completed" so the coordinator Cloud Function and dashboard,
    # which write/read this prefix, stay unchanged.
    COMPLETED = "completed"
    # UPLOADED = the upload worker confirmed the dir landed on HF (terminal).
    UPLOADED = "uploaded"
    FAILED = "failed"


# Full GPU SKU catalog (Azure NC/ND/NV/NCC + GCE T4/L4/A100/H100/H200/B200/MI300X)
# lives in _catalog/gpu_sku.py to keep this file under the 300-line cap.
# Re-export at module scope so existing callers
# (`from wisent_compute.models import GPU_SIZING`, etc.) keep working.
from ._catalog.gpu_sku import (  # noqa: E402
    AZURE_QUOTA_FAMILY_TO_ACCEL,
    AZURE_VM_HOURLY_RATE_USD,
    AZURE_VM_TO_ACCEL,
    GPU_HOURLY_RATE_USD,
    GPU_SIZING,
    GPU_TYPE_TO_MACHINE_TYPE,
    SPOT_DISCOUNT,
    VM_BUNDLE_HOURLY_RATE_USD,
)


DEPRECATED_ACTIVATION_ENTRYPOINT = "wisent.scripts.activations." + "extract_and_upload"


def deprecated_activation_command_reason(command: str) -> str:
    if DEPRECATED_ACTIVATION_ENTRYPOINT not in (command or ""):
        return ""
    return (
        "refusing deprecated foreground activation uploader; use "
        "wisent.scripts.activations.raw.extract_and_upload so extraction "
        "hands upload to the detached worker pool"
    )


def activation_extraction_must_share_gpu(command: str) -> bool:
    """Activation extraction jobs are VRAM-sized, not whole-GPU-exclusive."""
    command = command or ""
    return "wisent.scripts.activations.raw.extract_and_upload" in command


@dataclass
class Job:
    job_id: str
    command: str
    gpu_mem_gb: int = 0
    gpu_type: str = ""
    machine_type: str = ""
    provider: str = "gcp"
    batch_id: str = ""
    state: str = JobState.QUEUED.value
    created_at: str = ""
    started_at: str | None = None
    completed_at: str | None = None
    failed_at: str | None = None
    instance_ref: str | None = None
    restarts: int = 0
    max_restarts: int = 20
    last_restart: str | None = None
    image: str = "pytorch-2-9-cu129-ubuntu-2204-nvidia-580-v20260408"
    image_project: str = "deeplearning-platform-release"
    boot_disk_gb: int = 500
    startup_script_uri: str = ""
    error: str | None = None
    # New routing/cost fields. Default values keep all existing jobs behaving
    # exactly as before — older clients that don't set them keep working.
    preemptible: bool = False              # if true, dispatch on Spot
    pin_to_provider: bool = False          # if true, only the named provider claims
    max_cost_per_hour_usd: float = 0.0     # 0 = no cap
    preempt_count: int = 0                 # # times this job was preempted on Spot
    max_preempts_before_ondemand: int = 3  # after N preempts, fall back to on-demand
    priority: int = 0                      # higher = scheduled first within FIFO bucket
    # Tracks failed create_instance calls so a job that can't currently be
    # dispatched (zone exhausted, quota error, etc.) backs off instead of
    # being retried on every tick. Resets on successful dispatch.
    dispatch_attempts: int = 0
    last_dispatch_attempt: str | None = None
    # Submitter provenance. Populated by submit_job from $USER + os.uname().nodename
    # at submit time so every job in queue/running/completed/failed records who
    # put it there. Defaults are empty string for back-compat with old records.
    submitted_by: str = ""        # username on the submitting machine
    submitted_from: str = ""      # hostname of the submitting machine
    submitted_via: str = ""       # cli | api | other
    # run_id is the tracking primitive above a job: one `wc submit`
    # invocation = one run, written once to runs/<run_id>.json with the
    # member job_ids. Distinct from batch_id (a legacy free-text label).
    # submitter_app names the orchestrator that issued the run
    # (activation-extraction, failure-fixer, manual, ...) from
    # $WC_SUBMITTER_APP, so resubmits are distinguishable from manual runs.
    run_id: str = ""
    submitter_app: str = ""
    # Optional source repo to git clone before running command. The startup
    # script clones repo into ./{repo_workdir} and pip-installs the [train]
    # extra so 'cd {repo_workdir} && python -m train.foo' works without the
    # caller having to glue clone+install onto every command. Empty = skip.
    repo: str = ""                # https git URL (or git@host:owner/repo.git)
    repo_workdir: str = ""        # subdir name; defaults to repo basename
    repo_extras: str = "train"    # pip-install extras name; "" to skip install
    # Verification hook. If set, runs as a shell command AFTER the subprocess
    # exits 0; non-zero exit reverses the COMPLETED → FAILED. Catches the
    # class of bug where the subprocess swallows errors and exits 0 despite
    # not having done its work (e.g. wisent's extract_and_upload reports
    # "5/7 strategies failed" + HF 429 rate-limit errors but exits 0,
    # marking the job COMPLETED while no usable output was produced).
    # Confirmed live on 2026-05-06: 20/20 sampled "completed" jobs had this
    # exact failure pattern. Empty string = skip verify (back-compat).
    verify_command: str = ""
    # Centralized fleet-aware assignment: when set, ONLY the agent whose
    # consumer_id matches may claim this queued job. Coordinator's
    # _assign_jobs_to_agents fills this on every tick. Empty string means
    # unassigned and any-eligible-agent may claim (pre-0.4.100 behavior).
    assigned_to: str = ""
    # Optional runtime hint (seconds). When > 0 the makespan scheduler
    # uses this directly; otherwise it falls back to historical mean
    # runtime grouped by (model, task), then by model, then global. This
    # is what lets the scheduler optimize for total time-to-finish
    # rather than VRAM balance.
    runtime_seconds_estimate: float = 0.0
    # Shell snippet prefixed to job.command at agent runtime. Runs in the
    # same bash invocation so it can `export` env vars that affect the
    # subprocess (e.g. LD_LIBRARY_PATH for cu128/cu129 cuBLAS reconciliation
    # on DLVM images). Empty = no prelude.
    pre_command: str = ""
    # Apt packages to install on the agent VM before spawning the subprocess.
    # Applied on cloud-kind agents only (gcp/azure/aws); local-kind agents
    # refuse the job to avoid surprise installs on operator workstations.
    apt_packages: list = field(default_factory=list)
    # Additional GCS URI to mirror job output to after completion. Default
    # location at gs://$WC_BUCKET/status/<id>/output/ is always written;
    # this is an additive sync so artifacts can land in a project bucket
    # (e.g. gs://wisent-images-bucket/Jakubs-lora/run01_2026-05-14/)
    # without the job command itself having to gsutil cp them.
    output_uri: str = ""
    # Exclusive GPU use. When True, the agent only claims this job if it
    # has zero other active slots, AND refuses to claim any other job
    # while this one runs. Use for workloads that can't survive co-tenancy
    # — large diffusion training (Z-Image, SDXL), full-finetunes, anything
    # whose peak VRAM is hard to predict from on-disk model size. Catches
    # the TOCTOU race where neighbor processes ramp up CUDA allocations
    # AFTER admission and starve us mid-load. Per-job equivalent of the
    # config.is_exclusive_model policy (GCS model_overrides.json), which
    # is regex-on-command; this is the explicit per-job opt-in.
    exclusive: bool = False
    # Actual peak GPU memory (GiB) used by this job's process tree, sampled
    # from nvidia-smi --query-compute-apps during the run and written at
    # completion. 0 = not measured (older records, CPU jobs, or a GPU that
    # never reported a compute-app for the process tree). This is the
    # ground-truth signal the sizing heuristic learns from — it is the
    # MEASURED number, not a declared/estimated one.
    peak_vram_gb: int = 0
    # True iff peak_vram_gb was produced by the PER-GPU probe (0.4.241+):
    # nvidia-smi grouped by gpu_uuid, max single-GPU footprint. Records
    # written by the pre-0.4.241 probe summed used_memory ACROSS GPUs, so
    # on a multi-GPU host the figure is the cross-GPU total, not the
    # per-card requirement (gpt-oss-20b: ~89 summed vs ~50-74 real). Those
    # records are unusable as a single-GPU sizing signal; observed_vram_gb
    # trusts ONLY peaks with this flag True so a poisoned historical
    # sample can never dominate the max(). Default False = legacy/untrusted.
    peak_vram_per_gpu: bool = False
    # Set when this job was submitted by a recurring schedule
    # (gs://<bucket>/schedules/<id>.json) rather than a one-shot
    # `wc submit`. Records provenance and lets the schedule firer skip
    # a new fire while a prior instance of the same schedule is still
    # live (overlap_policy=skip). Empty = ad-hoc job (back-compat).
    schedule_id: str = ""
    # Provenance for re-submissions. When this job was queued as a re-run of
    # a prior failure, re_submission_of holds the ORIGINAL failed job_id.
    # The tracker (.work/tracker/build_tracker.sh) joins on this field to
    # report "for each entry in failed/, did its re-submission complete?".
    # Without it, the tracker has to guess by (task, prompt_format) match.
    # Empty string = not a re-submission (the default for fresh jobs).
    re_submission_of: str = ""
    # Cooperative-yield (background) job contract. When yieldable=True the
    # local agent may EVICT this slot to make room for a strictly-higher-
    # priority queued job that doesn't otherwise fit: it runs yield_command
    # (the job's save-and-sync hook, with WC_JOB_PID set to the job's
    # process-group leader), waits up to yield_grace_seconds for the process
    # to exit cleanly, SIGKILLs the group only if the grace is blown, then
    # requeues the job (running -> queue, state QUEUED, NOT failed). Resume
    # is the job's own business (checkpoint pull, server-side state, etc.).
    # yieldable is REFUSED at submit time unless yield_command is set — there
    # is no silent kill-and-lose-progress path. yield_count is bookkeeping;
    # after max_yields_before_protected yields the job stops being evictable
    # so a stream of high-priority work can't starve it forever.
    yieldable: bool = False
    yield_command: str = ""
    yield_grace_seconds: int = 120
    yield_count: int = 0
    max_yields_before_protected: int = 5

    def __post_init__(self):
        if not self.created_at:
            self.created_at = datetime.now(timezone.utc).isoformat()

    def to_dict(self) -> dict:
        return asdict(self)

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), indent=2)

    @classmethod
    def from_dict(cls, d: dict) -> Job:
        return cls(**{k: v for k, v in d.items() if k in cls.__dataclass_fields__})

    @classmethod
    def from_json(cls, s: str) -> Job:
        return cls.from_dict(json.loads(s))
