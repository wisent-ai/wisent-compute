"""Job scheduler: pick queued jobs and create instances."""
from __future__ import annotations

import sys
from datetime import datetime, timezone

from ..config import MAX_SCHEDULE_PER_TICK, INSTANCE_PREFIX
from ..models import Job, JobState
from ..queue.storage import JobStorage
from ..providers.base import Provider
from .quota import get_available_slots


def _log(msg):
    sys.stderr.write(f"[scheduler] {msg}\n")
    sys.stderr.flush()


def schedule_queued_jobs(
    store: JobStorage,
    provider: Provider,
    provider_name: str,
    secrets: dict,
) -> int:
    """Pick queued jobs that fit available GPU slots, create instances."""
    available = get_available_slots(store, provider, provider_name)
    _log(f"Available slots: {available}")

    if all(v == 0 for v in available.values()):
        _log("No GPU slots available")
        return 0

    queued = store.list_jobs("queue")
    queued.sort(key=lambda j: j.created_at)

    scheduled = 0
    for job in queued:
        if scheduled >= MAX_SCHEDULE_PER_TICK:
            _log(f"Hit per-tick cap ({MAX_SCHEDULE_PER_TICK})")
            break

        accel = job.gpu_type
        if not accel:
            accel = ""

        # CPU jobs (no GPU) always schedule
        if not accel:
            pass
        elif available.get(accel, 0) <= 0:
            continue

        # Download startup script
        script = store.download_script(job.job_id)

        # Inject secrets into script
        for key, val in secrets.items():
            script = script.replace(f"${{{key}}}", val)

        instance_name = f"{INSTANCE_PREFIX}-{job.job_id}"
        ref = provider.create_instance(
            name=instance_name,
            machine_type=job.machine_type,
            accel_type=accel,
            boot_disk_gb=job.boot_disk_gb,
            image=job.image,
            image_project=job.image_project,
            startup_script=script,
        )

        if ref is None:
            _log(f"Failed to create instance for {job.job_id}")
            continue

        job.instance_ref = ref
        job.state = JobState.RUNNING.value
        job.started_at = datetime.now(timezone.utc).isoformat()
        store.move_job(job, "queue", "running")

        if accel:
            available[accel] = available.get(accel, 0) - 1
        scheduled += 1
        _log(f"Scheduled {job.job_id} on {ref}")

    return scheduled
