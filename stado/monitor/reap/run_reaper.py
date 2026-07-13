"""By-run reaper: removes per-job cruft once a run is fully terminal.

A run is reapable when none of its member jobs are in queue/ or running/.
On reap we snapshot the final completed/failed counts into the run
manifest (a single-writer mutation — only the reaper does this, so no
fleet contention), then delete the heavy per-job blobs and their status
dirs. The lightweight run manifest is kept as the permanent record, so
the queue stops accumulating thousands of orphaned per-job blobs.
"""
from __future__ import annotations

import json
import time

from ...queue.runs import read_run, run_status, list_runs, RUN_PREFIX

TERMINAL_PREFIXES = ("completed", "failed")


def _delete_status_dir(store, job_id: str) -> None:
    """Delete every blob under status/<job_id>/."""
    for path in store._list_paths(f"status/{job_id}/"):
        store._delete_blob(path)


def reap_terminal_runs(store, *, limit: int = 0) -> dict:
    """Reap all fully-terminal runs. Returns a summary dict.

    limit>0 caps how many runs are reaped this tick (bounds per-tick work
    on a large backlog); 0 means no cap.
    """
    reaped_runs = 0
    deleted_jobs = 0
    examined = 0
    for run_id in list_runs(store):
        manifest = read_run(store, run_id)
        if manifest is None or manifest.get("reaped_at"):
            continue
        examined += 1
        status = run_status(store, run_id)
        if status is None or not status["all_terminal"]:
            continue

        # Snapshot the outcome before the per-job blobs disappear.
        manifest["reaped_at"] = time.strftime(
            "%Y-%m-%dT%H:%M:%S+00:00", time.gmtime()
        )
        manifest["final_counts"] = status["counts"]
        store._upload_text(
            f"{RUN_PREFIX}/{run_id}.json", json.dumps(manifest, indent=2)
        )

        for jid in manifest["job_ids"]:
            for prefix in TERMINAL_PREFIXES:
                if store.read_job(prefix, jid) is not None:
                    store.delete_job(prefix, jid)
                    deleted_jobs += 1
            _delete_status_dir(store, jid)

        reaped_runs += 1
        if limit and reaped_runs >= limit:
            break

    return {
        "reaped_runs": reaped_runs,
        "deleted_jobs": deleted_jobs,
        "examined_runs": examined,
    }
