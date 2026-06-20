"""The 'run' primitive: the tracking entity above a job.

One `wc submit` invocation = one run. Written once to runs/<run_id>.json
with the member job_ids and submitter provenance, then never mutated
(static manifest — avoids GCS read-modify-write contention across the
fleet). Run status is *derived* from the members' current prefixes, so
"is run X done?" costs O(run size) targeted reads instead of an
O(whole-queue) scan.
"""
from __future__ import annotations

import json
import os
import time
import uuid

RUN_PREFIX = "runs"
TERMINAL_PREFIXES = ("completed", "failed")
ALL_PREFIXES = ("queue", "running", "completed", "failed")
SUBMITTER_APP_KEY = "submitter_app"
MAX_RUN_NAME_TASKS = 3


def _dict_value(data: dict, key: str, default):
    return data[key] if key in data else default


def generate_run_id() -> str:
    return f"run-{int(time.time())}-{uuid.uuid4().hex[:8]}"


def derive_run_name(commands) -> str:
    """Auto-derive a readable name from the run's commands: shared
    module + model + the distinct --task values (or a count if many)."""
    modules, models, tasks = set(), set(), []
    for c in commands:
        toks = c.split()
        for i, t in enumerate(toks):
            nxt = toks[i + 1] if i + 1 < len(toks) else ""
            if t == "-m":
                modules.add(nxt.split(".")[-1])
            elif t == "--model":
                models.add(nxt.strip("'\"").split("/")[-1])
            elif t == "--task":
                tasks.append(nxt)
    parts = []
    if len(modules) == 1:
        parts.append(next(iter(modules)))
    if len(models) == 1:
        parts.append(next(iter(models)))
    uniq = list(dict.fromkeys(tasks))
    if 1 <= len(uniq) <= MAX_RUN_NAME_TASKS:
        parts.append("+".join(uniq))
    elif uniq:
        parts.append(f"{len(uniq)}tasks")
    parts.append(f"{len(commands)}jobs")
    return ":".join(parts)


def write_run_manifest(store, run_id, name, submitter_app, submitted_by,
                       submitted_from, commands, job_ids) -> None:
    """Write the immutable run manifest. Called once after all member
    jobs are queued so job_ids is complete. name: explicit WC_RUN_NAME
    if set, else auto-derived from commands."""
    manifest = {
        "run_id": run_id,
        "name": name or derive_run_name(commands),
        "created_at": time.strftime("%Y-%m-%dT%H:%M:%S+00:00", time.gmtime()),
        "submitter_app": submitter_app or "manual",
        "submitted_by": submitted_by,
        "submitted_from": submitted_from,
        "n_jobs": len(job_ids),
        "job_ids": list(job_ids),
        "commands": list(commands),
    }
    store._upload_text(
        f"{RUN_PREFIX}/{run_id}.json", json.dumps(manifest, indent=2)
    )


def read_run(store, run_id) -> dict | None:
    raw = store._download_text(f"{RUN_PREFIX}/{run_id}.json")
    return json.loads(raw) if raw else None


def _job_state(store, job_id) -> str | None:
    """Which prefix currently holds this job_id, or None if absent."""
    for prefix in ALL_PREFIXES:
        if store.read_job(prefix, job_id) is not None:
            return prefix
    return None


def run_status(store, run_id) -> dict | None:
    """Derive per-state counts for a run from its members' current
    prefixes. Returns None if the run manifest does not exist."""
    manifest = read_run(store, run_id)
    if manifest is None:
        return None
    counts = {p: 0 for p in ALL_PREFIXES}
    missing = 0
    for jid in manifest["job_ids"]:
        st = _job_state(store, jid)
        if st is None:
            missing += 1
        else:
            counts[st] += 1
    terminal = counts["completed"] + counts["failed"]
    in_flight = counts["queue"] + counts["running"]
    return {
        "run_id": run_id,
        "submitter_app": _dict_value(manifest, SUBMITTER_APP_KEY, ""),
        "n_jobs": manifest["n_jobs"],
        "counts": counts,
        "missing": missing,
        "in_flight": in_flight,
        "all_terminal": in_flight == 0 and missing == 0 and terminal > 0,
    }


def list_runs(store) -> list[str]:
    paths = store._list_paths(f"{RUN_PREFIX}/")
    out = []
    for p in paths:
        name = p.rsplit("/", 1)[-1]
        if name.endswith(".json"):
            out.append(name[:-5])
    return out
