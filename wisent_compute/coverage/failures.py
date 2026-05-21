"""Bridge from wisent-compute `failed/` blob store -> coverage state.

The Job model does not (yet) carry a `coverage_universe_id` /
`coverage_group_key` field, so failures cannot be propagated to the
universe state file from inside the coordinator's running -> failed
transition. Until that field lands, `scan_failed_commands` provides
the back-reference: it iterates `gs://<bucket>/failed/*.json` and
returns `{command: {error, failed_at, job_id, batch_id}}`. The
coverage orchestrator can then match `UniverseEntry.command` back
to its prior failure verbatim.

`correlate_failures_into_state(universe, store, state)` walks the
universe, looks up each entry's command in the failed-command index,
and writes the error into `state[group_key].last_error`. The next
`verify` call then promotes that group_key to UNFIXABLE with the real
error string (instead of "") once `COVERAGE_ATTEMPT_CAP` is reached.

`record_failure(universe_id, group_key, error_text)` is the
forward-direction write API. A future coordinator hook calls it once
the Job model grows the required fields; until then it's available
for ad-hoc operator use.
"""
from __future__ import annotations

import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Iterable

from ..config import BUCKET, COVERAGE_VERIFY_THREADS
from ..queue.storage import JobStorage
from . import Universe, state_load, state_save

FAILED_PREFIX = "failed/"
ERROR_PREVIEW_MAX = 1024


def _load_failed_blob(store: JobStorage, path: str) -> dict | None:
    txt = store._download_text(path)
    if not txt:
        return None
    try:
        return json.loads(txt)
    except Exception:
        return None


def scan_failed_commands(
    store: JobStorage,
    command_prefix: str | None = None,
    threads: int = COVERAGE_VERIFY_THREADS,
) -> dict[str, dict]:
    """Return {command: most_recent_failure_record} from `gs://<bucket>/failed/`.

    If `command_prefix` is given, only failed blobs whose `.command`
    starts with that prefix are kept (cheap filter before downloading
    the full blob field set). The most-recent-by-failed_at wins on
    duplicate-command collisions; older retries are dropped.
    """
    paths = [
        info.name for info in store.list_blobs_with_meta(FAILED_PREFIX)
        if info.name.endswith(".json")
    ]
    out: dict[str, dict] = {}
    with ThreadPoolExecutor(max_workers=threads) as ex:
        futs = {ex.submit(_load_failed_blob, store, p): p for p in paths}
        for f in as_completed(futs):
            blob = f.result()
            if not blob:
                continue
            cmd = blob.get("command") or ""
            if not cmd:
                continue
            if command_prefix and not cmd.startswith(command_prefix):
                continue
            prev = out.get(cmd)
            ts = blob.get("failed_at") or ""
            if prev is None or (prev.get("failed_at") or "") < ts:
                out[cmd] = {
                    "error": (blob.get("error") or "")[:ERROR_PREVIEW_MAX],
                    "failed_at": ts,
                    "job_id": blob.get("job_id") or "",
                    "batch_id": blob.get("batch_id") or "",
                }
    return out


def correlate_failures_into_state(
    universe: Universe,
    store: JobStorage | None = None,
    state: dict | None = None,
    command_prefix: str | None = None,
) -> dict:
    """Pre-seed the universe's coverage state with last_error/last_failure_at
    pulled from the failed/ index. Returns the merged state dict (also
    persisted to GCS). Run this immediately before `verify` so the
    UNFIXABLE branch carries the real error rather than an empty string.
    """
    store = store or JobStorage(BUCKET)
    state = state if state is not None else state_load(store, universe.id)
    failed = scan_failed_commands(store, command_prefix=command_prefix)
    if not failed:
        return state
    matched = 0
    for entry in universe.iter_entries():
        rec = failed.get(entry.command)
        if rec is None:
            continue
        slot = state.setdefault(entry.group_key, {})
        slot["last_error"] = rec["error"]
        slot["last_failure_at"] = rec["failed_at"]
        slot["last_failed_job_id"] = rec["job_id"]
        slot["last_failed_batch_id"] = rec["batch_id"]
        matched += 1
    if matched:
        state_save(store, universe.id, state)
    return state


def record_failure(
    universe_id: str,
    group_key: str,
    error_text: str,
    store: JobStorage | None = None,
) -> None:
    """Forward write: record a job's terminal error against its universe state.

    Used by a future coordinator hook once the Job model carries
    coverage_universe_id / coverage_group_key fields. Idempotent under
    repeated calls for the same group_key (overwrites last_error).
    """
    store = store or JobStorage(BUCKET)
    state = state_load(store, universe_id)
    slot = state.setdefault(group_key, {})
    slot["last_error"] = (error_text or "")[:ERROR_PREVIEW_MAX]
    slot["last_failure_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    state_save(store, universe_id, state)


def matched_failed_jids_for_universe(
    universe: Universe,
    store: JobStorage | None = None,
    command_prefix: str | None = None,
) -> dict[str, str]:
    """Return {group_key: failed_job_id} for entries whose command has
    a matching failed/ blob. Diagnostic helper used by operator scripts
    to map UNFIXABLE group_keys back to their concrete failed jobs."""
    store = store or JobStorage(BUCKET)
    failed = scan_failed_commands(store, command_prefix=command_prefix)
    out: dict[str, str] = {}
    for entry in universe.iter_entries():
        rec = failed.get(entry.command)
        if rec:
            out[entry.group_key] = rec["job_id"]
    return out


def iter_failed_commands(
    store: JobStorage | None = None,
    command_prefix: str | None = None,
) -> Iterable[tuple[str, dict]]:
    """Yield (command, failure_record) pairs. Convenience wrapper around
    scan_failed_commands for callers that want a streaming iterator
    instead of a fully-materialized dict."""
    store = store or JobStorage(BUCKET)
    for cmd, rec in scan_failed_commands(store, command_prefix=command_prefix).items():
        yield cmd, rec
