"""Autonomous failure-fixer: failure -> local Claude Code CLI -> ship fix -> retry.

The loop, per the operator's spec:
  1. A job fails (lands in gs://wisent-compute/failed/<jid>.json)
  2. scan_new_failures() picks it up
  3. dispatch_fix() exec's the local `claude` CLI with the fix prompt
  4. Claude Code diagnoses, ships the fix to PyPI, resubmits
  5. Per-job state at gs://wisent-compute/failure_fixes/<jid>.json so
     the same job is not re-dispatched on subsequent scans

One dispatch per failed job_id. No fingerprint clustering.

Authentication is via the local `claude` CLI's OAuth credentials
(maintained by wisent-claude-reauth on the mac mini). No
model-router POST, no HMAC, no trade_agents shoehorn. The fixer is
designed to run as a LaunchAgent on a machine that ALREADY has the
Claude Code CLI installed and an active OAuth session.

After FAILURE_FIXER_ATTEMPT_CAP attempts on the same job the job is
marked EXHAUSTED and stops being re-dispatched so a permanently-
broken job does not burn unlimited Claude session budget.
"""
from __future__ import annotations

import json
import shutil
import subprocess
import time
from dataclasses import dataclass
from typing import Iterator

from ..config import (
    BUCKET,
    FAILURE_FIX_PROMPT_ERROR_BYTES,
    FAILURE_FIXER_ATTEMPT_CAP,
    FAILURE_FIXER_STATE_PREFIX,
)
from ..queue.storage import JobStorage

EXHAUSTED = "exhausted"
DISPATCHED = "dispatched"
DISPATCH_FAILED = "dispatch_failed"
DRY_RUN = "dry_run"
ALREADY_DISPATCHED = "already_dispatched"
CLAUDE_NOT_FOUND = "claude_cli_not_found"


@dataclass
class FailureRecord:
    """One failed job's relevant fields, parsed from failed/<jid>.json."""
    job_id: str
    batch_id: str
    command: str
    error: str
    failed_at: str


def _parse_failed_blob(store: JobStorage, name: str) -> FailureRecord | None:
    txt = store._download_text(name)
    if not txt:
        return None
    try:
        blob = json.loads(txt)
    except Exception:
        return None
    return FailureRecord(
        job_id=blob.get("job_id") or "",
        batch_id=blob.get("batch_id") or "",
        command=blob.get("command") or "",
        error=blob.get("error") or "",
        failed_at=blob.get("failed_at") or "",
    )


def scan_new_failures(store: JobStorage, since_iso: str | None = None) -> Iterator[FailureRecord]:
    """Yield FailureRecords for every failed/<jid>.json whose failed_at
    is >= since_iso (ISO-8601 lexicographic compare). since_iso=None
    means scan every failed blob."""
    for info in store.list_blobs_with_meta("failed/"):
        if not info.name.endswith(".json"):
            continue
        rec = _parse_failed_blob(store, info.name)
        if rec is None:
            continue
        if since_iso and rec.failed_at < since_iso:
            continue
        yield rec


def _state_path(job_id: str) -> str:
    return f"{FAILURE_FIXER_STATE_PREFIX}/{job_id}.json"


def state_load(store: JobStorage, job_id: str) -> dict:
    txt = store._download_text(_state_path(job_id))
    return json.loads(txt) if txt else {}


def state_save(store: JobStorage, job_id: str, state: dict) -> None:
    store._upload_text(_state_path(job_id), json.dumps(state, indent=2, sort_keys=True))


def format_fix_prompt(rec: FailureRecord, max_error_chars: int = FAILURE_FIX_PROMPT_ERROR_BYTES) -> str:
    """Build the structured prompt passed to the local Claude Code CLI
    for ONE failed job."""
    err = rec.error[:max_error_chars]
    return (
        "You are the wisent-compute autonomous failure-fixer.\n"
        f"A wisent-compute job (job_id={rec.job_id} batch_id={rec.batch_id}) "
        f"failed at {rec.failed_at}.\n\n"
        "Diagnose the root cause from the traceback below, ship the fix to "
        "the appropriate repo (wisent / wisent-tools / wisent-compute), "
        "publish the patched package to PyPI, then resubmit this exact "
        "command via `wc submit <command> --verify <verify_command>`. The "
        "fleet's local agents drift-pick-up the new PyPI release on their "
        "next loop; cloud agents self-terminate on drift so a fresh VM with "
        "the new version claims the resubmitted job.\n\n"
        f"Failed command:\n  {rec.command}\n\n"
        f"Traceback (last {max_error_chars} chars of stderr):\n"
        f"---BEGIN TRACEBACK---\n{err}\n---END TRACEBACK---\n\n"
        "Constraints: never introduce mocks, soft-defaults, or silent "
        "error absorption. Diagnose root cause and patch the cause; if "
        "the root is in an upstream dependency the wisent-compute team "
        "cannot patch, surface that clearly instead of inventing a "
        "workaround."
    )


def _claude_bin() -> str | None:
    """Locate the local `claude` CLI binary. Returns None if not on PATH."""
    return shutil.which("claude")


def dispatch_fix(rec: FailureRecord, *, store: JobStorage | None = None, execute: bool = False) -> dict:
    """Exec the local `claude` CLI with the fix prompt for ONE failed job.
    Returns a dispatch record dict. When execute=False, returns the
    dispatch payload without exec'ing."""
    store = store or JobStorage(BUCKET)
    state = state_load(store, rec.job_id)
    attempts = state.get("attempts", 0)
    if attempts >= FAILURE_FIXER_ATTEMPT_CAP:
        return {"job_id": rec.job_id, "status": EXHAUSTED, "attempts": attempts}
    prompt = format_fix_prompt(rec)
    claude = _claude_bin()
    if not execute:
        return {
            "job_id": rec.job_id,
            "status": DRY_RUN,
            "attempts": attempts,
            "claude_bin": claude or "(not found on PATH)",
            "prompt_bytes": len(prompt),
            "prompt_preview": prompt[:500],
        }
    if claude is None:
        return {
            "job_id": rec.job_id,
            "status": CLAUDE_NOT_FOUND,
            "attempts": attempts,
            "error": "`claude` CLI not on PATH. Install Claude Code and "
                     "complete OAuth before running the failure-fixer.",
        }
    proc = subprocess.run(
        [claude, "-p", prompt],
        capture_output=True, text=True,
    )
    state["attempts"] = attempts + 1
    state["last_dispatched_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    state["last_returncode"] = proc.returncode
    state["last_stdout_preview"] = (proc.stdout or "")[:600]
    state["last_stderr_preview"] = (proc.stderr or "")[:600]
    state["failed_at"] = rec.failed_at
    state["batch_id"] = rec.batch_id
    state["command"] = rec.command
    state_save(store, rec.job_id, state)
    return {
        "job_id": rec.job_id,
        "status": DISPATCHED if proc.returncode == 0 else DISPATCH_FAILED,
        "attempts": state["attempts"],
        "returncode": proc.returncode,
        "stdout_preview": (proc.stdout or "")[:300],
    }


def scan_and_dispatch(
    *,
    since_iso: str | None = None,
    execute: bool = False,
    store: JobStorage | None = None,
    skip_dispatched: bool = True,
) -> list:
    """One-shot orchestrator: scan failed/ -> exec local `claude` per
    UNHANDLED failed job. Returns a list of per-job dispatch records.
    `skip_dispatched=True` (default) reads state and skips jobs whose
    state file already shows attempts>0."""
    store = store or JobStorage(BUCKET)
    out: list = []
    for rec in scan_new_failures(store, since_iso=since_iso):
        if skip_dispatched:
            prior = state_load(store, rec.job_id)
            if prior.get("attempts", 0) > 0:
                out.append({"job_id": rec.job_id, "status": ALREADY_DISPATCHED,
                            "attempts": prior["attempts"]})
                continue
        out.append(dispatch_fix(rec, store=store, execute=execute))
    return out
