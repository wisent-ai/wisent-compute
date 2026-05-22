"""Autonomous failure-fixer: failure -> Claude Code -> ship fix -> retry.

The loop, per the operator's spec:
  1. A job fails (lands in gs://wisent-compute/failed/<jid>.json)
  2. scan_new_failures() picks it up
  3. dispatch_fix() HMAC-POSTs the failure context to model-router's
     claude-code-subscription model
  4. Claude Code diagnoses, ships the fix to PyPI, resubmits
  5. Per-job state at gs://wisent-compute/failure_fixes/<jid>.json so
     the same job is not re-dispatched on subsequent scans

One dispatch per failed job_id. No fingerprint clustering, no
multi-failure grouping. Operator can layer that later as a cost
control if needed; the base loop is per-job.

Per-job state at gs://wisent-compute/failure_fixes/<job_id>.json
tracks attempts. After FAILURE_FIXER_ATTEMPT_CAP attempts the job is
marked EXHAUSTED and stops being re-dispatched.

The dispatch is HMAC-signed via WISENT_COMPUTE_AGENT_ID +
WISENT_COMPUTE_AGENT_AUTH_SECRET env vars (mirrors the convention
documented in content-platform/src/lib/api/model-router-hmac.ts).
"""
from __future__ import annotations

import hashlib
import hmac
import json
import os
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from typing import Iterator

from ..config import (
    BUCKET,
    FAILURE_FIX_PROMPT_ERROR_BYTES,
    FAILURE_FIXER_ATTEMPT_CAP,
    FAILURE_FIXER_STATE_PREFIX,
    MODEL_ROUTER_MODEL,
    MODEL_ROUTER_URL,
)
from ..queue.storage import JobStorage

EXHAUSTED = "exhausted"
DISPATCHED = "dispatched"
DISPATCH_FAILED = "dispatch_failed"
DRY_RUN = "dry_run"
ALREADY_DISPATCHED = "already_dispatched"


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
    """Build the structured prompt sent to Claude Code via model-router
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
        "Constraints: never introduce mocks, soft-defaults, or silent error "
        "absorption. Diagnose root cause and patch the cause; if the root "
        "is in an upstream dependency the wisent-compute team cannot patch, "
        "surface that clearly instead of inventing a workaround."
    )


def _hmac_headers(body: str) -> dict:
    """Sign body per content-platform/src/lib/api/model-router-hmac.ts:
        ts        = unix seconds
        bodyHash  = sha256(body).hex
        message   = `${agentId}:${ts}:${bodyHash}`
        signature = hmac_sha256(secret, message).hex
    """
    agent_id = os.environ.get("WISENT_COMPUTE_AGENT_ID", "")
    secret = os.environ.get("WISENT_COMPUTE_AGENT_AUTH_SECRET", "")
    if not agent_id or not secret:
        raise RuntimeError(
            "WISENT_COMPUTE_AGENT_ID and WISENT_COMPUTE_AGENT_AUTH_SECRET must "
            "be set to dispatch fix requests to model-router. Provision a row "
            "in trade_agents + trade_agent_secrets for the wisent-compute "
            "service identity (mirror the wisent-app row)."
        )
    ts = str(int(time.time()))
    body_hash = hashlib.sha256(body.encode("utf-8")).hexdigest()
    message = f"{agent_id}:{ts}:{body_hash}"
    sig = hmac.new(secret.encode("utf-8"), message.encode("utf-8"), hashlib.sha256).hexdigest()
    return {
        "x-agent-id": agent_id,
        "x-agent-timestamp": ts,
        "x-agent-signature": sig,
        "Content-Type": "application/json",
    }


def dispatch_fix(rec: FailureRecord, *, store: JobStorage | None = None, execute: bool = False) -> dict:
    """Post the fix prompt to model-router for ONE failed job. Returns
    a dispatch record dict. When execute=False, returns the dispatch
    payload without POSTing."""
    store = store or JobStorage(BUCKET)
    state = state_load(store, rec.job_id)
    attempts = state.get("attempts", 0)
    if attempts >= FAILURE_FIXER_ATTEMPT_CAP:
        return {"job_id": rec.job_id, "status": EXHAUSTED, "attempts": attempts}
    prompt = format_fix_prompt(rec)
    payload = json.dumps({
        "model": MODEL_ROUTER_MODEL,
        "messages": [{"role": "user", "content": prompt}],
        "max_tokens": 8192,
    })
    if not execute:
        return {
            "job_id": rec.job_id,
            "status": DRY_RUN,
            "attempts": attempts,
            "would_post_bytes": len(payload),
            "endpoint": f"{MODEL_ROUTER_URL}/v1/chat/completions",
            "prompt_preview": prompt[:500],
        }
    req = urllib.request.Request(
        f"{MODEL_ROUTER_URL}/v1/chat/completions",
        data=payload.encode("utf-8"),
        headers=_hmac_headers(payload),
        method="POST",
    )
    try:
        with urllib.request.urlopen(req) as r:
            resp_body = r.read().decode("utf-8", errors="replace")
            resp_status = r.status
    except urllib.error.HTTPError as e:
        resp_body = e.read().decode("utf-8", errors="replace") if e.fp else ""
        resp_status = e.code
    state["attempts"] = attempts + 1
    state["last_dispatched_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    state["last_response_status"] = resp_status
    state["last_response_preview"] = resp_body[:600]
    state["failed_at"] = rec.failed_at
    state["batch_id"] = rec.batch_id
    state["command"] = rec.command
    state_save(store, rec.job_id, state)
    return {
        "job_id": rec.job_id,
        "status": DISPATCHED if resp_status < 400 else DISPATCH_FAILED,
        "attempts": state["attempts"],
        "response_status": resp_status,
        "response_preview": resp_body[:300],
    }


def scan_and_dispatch(
    *,
    since_iso: str | None = None,
    execute: bool = False,
    store: JobStorage | None = None,
    skip_dispatched: bool = True,
) -> list:
    """One-shot orchestrator: scan failed/ -> dispatch one Claude Code
    session per UNHANDLED failed job. Returns a list of per-job
    dispatch records. `skip_dispatched=True` (default) reads state
    and skips jobs whose state file already shows attempts>0."""
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
