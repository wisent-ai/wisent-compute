"""Autonomous failure-fixer: failure -> Claude Code -> ship fix -> retry.

Closes the diagnostic loop the operator called out: every job that
exits to failed/ has its full traceback sitting in
gs://wisent-compute/failed/<jid>.json, but until now nothing read it
and nothing dispatched a remediation. This module walks failed/ on
demand (or on each coordinator tick), fingerprints failures by
stack-trace tail so identical-root-cause failures cluster, and POSTs
the failure context to model-router's claude-code-subscription so a
Claude Code session diagnoses the root cause, ships the code fix (via
the existing wisent-tools / wisent-compute PyPI publish path), and
resubmits the failed jobs.

Per-fingerprint state lives at
gs://wisent-compute/failure_fixes/<fingerprint>.json with attempts +
last dispatch timestamp + last response summary. After
FAILURE_FIXER_ATTEMPT_CAP attempts the fingerprint is marked
EXHAUSTED and the fixer stops dispatching it.

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
from dataclasses import dataclass, field
from typing import Iterator

from ..config import (
    BUCKET,
    FAILURE_FINGERPRINT_TAIL_BYTES,
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


@dataclass
class FailureRecord:
    """One failed job's relevant fields, parsed from failed/<jid>.json."""
    job_id: str
    batch_id: str
    command: str
    error: str
    failed_at: str
    fingerprint: str


@dataclass
class FingerprintGroup:
    """All FailureRecords with the same stack-trace tail."""
    fingerprint: str
    records: list = field(default_factory=list)

    @property
    def count(self) -> int:
        return len(self.records)

    @property
    def latest(self) -> FailureRecord:
        return max(self.records, key=lambda r: r.failed_at)


def fingerprint_failure(error_text: str) -> str:
    """sha256 over the NORMALIZED root-cause signature: the final
    Exception line + the bottom-most `File "...", line N, in func`
    frame, with file paths stripped to basenames. This clusters by
    root cause across jobs that differ only in workdir / task name /
    prompt format / pid. When no Python traceback markers are found
    (e.g. shell crash, OOM kill, exit-code-only failure), the raw
    tail substring is used so distinct shell-level failures still
    cluster correctly.
    """
    import re as _re
    text = error_text or ""
    exc_matches = _re.findall(r"^[A-Za-z_][A-Za-z0-9_]*Error:[^\n]*", text, _re.MULTILINE)
    exc_line = exc_matches[-1] if exc_matches else ""
    file_matches = _re.findall(
        r'File "([^"]+)", line (\d+), in (\S+)', text
    )
    if file_matches:
        path, lineno, func = file_matches[-1]
        basename = path.rsplit("/", 1)[-1]
        frame = f"{basename}:{lineno}:{func}"
    else:
        frame = ""
    if exc_line or frame:
        signature = f"{exc_line}||{frame}"
    else:
        signature = text[-FAILURE_FINGERPRINT_TAIL_BYTES:]
    return hashlib.sha256(signature.encode("utf-8", errors="replace")).hexdigest()[:16]


def _parse_failed_blob(store: JobStorage, name: str) -> FailureRecord | None:
    txt = store._download_text(name)
    if not txt:
        return None
    try:
        blob = json.loads(txt)
    except Exception:
        return None
    err = (blob.get("error") or "")
    return FailureRecord(
        job_id=blob.get("job_id") or "",
        batch_id=blob.get("batch_id") or "",
        command=blob.get("command") or "",
        error=err,
        failed_at=blob.get("failed_at") or "",
        fingerprint=fingerprint_failure(err),
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


def group_by_fingerprint(records: list) -> dict:
    """Bucket records by fingerprint."""
    out: dict[str, FingerprintGroup] = {}
    for r in records:
        out.setdefault(r.fingerprint, FingerprintGroup(fingerprint=r.fingerprint)).records.append(r)
    return out


def _state_path(fingerprint: str) -> str:
    return f"{FAILURE_FIXER_STATE_PREFIX}/{fingerprint}.json"


def state_load(store: JobStorage, fingerprint: str) -> dict:
    txt = store._download_text(_state_path(fingerprint))
    return json.loads(txt) if txt else {}


def state_save(store: JobStorage, fingerprint: str, state: dict) -> None:
    store._upload_text(_state_path(fingerprint), json.dumps(state, indent=2, sort_keys=True))


def format_fix_prompt(group: FingerprintGroup, max_error_chars: int = FAILURE_FIX_PROMPT_ERROR_BYTES) -> str:
    """Build the structured prompt sent to Claude Code via model-router."""
    latest = group.latest
    sample = group.records[:3]
    sample_summary = "\n".join(
        f"- job_id={r.job_id} batch_id={r.batch_id} failed_at={r.failed_at}" for r in sample
    )
    err = latest.error[:max_error_chars]
    return (
        "You are the wisent-compute autonomous failure-fixer.\n"
        f"A wisent-compute job batch has {group.count} jobs in failed/ with "
        f"the same root cause (fingerprint {group.fingerprint}).\n\n"
        "Diagnose the root cause from the traceback below, ship the fix to the "
        "appropriate repo (wisent / wisent-tools / wisent-compute), publish the "
        "patched package to PyPI, then resubmit the failed jobs via "
        "`wc submit <command> --verify <verify_command>`. The fleet's local "
        "agents drift-pick-up the new PyPI release on their next loop; cloud "
        "agents self-terminate on drift so a fresh VM with the new version "
        "claims the resubmitted jobs.\n\n"
        f"Sample failing job command (one of {group.count} with same fingerprint):\n"
        f"  {latest.command}\n\n"
        f"Sample failed_at timestamps:\n{sample_summary}\n\n"
        f"Traceback tail (last {max_error_chars} chars of stderr):\n"
        f"---BEGIN TRACEBACK---\n{err}\n---END TRACEBACK---\n\n"
        "Constraints: never introduce mocks, soft-defaults, or silent error "
        "absorption. Diagnose root cause and patch the cause; if the root is "
        "in an upstream dependency the wisent-compute team cannot patch, "
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


def dispatch_fix(group: FingerprintGroup, *, store: JobStorage | None = None, execute: bool = False) -> dict:
    """Post the fix prompt to model-router. Returns a dispatch record dict.
    When execute=False, returns the dispatch payload without POSTing."""
    store = store or JobStorage(BUCKET)
    state = state_load(store, group.fingerprint)
    attempts = state.get("attempts", 0)
    if attempts >= FAILURE_FIXER_ATTEMPT_CAP:
        return {
            "fingerprint": group.fingerprint,
            "status": EXHAUSTED,
            "attempts": attempts,
            "last_error": state.get("last_error", ""),
        }
    prompt = format_fix_prompt(group)
    payload = json.dumps({
        "model": MODEL_ROUTER_MODEL,
        "messages": [{"role": "user", "content": prompt}],
        "max_tokens": 8192,
    })
    if not execute:
        return {
            "fingerprint": group.fingerprint,
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
    state["last_group_count"] = group.count
    state["last_sample_job_id"] = group.latest.job_id
    state_save(store, group.fingerprint, state)
    return {
        "fingerprint": group.fingerprint,
        "status": DISPATCHED if resp_status < 400 else DISPATCH_FAILED,
        "attempts": state["attempts"],
        "response_status": resp_status,
        "response_preview": resp_body[:300],
    }


def scan_and_dispatch(*, since_iso: str | None = None, execute: bool = False, store: JobStorage | None = None) -> list:
    """One-shot orchestrator: scan failed/ -> group -> dispatch new fingerprints.
    Returns a list of per-fingerprint dispatch records."""
    store = store or JobStorage(BUCKET)
    records = list(scan_new_failures(store, since_iso=since_iso))
    groups = group_by_fingerprint(records)
    return [dispatch_fix(g, store=store, execute=execute) for g in groups.values()]
