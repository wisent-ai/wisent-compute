"""GCS-backed global token buckets for HuggingFace API rate limits.

HF enforces two separate caps that a multi-agent fleet hits:
  - 1000 requests / 5-minute window (account-wide)
  - 128 repository commits / hour (per repo) — the figure HF returns in
    the 429 commit-rate body; confirmed live on wisent-ai/activations
    2026-05-24 when per-chunk uploads from 15 concurrent jobs blew past it.
Each cap is modeled as its own shared token bucket in a small GCS object,
updated with generation-match atomic CAS so every agent coordinates on
one counter. Callers block until a token is free, then proceed — i.e.
"if under the cap upload, else wait until the fleet is under the cap".

Usage:
    from wisent_compute.providers.local.hf_rate import (
        wait_for_hf_token, wait_for_hf_commit_token)
    wait_for_hf_commit_token()   # blocks until fleet < 128 commits/hr
    api.upload_folder(...)       # then commit
"""
from __future__ import annotations

import json
import os
import random
import time
from typing import Optional

_BUCKET_ENV = "WC_BUCKET"
_DEFAULT_BUCKET = "wisent-compute"
# Request bucket: HF 1000 requests / 5-minute window.
_RATE_OBJECT = "hf_rate/tokens.json"
_MAX_TOKENS = 1000
_REFILL_PER_SECOND = 200.0 / 60.0  # 200 tokens/min
# Commit bucket: HF 128 commits / hour per repo (HF's own 429 figure).
_COMMIT_OBJECT = "hf_rate/commit_tokens.json"
_COMMIT_MAX = 128
_COMMIT_REFILL_PER_SECOND = 128.0 / 3600.0  # 128 commits/hour
_POLL_BACKOFF_BASE = 0.5
_POLL_BACKOFF_MAX = 30.0


def _get_bucket():
    try:
        from google.cloud import storage as _gcs
    except Exception:
        return None
    bucket_name = os.environ.get(_BUCKET_ENV, _DEFAULT_BUCKET)
    try:
        return _gcs.Client().bucket(bucket_name)
    except Exception:
        return None


def _now() -> float:
    return time.time()


def _read_state(bucket, obj: str, max_tokens: int):
    blob = bucket.blob(obj)
    if not blob.exists():
        # Initialize full. Race-safe: concurrent inits both write
        # {tokens=max, refilled_at=now}; whichever lands second is fine.
        return None, {"tokens": max_tokens, "refilled_at": _now()}
    blob.reload()
    return blob.generation, json.loads(blob.download_as_text())


def _refill(state: dict, max_tokens: int, refill_per_sec: float) -> dict:
    now = _now()
    elapsed = max(0.0, now - float(state.get("refilled_at", now)))
    tokens = min(max_tokens, float(state.get("tokens", 0)) + elapsed * refill_per_sec)
    return {"tokens": tokens, "refilled_at": now}


def _acquire(obj: str, max_tokens: int, refill_per_sec: float,
             n: int, timeout: Optional[float]) -> None:
    """Block until n tokens in the named bucket are available + atomically
    deducted. Best-effort: no-op if GCS is unreachable, and on timeout it
    falls through so the caller proceeds (and retries on its own 429)
    rather than hard-blocking on infra failure."""
    bucket = _get_bucket()
    if bucket is None:
        return
    deadline = None if timeout is None else _now() + timeout
    attempt = 0
    while True:
        try:
            generation, state = _read_state(bucket, obj, max_tokens)
            state = _refill(state, max_tokens, refill_per_sec)
            if state["tokens"] >= n:
                state["tokens"] -= n
                blob = bucket.blob(obj)
                kw = {"if_generation_match": generation} if generation else {"if_generation_match": 0}
                blob.upload_from_string(json.dumps(state), **kw)
                return
            deficit = n - state["tokens"]
            wait = deficit / refill_per_sec
            wait = min(_POLL_BACKOFF_MAX, max(_POLL_BACKOFF_BASE, wait))
            wait += random.uniform(0, _POLL_BACKOFF_BASE)
        except Exception:
            attempt += 1
            wait = min(_POLL_BACKOFF_MAX, _POLL_BACKOFF_BASE * (2 ** min(attempt, 5)))
            wait += random.uniform(0, _POLL_BACKOFF_BASE)
        if deadline is not None and _now() >= deadline:
            return
        time.sleep(wait)


def wait_for_hf_token(n: int = 1, timeout: Optional[float] = 600.0) -> None:
    """Block until n request-tokens are free (HF 1000 requests / 5 min)."""
    _acquire(_RATE_OBJECT, _MAX_TOKENS, _REFILL_PER_SECOND, n, timeout)


def wait_for_hf_commit_token(n: int = 1, timeout: Optional[float] = 3600.0) -> None:
    """Block until n commit-tokens are free (HF 128 commits / hour / repo)."""
    _acquire(_COMMIT_OBJECT, _COMMIT_MAX, _COMMIT_REFILL_PER_SECOND, n, timeout)
