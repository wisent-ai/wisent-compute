"""Live re-submission tracking.

JobStorage.move_job calls on_transition() after every state move. When the
moved job carries re_submission_of and the destination is a terminal state
(completed/uploaded/failed), this writes
  gs://wisent-compute/{fixed,failed_again}/<orig_jid>.json
keyed on the ORIGINAL failed job id. The tracker then answers
"is original X fixed?" via a single GCS-list diff:
  still_broken = list(failed/) - list(fixed/)
instead of a full per-blob rescan.

Never raises into the agent loop — a marker write failure is logged but
must not crash the state transition.
"""
from __future__ import annotations

import json
import sys


_TERMINAL_TO_MARKER = {
    "completed": "fixed",
    "uploaded":  "fixed",
    "failed":    "failed_again",
}


def on_transition(store, job, to_prefix: str) -> None:
    """Write a tombstone if the move terminates a re-submitted job."""
    orig = getattr(job, "re_submission_of", "") or ""
    if not orig:
        return
    marker_prefix = _TERMINAL_TO_MARKER.get(to_prefix)
    if marker_prefix is None:
        return
    body = json.dumps({
        "orig_jid":  orig,
        "new_jid":   getattr(job, "job_id", "") or "",
        "new_state": to_prefix,
        "batch_id":  getattr(job, "batch_id", "") or "",
        "ts": (getattr(job, "completed_at", "") or
               getattr(job, "failed_at", "") or ""),
    })
    try:
        store._upload_text(f"{marker_prefix}/{orig}.json", body)
    except Exception as exc:
        sys.stderr.write(f"[tombstone] write failed for {orig}: {exc!r}\n")
        sys.stderr.flush()
