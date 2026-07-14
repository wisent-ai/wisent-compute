"""GCS persistence for Schedule objects under the schedules/ prefix.

Reuses JobStorage's prefix-agnostic blob helpers for the common paths.
The one special case is claim_due(): an atomic compare-and-set on
next_due_at via a GCS if_generation_match precondition, so two
overlapping Cloud-Function coordinator invocations can never double-fire
the same occurrence. The local daemon path is already single-writer
(coordinator.py enforces exactly one active coordinator), so it falls
back to a plain write when the SDK bucket isn't available.
"""
from __future__ import annotations

from .model import Schedule

PREFIX = "schedules"


def _path(schedule_id: str) -> str:
    return f"{PREFIX}/{schedule_id}.json"


def _read_fresh_text(store, path: str) -> str | None:
    """Fresh, generation-pinned read of `path`.

    A plain no-generation download on the wisent-compute bucket can return
    a stale (edge-cached) copy of an object that was just overwritten in
    place — confirmed live 2026-06-01: a schedule's next_due_at update read
    back as the OLD value via store._download_text even though the new
    generation was already the latest. The existing queue never hit this
    because it is write-once-then-delete; schedules overwrite the same blob
    every tick (read-modify-write of next_due_at), which is exactly the
    pattern the cache breaks. get_blob() fetches current metadata including
    the generation, and the subsequent download pins to it, so the bytes
    are guaranteed to be the latest. Falls back to the plain read on the
    gsutil/Azure paths where no SDK bucket is available.
    """
    if store._sdk_bucket is None:
        return store._download_text(path)
    blob = store._sdk_bucket.get_blob(path)
    if blob is None:
        return None
    return blob.download_as_text()


def list_schedule_ids(store) -> list[str]:
    out = []
    for name in store._list_paths(f"{PREFIX}/"):
        base = name.rsplit("/", 1)[-1]
        if base.endswith(".json"):
            out.append(base[: -len(".json")])
    return out


def read_schedule(store, schedule_id: str) -> Schedule | None:
    data = _read_fresh_text(store, _path(schedule_id))
    return Schedule.from_json(data) if data else None


def list_schedules(store) -> list[Schedule]:
    out = []
    for sid in list_schedule_ids(store):
        s = read_schedule(store, sid)
        if s is not None:
            out.append(s)
    return out


def write_schedule(store, sched: Schedule) -> None:
    store._upload_text(_path(sched.schedule_id), sched.to_json())


def delete_schedule(store, schedule_id: str) -> bool:
    if read_schedule(store, schedule_id) is None:
        return False
    store._delete_blob(_path(schedule_id))
    return True


def _current_generation(store, schedule_id: str):
    """GCS object generation for the schedule blob, or None when the SDK
    bucket isn't in play (gsutil/Azure path → no CAS, single-writer)."""
    if store._sdk_bucket is None:
        return None
    blob = store._sdk_bucket.blob(_path(schedule_id))
    if not blob.exists():
        return None
    blob.reload()
    return blob.generation


def claim_due(store, sched: Schedule, new_next_due_at: str) -> bool:
    """Advance `sched.next_due_at` to `new_next_due_at` and persist, but
    only if no other writer has touched the blob since it was read.

    Returns True if THIS caller won the claim (and should now submit the
    job), False if a concurrent coordinator already advanced it. On the
    non-SDK path there is no contention, so it always claims.
    """
    sched.next_due_at = new_next_due_at
    if store._sdk_bucket is None:
        store._upload_text(_path(sched.schedule_id), sched.to_json())
        return True
    gen = _current_generation(store, sched.schedule_id)
    blob = store._sdk_bucket.blob(_path(sched.schedule_id))
    try:
        # if_generation_match=0 means "only if it does not yet exist";
        # a real generation means "only if unchanged since read".
        blob.upload_from_string(
            sched.to_json(),
            if_generation_match=(gen if gen is not None else 0),
        )
        return True
    except Exception as exc:  # google.api_core.exceptions.PreconditionFailed
        from google.api_core.exceptions import PreconditionFailed
        if isinstance(exc, PreconditionFailed):
            return False
        raise
