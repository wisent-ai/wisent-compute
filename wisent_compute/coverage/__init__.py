"""Generic job-completion coverage verifier + retry orchestrator.

Universe-agnostic. Nothing here knows about activation extraction,
training, eval, or any specific job type. Submitters register a
Universe via Python entry_points group `wisent_compute.coverage_universes`;
each Universe yields UniverseEntry tuples (group_key, command,
expected_uri) and supplies a Verifier. The VerifyAndRetry orchestrator
walks the universe, checks each expected output via its verifier, diffs
against current wisent-compute queue state, re-submits the gap subset
via submit_batch, and tracks per-group_key attempts on
gs://wisent-compute/<COVERAGE_STATE_PREFIX>/<universe_id>/state.json.

After COVERAGE_ATTEMPT_CAP attempts a group_key is marked UNFIXABLE
and surfaced but not re-submitted -- structural failures (unreachable
benchmark names, broken executor envs, etc.) need code fixes, not
retries.

Discovery via entry_points lets each consumer ship its own adapter
(activation extraction -> HF shard verifier; training runs ->
checkpoint-blob verifier; eval -> result-jsonl verifier) without this
module depending on any of them.
"""
from __future__ import annotations

import importlib.metadata as _md
import json
import time
import urllib.error
import urllib.request
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from typing import Iterator

from ..config import (
    BUCKET,
    COVERAGE_ATTEMPT_CAP,
    COVERAGE_HTTP_RETRY_CAP,
    COVERAGE_PROGRESS_LOG_EVERY,
    COVERAGE_STATE_PREFIX,
    COVERAGE_VERIFY_BACKOFF_BASE,
    COVERAGE_VERIFY_THREADS,
)
from ..queue.storage import JobStorage
from ..queue.submit import submit_batch, submit_job

PRESENT = "present"
MISSING = "missing"
UNFIXABLE = "unfixable"
ENTRY_POINT_GROUP = "wisent_compute.coverage_universes"


@dataclass(frozen=True)
class UniverseEntry:
    """One expected job in a coverage-bound batch.

    group_key uniquely identifies the entry within the universe (used
    as the state-file key); command is the shell command that would
    produce expected_uri when it succeeds; extra carries per-entry
    arguments forwarded to submit_batch on retry.
    """
    group_key: str
    command: str
    expected_uri: str
    extra: dict = field(default_factory=dict)


class Verifier(ABC):
    """Returns whether an expected output URI is present."""

    @abstractmethod
    def check(self, expected_uri: str) -> str:
        """Return PRESENT or MISSING. Raise on transport errors so the
        orchestrator can fail fast rather than silently retry."""


class URIExistsVerifier(Verifier):
    """HEAD against an http(s) URI; optional bearer token for HF/private."""

    def __init__(self, bearer_token: str = ""):
        self._token = bearer_token

    def check(self, expected_uri: str) -> str:
        headers = {"Authorization": f"Bearer {self._token}"} if self._token else {}
        req = urllib.request.Request(expected_uri, method="HEAD", headers=headers)
        for attempt in range(COVERAGE_HTTP_RETRY_CAP):
            try:
                with urllib.request.urlopen(req) as r:
                    return PRESENT if r.status < 400 else MISSING
            except urllib.error.HTTPError as e:
                if e.code == 404:
                    return MISSING
                if e.code == 429:
                    time.sleep(COVERAGE_VERIFY_BACKOFF_BASE ** attempt)
                    continue
                raise
        raise RuntimeError(f"HEAD {expected_uri}: retry-cap exceeded")


class GCSBlobExistsVerifier(Verifier):
    """Existence check for gs://<bucket>/<path> via JobStorage."""

    def __init__(self, store: JobStorage | None = None):
        self._store = store or JobStorage(BUCKET)

    def check(self, expected_uri: str) -> str:
        if not expected_uri.startswith("gs://"):
            raise ValueError(f"GCSBlobExistsVerifier expects gs:// URI, got {expected_uri}")
        rest = expected_uri[len("gs://"):]
        bucket, _, path = rest.partition("/")
        if bucket != self._store.bucket_name:
            raise ValueError(
                f"verifier bound to bucket {self._store.bucket_name} but URI is {bucket}"
            )
        txt = self._store._download_text(path)
        return PRESENT if txt is not None else MISSING


class Universe(ABC):
    """Submitter-defined contract describing the expected batch."""

    @property
    @abstractmethod
    def id(self) -> str:
        """Stable identifier for state-file scoping. URL-safe."""

    @abstractmethod
    def iter_entries(self) -> Iterator[UniverseEntry]:
        """Yield every (group_key, command, expected_uri) tuple."""

    @abstractmethod
    def verifier(self) -> Verifier:
        """Verifier used to check expected_uri for entries from this universe."""

    def submit_kwargs(self) -> dict:
        """Forwarded to submit_batch on retry. Override for provider/priority/etc."""
        return {}


def discover_universes() -> dict[str, type]:
    """Return {entry_point_name: Universe subclass} from registered entry
    points. Callers instantiate with their own kwargs (e.g. models, limit)
    so the same Universe class can be parametrized differently per run.
    """
    out: dict[str, type] = {}
    for ep in _md.entry_points(group=ENTRY_POINT_GROUP):
        cls = ep.load()
        if not (isinstance(cls, type) and issubclass(cls, Universe)):
            raise TypeError(
                f"entry point {ep.name} -> {cls!r} is not a Universe subclass"
            )
        out[ep.name] = cls
    return out


def _state_path(universe_id: str) -> str:
    return f"{COVERAGE_STATE_PREFIX}/{universe_id}/state.json"


def state_load(store: JobStorage, universe_id: str) -> dict:
    txt = store._download_text(_state_path(universe_id))
    return json.loads(txt) if txt else {}


def state_save(store: JobStorage, universe_id: str, state: dict) -> None:
    store._upload_text(
        _state_path(universe_id),
        json.dumps(state, indent=2, sort_keys=True),
    )


def _check_one(verifier: Verifier, entry: UniverseEntry) -> tuple[UniverseEntry, str]:
    return entry, verifier.check(entry.expected_uri)


@dataclass
class CoverageReport:
    universe_id: str
    total_entries: int
    present: int
    missing: int
    unfixable: list[tuple[str, str]]
    gaps: list[UniverseEntry]
    opaque: list[UniverseEntry] = field(default_factory=list)

    def as_dict(self) -> dict:
        return {
            "universe_id": self.universe_id,
            "total_entries": self.total_entries,
            "present": self.present,
            "missing": self.missing,
            "unfixable_count": len(self.unfixable),
            "gap_count": len(self.gaps),
            "opaque_count": len(self.opaque),
        }


def verify(
    universe: Universe,
    *,
    threads: int = COVERAGE_VERIFY_THREADS,
    state: dict | None = None,
    log=None,
) -> CoverageReport:
    """Walk the universe in parallel, classify each entry, build a report."""
    entries = list(universe.iter_entries())
    verifier = universe.verifier()
    state = state or {}
    present_n = 0
    gaps: list[UniverseEntry] = []
    unfix: list[tuple[str, str]] = []
    with ThreadPoolExecutor(max_workers=threads) as ex:
        futs = [ex.submit(_check_one, verifier, e) for e in entries]
        done = 0
        for f in as_completed(futs):
            entry, status = f.result()
            done += 1
            if log and done % COVERAGE_PROGRESS_LOG_EVERY == 0:
                log(f"[{universe.id}] {done}/{len(entries)}")
            if status == PRESENT:
                present_n += 1; continue
            attempts = state.get(entry.group_key, {}).get("attempts", 0)
            if attempts >= COVERAGE_ATTEMPT_CAP:
                last_err = state[entry.group_key].get("last_error", "")
                unfix.append((entry.group_key, last_err)); continue
            gaps.append(entry)
    return CoverageReport(
        universe_id=universe.id, total_entries=len(entries),
        present=present_n, missing=len(entries) - present_n,
        unfixable=unfix, gaps=gaps,
    )


def retry_gaps(
    universe: Universe,
    report: CoverageReport,
    *,
    state: dict,
    store: JobStorage,
    batch_label: str = "",
    log=None,
) -> int:
    """Submit each gap entry via submit_job so per-entry verify_command
    (from entry.extra) reaches the agent. submit_batch flattens kwargs
    across all jobs and cannot carry per-entry verify_command. Without
    it the agent marks the job COMPLETED on exit=0 even if no expected
    output was produced -- the verify_command is what enforces 'output
    actually exists' at job-completion time."""
    if not report.gaps:
        return 0
    batch_id = batch_label or f"coverage-retry-{universe.id}-{int(time.time())}"
    kwargs = universe.submit_kwargs()
    submitted = 0
    for g in report.gaps:
        submit_job(
            g.command, batch_id=batch_id, bucket=BUCKET,
            verify_command=g.extra.get("verify_command", ""),
            **kwargs,
        )
        submitted += 1
    now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    for entry in report.gaps:
        slot = state.setdefault(entry.group_key, {})
        slot["attempts"] = slot.get("attempts", 0) + 1
        slot["last_batch_id"] = batch_id
        slot["last_submitted_at"] = now
    state_save(store, universe.id, state)
    if log:
        log(f"[{universe.id}] submitted {submitted}/{len(report.gaps)} in batch {batch_id}")
    return submitted


def verify_and_retry(
    universe: Universe,
    *,
    execute: bool = False,
    threads: int = COVERAGE_VERIFY_THREADS,
    log=None,
) -> CoverageReport:
    """One-shot orchestrator: load state -> verify -> if execute, retry gaps."""
    store = JobStorage(BUCKET)
    state = state_load(store, universe.id)
    report = verify(universe, threads=threads, state=state, log=log)
    if execute:
        retry_gaps(universe, report, state=state, store=store, log=log)
    return report


def list_universes() -> list[str]:
    return sorted(discover_universes().keys())
