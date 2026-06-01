"""Schedule data model and cron next-due computation.

A Schedule is a recurring job spec — NOT a Job. It is the frozen submit
payload (command + sizing/routing kwargs) plus a 5-field cron expression
and firing bookkeeping. The coordinator tick evaluates each enabled
schedule's `next_due_at` and submits a fresh job when it comes due.

Stored at gs://<bucket>/schedules/<schedule_id>.json. Lifecycle and CLI
live in store.py / fire.py / cli (wc schedule …).
"""
from __future__ import annotations

import json
import os
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone


def generate_schedule_id() -> str:
    """8-hex id, namespaced so a schedule id is never confused with a
    job id in logs (`sch-<hex>`)."""
    return f"sch-{os.urandom(4).hex()}"


def compute_next_due(cron: str, after_utc: datetime, tz: str = "UTC") -> datetime:
    """First cron occurrence strictly after `after_utc`, returned as a
    timezone-aware UTC datetime.

    The cron is interpreted in `tz` (so "0 2 * * *" means 02:00 in that
    zone, DST included), then converted back to UTC for storage. croniter
    preserves the tzinfo of its base datetime, so we hand it a base
    already converted into the schedule's zone.
    """
    from croniter import croniter
    try:
        from zoneinfo import ZoneInfo
        zone = ZoneInfo(tz)
    except Exception:
        zone = timezone.utc
    if after_utc.tzinfo is None:
        after_utc = after_utc.replace(tzinfo=timezone.utc)
    base_local = after_utc.astimezone(zone)
    nxt_local = croniter(cron, base_local).get_next(datetime)
    return nxt_local.astimezone(timezone.utc)


def cron_is_valid(cron: str) -> bool:
    """True iff `cron` parses as a croniter expression."""
    try:
        from croniter import croniter
        return bool(croniter.is_valid(cron))
    except Exception:
        return False


@dataclass
class Schedule:
    schedule_id: str
    cron: str
    command: str
    enabled: bool = True
    tz: str = "UTC"
    # ---- frozen submit kwargs (mirror submit_job's GCS-path params) ----
    provider: str = "gcp"
    gpu_type: str = ""
    vram_gb: int = 0
    machine_type: str = ""
    priority: int = 0
    preemptible: bool = False
    max_cost_per_hour_usd: float = 0.0
    pin_to_provider: bool = False
    repo: str = ""
    repo_workdir: str = ""
    repo_extras: str = "train"
    pre_command: str = ""
    apt_packages: list = field(default_factory=list)
    output_uri: str = ""
    verify_command: str = ""
    exclusive: bool = False
    # ---- firing bookkeeping ----
    created_at: str = ""
    created_by: str = ""
    next_due_at: str = ""       # ISO-8601 UTC; "" disables firing
    last_fired_at: str | None = None
    last_run_id: str = ""
    last_job_id: str = ""       # most recent fire's job_id (overlap-skip check)
    fire_count: int = 0
    # skip: do not fire while last_job_id is still in queue/ or running/.
    # allow: fire regardless of prior instance.
    overlap_policy: str = "skip"
    # skip: a coordinator-downtime gap collapses to a single fire and
    #   next_due_at jumps to the next future occurrence.
    # each: not yet honored beyond skip — reserved (documented in fire.py).
    catchup_policy: str = "skip"

    def __post_init__(self):
        if not self.created_at:
            self.created_at = datetime.now(timezone.utc).isoformat()

    def submit_kwargs(self) -> dict:
        """The kwargs to hand submit_job for one fire of this schedule."""
        return dict(
            provider=self.provider,
            gpu_type=self.gpu_type,
            vram_gb=self.vram_gb,
            machine_type=self.machine_type,
            priority=self.priority,
            preemptible=self.preemptible,
            max_cost_per_hour_usd=self.max_cost_per_hour_usd,
            pin_to_provider=self.pin_to_provider,
            repo=self.repo,
            repo_workdir=self.repo_workdir,
            repo_extras=self.repo_extras,
            pre_command=self.pre_command,
            apt_packages=list(self.apt_packages or []),
            output_uri=self.output_uri,
            verify_command=self.verify_command,
            exclusive=self.exclusive,
        )

    def to_dict(self) -> dict:
        return asdict(self)

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), indent=2)

    @classmethod
    def from_dict(cls, d: dict) -> "Schedule":
        return cls(**{k: v for k, v in d.items() if k in cls.__dataclass_fields__})

    @classmethod
    def from_json(cls, s: str) -> "Schedule":
        return cls.from_dict(json.loads(s))
