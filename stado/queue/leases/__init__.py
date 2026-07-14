"""Fenced provider-resource leases stored with backend compare-and-swap."""
from __future__ import annotations

import json
import re
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from uuid import uuid4

_MAX_LEASE_BYTES = int("65536")
_SAFE_JOB_ID = re.compile(r"^[A-Za-z0-9._-]+$")


class LeaseConflict(RuntimeError):
    """The lease changed or is owned by a live coordinator."""


class LeaseState(str, Enum):
    ALLOCATING = "allocating"
    PROVISIONING = "provisioning"
    READY = "ready"
    STARTING = "starting"
    RUNNING = "running"
    COLLECTING = "collecting"
    RELEASING = "releasing"
    RELEASED = "released"
    FAILED = "failed"


_ALLOWED_TRANSITIONS = {
    LeaseState.ALLOCATING: {LeaseState.PROVISIONING, LeaseState.FAILED},
    LeaseState.PROVISIONING: {LeaseState.READY, LeaseState.FAILED},
    LeaseState.READY: {LeaseState.STARTING, LeaseState.FAILED},
    LeaseState.STARTING: {LeaseState.RUNNING, LeaseState.FAILED},
    LeaseState.RUNNING: {LeaseState.COLLECTING, LeaseState.FAILED},
    LeaseState.COLLECTING: {LeaseState.RELEASING, LeaseState.FAILED},
    LeaseState.RELEASING: {LeaseState.RELEASED, LeaseState.FAILED},
    LeaseState.FAILED: {LeaseState.RELEASING, LeaseState.RELEASED},
    LeaseState.RELEASED: set(),
}


@dataclass
class ProviderLease:
    job_id: str
    provider: str
    owner_id: str
    fence_token: str
    state: str
    owner_expires_at: str
    resource_expires_at: str
    provider_resource_id: str = ""
    operation_id: str = ""
    operation_started_at: str = ""
    prompt_id: str = ""
    last_error: str = ""
    result_state: str = ""
    created_at: str = ""
    updated_at: str = ""
    version: str = field(default="", repr=False, compare=False)

    @classmethod
    def new(cls, job_id: str, provider: str, owner_id: str,
            owner_ttl_seconds: int, resource_ttl_seconds: int) -> "ProviderLease":
        now = datetime.now(timezone.utc)
        return cls(
            job_id=job_id,
            provider=provider,
            owner_id=owner_id,
            fence_token=uuid4().hex,
            state=LeaseState.ALLOCATING.value,
            owner_expires_at=(now + timedelta(seconds=owner_ttl_seconds)).isoformat(),
            resource_expires_at=(now + timedelta(seconds=resource_ttl_seconds)).isoformat(),
            created_at=now.isoformat(),
            updated_at=now.isoformat(),
        )

    def to_dict(self) -> dict[str, object]:
        value = asdict(self)
        value.pop("version", None)
        return value

    @classmethod
    def from_dict(cls, value: dict[str, object], version: str = "") -> "ProviderLease":
        fields = cls.__dataclass_fields__
        kwargs = {key: item for key, item in value.items() if key in fields and key != "version"}
        return cls(**kwargs, version=version)

    def owner_expired(self, now: datetime | None = None) -> bool:
        current = now or datetime.now(timezone.utc)
        expires = datetime.fromisoformat(self.owner_expires_at.replace("Z", "+00:00"))
        return current >= expires

    def assert_fence(self, owner_id: str, fence_token: str) -> None:
        if self.owner_id != owner_id or self.fence_token != fence_token or self.owner_expired():
            raise LeaseConflict("provider lease fence is no longer valid")

    def renew_owner(self, owner_id: str, fence_token: str, ttl_seconds: int) -> None:
        self.assert_fence(owner_id, fence_token)
        now = datetime.now(timezone.utc)
        self.owner_expires_at = (now + timedelta(seconds=ttl_seconds)).isoformat()
        self.updated_at = now.isoformat()

    def renew_resource(self, owner_id: str, fence_token: str, ttl_seconds: int) -> None:
        self.assert_fence(owner_id, fence_token)
        now = datetime.now(timezone.utc)
        self.resource_expires_at = (now + timedelta(seconds=ttl_seconds)).isoformat()
        self.updated_at = now.isoformat()

    def transition(self, state: LeaseState, owner_id: str, fence_token: str) -> None:
        self.assert_fence(owner_id, fence_token)
        current = LeaseState(self.state)
        if state not in _ALLOWED_TRANSITIONS[current]:
            raise ValueError(f"invalid provider lease transition {current.value} -> {state.value}")
        self.state = state.value
        self.updated_at = datetime.now(timezone.utc).isoformat()

    def takeover(self, owner_id: str, owner_ttl_seconds: int) -> None:
        if not self.owner_expired():
            raise LeaseConflict("provider lease owner is still live")
        now = datetime.now(timezone.utc)
        self.owner_id = owner_id
        self.fence_token = uuid4().hex
        self.owner_expires_at = (now + timedelta(seconds=owner_ttl_seconds)).isoformat()
        self.updated_at = now.isoformat()

    def relinquish(self, owner_id: str, fence_token: str) -> None:
        self.assert_fence(owner_id, fence_token)
        now = datetime.now(timezone.utc)
        self.owner_expires_at = now.isoformat()
        self.updated_at = now.isoformat()


class ProviderLeaseStore:
    """Conditional persistence over the configured JobStorage backend."""

    def __init__(self, job_storage: object):
        self.storage = job_storage

    def _require_conditional_backend(self) -> None:
        if (
            getattr(self.storage, "_azure_backend", None) is None
            and getattr(self.storage, "_sdk_bucket", None) is None
        ):
            raise RuntimeError(
                "provider leases require GCS SDK or Azure Blob conditional storage"
            )

    @staticmethod
    def _path(job_id: str) -> str:
        if not _SAFE_JOB_ID.fullmatch(job_id or ""):
            raise ValueError("job id is unsafe for provider lease storage")
        return f"provider-leases/{job_id}.json"

    @staticmethod
    def _encode(lease: ProviderLease) -> str:
        return json.dumps(lease.to_dict(), separators=(",", ":"), sort_keys=True)

    def load(self, job_id: str) -> ProviderLease | None:
        self._require_conditional_backend()
        value = self.storage.read_text_versioned(self._path(job_id))
        if value is None:
            return None
        return self._decode(value.content, value.version)

    @staticmethod
    def _decode(raw: str, version: str) -> ProviderLease:
        if len(raw.encode("utf-8")) > _MAX_LEASE_BYTES:
            raise RuntimeError("provider lease exceeded size bound")
        value = json.loads(raw)
        if not isinstance(value, dict):
            raise RuntimeError("provider lease is not an object")
        return ProviderLease.from_dict(value, version)

    def create(self, lease: ProviderLease) -> ProviderLease:
        self._require_conditional_backend()
        path = self._path(lease.job_id)
        if not self.storage.create_text_if_absent(path, self._encode(lease)):
            raise LeaseConflict("provider lease already exists")
        created = self.load(lease.job_id)
        if created is None:
            raise LeaseConflict("provider lease disappeared after creation")
        if created.to_dict() != lease.to_dict():
            raise LeaseConflict("provider lease changed before creation was confirmed")
        lease.version = created.version
        return lease

    def save(self, lease: ProviderLease, expected_version: str) -> ProviderLease:
        self._require_conditional_backend()
        if not expected_version or lease.version != expected_version:
            raise LeaseConflict("provider lease version was not read by this owner")
        try:
            new_version = self.storage.compare_and_swap_text(
                self._path(lease.job_id),
                expected_version,
                self._encode(lease),
            )
        except Exception as exc:
            from ..storage import StorageConflict

            if isinstance(exc, StorageConflict):
                raise LeaseConflict("provider lease changed concurrently") from None
            raise
        if not new_version:
            raise RuntimeError("conditional lease write did not return a version")
        lease.version = new_version
        return lease


    def acquire(self, job_id: str, provider: str, owner_id: str,
                owner_ttl_seconds: int, resource_ttl_seconds: int) -> ProviderLease:
        lease = ProviderLease.new(
            job_id, provider, owner_id, owner_ttl_seconds, resource_ttl_seconds
        )
        try:
            return self.create(lease)
        except LeaseConflict:
            current = self.load(job_id)
            if current is None:
                raise LeaseConflict("provider lease disappeared during acquisition")
            if current.job_id != job_id or current.provider != provider:
                raise LeaseConflict("provider lease identity mismatch")
            if not current.owner_expired():
                raise LeaseConflict("provider lease owner is still live")
            version = current.version
            current.takeover(owner_id, owner_ttl_seconds)
            return self.save(current, version)
