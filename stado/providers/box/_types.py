"""Bounded Box API value objects and redacted failures."""
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

DEFAULT_BOX_API_URL = "https://ascii.dev/api/box/v1"
DEFAULT_TIMEOUT_SECONDS = float("70")
MAX_JSON_BYTES = int("65536")
ONE = int("1")
HTTP_NOT_FOUND = int("404")
TRANSIENT_HTTP = frozenset({int("429"), int("500"), int("502"), int("503"), int("504")})
BOX_ID_PATTERN = re.compile(r"^bx_[23456789abcdefghjkmnpqrstuvwxyz]{8}$")
BOX_KEY_PATTERN = re.compile(r"box_[A-Za-z0-9_-]+")
URL_TOKEN_PATTERN = re.compile(r"([?&](?:_token|token|key|access_token)=)[^&\s]+", re.IGNORECASE)


class BoxConfigurationError(RuntimeError):
    """Local Box integration configuration is invalid."""


class BoxTransportError(RuntimeError):
    """A bounded network or response transport failure."""


class BoxAPIError(RuntimeError):
    """Structured, redacted Box API failure."""

    def __init__(self, status: int, code: str, message: str,
                 request_id: str = "", retryable: bool = False):
        self.status = int(status)
        self.code = safe_text(code, "box_error")
        self.message = safe_text(message, "Box API request failed")
        self.request_id = safe_text(request_id, "")
        self.retryable = bool(retryable)
        super().__init__(self.__str__())

    def __str__(self) -> str:
        suffix = f" request_id={self.request_id}" if self.request_id else ""
        return f"Box API HTTP {self.status} [{self.code}]: {self.message}{suffix}"

    def to_record(self) -> dict[str, object]:
        return {
            "status": self.status,
            "code": self.code,
            "message": self.message,
            "request_id": self.request_id,
            "retryable": self.retryable,
        }


@dataclass(frozen=True)
class BoxInfo:
    box_id: str
    name: str
    state: str
    ip: str = ""
    url: str = ""
    subdomain: str = ""
    created_at: str = ""
    updated_at: str = ""
    archive_after: str = ""
    snapshot_available: bool = False
    snapshot_completed_at: str = ""
    last_snapshot_attempt_at: str = ""
    last_snapshot_status: str = ""


@dataclass(frozen=True)
class BoxLimits:
    can_start: bool
    active_boxes: int
    max_active_boxes: int
    billing_status: str
    blocked_reason: str = ""
    credit_balance_seconds: int = int("0")


@dataclass(frozen=True)
class BoxCommandResult:
    success: bool
    exit_code: int | None
    signal: str
    stdout: str
    stderr: str
    stdout_truncated: bool
    stderr_truncated: bool
    timed_out: bool


@dataclass(frozen=True)
class BoxPromptRun:
    prompt_id: str
    status: str
    done: bool
    raw: dict[str, Any]


@dataclass(frozen=True)
class BoxEventPage:
    events: tuple[dict[str, Any], ...]
    next_cursor: str
    has_more: bool


def safe_text(value: object, default_text: str, limit: int = int("512")) -> str:
    text = str(value or default_text).replace("\r", " ").replace("\n", " ")
    text = BOX_KEY_PATTERN.sub("[REDACTED]", text)
    text = URL_TOKEN_PATTERN.sub(r"\1[REDACTED]", text)
    text = re.sub(r"(?i)authorization\s*[:=]\s*[^,;\s]+", "Authorization=[REDACTED]", text)
    return text[:limit]


def required_dict(value: object, context: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise BoxTransportError(f"Box {context} response is not an object")
    return value


def parse_box_info(payload: dict[str, Any]) -> BoxInfo:
    box = required_dict(payload.get("box", payload), "box")
    box_id = str(box.get("id") or "")
    if not BOX_ID_PATTERN.fullmatch(box_id):
        raise BoxTransportError("Box response contains an invalid box id")
    return BoxInfo(
        box_id=box_id,
        name=str(box.get("name") or ""),
        state=str(box.get("state") or ""),
        ip=str(box.get("ip") or ""),
        url=str(box.get("url") or ""),
        subdomain=str(box.get("subdomain") or ""),
        created_at=str(box.get("createdAt") or ""),
        updated_at=str(box.get("updatedAt") or ""),
        archive_after=str(box.get("archiveAfter") or ""),
        snapshot_available=bool(box.get("snapshotAvailable")),
        snapshot_completed_at=str(box.get("snapshotCompletedAt") or ""),
        last_snapshot_attempt_at=str(box.get("lastSnapshotAttemptAt") or ""),
        last_snapshot_status=str(box.get("lastSnapshotStatus") or ""),
    )
