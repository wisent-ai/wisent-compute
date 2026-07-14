"""Typed operations for Box Public API v1."""
from __future__ import annotations

import time
from typing import Any, Iterable

from ._http import BoxHTTPTransport
from ._types import (
    BOX_ID_PATTERN, HTTP_NOT_FOUND, BoxAPIError, BoxCommandResult,
    BoxEventPage, BoxInfo, BoxLimits, BoxPromptRun, BoxTransportError,
    parse_box_info,
)


class BoxClient(BoxHTTPTransport):
    """Box lifecycle, command, file, event, artifact, and prompt operations."""

    @staticmethod
    def validate_box_id(box_id: str) -> str:
        if not BOX_ID_PATTERN.fullmatch(box_id or ""):
            raise ValueError("invalid Box id")
        return box_id

    def limits(self) -> BoxLimits:
        value = self.request("GET", "/limits", expected_types=("limits.info",))
        return BoxLimits(
            can_start=bool(value.get("canStart")),
            active_boxes=int(value.get("activeBoxes") or "0"),
            max_active_boxes=int(value.get("maxActiveBoxes") or "0"),
            billing_status=str(value.get("billingStatus") or ""),
            blocked_reason=str(value.get("startBlockedReason") or value.get("blockedReason") or ""),
            credit_balance_seconds=int(value.get("creditBalanceSeconds") or "0"),
        )

    def create_box(self, ttl_seconds: int | None, no_env: bool = True) -> BoxInfo:
        value = self.request(
            "POST", "/boxes", {"ttlSeconds": ttl_seconds, "noEnv": bool(no_env)},
            expected_types=("box.created",),
        )
        return parse_box_info(value)

    def get_box(self, box_id: str) -> BoxInfo:
        value = self.request(
            "GET", f"/boxes/{self.validate_box_id(box_id)}",
            expected_types=("box.info", "box.get"),
        )
        return parse_box_info(value)

    def list_boxes(self) -> list[BoxInfo]:
        boxes: list[BoxInfo] = []
        cursor = ""
        while True:
            value = self.request(
                "GET", "/boxes", query={"cursor": cursor, "sort": "asc"},
                expected_types=("box.list",),
            )
            rows = value.get("boxes") or []
            if not isinstance(rows, list):
                raise BoxTransportError("Box list response has invalid boxes")
            boxes.extend(parse_box_info(row) for row in rows)
            page = value.get("pageInfo") if isinstance(value.get("pageInfo"), dict) else {}
            if not page.get("hasMore"):
                return boxes
            cursor = str(page.get("nextCursor") or "")
            if not cursor:
                raise BoxTransportError("Box list pagination omitted next cursor")

    def update_box(self, box_id: str, *, name: str | None = None,
                   ttl_seconds: int | None | object = ...) -> BoxInfo:
        body: dict[str, Any] = {}
        if name is not None:
            body["name"] = name
        if ttl_seconds is not ...:
            body["ttlSeconds"] = ttl_seconds
        if not body:
            raise ValueError("Box update requires at least one field")
        value = self.request(
            "PATCH", f"/boxes/{self.validate_box_id(box_id)}", body,
            expected_types=("box.updated", "box.info"),
        )
        return parse_box_info(value)

    def stop_box(self, box_id: str) -> dict[str, Any]:
        return self.request(
            "POST", f"/boxes/{self.validate_box_id(box_id)}/stop",
            expected_types=("box.stopping", "box.action"),
        )

    def resume_box(self, box_id: str, no_env: bool | None = None) -> dict[str, Any]:
        body = {"noEnv": no_env} if no_env is not None else None
        return self.request(
            "POST", f"/boxes/{self.validate_box_id(box_id)}/resume", body,
            expected_types=("box.resuming", "box.action"),
        )

    def fork_box(self, box_id: str, no_env: bool | None = None) -> BoxInfo:
        body = {"noEnv": no_env} if no_env is not None else None
        value = self.request(
            "POST", f"/boxes/{self.validate_box_id(box_id)}/fork", body,
            expected_types=("box.forking",),
        )
        return parse_box_info(value)

    def delete_box(self, box_id: str) -> None:
        try:
            self.request(
                "DELETE", f"/boxes/{self.validate_box_id(box_id)}",
                expected_types=("box.deleted",),
            )
        except BoxAPIError as exc:
            if exc.status != HTTP_NOT_FOUND:
                raise

    def execute_command(self, box_id: str, command: str, *, cwd: str = "",
                        timeout_seconds: int = int("30")) -> BoxCommandResult:
        if not command:
            raise ValueError("Box command is required")
        if timeout_seconds < int("1") or timeout_seconds > int("60"):
            raise ValueError("Box command timeout is outside API bounds")
        body: dict[str, Any] = {"command": command, "timeoutSeconds": timeout_seconds}
        if cwd:
            body["cwd"] = cwd
        value = self.request(
            "POST", f"/boxes/{self.validate_box_id(box_id)}/commands", body,
            expected_types=("command.finished",),
        )
        return BoxCommandResult(
            success=bool(value.get("success")),
            exit_code=value.get("exitCode") if isinstance(value.get("exitCode"), int) else None,
            signal=str(value.get("signal") or ""),
            stdout=str(value.get("stdout") or ""),
            stderr=str(value.get("stderr") or ""),
            stdout_truncated=bool(value.get("stdoutTruncated")),
            stderr_truncated=bool(value.get("stderrTruncated")),
            timed_out=bool(value.get("timedOut")),
        )

    def read_file(self, box_id: str, path: str, encoding: str = "utf8") -> dict[str, Any]:
        return self.request(
            "GET", f"/boxes/{self.validate_box_id(box_id)}/files",
            query={"path": path, "encoding": encoding}, expected_types=("file.read",),
        )

    def write_file(self, box_id: str, path: str, content: str,
                   encoding: str = "utf8") -> dict[str, Any]:
        return self.request(
            "PUT", f"/boxes/{self.validate_box_id(box_id)}/files",
            {"path": path, "content": content, "encoding": encoding},
            expected_types=("file.written", "file.write"),
        )

    def download_artifact(self, box_id: str, path: str, max_bytes: int) -> bytes:
        if max_bytes <= int("0"):
            raise ValueError("artifact max_bytes must be positive")
        return self.request(
            "GET", f"/boxes/{self.validate_box_id(box_id)}/artifacts",
            query={"path": path}, binary=True, max_bytes=max_bytes,
        )

    def list_events(self, box_id: str, *, cursor: str = "", limit: int = int("100"),
                    sort: str = "asc", event_type: str = "") -> BoxEventPage:
        value = self.request(
            "GET", f"/boxes/{self.validate_box_id(box_id)}/events",
            query={"cursor": cursor, "limit": limit, "sort": sort, "type": event_type},
            expected_types=("events.list",),
        )
        events = value.get("events") or []
        if not isinstance(events, list) or not all(isinstance(event, dict) for event in events):
            raise BoxTransportError("Box events response has invalid events")
        page = value.get("pageInfo") if isinstance(value.get("pageInfo"), dict) else {}
        return BoxEventPage(
            tuple(events), str(page.get("nextCursor") or ""), bool(page.get("hasMore"))
        )

    def configure_ssh_key(self, box_id: str, public_key: str) -> dict[str, Any]:
        if not public_key.startswith("ssh-"):
            raise ValueError("OpenSSH public key is required")
        return self.request(
            "POST", f"/boxes/{self.validate_box_id(box_id)}/sshkey", {"key": public_key},
            expected_types=("sshkey.configured", "box.sshkey"),
        )

    def prompt(self, box_id: str, prompt: str, *, provider: str,
               model: str = "", reasoning_effort: str = "") -> BoxPromptRun:
        body: dict[str, Any] = {"provider": provider, "prompt": prompt}
        if model:
            body["model"] = model
        if reasoning_effort:
            body["reasoningEffort"] = reasoning_effort
        value = self.request(
            "POST", f"/boxes/{self.validate_box_id(box_id)}/prompt", body,
            expected_types=("prompt.queued",),
        )
        prompt_run = value.get("promptRun")
        if not isinstance(prompt_run, dict):
            raise BoxTransportError("Box prompt response omitted promptRun")
        prompt_id = str(value.get("promptId") or prompt_run.get("promptId") or "")
        if not prompt_id or str(prompt_run.get("promptId") or "") != prompt_id:
            raise BoxTransportError("Box prompt response has an invalid prompt id")
        return BoxPromptRun(prompt_id, str(prompt_run.get("status") or "queued"),
                            bool(prompt_run.get("done")), prompt_run)

    def prompt_status(self, box_id: str, prompt_id: str) -> BoxPromptRun:
        value = self.request(
            "GET", f"/boxes/{self.validate_box_id(box_id)}/prompts/{prompt_id}",
            expected_types=("prompt.run",),
        )
        prompt_run = value.get("promptRun")
        if not isinstance(prompt_run, dict):
            raise BoxTransportError("Box prompt status omitted promptRun")
        returned_id = str(prompt_run.get("promptId") or "")
        if returned_id != prompt_id:
            raise BoxTransportError("Box prompt status id mismatch")
        status = str(prompt_run.get("status") or "")
        if status not in {"sending", "queued", "running", "finished", "failed"}:
            raise BoxTransportError("Box prompt status is invalid")
        return BoxPromptRun(prompt_id, status, bool(prompt_run.get("done")), prompt_run)

    def interrupt(self, box_id: str) -> dict[str, Any]:
        return self.request(
            "POST", f"/boxes/{self.validate_box_id(box_id)}/interrupt",
            expected_types=("box.interrupted",),
        )

    def wait_for_state(self, box_id: str, states: Iterable[str], *,
                       deadline_seconds: float, poll_seconds: float = float("1")) -> BoxInfo:
        wanted = frozenset(states)
        deadline = time.monotonic() + deadline_seconds
        while True:
            info = self.get_box(box_id)
            if info.state in wanted:
                return info
            remaining = deadline - time.monotonic()
            if remaining <= float("0"):
                raise BoxTransportError("timed out waiting for Box state")
            time.sleep(min(poll_seconds, remaining))
