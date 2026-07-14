"""Bounded HTTP transport for Box Public API v1."""
from __future__ import annotations

import json
import socket
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Callable, Iterable

from ._types import (
    DEFAULT_BOX_API_URL, DEFAULT_TIMEOUT_SECONDS, MAX_JSON_BYTES, ONE,
    TRANSIENT_HTTP, BoxAPIError, BoxConfigurationError, BoxTransportError,
    required_dict, safe_text,
)


class BoxHTTPTransport:
    """Validated transport with no import-time or construction-time requests."""

    def __init__(self, api_key: str, base_url: str = DEFAULT_BOX_API_URL,
                 timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS,
                 opener: Callable[..., Any] | None = None):
        key = (api_key or "").strip()
        if not key:
            raise BoxConfigurationError("BOX_API_KEY is required for Box provider")
        parsed = urllib.parse.urlsplit(base_url.rstrip("/"))
        if parsed.scheme != "https" or not parsed.netloc or parsed.query or parsed.fragment:
            raise BoxConfigurationError("BOX_API_URL must be an HTTPS API base without query or fragment")
        self._api_key = key
        self._base_url = urllib.parse.urlunsplit(
            (parsed.scheme, parsed.netloc, parsed.path.rstrip("/"), "", "")
        )
        self._timeout = float(timeout_seconds)
        if self._timeout <= float("0"):
            raise BoxConfigurationError("Box request timeout must be positive")
        self._opener = opener or urllib.request.urlopen

    def _url(self, path: str, query: dict[str, object] | None = None) -> str:
        clean = "/".join(
            urllib.parse.quote(segment, safe="")
            for segment in path.strip("/").split("/") if segment
        )
        url = f"{self._base_url}/{clean}" if clean else self._base_url
        pairs = [] if not query else [
            (key, str(value)) for key, value in query.items()
            if value is not None and value != ""
        ]
        return f"{url}?{urllib.parse.urlencode(pairs)}" if pairs else url

    @staticmethod
    def _read_bounded(response: Any, limit: int) -> bytes:
        raw = response.read(limit + ONE)
        if len(raw) > limit:
            raise BoxTransportError("Box response exceeded configured size bound")
        return raw

    def request(self, method: str, path: str, body: dict[str, Any] | None = None,
                query: dict[str, object] | None = None,
                expected_types: Iterable[str] = (), binary: bool = False,
                max_bytes: int = MAX_JSON_BYTES) -> Any:
        headers = {"Authorization": f"Bearer {self._api_key}", "Accept": "application/json"}
        data = None
        if body is not None:
            data = json.dumps(body, separators=(",", ":")).encode("utf-8")
            headers["Content-Type"] = "application/json"
        request = urllib.request.Request(
            self._url(path, query), data=data, headers=headers, method=method
        )
        try:
            with self._opener(request, timeout=self._timeout) as response:
                raw = self._read_bounded(response, max_bytes)
        except urllib.error.HTTPError as exc:
            self._raise_http_error(exc)
        except (urllib.error.URLError, TimeoutError, socket.timeout, OSError) as exc:
            kind = safe_text(type(exc).__name__, "transport_error")
            raise BoxTransportError(f"Box transport failed: {kind}") from None
        if binary:
            return raw
        payload = self._parse_json(raw)
        if payload.get("ok") is not True:
            raise BoxTransportError("Box success response lacks ok=true")
        expected = frozenset(expected_types)
        if expected and payload.get("type") not in expected:
            raise BoxTransportError("Box response has an unexpected type")
        return payload

    @staticmethod
    def _parse_json(raw: bytes) -> dict[str, Any]:
        if not raw:
            return {}
        try:
            return required_dict(json.loads(raw.decode("utf-8")), "JSON")
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise BoxTransportError("Box returned invalid JSON") from exc

    @staticmethod
    def _raise_http_error(exc: urllib.error.HTTPError) -> None:
        try:
            raw = exc.read(MAX_JSON_BYTES + ONE)
        except Exception:
            raw = b""
        if len(raw) > MAX_JSON_BYTES:
            raw = b""
        try:
            payload = required_dict(json.loads(raw.decode("utf-8")), "error") if raw else {}
        except Exception:
            payload = {}
        error = payload.get("error") if isinstance(payload.get("error"), dict) else {}
        raise BoxAPIError(
            exc.code,
            str(payload.get("code") or error.get("code") or "http_error"),
            str(payload.get("message") or error.get("message") or "Box API request failed"),
            str(payload.get("requestId") or ""),
            exc.code in TRANSIENT_HTTP,
        ) from None
