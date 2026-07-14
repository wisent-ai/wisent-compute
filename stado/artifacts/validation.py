"""Backend-independent artifact manifest validation."""
from __future__ import annotations

import re
from urllib.parse import parse_qsl, urlsplit

from .models import ArtifactManifest

_ALLOWED_SCHEMES = {"az", "gs", "hf", "https"}
_SENSITIVE_QUERY_KEY = re.compile(
    r"(^|[-_])(access[-_]?token|api[-_]?key|credential|password|secret|signature|sig|token)($|[-_])",
    re.IGNORECASE,
)
_HEX_SHA256 = re.compile(r"^[0-9a-f]{64}$")


def validate_manifest(manifest: ArtifactManifest) -> tuple[str, ...]:
    issues: list[str] = []
    if manifest.schema_version != 1:
        issues.append(f"unsupported schema_version: {manifest.schema_version}")
    if not manifest.title.strip():
        issues.append("title is required")
    if not manifest.locations:
        issues.append("at least one location is required")
    if manifest.dependencies and manifest.ref in manifest.dependencies:
        issues.append("artifact cannot depend on itself")

    primary = 0
    for index, location in enumerate(manifest.locations):
        prefix = f"locations[{index}]"
        if location.role == "primary":
            primary += 1
        parsed = urlsplit(location.uri)
        if parsed.scheme not in _ALLOWED_SCHEMES:
            issues.append(f"{prefix}.uri uses unsupported scheme: {parsed.scheme or '<none>'}")
        if parsed.username or parsed.password:
            issues.append(f"{prefix}.uri must not embed credentials")
        for key, _ in parse_qsl(parsed.query, keep_blank_values=True):
            if _SENSITIVE_QUERY_KEY.search(key):
                issues.append(f"{prefix}.uri contains sensitive query field: {key}")
        if location.sha256 and not _HEX_SHA256.fullmatch(location.sha256.lower()):
            issues.append(f"{prefix}.sha256 must contain 64 hexadecimal characters")
        if location.size_bytes is not None and location.size_bytes < 0:
            issues.append(f"{prefix}.size_bytes cannot be negative")
        if location.file_count is not None and location.file_count < 0:
            issues.append(f"{prefix}.file_count cannot be negative")
    if primary != 1:
        issues.append("exactly one primary location is required")

    for key, value in manifest.labels.items():
        if not key or len(key) > 128 or len(value) > 512:
            issues.append("labels must have non-empty keys <=128 and values <=512 characters")
    return tuple(issues)
