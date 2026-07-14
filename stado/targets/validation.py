"""Validation for the canonical compute-target registry."""
from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

_REGISTRY_VERSION = 2
_TARGET_NAME_RE = re.compile(r"^[a-z0-9](?:[a-z0-9._-]*[a-z0-9])?$")
_ACTION_RE = re.compile(r"^[a-z0-9_]+$")
_VALID_KINDS = frozenset({"local", "gcp", "vast"})


class RegistryValidationError(ValueError):
    """Raised when a registry does not satisfy the version 2 contract."""


def normalize_hostname(value: str) -> str:
    """Return the canonical form used for host identity comparisons."""
    return value.strip().lower().rstrip(".")

def ssh_hostname(value: str) -> str:
    """Extract and normalize a hostname from a legacy SSH destination."""
    host_and_port = value.strip().rsplit("@", 1)[-1]
    if host_and_port.startswith("["):
        closing = host_and_port.find("]")
        host = host_and_port[1:closing] if closing > 1 else ""
    else:
        host = host_and_port.split(":", 1)[0]
    return normalize_hostname(host)


def _fail(location: str, message: str) -> None:
    raise RegistryValidationError(f"{location}: {message}")


def _validate_action_list(value: Any, location: str) -> None:
    if not isinstance(value, list):
        _fail(location, "must be an array")
    seen: set[str] = set()
    for index, action in enumerate(value):
        item_location = f"{location}[{index}]"
        if not isinstance(action, str) or not action or action != action.strip():
            _fail(item_location, "must be a non-empty string without surrounding whitespace")
        if action != "*" and not _ACTION_RE.fullmatch(action):
            _fail(item_location, "must be '*' or an exact lowercase action identifier")
        if action in seen:
            _fail(item_location, f"duplicate action {action!r}")
        seen.add(action)
    if "*" in seen and len(seen) != 1:
        _fail(location, "wildcard '*' must be the only action")


def _require_int(value: Any, location: str, minimum: int, maximum: int | None = None) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        _fail(location, "must be an integer")
    if value < minimum or (maximum is not None and value > maximum):
        upper = f" and <= {maximum}" if maximum is not None else ""
        _fail(location, f"must be >= {minimum}{upper}")
    return value


def _validate_disk_cleanup(value: Any, location: str) -> None:
    if not isinstance(value, dict):
        _fail(location, "must be an object")
    required = {
        "mode", "check_interval_seconds", "low_free_gb", "target_free_gb",
        "max_bytes_per_pass", "max_items_per_pass", "max_scan_items", "cleaners",
    }
    if set(value) != required:
        _fail(location, f"must contain exactly {sorted(required)!r}")
    if value["mode"] not in {"off", "report", "enforce"}:
        _fail(f"{location}.mode", "must be one of 'off', 'report', or 'enforce'")
    _require_int(value["check_interval_seconds"], f"{location}.check_interval_seconds", 60, 86400)
    low = _require_int(value["low_free_gb"], f"{location}.low_free_gb", 1)
    target = _require_int(value["target_free_gb"], f"{location}.target_free_gb", 1)
    if target <= low:
        _fail(f"{location}.target_free_gb", "must be greater than low_free_gb")
    _require_int(value["max_bytes_per_pass"], f"{location}.max_bytes_per_pass", 1024 ** 2, 1024 ** 4)
    max_items = _require_int(value["max_items_per_pass"], f"{location}.max_items_per_pass", 1, 10000)
    max_scan = _require_int(value["max_scan_items"], f"{location}.max_scan_items", 1, 100000)
    if max_scan < max_items:
        _fail(f"{location}.max_scan_items", "must be >= max_items_per_pass")
    cleaners = value["cleaners"]
    if not isinstance(cleaners, dict):
        _fail(f"{location}.cleaners", "must be an object")
    allowed = {"huggingface_cache", "weles_recordings"}
    unknown = set(cleaners) - allowed
    if unknown:
        _fail(f"{location}.cleaners", f"unknown cleaners {sorted(unknown)!r}")
    for name, cleaner in cleaners.items():
        cleaner_location = f"{location}.cleaners.{name}"
        if not isinstance(cleaner, dict) or set(cleaner) != {"min_age_seconds"}:
            _fail(cleaner_location, "must contain exactly 'min_age_seconds'")
        minimum = 3600 if name == "huggingface_cache" else 86400
        _require_int(cleaner["min_age_seconds"], f"{cleaner_location}.min_age_seconds", minimum)

def _target_identities(target: dict[str, Any], location: str) -> list[tuple[str, str]]:
    identities: list[tuple[str, str]] = []
    name = target["name"]
    identities.append((normalize_hostname(name), f"{location}.name"))

    hostnames = target.get("hostnames", [])
    if not isinstance(hostnames, list):
        _fail(f"{location}.hostnames", "must be an array")
    for index, hostname in enumerate(hostnames):
        item_location = f"{location}.hostnames[{index}]"
        if not isinstance(hostname, str):
            _fail(item_location, "must be a string")
        normalized = normalize_hostname(hostname)
        if not normalized:
            _fail(item_location, "must not be empty")
        if hostname != normalized:
            _fail(item_location, f"must be normalized as {normalized!r}")
        if any(character.isspace() for character in normalized) or "@" in normalized or "/" in normalized:
            _fail(item_location, "must be a hostname, not a URL or SSH destination")
        identities.append((normalized, item_location))

    ssh = target.get("ssh")
    if ssh is not None:
        if not isinstance(ssh, str):
            _fail(f"{location}.ssh", "must be a string or null")
        ssh_identity = ssh_hostname(ssh)
        if not ssh_identity:
            _fail(f"{location}.ssh", "must include a host")
        identities.append((ssh_identity, f"{location}.ssh"))
    return identities


def validate_registry(data: Any) -> dict[str, Any]:
    """Validate and return a registry-v2 document without modifying it."""
    if not isinstance(data, dict):
        _fail("registry", "must be an object")
    if data.get("schema_version") != _REGISTRY_VERSION or isinstance(data.get("schema_version"), bool):
        _fail("registry.schema_version", f"must be {_REGISTRY_VERSION}")

    targets = data.get("targets")
    if not isinstance(targets, list):
        _fail("registry.targets", "must be an array")

    names: set[str] = set()
    identities: dict[str, str] = {}
    for index, target in enumerate(targets):
        location = f"registry.targets[{index}]"
        if not isinstance(target, dict):
            _fail(location, "must be an object")

        name = target.get("name")
        if not isinstance(name, str) or not _TARGET_NAME_RE.fullmatch(name):
            _fail(f"{location}.name", "must be a lowercase target identifier")
        if name in names:
            _fail(f"{location}.name", f"duplicate target name {name!r}")
        names.add(name)

        kind = target.get("kind")
        if kind not in _VALID_KINDS:
            _fail(f"{location}.kind", f"must be one of {sorted(_VALID_KINDS)!r}")

        if "weles" in target:
            weles = target["weles"]
            if kind != "local":
                _fail(f"{location}.weles", "is allowed only for kind='local'")
            if not isinstance(weles, dict):
                _fail(f"{location}.weles", "must be an object")
            if set(weles) != {"enabled", "actions"}:
                _fail(f"{location}.weles", "must contain exactly 'enabled' and 'actions'")
            if not isinstance(weles["enabled"], bool):
                _fail(f"{location}.weles.enabled", "must be a boolean")
            _validate_action_list(weles["actions"], f"{location}.weles.actions")

        if "disk_cleanup" in target:
            if kind != "local":
                _fail(f"{location}.disk_cleanup", "is allowed only for kind='local'")
            _validate_disk_cleanup(target["disk_cleanup"], f"{location}.disk_cleanup")
        for identity, identity_location in _target_identities(target, location):
            previous = identities.get(identity)
            if previous is not None:
                _fail(identity_location, f"host identity {identity!r} is already declared by {previous}")
            identities[identity] = identity_location

    return data


def validate_registry_file(path: str | Path) -> dict[str, Any]:
    """Load and validate a registry-v2 JSON file."""
    registry_path = Path(path)
    try:
        with registry_path.open(encoding="utf-8") as registry_file:
            data = json.load(registry_file)
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise RegistryValidationError(f"{registry_path}: {exc}") from exc
    return validate_registry(data)
