"""User-facing configuration file for stado.

Resolution order for every setting: environment variable wins, then the
config file, then the built-in default. The file is plain JSON (no new
dependency) and is searched at, in order: $STADO_CONFIG, ./stado.config.json,
~/.config/stado/config.json, ~/.stado/config.json.

Structured sections (storage/providers/azure/dashboard/alerts/billing) are
flattened onto the flat constant names config.py already consumes, so no
consumer changes are required to adopt a file-driven deployment.
"""
from __future__ import annotations

import json
import os
from pathlib import Path

FILE_ENV = "STADO_CONFIG"
CANDIDATES = (
    "stado.config.json",
    "~/.config/stado/config.json",
    "~/.stado/config.json",
)

_CACHE: dict = {"loaded": None, "path": None}


def find_config_file() -> Path | None:
    override = os.environ.get(FILE_ENV, "").strip()
    if override:
        candidate = Path(override).expanduser()
        return candidate if candidate.exists() else None
    for entry in CANDIDATES:
        candidate = Path(entry).expanduser()
        if candidate.exists():
            return candidate
    return None


def load_config_file() -> dict:
    if _CACHE["loaded"] is not None:
        return _CACHE["loaded"]
    path = find_config_file()
    _CACHE["path"] = path
    if path is None:
        _CACHE["loaded"] = {}
        return _CACHE["loaded"]
    try:
        data = json.loads(path.read_text())
    except Exception as exc:
        raise ValueError(f"invalid stado config file {path}: {exc}") from exc
    if not isinstance(data, dict):
        raise ValueError(f"stado config file {path} must contain a JSON object")
    _CACHE["loaded"] = data
    return data


def config_path() -> Path | None:
    load_config_file()
    return _CACHE["path"]


def _get(data: dict, dotted: str, fallback):
    value = data
    for part in dotted.split("."):
        if not isinstance(value, dict) or part not in value:
            return fallback
        value = value[part]
    return value


def get(dotted: str, fallback=None):
    """Read a dotted key (e.g. 'storage.gcs.bucket') from the loaded file."""
    return _get(load_config_file(), dotted, fallback)


def resolve(env_name: str, dotted: str, default):
    """env > config file (dotted) > built-in default."""
    value = os.environ.get(env_name)
    if value is not None and value != "":
        return value
    return _get(load_config_file(), dotted, default)


def resolve_list(env_name: str, dotted: str, default: list[str]) -> list[str]:
    """List-valued resolve: env comma-list > config file list > default."""
    value = os.environ.get(env_name)
    if value:
        return [part.strip() for part in value.split(",") if part.strip()]
    candidate = _get(load_config_file(), dotted, None)
    if isinstance(candidate, list):
        return [str(part).strip() for part in candidate if str(part).strip()]
    return list(default)


def validate(data: dict) -> list[str]:
    """Structural validation of a config dict; returns a list of problems."""
    problems: list[str] = []
    storage = data.get("storage", {})
    backend = storage.get("backend")
    if backend is not None and backend not in ("gcs", "azure", "s3", "local"):
        problems.append(f"storage.backend must be gcs|azure|s3|local, got {backend!r}")
    if backend == "gcs" and not storage.get("gcs", {}).get("bucket") and not data.get("bucket"):
        problems.append("storage.backend=gcs needs storage.gcs.bucket")
    if backend == "s3" and not storage.get("s3", {}).get("bucket"):
        problems.append("storage.backend=s3 needs storage.s3.bucket")
    providers = data.get("providers")
    if providers is not None:
        if not isinstance(providers, list) or not providers:
            problems.append("providers must be a non-empty list")
        else:
            for provider in providers:
                if provider not in ("gcp", "azure", "aws", "local"):
                    problems.append(f"unknown provider: {provider!r}")
    dashboard = data.get("dashboard", {})
    port = dashboard.get("port")
    if port is not None and not (isinstance(port, int) and 0 < port < 65536):
        problems.append("dashboard.port must be an int between 1 and 65535")
    return problems


def template() -> dict:
    return {
        "project": "wisent-480400",
        "regions": ["us-central1"],
        "storage": {
            "backend": "gcs",
            "gcs": {"bucket": "stado"},
            "azure": {"account": "", "container": "wisent-compute"},
            "s3": {"bucket": "", "region": "us-east-1"},
            "local": {"path": "~/.stado/local-storage"},
        },
        "providers": ["gcp"],
        "azure": {
            "subscription_id": "",
            "resource_group": "wisent-compute",
            "locations": ["eastus", "westus3"],
            "vnet": "wisent-compute-vnet",
            "subnet": "wisent-compute-subnet",
            "nsg": "wisent-compute-nsg",
            "image_urn": "microsoft-dsvm:ubuntu-hpc:2204:latest",
            "vm_username": "wisent",
            "ssh_public_key": "",
        },
        "dashboard": {"bind": "127.0.0.1", "port": 8765, "refresh_seconds": 10},
        "alerts": {"topic": ""},
        "billing": {"dataset": "billing_export", "table": "", "net_alert_usd": 100},
    }
