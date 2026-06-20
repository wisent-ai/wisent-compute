"""VAST_API_KEY resolver: env first, then GCP Secret Manager.

Factored out so providers/vast/__init__.py stays under the 300-line
cap and so any other caller (cli.py auto-enable gate,
providers/local/helpers._vast_has_renter) can share one resolution
path instead of duplicating the env+SecretManager logic.

Secret Manager name: vast-api-key in $GCP_PROJECT (default
wisent-480400). The agent's service account needs
roles/secretmanager.secretAccessor on this secret.
"""
from __future__ import annotations

import os


_SECRET_NAME = "vast-api-key"
SECRET_TEXT_ENCODING = "utf-8"


def resolve_vast_api_key() -> str:
    """Return the Vast API key, or '' if neither env nor Secret Manager
    has it."""
    key = os.environ.get("VAST_API_KEY", "").strip()
    if key:
        return key
    try:
        from google.cloud import secretmanager_v1
        proj = os.environ.get("GCP_PROJECT", "wisent-480400")
        cli = secretmanager_v1.SecretManagerServiceClient()
        resp = cli.access_secret_version(request={
            "name": f"projects/{proj}/secrets/{_SECRET_NAME}/versions/latest"
        })
        return (resp.payload.data.decode(SECRET_TEXT_ENCODING) or "").strip()
    except Exception:
        return ""


def vast_api_key_available() -> bool:
    """True iff resolve_vast_api_key() returns a non-empty string."""
    return bool(resolve_vast_api_key())
