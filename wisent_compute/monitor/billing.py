"""Billing-credits collector.

Each Cloud Function tick this writes gs://<BUCKET>/billing_health/credits.json
following the exact convention of host_health/<host>.json: a single JSON blob,
an ISO-8601 reported_at, and per-source sections that either carry data or the
EXACT upstream error (never a mock, never a silent skip — a failed source
records its real status/detail so the cause is visible without log spelunking).

GCP section: derived entirely from the BigQuery billing export. Gross cost,
credits applied (negative), net cost, per-credit cumulative consumption, and a
7-day credit burn rate. The depletion signal is the latest-month net_cost
crossing BILLING_NET_ALERT_USD — this needs no knowledge of the original grant
ceiling, which no GCP API exposes, so the tracker stays fully automated.

Azure section: available credit balance via the ARM REST API, authenticated
with a service-principal stored in Secret Manager. A missing secret is an
explicit no_credentials status so Azure tracking activates automatically the
moment the secret is provisioned, with zero code change.
"""
from __future__ import annotations

import json
import re
import sys
import urllib.error
import urllib.request
from datetime import datetime, timezone

from ..config import (
    PROJECT,
    BILLING_DATASET,
    BILLING_TABLE,
    BILLING_NET_ALERT_USD,
    AZURE_BILLING_SECRET,
)

_BLOB = "billing_health/credits.json"
# BigQuery dataset/table identifiers cannot be bound as query parameters.
# They originate from controlled config, but we still hard-validate the
# shape so an env override can never inject SQL.
_IDENT_RE = re.compile(r"^[A-Za-z0-9_]+$")
_ARM = "https://management.azure.com"


def _log(msg: str) -> None:
    sys.stderr.write(f"[tick] {msg}\n")
    sys.stderr.flush()


def _gcp_section() -> dict:
    """Spend/credits/burn from the BigQuery billing export. Raises only on a
    genuine client/permission fault; the caller records that as the section's
    error so one broken source never suppresses the other."""
    if not _IDENT_RE.match(BILLING_DATASET) or not _IDENT_RE.match(BILLING_TABLE):
        return {
            "status": "config_error",
            "detail": f"invalid dataset/table identifier "
            f"{BILLING_DATASET!r}/{BILLING_TABLE!r}",
        }
    from google.cloud import bigquery

    client = bigquery.Client(project=PROJECT)
    fq = f"`{PROJECT}.{BILLING_DATASET}.{BILLING_TABLE}`"

    monthly_sql = f"""
        SELECT FORMAT_TIMESTAMP('%Y-%m', usage_start_time) AS month,
               ROUND(SUM(cost), 2) AS gross,
               ROUND(SUM(IFNULL((SELECT SUM(c.amount)
                       FROM UNNEST(credits) c), 0)), 2) AS credits,
               ROUND(SUM(cost) + SUM(IFNULL((SELECT SUM(c.amount)
                       FROM UNNEST(credits) c), 0)), 2) AS net,
               ANY_VALUE(currency) AS currency
        FROM {fq}
        WHERE usage_start_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(),
                                                INTERVAL 90 DAY)
        GROUP BY month ORDER BY month
    """
    credit_sql = f"""
        SELECT c.name AS name, c.type AS type,
               ROUND(SUM(c.amount), 2) AS cumulative,
               ANY_VALUE(currency) AS currency
        FROM {fq}, UNNEST(credits) c
        GROUP BY name, type ORDER BY cumulative
    """
    burn_sql = f"""
        SELECT ROUND(AVG(daily), 2) AS avg_daily_credit_7d FROM (
          SELECT DATE(usage_start_time) AS d,
                 SUM(IFNULL((SELECT SUM(c.amount)
                     FROM UNNEST(credits) c), 0)) AS daily
          FROM {fq}
          WHERE usage_start_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(),
                                                  INTERVAL 7 DAY)
          GROUP BY d)
    """

    months = [dict(r) for r in client.query(monthly_sql).result()]
    credits = [dict(r) for r in client.query(credit_sql).result()]
    burn_rows = [dict(r) for r in client.query(burn_sql).result()]
    burn = burn_rows[0]["avg_daily_credit_7d"] if burn_rows else None

    latest_net = months[-1]["net"] if months else None
    depleted = latest_net is not None and latest_net > BILLING_NET_ALERT_USD

    return {
        "status": "ok",
        "monthly": months,
        "credits": credits,
        "avg_daily_credit_applied_7d": burn,
        "latest_month_net_usd": latest_net,
        "net_alert_threshold_usd": BILLING_NET_ALERT_USD,
        "credit_depleted": depleted,
    }


def _fetch_azure_sp(secret_name: str) -> dict | None:
    """Read the Azure SP JSON from Secret Manager. Returns None (not an
    exception) when the secret simply does not exist, so the caller can
    record an explicit no_credentials status."""
    from google.cloud import secretmanager_v1
    from google.api_core.exceptions import NotFound, PermissionDenied

    client = secretmanager_v1.SecretManagerServiceClient()
    name = f"projects/{PROJECT}/secrets/{secret_name}/versions/latest"
    try:
        r = client.access_secret_version(request={"name": name})
    except (NotFound, PermissionDenied):
        return None
    return json.loads(r.payload.data.decode("utf-8"))


def _arm_get(url: str, token: str) -> dict:
    req = urllib.request.Request(
        url, headers={"Authorization": f"Bearer {token}"}
    )
    with urllib.request.urlopen(req) as resp:
        return json.loads(resp.read())


def _azure_section() -> dict:
    """Available credit balance via ARM. Every failure path records the
    EXACT cause (missing secret, auth failure, ARM HTTP body) so the status
    is actionable without reading logs."""
    sp = _fetch_azure_sp(AZURE_BILLING_SECRET)
    if sp is None:
        return {
            "status": "no_credentials",
            "detail": f"Secret Manager secret '{AZURE_BILLING_SECRET}' not "
            f"present in project {PROJECT}; create it (JSON: tenant_id, "
            f"client_id, client_secret, billing_account+billing_profile or "
            f"subscription_id) to activate Azure credit tracking",
        }
    missing = [k for k in ("tenant_id", "client_id", "client_secret")
               if not sp.get(k)]
    if missing:
        return {"status": "config_error",
                "detail": f"Azure SP secret missing keys: {missing}"}

    try:
        from azure.identity import ClientSecretCredential

        cred = ClientSecretCredential(
            tenant_id=sp["tenant_id"],
            client_id=sp["client_id"],
            client_secret=sp["client_secret"],
        )
        token = cred.get_token(f"{_ARM}/.default").token
    except Exception as e:  # exact auth failure surfaced, not masked
        return {"status": "auth_error",
                "detail": f"{type(e).__name__}: {e}"}

    ba, bp = sp.get("billing_account"), sp.get("billing_profile")
    sub = sp.get("subscription_id")
    if ba and bp:
        url = (f"{_ARM}/providers/Microsoft.Billing/billingAccounts/{ba}"
               f"/billingProfiles/{bp}/availableBalance"
               f"?api-version=2023-05-01")
    elif sub:
        url = (f"{_ARM}/subscriptions/{sub}/providers/"
               f"Microsoft.Consumption/balances?api-version=2019-10-01")
    else:
        return {"status": "config_error",
                "detail": "Azure SP secret needs billing_account+"
                "billing_profile or subscription_id"}

    try:
        body = _arm_get(url, token)
    except urllib.error.HTTPError as e:
        return {"status": "arm_error",
                "detail": f"HTTP {e.code}: "
                f"{e.read()[:400].decode('utf-8', 'replace')}",
                "endpoint": url}
    except urllib.error.URLError as e:
        return {"status": "arm_error", "detail": f"{e.reason}",
                "endpoint": url}

    props = body.get("properties", body)
    amount = None
    if isinstance(props, dict):
        amt = props.get("amount") or props.get("availableBalance")
        if isinstance(amt, dict):
            amount = amt.get("value")
        elif amt is not None:
            amount = amt
    return {"status": "ok", "available_balance": amount, "raw": props}


def collect_billing(store) -> None:
    """Assemble and upload billing_health/credits.json. Each source is
    isolated: a raised exception from one is captured into its section as
    the exact error string, so the blob is always written and the other
    source is never lost. Emits a [tick] BILLING ALERT log line on credit
    depletion so existing log-based alerting fires with no extra wiring."""
    now = datetime.now(timezone.utc).isoformat()
    try:
        gcp = _gcp_section()
    except Exception as e:
        gcp = {"status": "error", "detail": f"{type(e).__name__}: {e}"}
    try:
        azure = _azure_section()
    except Exception as e:
        azure = {"status": "error", "detail": f"{type(e).__name__}: {e}"}

    doc = {
        "reported_at": now,
        "project": PROJECT,
        "gcp": gcp,
        "azure": azure,
    }
    store._upload_text(_BLOB, json.dumps(doc, indent=2, default=str))

    if gcp.get("credit_depleted"):
        _log(
            f"BILLING ALERT: GCP latest-month net "
            f"${gcp.get('latest_month_net_usd')} exceeds "
            f"${gcp.get('net_alert_threshold_usd')} — promotion credit "
            f"exhausted or rate-capped"
        )
    bal = azure.get("available_balance")
    if azure.get("status") == "ok" and isinstance(bal, (int, float)) \
            and bal < BILLING_NET_ALERT_USD:
        _log(
            f"BILLING ALERT: Azure available credit balance {bal} "
            f"below {BILLING_NET_ALERT_USD}"
        )
    _log(f"billing: gcp={gcp.get('status')} azure={azure.get('status')} "
         f"-> {_BLOB}")
