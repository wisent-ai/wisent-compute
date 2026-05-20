"""Cloud Quotas API quota-increase orchestrator.

Wraps google-cloud-cloudquotas CreateQuotaPreference (GCP) and
azure-mgmt-quota Quota.begin_create_or_update (Azure) so a single
`wc quota request <accel> --to N` invocation fans out one quota-
increase request per (provider, region) across every provider in
WC_PROVIDERS. Co-located in scheduler/dispatch/ because submitting
a quota preference is the write-side mirror of dispatch's read-side
get_available_slots: both treat per-(provider, region, accel) GPU
ceilings as the unit of work. scheduler/quota.py stays focused on
the read path (load_quotas / get_available_slots / summarize_quotas)
and this module owns the write path.

GCP: the newer Cloud Quotas API expresses GPU quotas as a single
quota_id `GPUS-PER-GPU-FAMILY-per-project-region` parameterized by a
dimensions={"region": ..., "gpu_family": ...} map (see Google docs
"Manage quotas using the gcloud CLI" and "Implement common use
cases"). Submission is non-blocking: the QuotaPreference is created
or updated and Google's reviewer approves/declines asynchronously.
ALREADY_EXISTS is converted to update_quota_preference so re-running
the command bumps an existing pending request to the new
preferred_value rather than erroring.

Azure: Microsoft.Quota provider, Quota.begin_create_or_update against
`subscriptions/{sub}/providers/Microsoft.Compute/locations/{loc}`
with resource_name = the SKU family name. The Azure SDK is optional —
azure-mgmt-quota / azure-identity import errors surface as a
non-fatal "not installed" entry in the result list (same pattern
scheduler.quota._fetch_quotas_azure uses on the read side).
"""
from __future__ import annotations

import os


# Cloud Quotas API dimensions[gpu_family] values for each accel we
# dispatch. Keep in sync with _GCP_METRIC_TO_ACCEL in scheduler.quota.
_GCP_ACCEL_TO_GPU_FAMILY = {
    "nvidia-tesla-t4": "NVIDIA_T4",
    "nvidia-l4": "NVIDIA_L4",
    "nvidia-tesla-a100": "NVIDIA_A100",
    "nvidia-a100-80gb": "NVIDIA_A100_80GB",
}


def _gcp_request_increase(
    project: str,
    region: str,
    accel: str,
    new_limit: int,
    justification: str,
    contact_email: str,
) -> dict:
    """Submit a Cloud Quotas QuotaPreference for a regional GPU quota.

    Returns {"name": <resource>, "created": True|False}. Approval is
    asynchronous; this call only submits the request. ALREADY_EXISTS
    is turned into an UpdateQuotaPreference so a second invocation
    bumps the prior pending request's preferred_value rather than
    erroring out.
    """
    from google.cloud import cloudquotas_v1

    family = _GCP_ACCEL_TO_GPU_FAMILY.get(accel)
    if not family:
        raise ValueError(
            f"no GCP gpu_family mapping for accel '{accel}'; "
            f"known: {sorted(_GCP_ACCEL_TO_GPU_FAMILY)}"
        )
    client = cloudquotas_v1.CloudQuotasClient()
    qp = cloudquotas_v1.QuotaPreference(
        service="compute.googleapis.com",
        quota_id="GPUS-PER-GPU-FAMILY-per-project-region",
        quota_config=cloudquotas_v1.QuotaConfig(preferred_value=new_limit),
        dimensions={"region": region, "gpu_family": family},
        justification=justification,
        contact_email=contact_email,
    )
    pref_id = (
        f"compute-gpus-{region}-{family}".lower().replace("_", "-")
    )
    parent = f"projects/{project}/locations/global"
    try:
        resp = client.create_quota_preference(
            parent=parent,
            quota_preference=qp,
            quota_preference_id=pref_id,
        )
        return {"name": resp.name, "created": True}
    except Exception as exc:
        msg = str(exc)
        if "ALREADY_EXISTS" not in msg and "already exists" not in msg.lower():
            raise
        qp.name = f"{parent}/quotaPreferences/{pref_id}"
        resp = client.update_quota_preference(quota_preference=qp)
        return {"name": resp.name, "created": False}


def _azure_request_increase(
    subscription: str,
    location: str,
    family_name: str,
    new_limit: int,
) -> dict:
    """Submit an Azure Microsoft.Quota create_or_update for a compute family.

    Returns {"available": True, "name": ...} on success or
    {"available": False, "reason": ...} when the optional SDK isn't
    installed or AZURE_SUBSCRIPTION_ID is empty. The latter cases
    surface as informational result-list entries instead of aborting
    a multi-provider fan-out.
    """
    try:
        from azure.identity import DefaultAzureCredential
        from azure.mgmt.quota import AzureQuotaExtensionAPI
    except ImportError:
        return {"available": False, "reason": "azure-mgmt-quota not installed"}
    if not subscription:
        return {"available": False, "reason": "AZURE_SUBSCRIPTION_ID unset"}
    client = AzureQuotaExtensionAPI(DefaultAzureCredential(), subscription)
    scope = (
        f"subscriptions/{subscription}/providers/Microsoft.Compute"
        f"/locations/{location}"
    )
    poller = client.quota.begin_create_or_update(
        resource_name=family_name,
        scope=scope,
        create_quota_request={
            "properties": {
                "limit": {
                    "limit_object_type": "LimitValue",
                    "value": new_limit,
                },
                "name": {"value": family_name},
                "resourceType": "dedicated",
            }
        },
    )
    resp = poller.result()
    return {"name": getattr(resp, "id", family_name), "available": True}


def _gcp_fanout(
    accel: str,
    new_limit: int,
    regions: list[str] | None,
    justification: str,
    contact_email: str,
) -> list[dict]:
    from ...config import REGIONS
    project = os.environ.get("GCP_PROJECT", "wisent-480400")
    targets = regions or REGIONS
    out: list[dict] = []
    for region in targets:
        try:
            r = _gcp_request_increase(
                project, region, accel, new_limit,
                justification, contact_email,
            )
            out.append({
                "provider": "gcp", "region": region, "ok": True, **r,
            })
        except Exception as exc:
            out.append({
                "provider": "gcp", "region": region, "ok": False,
                "error": f"{type(exc).__name__}: {exc}",
            })
    return out


def _azure_fanout(
    accel: str,
    new_limit: int,
    regions: list[str] | None,
) -> list[dict]:
    from ...config import AZURE_LOCATIONS, AZURE_SUBSCRIPTION_ID
    from ...models import AZURE_QUOTA_FAMILY_TO_ACCEL
    families = [
        f for f, a in AZURE_QUOTA_FAMILY_TO_ACCEL.items() if a == accel
    ]
    if not families:
        return [{
            "provider": "azure", "ok": False,
            "error": f"no Azure compute family matches accel '{accel}'",
        }]
    targets = regions or AZURE_LOCATIONS
    out: list[dict] = []
    for loc in targets:
        for fam in families:
            try:
                r = _azure_request_increase(
                    AZURE_SUBSCRIPTION_ID, loc, fam, new_limit,
                )
                if not r.get("available", True):
                    out.append({
                        "provider": "azure", "location": loc,
                        "family": fam, "ok": False,
                        "error": r.get("reason", "not available"),
                    })
                else:
                    out.append({
                        "provider": "azure", "location": loc,
                        "family": fam, "ok": True, **r,
                    })
            except Exception as exc:
                out.append({
                    "provider": "azure", "location": loc, "family": fam,
                    "ok": False,
                    "error": f"{type(exc).__name__}: {exc}",
                })
    return out


def request_quota_increases(
    *,
    accel: str,
    new_limit: int,
    providers: list[str],
    regions: list[str] | None,
    justification: str,
    contact_email: str,
) -> list[dict]:
    """Fan out quota-increase requests across providers and regions.

    For each provider in `providers`, iterate `regions` (or the
    provider's configured region/location list when None) and submit
    one quota-increase request per (provider, region). Per-target
    failures are captured in the result list rather than aborting the
    rest of the fan-out: each entry carries `provider`, a region/
    location key, `ok` (bool), and either `name` (success) or `error`.
    """
    out: list[dict] = []
    for p in providers:
        if p == "gcp":
            out.extend(_gcp_fanout(
                accel, new_limit, regions, justification, contact_email,
            ))
        elif p == "azure":
            out.extend(_azure_fanout(accel, new_limit, regions))
        else:
            out.append({
                "provider": p, "ok": False,
                "error": "no quota-increase impl for this provider",
            })
    return out
