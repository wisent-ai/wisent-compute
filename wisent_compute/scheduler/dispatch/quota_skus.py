"""Cross-provider GPU catalog enumerator.

provider_catalog(provider) returns the full list of GPU-related
SKUs/families the provider supports along with the current per-region
limit on file for our project. Backs `wc quota catalog` (read-side
enumeration) and `wc quota request-all` (bulk fan-out of
CreateQuotaPreference across every enumerated family × every
configured region).

GCP path uses google-cloud-quotas list_quota_infos to enumerate
quotas under compute.googleapis.com, filtered to GPU-related
quota_ids:
  - NVIDIA-{FAMILY}-GPUS-per-project-region   (legacy per-family
    quotas, one per GPU model, dimensioned by region)
  - GPUS-PER-GPU-FAMILY-per-project-region    (newer unified quota,
    dimensioned by gpu_family + region)

The newer GPUS-PER-GPU-FAMILY quota is the right submission target;
the legacy per-family quotas are kept for read-side completeness so
a catalog dump shows everything Google tracks.

Azure path uses az vm list-skus to enumerate Compute GPU VM families
in the subscription. Each family corresponds to a Microsoft.Quota
resource_name we can target with begin_create_or_update.
"""
from __future__ import annotations

import json
import re
import subprocess


# Note: no hardcoded family list. The bulk-submit path discovers the
# real set of gpu_family values from the live cloudquotas API via
# _gcp_catalog() — rows whose quota_id is the unified
# GPUS-PER-GPU-FAMILY-per-project-region quota carry a populated
# `gpu_family` dimension that is the ground truth for what families
# Google currently models in this project. Anything else would
# reintroduce the "hardcoded list drifts from reality" problem.


def _gcp_catalog() -> list[dict]:
    """Enumerate every GPU-related compute.googleapis.com QuotaInfo
    in the project, returning one entry per (quota_id, region) with
    its current limit. Limit comes from QuotaInfo.dimensionsInfos:
    each DimensionsInfo carries an applicableLocations list + a
    details.value field that is the current per-region cap.
    """
    from google.cloud import cloudquotas_v1
    import os as _os
    project = _os.environ.get("GCP_PROJECT", "wisent-480400")
    client = cloudquotas_v1.CloudQuotasClient()
    parent = (
        f"projects/{project}/locations/global/services/compute.googleapis.com"
    )
    out: list[dict] = []
    for info in client.list_quota_infos(parent=parent):
        qid = info.quota_id or ""
        is_gpu_family = "GPUS-PER-GPU-FAMILY" in qid
        is_legacy_gpu = bool(re.search(r"NVIDIA-[A-Z0-9_-]+-GPUS", qid))
        if not (is_gpu_family or is_legacy_gpu):
            continue
        # dimensionsInfos: per-region cap. Each row carries
        # applicableLocations and details.value.
        for di in (info.dimensions_infos or []):
            locs = list(di.applicable_locations or [])
            value = (
                di.details.value if (di.details and di.details.value is not None)
                else None
            )
            dims = dict(di.dimensions or {})
            for loc in locs or ["global"]:
                out.append({
                    "provider": "gcp",
                    "quota_id": qid,
                    "metric": info.metric_display_name or info.metric,
                    "gpu_family": dims.get("gpu_family", ""),
                    "region": loc,
                    "limit": int(value) if value is not None else None,
                })
    return out


def _azure_catalog() -> list[dict]:
    """Enumerate Azure Compute GPU VM families across every location
    the subscription has access to. Each row: (family, location,
    available). az vm list-skus is the only API that maps every SKU
    to its containing family, which is what Microsoft.Quota's
    create_or_update keys on as resource_name.
    """
    try:
        r = subprocess.run(
            ["az", "vm", "list-skus", "--resource-type", "virtualMachines",
             "-o", "json"],
            check=True, capture_output=True, text=True,
        )
    except (subprocess.CalledProcessError, FileNotFoundError) as exc:
        return [{
            "provider": "azure", "ok": False,
            "error": f"{type(exc).__name__}: {exc}",
        }]
    skus = json.loads(r.stdout) if r.stdout.strip() else []
    out: list[dict] = []
    for sku in skus:
        fam = sku.get("family") or ""
        if not any(t in fam for t in ("NC", "ND", "NV", "GPU")):
            continue
        locs = sku.get("locations") or []
        name = sku.get("name") or ""
        # Mark each (family, location) row once; the SKU table has
        # many SKUs per family, so we dedupe at print/aggregate time.
        for loc in locs:
            out.append({
                "provider": "azure",
                "family": fam,
                "sku": name,
                "location": loc,
            })
    return out


def provider_catalog(provider: str) -> list[dict]:
    """Return the full GPU catalog for `provider` (gcp | azure).

    Rows are dicts keyed by what's meaningful per provider:
      gcp:   {quota_id, gpu_family, region, limit, metric}
      azure: {family, sku, location}
    A failure to fetch surfaces as a single row with `ok=False` plus
    an `error` field so the caller can print it without dying.
    """
    if provider == "gcp":
        return _gcp_catalog()
    if provider == "azure":
        return _azure_catalog()
    return [{"provider": provider, "ok": False,
             "error": "no catalog impl for this provider"}]


def all_catalogs(providers: list[str]) -> dict[str, list[dict]]:
    """provider_name -> list of catalog rows. Iterates `providers`
    in order so the caller can preserve WC_PROVIDERS ordering."""
    return {p: provider_catalog(p) for p in providers}


def gcp_request_all_families(
    *,
    new_limit: int,
    regions: list[str],
    contact_email: str,
    justification: str,
) -> list[dict]:
    """Fan out CreateQuotaPreference for every gpu_family the live
    cloudquotas API reports under compute.googleapis.com, in every
    region passed. Uses the unified GPUS-PER-GPU-FAMILY-per-project-
    region quota (the one that takes a gpu_family dimension); the
    set of families is discovered, not hardcoded — anything Google
    drops or adds tomorrow is picked up on the next call without a
    package release.
    """
    from .quota_request import _gcp_request_for_family
    import os as _os
    project = _os.environ.get("GCP_PROJECT", "wisent-480400")
    # Build the real (gpu_family -> applicable_regions) map from the
    # catalog so we never submit a request for a (family, region) pair
    # Google doesn't actually carry. Each catalog row already carries
    # the region from QuotaInfo.dimensionsInfos.applicableLocations.
    fam_regions: dict[str, set[str]] = {}
    for row in _gcp_catalog():
        if row.get("quota_id") != "GPUS-PER-GPU-FAMILY-per-project-region":
            continue
        fam = row.get("gpu_family") or ""
        region = row.get("region") or ""
        if fam and region:
            fam_regions.setdefault(fam, set()).add(region)
    requested = set(regions)
    out: list[dict] = []
    for fam in sorted(fam_regions):
        targets = sorted(fam_regions[fam] & requested) if requested else sorted(fam_regions[fam])
        for region in targets:
            try:
                r = _gcp_request_for_family(
                    project, region, fam, new_limit,
                    justification, contact_email,
                )
                out.append({
                    "provider": "gcp", "region": region,
                    "gpu_family": fam, "ok": True, **r,
                })
            except Exception as exc:
                out.append({
                    "provider": "gcp", "region": region,
                    "gpu_family": fam, "ok": False,
                    "error": f"{type(exc).__name__}: {exc}",
                })
    return out


def azure_request_all_families(
    *,
    new_limit: int,
    locations: list[str],
) -> list[dict]:
    """Fan out Microsoft.Quota create_or_update for every distinct
    GPU family the subscription advertises in each `locations`
    entry."""
    from .quota_request import _azure_request_increase
    from ...config import AZURE_SUBSCRIPTION_ID
    catalog = _azure_catalog()
    families_per_loc: dict[str, set[str]] = {}
    for row in catalog:
        if not row.get("family"):
            continue
        loc = row.get("location", "")
        if locations and loc not in locations:
            continue
        families_per_loc.setdefault(loc, set()).add(row["family"])
    out: list[dict] = []
    for loc, families in families_per_loc.items():
        for fam in sorted(families):
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
                    "provider": "azure", "location": loc,
                    "family": fam, "ok": False,
                    "error": f"{type(exc).__name__}: {exc}",
                })
    return out
