"""Automated responder for Open Azure quota support tickets.

Microsoft Capacity CX opens a support ticket for every Azure quota
increase and follows up with the same five-question template
(Region / Deployment Model / Service Type / Planned VM Families /
Planned Compute Usage in Cores). When the customer does not reply
within a few days, Microsoft archives the ticket and the quota
request is silently dropped. This module scans Open quota tickets
in the configured subscription and posts a single canonical reply
per ticket so the request progresses without manual triage.

Uses az CLI subprocess against the box's existing Azure auth instead
of adding azure-mgmt-support as a hard dep — Azure responses are an
operator-side task (the mac-mini coordinator or a workstation has az
+ DefaultAzureCredential already; the Cloud Function does not, and
should not, hold Azure creds).

The reply only fires when:
  - ticket.status == "Open"
  - the most recent communication is FROM Microsoft (sender domain
    contains "@techsupport.microsoft.com" or "@microsoft.com"),
    i.e. the customer has not already replied,
  - the ticket is a quota-classification (problemClassification
    contains "Quota" or "subscription limit").
Dry-run prints the (ticket, region, planned body length) and skips
the create_communication call.
"""
from __future__ import annotations

import json
import re
import subprocess
import time


_REGION_RE = re.compile(r"\(([^)]+)\)\s*$")
_MS_SENDER = (
    "techsupport.microsoft.com",
    "microsoft.com",
)


def _az(args: list[str]) -> dict | list:
    """Invoke the az CLI returning parsed JSON. Raises CalledProcessError
    on non-zero exit so a misconfigured Azure auth surfaces immediately
    instead of producing empty results that look like 'nothing to do'."""
    r = subprocess.run(
        ["az", *args, "-o", "json"],
        check=True,
        capture_output=True,
        text=True,
    )
    return json.loads(r.stdout) if r.stdout.strip() else []


def _last_communication(ticket_name: str) -> dict:
    """Return the latest communication on a ticket as a plain dict
    {sender, createdDate, subject, body_snippet}. Empty dict if none."""
    comms = _az([
        "support", "in-subscription", "communication", "list",
        "--ticket-name", ticket_name,
        "--query", "[0]",
    ])
    if not isinstance(comms, dict):
        return {}
    body = comms.get("body") or ""
    snippet = re.sub(r"<[^>]+>", "", body)
    snippet = re.sub(r"\s+", " ", snippet).strip()[:240]
    return {
        "sender": comms.get("sender") or "",
        "createdDate": comms.get("createdDate") or "",
        "subject": comms.get("subject") or "",
        "body_snippet": snippet,
    }


def _last_communication_is_from_ms(ticket_name: str) -> bool:
    last = _last_communication(ticket_name)
    sender = (last.get("sender") or "").lower()
    return any(dom in sender for dom in _MS_SENDER)


def _open_quota_tickets() -> list[dict]:
    rows = _az([
        "support", "in-subscription", "tickets", "list",
        "--query",
        "[?status=='Open'].{name:name, title:title, "
        "problem:problemClassificationDisplayName}",
    ])
    if not isinstance(rows, list):
        return []
    return [
        r for r in rows
        if "quota" in (r.get("problem") or "").lower()
        or "subscription limit" in (r.get("problem") or "").lower()
    ]


def _region_from_title(title: str) -> str:
    m = _REGION_RE.search(title or "")
    return m.group(1).strip() if m else ""


def list_open_azure_tickets() -> list[dict]:
    """Reusable enumerator: one row per Open quota-classification
    Azure support ticket, joined with the latest communication's
    sender / sent / subject / body_snippet. The bulk-respond
    function and any read-only status view both consume this.

    Each row carries:
      name              : Azure support ticket id
      title             : human title (used to parse region)
      region            : value in title parens (e.g. 'eastus')
      last_sender       : email/string of the latest communication's sender
      last_sent         : ISO timestamp of that communication
      last_subject      : subject line of that communication
      last_body_snippet : first 240 chars (HTML stripped) of that body
      awaiting_customer : True iff last_sender domain is Microsoft —
                          i.e. Microsoft is waiting on a customer reply.
    """
    out: list[dict] = []
    for t in _open_quota_tickets():
        last = _last_communication(t.get("name", ""))
        sender = (last.get("sender") or "").lower()
        awaiting = any(d in sender for d in _MS_SENDER)
        out.append({
            "name": t.get("name", ""),
            "title": t.get("title", ""),
            "region": _region_from_title(t.get("title", "")),
            "last_sender": last.get("sender", ""),
            "last_sent": last.get("createdDate", ""),
            "last_subject": last.get("subject", ""),
            "last_body_snippet": last.get("body_snippet", ""),
            "awaiting_customer": awaiting,
        })
    return out


def _reply_body(subscription: str, region: str, contact_email: str) -> str:
    return (
        f"Hello,\n\n"
        f"Thank you for following up. Please find the requested "
        f"information below to proceed with the GPU quota increase on "
        f"subscription {subscription}.\n\n"
        f"Region to Enable: {region}\n"
        f"Deployment Model: ARM\n"
        f"Service Type: Compute VM\n\n"
        f"Planned VM Families and Cores per family in this region:\n"
        f"  - Standard_NC24ads_A100_v4 (NCadsA100v4): 192 cores\n"
        f"  - Standard_ND96asr_A100_v4 (NDasrA100v4): 192 cores\n"
        f"  - Standard_NC40ads_H100_v5 (NCadsH100v5): 200 cores\n"
        f"  - Standard_ND96isr_H100_v5 (NDisrH100v5): 200 cores\n\n"
        f"Use case: wisent-compute is our GPU job orchestrator. It "
        f"dispatches transient (per-job, on-demand, no Spot) workloads "
        f"for LLM activation extraction, fine-tuning, and steered "
        f"inference across multiple cloud providers (GCP + this Azure "
        f"subscription). We need Azure GPU capacity in {region} to give "
        f"the autoscaler regional headroom beyond GCP's regional "
        f"A100/H100 limits, so a burst of queued jobs is not bottlenecked "
        f"on one cloud's regional ceiling. All VMs are released as soon "
        f"as the job completes; we do not hold capacity.\n\n"
        f"Please proceed with the increase. Happy to provide any "
        f"additional information.\n\n"
        f"Regards,\n"
        f"Lukasz Bartoszcze\n"
        f"{contact_email}"
    )


def _subscription_id() -> str:
    r = _az(["account", "show", "--query", "id"])
    return r if isinstance(r, str) else ""


def respond_to_open_quota_tickets(
    *,
    contact_email: str,
    dry_run: bool = False,
) -> list[dict]:
    """Scan Open quota tickets and post one canonical reply per ticket
    whose last communication is from Microsoft. Returns a per-ticket
    result list: {name, region, ok, reason, action} where action is
    one of `replied`, `skip_customer_already_replied`,
    `skip_no_region_in_title`, `dry_run`.
    """
    subscription = _subscription_id()
    tickets = _open_quota_tickets()
    if not tickets:
        return []
    ts = int(time.time())
    out: list[dict] = []
    for t in tickets:
        name = t.get("name", "")
        title = t.get("title", "")
        region = _region_from_title(title)
        if not region:
            out.append({
                "name": name, "ok": False,
                "action": "skip_no_region_in_title",
                "title": title,
            })
            continue
        if not _last_communication_is_from_ms(name):
            out.append({
                "name": name, "region": region, "ok": True,
                "action": "skip_customer_already_replied",
            })
            continue
        if dry_run:
            out.append({
                "name": name, "region": region, "ok": True,
                "action": "dry_run",
                "body_chars": len(_reply_body(subscription, region, contact_email)),
            })
            continue
        body = _reply_body(subscription, region, contact_email)
        subject = f"RE: GPU quota across NC/ND/NV families ({region})"
        comm_name = "wc-quota-reply-" + re.sub(
            r"[^A-Za-z0-9-]", "-", f"{region}-{ts}",
        )
        try:
            _az([
                "support", "in-subscription", "communication", "create",
                "--ticket-name", name,
                "--communication-name", comm_name,
                "--communication-subject", subject,
                "--communication-body", body,
                "--no-wait",
            ])
            out.append({
                "name": name, "region": region, "ok": True,
                "action": "replied",
            })
        except subprocess.CalledProcessError as exc:
            out.append({
                "name": name, "region": region, "ok": False,
                "action": "error",
                "error": (exc.stderr or str(exc))[:240],
            })
    return out
