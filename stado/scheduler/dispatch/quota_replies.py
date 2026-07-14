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

# Patterns Azure Capacity CX uses when the issue is BILLING (payment
# history, bank decline, outstanding balance), not a request for
# customer info. Auto-replying the standard 5-answer template against
# a billing-decline message is useless — the operator has to fix the
# payment side before any quota can be granted. Detect and route those
# to a skip_billing_decline action instead of replying.
_BILLING_DECLINE_RE = re.compile(
    r"insufficient payment history|bank decline|outstanding balance|"
    r"unpaid invoice|payment issues|pay now to resolve|billing issue",
    re.IGNORECASE,
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
        f"Hello,\n\nThank you for following up. Please find the requested "
        f"information below to proceed with the GPU quota increase on "
        f"subscription {subscription}.\n\nRegion to Enable: {region}\n"
        f"Deployment Model: ARM\nService Type: Compute VM\n\n"
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
        f"as the job completes; we do not hold capacity.\n\nPlease "
        f"proceed with the increase. Happy to provide any additional "
        f"information.\n\nRegards,\nLukasz Bartoszcze\n{contact_email}"
    )


def _subscription_id() -> str:
    r = _az(["account", "show", "--query", "id"])
    return r if isinstance(r, str) else ""


def _subscription_quota_id() -> str:
    """quotaId proves the subscription is sponsored (Sponsored_*).
    az account show does NOT include subscriptionPolicies by default,
    so hit management.azure.com via `az rest` directly."""
    sub = _subscription_id()
    if not sub:
        return ""
    r = _az(["rest", "--method", "GET", "--uri",
             f"https://management.azure.com/subscriptions/{sub}?api-version=2022-12-01",
             "--query", "subscriptionPolicies.quotaId"])
    return r if isinstance(r, str) else ""


def _escalation_body(subscription: str, quota_id: str, region: str, email: str) -> str:
    return (
        f"Hello,\n\nThe denial reason cited (insufficient payment history / "
        f"bank decline / outstanding balance) is structurally inapplicable "
        f"to this subscription:\n\nSubscription ID: {subscription}\n"
        f"Subscription quotaId: {quota_id}\n\nThis is a credit-funded "
        f"sponsored Azure subscription (quotaId begins with 'Sponsored_'). "
        f"It has no invoice/payment history to evaluate: usage is paid "
        f"from a Microsoft-granted credit balance, not from a customer "
        f"payment instrument. There is no outstanding balance (credits "
        f"are consumed in real time) and no prior bank decline (no bank "
        f"instrument is attached).\n\nPlease escalate this ticket to the "
        f"capacity team that handles sponsored / credit-funded "
        f"subscriptions, or to your manager. The quota increase for "
        f"{region} is needed for wisent-compute's GPU job orchestrator — "
        f"same use case as the prior message (LLM activation extraction + "
        f"fine-tuning, on-demand, no Spot, VMs released on job completion)."
        f"\n\nIf you cannot escalate, please indicate the correct team or "
        f"process and we will re-route directly.\n\nRegards,\n"
        f"Lukasz Bartoszcze\n{email}"
    )


def respond_to_open_quota_tickets(
    *,
    contact_email: str,
    dry_run: bool = False,
    escalate_billing: bool = False,
) -> list[dict]:
    """Scan Open quota tickets and post a reply per ticket whose last
    message is from Microsoft. Two reply templates:

      - default (escalate_billing=False): the 5-answer info template.
        Billing-decline tickets get action=skip_billing_decline (no
        reply posted; standard template wouldn't help — fix the
        billing side).
      - escalate_billing=True: billing-decline tickets get the
        credit-funded-subscription escalation message instead; other
        tickets still get the standard info reply.

    Actions: replied / escalated / dry_run / skip_billing_decline /
    skip_customer_already_replied / skip_no_region_in_title / error.
    """
    subscription = _subscription_id()
    quota_id = _subscription_quota_id() if escalate_billing else ""
    ts = int(time.time())
    out: list[dict] = []
    for t in list_open_azure_tickets():
        name = t["name"]
        region = t["region"]
        if not region:
            out.append({
                "name": name, "ok": False,
                "action": "skip_no_region_in_title",
                "title": t.get("title", ""),
            })
            continue
        if not t["awaiting_customer"]:
            out.append({
                "name": name, "region": region, "ok": True,
                "action": "skip_customer_already_replied",
            })
            continue
        is_billing = bool(_BILLING_DECLINE_RE.search(t.get("last_body_snippet", "") or ""))
        if is_billing and not escalate_billing:
            out.append({
                "name": name, "region": region, "ok": True,
                "action": "skip_billing_decline",
                "last_body_snippet": t.get("last_body_snippet", ""),
            })
            continue
        if is_billing:
            body = _escalation_body(subscription, quota_id, region, contact_email)
            subject = f"RE: GPU quota across NC/ND/NV families ({region}) — escalation: sponsored subscription"
            action_label, prefix = "escalated", "wc-quota-escalate-"
        else:
            body = _reply_body(subscription, region, contact_email)
            subject = f"RE: GPU quota across NC/ND/NV families ({region})"
            action_label, prefix = "replied", "wc-quota-reply-"
        if dry_run:
            out.append({"name": name, "region": region, "ok": True,
                        "action": "dry_run", "would": action_label,
                        "body_chars": len(body)})
            continue
        comm_name = prefix + re.sub(r"[^A-Za-z0-9-]", "-", f"{region}-{ts}")
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
                "action": action_label,
            })
        except subprocess.CalledProcessError as exc:
            out.append({
                "name": name, "region": region, "ok": False,
                "action": "error",
                "error": (exc.stderr or str(exc))[:240],
            })
    return out
