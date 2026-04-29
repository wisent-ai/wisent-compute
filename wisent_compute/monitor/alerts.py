"""Multi-channel alert dispatch: Slack, Email, Telegram, Pub/Sub."""
from __future__ import annotations

import json
import os
import sys
import urllib.request
import urllib.error


def _log(msg):
    sys.stderr.write(f"[alert] {msg}\n")
    sys.stderr.flush()


def _slack(message: str):
    url = os.environ.get("WC_SLACK_WEBHOOK")
    if not url:
        return
    payload = json.dumps({"text": message}).encode()
    req = urllib.request.Request(url, data=payload, headers={"Content-Type": "application/json"})
    try:
        urllib.request.urlopen(req)
        _log("Slack sent")
    except Exception as e:
        _log(f"Slack failed: {e}")


def _telegram(message: str):
    token = os.environ.get("WC_TELEGRAM_BOT_TOKEN")
    chat_id = os.environ.get("WC_TELEGRAM_CHAT_ID")
    if not token or not chat_id:
        return
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = json.dumps({"chat_id": chat_id, "text": message, "parse_mode": "Markdown"}).encode()
    req = urllib.request.Request(url, data=payload, headers={"Content-Type": "application/json"})
    try:
        urllib.request.urlopen(req)
        _log("Telegram sent")
    except Exception as e:
        _log(f"Telegram failed: {e}")


def _email(subject: str, body: str):
    api_key = os.environ.get("WC_SENDGRID_API_KEY")
    to_addr = os.environ.get("WC_EMAIL_TO")
    from_addr = os.environ.get("WC_EMAIL_FROM", "compute@example.com")
    if not api_key or not to_addr:
        return
    url = "https://api.sendgrid.com/v3/mail/send"
    payload = json.dumps({
        "personalizations": [{"to": [{"email": to_addr}]}],
        "from": {"email": from_addr},
        "subject": subject,
        "content": [{"type": "text/plain", "value": body}],
    }).encode()
    req = urllib.request.Request(url, data=payload, headers={
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    })
    try:
        urllib.request.urlopen(req)
        _log("Email sent")
    except Exception as e:
        _log(f"Email failed: {e}")


def _pubsub(publisher, topic: str, message: str):
    if not publisher or not topic:
        return
    try:
        publisher.publish(topic, message.encode("utf-8"))
        _log("Pub/Sub sent")
    except Exception as e:
        _log(f"Pub/Sub failed: {e}")


def send_alert(publisher, topic: str, message: str, subject: str = ""):
    """Send alert to all configured channels."""
    _log(message)
    _slack(message)
    _telegram(message)
    _email(subject or message[:80], message)
    _pubsub(publisher, topic, message)
