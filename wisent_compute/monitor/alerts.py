"""Pub/Sub alert dispatch."""
from __future__ import annotations

import sys


def send_alert(publisher, topic: str, message: str):
    """Publish alert message. Swallows errors."""
    sys.stderr.write(f"[alert] {message}\n")
    sys.stderr.flush()
    if publisher and topic:
        try:
            publisher.publish(topic, message.encode("utf-8"))
        except Exception:
            pass
