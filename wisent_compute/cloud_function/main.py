"""Cloud Function entry point. Triggered every 3 min by Cloud Scheduler."""
from __future__ import annotations

import sys
from google.cloud import pubsub_v1, secretmanager_v1

from wisent_compute.config import PROJECT, BUCKET, ALERTS_TOPIC, WC_PROVIDERS
from wisent_compute.queue.storage import JobStorage
from wisent_compute.providers import get_provider
from wisent_compute.monitor import check_running_jobs, reap_dead_agents
from wisent_compute.scheduler import schedule_queued_jobs

_publisher = None
_secrets = None


def _log(msg):
    sys.stderr.write(f"[tick] {msg}\n")
    sys.stderr.flush()


def _load_secrets():
    global _secrets
    if _secrets is not None:
        return _secrets
    client = secretmanager_v1.SecretManagerServiceClient()
    _secrets = {}
    for name in ("wisent-hf-token", "wisent-gh-token"):
        try:
            r = client.access_secret_version(request={
                "name": f"projects/{PROJECT}/secrets/{name}/versions/latest"
            })
            key = name.replace("wisent-", "").replace("-", "_").upper()
            _secrets[key] = r.payload.data.decode("utf-8")
        except Exception:
            pass
    return _secrets


def monitor_jobs(request=None):
    """Main tick: per provider in WC_PROVIDERS, check running + reap + schedule."""
    global _publisher
    _log("Tick started")

    store = JobStorage(BUCKET)
    if _publisher is None:
        _publisher = pubsub_v1.PublisherClient()

    secrets = _load_secrets()
    total_reaped = 0
    total_scheduled = 0
    for name in WC_PROVIDERS:
        try:
            provider = get_provider(name)
        except Exception as exc:
            _log(f"skip {name}: {exc!r}")
            continue
        check_running_jobs(store, provider, _publisher)
        total_reaped += reap_dead_agents(store, provider, kind=name)
        total_scheduled += schedule_queued_jobs(store, provider, name, secrets)
    _log(
        f"Tick done: reaped {total_reaped} dead-agent VMs, "
        f"scheduled {total_scheduled} (providers={WC_PROVIDERS})"
    )

    return "OK"
