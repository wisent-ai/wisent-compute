"""Tests for Job.pinned_host (operator hard-pin) and the registry-driven
pinned_only agent claim mode."""
from stado.models import Job
from stado.providers.local.helpers import _job_eligible
from stado.targets import _from_dict


def _job(**kw):
    kw.setdefault("job_id", "t1234567")
    kw.setdefault("command", "echo hi")
    kw.setdefault("provider", "local")
    return Job(**kw)


def test_pinned_host_other_consumer_refused():
    j = _job(pinned_host="local-mac-mini")
    assert not _job_eligible(j, "apple-mps", 1, consumer_id="local-macbook")


def test_pinned_host_named_consumer_claims():
    j = _job(pinned_host="local-mac-mini")
    assert _job_eligible(j, "apple-mps", 1, consumer_id="local-mac-mini")


def test_unpinned_job_open_to_all_by_default():
    j = _job()
    assert _job_eligible(j, "apple-mps", 1, consumer_id="local-macbook")


def test_pinned_only_agent_rejects_unrouted_jobs():
    j = _job()
    assert not _job_eligible(j, "apple-mps", 1, consumer_id="local-mac-mini",
                             pinned_only=True)


def test_pinned_only_agent_accepts_assigned_jobs():
    j = _job(assigned_to="local-mac-mini")
    assert _job_eligible(j, "apple-mps", 1, consumer_id="local-mac-mini",
                         pinned_only=True)


def test_pinned_only_agent_accepts_pinned_jobs():
    j = _job(pinned_host="local-mac-mini")
    assert _job_eligible(j, "apple-mps", 1, consumer_id="local-mac-mini",
                         pinned_only=True)


def test_pinned_host_survives_serialization_roundtrip():
    j = _job(pinned_host="local-mac-mini")
    assert Job.from_dict(j.to_dict()).pinned_host == "local-mac-mini"


def test_old_queue_blob_without_pin_reads_clean():
    j = _job()
    d = j.to_dict()
    del d["pinned_host"]
    assert Job.from_dict(d).pinned_host == ""


def test_registry_target_parses_pinned_only():
    t = _from_dict({"name": "mac-mini", "kind": "local", "pinned_only": True})
    assert t.pinned_only is True
    t2 = _from_dict({"name": "plain", "kind": "local"})
    assert t2.pinned_only is False
