#!/usr/bin/env python3
"""Authoritative offline smoke test for the Box by ASCII execution path."""
import json, os, socket, sys, types, urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path
Z, O, N4, N5, N8, N9 = map(int, ("0", "1", "4", "5", "8", "9"))
N60, N70, N80, N81, N123, N503, N600, N901 = map(int, ("60", "70", "80", "81", "123", "503", "600", "901"))
ROOT = Path(__file__).resolve().parents[O]
sys.path.insert(Z, str(ROOT))
NETWORK_ATTEMPTS = []
def deny_network(*args, **kwargs):
    NETWORK_ATTEMPTS.append((args, kwargs))
    raise AssertionError("network is forbidden in Box smoke test")
urllib.request.urlopen = deny_network
socket.create_connection = deny_network
socket.socket = deny_network
for key in ("BOX_API_KEY", "BOX_API_URL", "BOX_TTL_SECONDS", "BOX_RELEASE_MODE"):
    os.environ.pop(key, None)
from stado.models import Job, JobState
from stado.providers.box import BoxClient, BoxConfigurationError, BoxProvider
from stado.providers.box._types import (
    BoxAPIError, BoxCommandResult, BoxEventPage, BoxInfo, BoxLimits, BoxPromptRun,
    BoxTransportError,
)
from stado.queue.leases import LeaseConflict, LeaseState, ProviderLease, ProviderLeaseStore
from stado.queue.storage import StorageConflict, VersionedText
from stado.scheduler.dispatch.box import cancel_box_job, dispatch_box_jobs, reconcile_box_jobs
from stado.targets.capabilities import BOX_CAPABILITIES, admit_job
def check(condition, message):
    if not condition: raise AssertionError(message)
def raises(kind, function, contains=""):
    try:
        function()
    except kind as exc:
        check(not contains or contains in str(exc), f"missing error text {contains!r}: {exc}")
        return exc
    raise AssertionError(f"expected {kind.__name__}")
opened = []
raises(BoxConfigurationError, lambda: BoxClient("", opener=lambda *a, **k: opened.append(a)), "BOX_API_KEY")
raises(BoxConfigurationError, BoxProvider, "BOX_API_KEY")
check(not opened and not NETWORK_ATTEMPTS, "configuration/import unexpectedly attempted network")
class Response:
    def __init__(self, value): self.raw = value if isinstance(value, bytes) else json.dumps(value).encode()
    def __enter__(self): return self
    def __exit__(self, *unused): return False
    def read(self, limit=None): return self.raw if limit is None else self.raw[:limit]
requests = []
def opener(request, timeout):
    requests.append((request, timeout))
    return Response({"ok": True, "type": "box.created", "box": {
        "id": "bx_23456789", "state": "ready", "name": "smoke"}})
client = BoxClient("box_test_secret", base_url="https://invalid.example/v1", opener=opener)
box = client.create_box(N123, no_env=True)
request, timeout = requests.pop()
check(box.box_id == "bx_23456789" and timeout == float(N70), "typed create parsing failed")
check(request.get_header("Authorization") == "Bearer box_test_secret", "bearer auth missing")
check(json.loads(request.data) == {"ttlSeconds": N123, "noEnv": True}, "request body changed")
bad = BoxClient("key", base_url="https://invalid.example", opener=lambda *a, **k: Response(
    {"ok": True, "type": "wrong.type"}))
raises(BoxTransportError, lambda: bad.create_box(O), "unexpected type")
envelopes = [
    {"ok": True, "type": "prompt.queued", "promptId": "task-1",
     "promptRun": {"promptId": "task-1", "status": "queued", "done": False}},
    {"ok": True, "type": "prompt.run",
     "promptRun": {"promptId": "task-1", "status": "finished", "done": True}},
    {"ok": True, "type": "box.interrupted"},
]
official = BoxClient("key", base_url="https://invalid.example",
                     opener=lambda *a, **k: Response(envelopes.pop(Z)))
check(official.prompt("bx_23456789", "p", provider="openai").prompt_id == "task-1",
      "official nested prompt start envelope failed")
check(official.prompt_status("bx_23456789", "task-1").status == "finished",
      "official nested prompt.run envelope failed")
official.interrupt("bx_23456789")
check(not envelopes, "official box.interrupted envelope was not consumed")
secret = "box_SUPERSECRET"
error = BoxAPIError(N503, "bad\n" + secret, "Authorization: " + secret + "?token=hidden")
check(secret not in str(error) and "\n" not in str(error) and error.retryable is False,
      "API errors did not sanitize secrets/control characters")
matching = Job("admit-ok", "true", provider="box", pin_to_provider=True,
               executor="box-command", platform_os="linux", architecture="x86_64",
               cpu_cores=N4, memory_gb=N8, disk_gb=N80)
check(admit_job(matching, BOX_CAPABILITIES).accepted, "fixed shape rejected exact workload")
rejecting = Job("admit-no", "true", executor="other", platform_os="darwin",
                architecture="arm64", cpu_cores=N5, memory_gb=N9, disk_gb=N81,
                gpu_mem_gb=O, preemptible=True, region="moon")
decision = admit_job(rejecting, BOX_CAPABILITIES)
check(not decision.accepted and len(decision.reasons) == N9, "admission reject branches incomplete")
fields = dict(executor="box-prompt", platform_os="linux", architecture="x86_64",
              cpu_cores=N4, memory_gb=N8, disk_gb=N80, box_ttl_seconds=N901,
              prompt="answer", prompt_provider="openai", prompt_model="m",
              prompt_reasoning_effort="high", artifact_paths=["/tmp/a"])
round_trip = Job.from_json(Job("round-trip", "", provider="box", **fields).to_json())
check(all(getattr(round_trip, key) == value for key, value in fields.items()),
      "Box Job fields failed JSON round trip")
class PreconditionFailed(Exception):
    pass
google = types.ModuleType("google")
api_core = types.ModuleType("google.api_core")
exceptions = types.ModuleType("google.api_core.exceptions")
exceptions.PreconditionFailed = PreconditionFailed
api_core.exceptions = exceptions
google.api_core = api_core
sys.modules.update({"google": google, "google.api_core": api_core,
                    "google.api_core.exceptions": exceptions})
class Blob:
    def __init__(self, bucket, name):
        self.bucket, self.name, self.generation = bucket, name, None
    def exists(self): return self.name in self.bucket.data
    def reload(self): self.generation = self.bucket.data[self.name][Z]
    def download_as_text(self, if_generation_match=None):
        generation, content = self.bucket.data[self.name]
        if if_generation_match is not None and generation != if_generation_match:
            raise PreconditionFailed()
        return content
    def upload_from_string(self, content, if_generation_match=None):
        current = self.bucket.data.get(self.name)
        generation = current[Z] if current else Z
        if if_generation_match is not None and generation != if_generation_match:
            raise PreconditionFailed()
        self.bucket.data[self.name] = (generation + O, content)
        self.generation = generation + O
class Bucket:
    def __init__(self): self.data = {}
    def blob(self, name): return Blob(self, name)
class Crash(BaseException): pass
class Store:
    def __init__(self):
        self._sdk_bucket = Bucket()
        self._azure_backend = None
        self.jobs = {name: {} for name in ("queue", "running", "completed", "failed")}
        self.uploads, self.moves = {}, []
        self.crash_queue_move = False
        self.crash_terminal_move = False
    def create_text_if_absent(self, path, content):
        blob = self._sdk_bucket.blob(path)
        try:
            blob.upload_from_string(content, if_generation_match=Z)
            return True
        except PreconditionFailed:
            return False
    def read_text_versioned(self, path):
        blob = self._sdk_bucket.blob(path)
        if not blob.exists():
            return None
        blob.reload()
        return VersionedText(
            blob.download_as_text(if_generation_match=blob.generation),
            str(blob.generation),
        )
    def compare_and_swap_text(self, path, expected_version, content):
        blob = self._sdk_bucket.blob(path)
        try:
            blob.upload_from_string(content, if_generation_match=int(expected_version))
        except PreconditionFailed:
            raise StorageConflict("changed concurrently") from None
        return str(blob.generation)
    def list_jobs_priority_first(self, prefix, cap): return list(self.jobs[prefix].values())[:cap]
    def list_jobs(self, prefix, **unused): return list(self.jobs[prefix].values())
    def move_job(self, job, source, destination):
        if self.crash_queue_move and source == "queue":
            self.crash_queue_move = False
            raise Crash("injected crash after lease/resource save")
        if self.crash_terminal_move and source == "running":
            self.crash_terminal_move = False
            raise Crash("injected crash after released lease save")
        self.jobs[source].pop(job.job_id, None)
        self.jobs[destination][job.job_id] = job
        self.moves.append((job.job_id, source, destination))
    def _upload_text(self, path, content): self.uploads[path] = content
lease_store = Store()
leases = ProviderLeaseStore(lease_store)
lease = leases.acquire("lease-job", "box", "one", N60, N600)
lease.transition(LeaseState.PROVISIONING, "one", lease.fence_token)
lease = leases.save(lease, lease.version)
stale = ProviderLease.from_dict(lease.to_dict(), lease.version)
lease.transition(LeaseState.READY, "one", lease.fence_token)
lease = leases.save(lease, lease.version)
stale.transition(LeaseState.READY, "one", stale.fence_token)
raises(LeaseConflict, lambda: leases.save(stale, stale.version), "concurrently")
raises(LeaseConflict, lambda: leases.acquire("lease-job", "box", "two", N60, N600), "still live")
old_fence = lease.fence_token
lease.owner_expires_at = (datetime.now(timezone.utc) - timedelta(seconds=O)).isoformat()
lease = leases.save(lease, lease.version)
taken = leases.acquire("lease-job", "box", "two", N60, N600)
check(taken.owner_id == "two" and taken.fence_token != old_fence, "expired lease takeover failed")
class FakeClient:
    def __init__(self):
        self.created = Z
        self.commands, self.writes, self.stopped, self.interrupted = [], [], [], []
        self.files = {}
        self.crash_start = self.crash_artifact = self.crash_stop = self.fail_start = self.raise_start = False
    def limits(self): return BoxLimits(True, Z, N4, "active")
    def create_box(self, ttl, no_env=True):
        self.created += O
        return BoxInfo("bx_23456789", "smoke", "ready")
    def get_box(self, box_id): return BoxInfo(box_id, "smoke", "ready")
    def update_box(self, box_id, **unused): return self.get_box(box_id)
    def write_file(self, box_id, path, content, **unused):
        self.writes.append((path, content)); return {}
    def execute_command(self, box_id, command, **unused):
        self.commands.append(command)
        if self.crash_start and "setsid nohup" in command:
            self.crash_start = False
            self.files[self.writes[-O][Z].replace("run.sh", "launch_intent")] = "intent"
            raise Crash("injected crash in STARTING")
        if self.raise_start: raise RuntimeError("injected recoverable STARTING exception")
        return BoxCommandResult(not self.fail_start, Z, "", "", "", False, False, False)
    def read_file(self, box_id, path, **unused): return {"content": self.files[path]}
    def stop_box(self, box_id):
        if self.crash_stop:
            self.crash_stop = False; raise Crash("injected crash in RELEASING")
        self.stopped.append(box_id); return {}
    def prompt(self, box_id, prompt, **kwargs): return BoxPromptRun("prompt-1", "queued", False, {})
    def prompt_status(self, box_id, prompt_id): return BoxPromptRun(prompt_id, "finished", True, {})
    def list_events(self, box_id, cursor="", event_type="", **unused):
        if event_type == "response" and not cursor:
            return BoxEventPage((
                {"type": "response", "taskId": "other", "data": {"content": "wrong"}},
                {"type": "response", "taskId": "prompt-1", "data": {"content": "prompt"}},
            ), "next", True)
        if event_type == "response":
            return BoxEventPage(({"type": "response", "taskId": "prompt-1",
                                  "data": {"content": "done"}},), "", False)
        return BoxEventPage((), "", False)
    def download_artifact(self, box_id, path, max_bytes):
        if self.crash_artifact:
            self.crash_artifact = False; raise Crash("injected crash in COLLECTING")
        return b"artifact"
    def interrupt(self, box_id):
        self.interrupted.append(box_id); return {}
os.environ["BOX_TTL_SECONDS"] = "600"
fake = FakeClient()
provider = BoxProvider(fake)
store = Store()
job = Job("e2e-command", "printf done", provider="box", pin_to_provider=True,
          executor="box-command", platform_os="linux", architecture="x86_64",
          artifact_paths=["result.bin"])
store.jobs["queue"][job.job_id] = job
store.crash_queue_move = True
raises(Crash, lambda: dispatch_box_jobs(store, provider, "owner"), "injected crash")
check(fake.created == O and ProviderLeaseStore(store).load(job.job_id).provider_resource_id,
      "crash point did not persist resource identity")
check(dispatch_box_jobs(store, provider, "owner") == O and fake.created == O,
      "crash recovery duplicated allocation")
leases = ProviderLeaseStore(store)
seed = leases.acquire(job.job_id, "box", "seed", N60, N600)
seed.transition(LeaseState.READY, "seed", seed.fence_token)
seed = leases.save(seed, seed.version)
check(seed.state == LeaseState.READY.value, "READY recovery state was not durable")
seed.relinquish("seed", seed.fence_token)
leases.save(seed, seed.version)
fake.crash_start = True
raises(Crash, lambda: reconcile_box_jobs(store, provider, "owner"), "STARTING")
check(leases.load(job.job_id).state == LeaseState.STARTING.value, "STARTING was not durable")
check(reconcile_box_jobs(store, provider, "owner") == O, "STARTING recovery did not launch")
launch = fake.commands[-O]
check(len(fake.writes) == O and launch.index("launch_intent") < launch.index("setsid nohup")
      and fake.writes[-O][Z].startswith(".stado/") and "tail -c 57344" in fake.writes[-O][O],
      "durable launch intent, no-rewrite recovery, relative paths, or output bound failed")
root = ".stado/e2e-command/"
(fake.files.update({root + "exit_code": "0", root + "stdout.log": "hello", root + "stderr.log": "warn"}), (command_version := int(leases.load(job.job_id).version)))
fake.crash_artifact = True
raises(Crash, lambda: reconcile_box_jobs(store, provider, "owner"), "COLLECTING")
check(leases.load(job.job_id).state == LeaseState.COLLECTING.value and int(leases.load(job.job_id).version) >= command_version + N8, "COLLECTING durability or command/storage keepalive failed")
collect_version = int(leases.load(job.job_id).version)
fake.crash_stop = True
raises(Crash, lambda: reconcile_box_jobs(store, provider, "owner"), "RELEASING")
check(leases.load(job.job_id).state == LeaseState.RELEASING.value
      and int(leases.load(job.job_id).version) >= collect_version + N5,
      "RELEASING durability or artifact-loop lease keepalive failed")
store.crash_terminal_move = True
raises(Crash, lambda: reconcile_box_jobs(store, provider, "owner"), "released lease")
check(leases.load(job.job_id).state == LeaseState.RELEASED.value, "RELEASED was not durable")
check(reconcile_box_jobs(store, provider, "owner") == O and job.state == JobState.COMPLETED.value,
      "RELEASED recovery did not persist terminal job")
check(set(store.uploads.values()) >= {"hello", "warn"} and fake.stopped, "release preceded persistence")
dead = Job("dead-marker", "true", provider="box", pin_to_provider=True, executor="box-command")
store.jobs["running"][dead.job_id] = dead
dl = ProviderLease.new(dead.job_id, "box", "dead", N60, N600); dl.provider_resource_id = "bx_23456789"
for state in (LeaseState.PROVISIONING, LeaseState.READY, LeaseState.STARTING): dl.transition(state, "dead", dl.fence_token)
dl.operation_id = "stado-dead"; dl = leases.create(dl); dl.relinquish("dead", dl.fence_token); leases.save(dl, dl.version)
fake.files[".stado/dead-marker/launch_intent"] = "intent"; fake.fail_start = True; written = len(fake.writes)
check(reconcile_box_jobs(store, provider, "owner") == O and dead.state == JobState.FAILED.value
      and "launch marker" in dead.error and len(fake.writes) == written, "dead marker recovery was not failed-unknown")
fake.fail_start = False
if True:
    aged = Job("aged-start", "true", provider="box", pin_to_provider=True, executor="box-command"); store.jobs["running"][aged.job_id] = aged
    al = ProviderLease.new(aged.job_id, "box", "aged", N60, N600); al.provider_resource_id = "bx_23456789"
    for state in (LeaseState.PROVISIONING, LeaseState.READY, LeaseState.STARTING): al.transition(state, "aged", al.fence_token)
    al.operation_id = "stado-aged"; al.operation_started_at = (datetime.now(timezone.utc) - timedelta(seconds=N123)).isoformat()
    al = leases.create(al); al.relinquish("aged", al.fence_token); leases.save(al, al.version)
    fake.files[".stado/aged-start/launch_intent"] = "intent"; fake.raise_start = True
    check(reconcile_box_jobs(store, provider, "owner") == O and aged.state == JobState.FAILED.value and leases.load(aged.job_id).state == LeaseState.RELEASED.value, "aged STARTING exception retried forever")
    fake.raise_start = False
    prompt = Job("e2e-prompt", "", provider="box", pin_to_provider=True, executor="box-prompt", prompt="do it", prompt_provider="openai")
store.jobs["queue"][prompt.job_id] = prompt
check(dispatch_box_jobs(store, provider, "owner") == O, "prompt dispatch failed")
check(reconcile_box_jobs(store, provider, "owner") == O, "prompt start failed")
prompt_version = int(leases.load(prompt.job_id).version)
check(reconcile_box_jobs(store, provider, "owner") == O and prompt.state == JobState.COMPLETED.value
      and int(leases.load(prompt.job_id).version) >= prompt_version + N5,
      "prompt status completion or event-loop lease keepalive failed")
check(store.uploads["status/e2e-prompt/output/prompt_output.txt"] == "prompt\ndone", "paged prompt output was not task-filtered")
storage_module = __import__("stado.queue.storage", fromlist=["JobStorage"])
for suffix, executor in (("command", "box-command"), ("prompt", "box-prompt")):
    item = Job("cancel-" + suffix, "sleep 9", provider="box", pin_to_provider=True,
               executor=executor, prompt="wait" if executor == "box-prompt" else "", prompt_provider="openai" if executor == "box-prompt" else "")
    store.jobs["queue"][item.job_id] = item; dispatch_box_jobs(store, provider, "owner"); reconcile_box_jobs(store, provider, "owner")
    if executor == "box-command":
        original, move_count = storage_module.JobStorage, len(store.moves)
        storage_module.JobStorage = lambda unused: store
        provider.delete_instance(item.instance_ref)
        storage_module.JobStorage = original
        check(item.job_id in store.jobs["running"] and len(store.moves) == move_count, "legacy delete bridge moved the queue job")
        item.state, item.error = JobState.FAILED.value, "cancelled"; store.move_job(item, "running", "failed")
    else: cancel_box_job(store, provider, item, "owner")
    check(item.state == JobState.FAILED.value and item.error == "cancelled", "cancel state failed")
check(any(command.startswith("kill -- -$(cat ") for command in fake.commands), "command cancel omitted kill")
check(fake.interrupted == ["bx_23456789"] and not NETWORK_ATTEMPTS, "prompt cancel or network denial failed")
print("PASS box offline smoke: 20 contracts; network denied; final recovery verified")
