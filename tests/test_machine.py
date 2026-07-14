"""Behavioral contracts for the stable ``stado machine`` automation facade."""
from __future__ import annotations

import hashlib
import io
import json
import tempfile
import tarfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from click.testing import CliRunner

from stado.machine import MachineError, MachineFacade, machine, normalize_job
from stado.models import Job


NORMALIZED_JOB_FIELDS = {
    "job_id",
    "run_id",
    "batch_id",
    "state",
    "command",
    "provider",
    "gpu_type",
    "gpu_mem_gb",
    "machine_type",
    "created_at",
    "started_at",
    "completed_at",
    "failed_at",
    "error",
    "output_uri",
}


def make_job(state: str = "queued", **overrides: object) -> Job:
    values: dict[str, object] = {
        "job_id": "job-123",
        "run_id": "run-1",
        "batch_id": "batch-1",
        "state": state,
        "command": "python3 train.py",
        "provider": "gcp",
        "gpu_type": "nvidia-rtx-pro-6000",
        "gpu_mem_gb": 24,
        "machine_type": "g4-standard-48",
        "created_at": "2026-07-13T10:00:00+00:00",
        "started_at": None,
        "completed_at": None,
        "failed_at": None,
        "error": None,
        "output_uri": "",
    }
    values.update(overrides)
    return Job(**values)


def write_source_archive(path: Path, content: bytes = b"print('validation')\n") -> str:
    with tarfile.open(path, mode="w:gz") as archive:
        member = tarfile.TarInfo("paper/experiment.py")
        member.size = len(content)
        archive.addfile(member, io.BytesIO(content))
    return hashlib.sha256(path.read_bytes()).hexdigest()


def write_archive_with_member(path: Path, member: tarfile.TarInfo) -> None:
    with tarfile.open(path, mode="w:gz") as archive:
        payload = None if member.size == 0 else io.BytesIO(b"x" * member.size)
        archive.addfile(member, payload)


class FakeStorage:
    """In-memory storage boundary: deliberately has no SDK or network client."""

    def __init__(self) -> None:
        self.bucket_name = "unit-test-bucket"
        self.backend_name = "gcs"
        self.jobs: dict[tuple[str, str], Job] = {}
        self.text: dict[str, str] = {}
        self.binary: dict[str, bytes] = {}
        self.read_job_calls: list[tuple[str, str]] = []
        self.list_calls: list[str] = []
        self.writes: list[tuple[str, str]] = []
        self.deletes: list[str] = []
        self.events: list[tuple[str, str]] = []

    def read_job(self, prefix: str, job_id: str) -> Job | None:
        self.read_job_calls.append((prefix, job_id))
        return self.jobs.get((prefix, job_id))

    def write_job(self, prefix: str, job: Job) -> None:
        self.jobs[(prefix, job.job_id)] = job
        self.writes.append((prefix, job.job_id))
        self.events.append(("write_job", f"{prefix}/{job.job_id}.json"))

    def delete_job(self, prefix: str, job_id: str) -> None:
        self.jobs.pop((prefix, job_id), None)
        self.deletes.append(f"{prefix}/{job_id}.json")
        self.events.append(("delete_job", f"{prefix}/{job_id}.json"))

    def create_text_if_absent(self, path: str, content: str) -> bool:
        if path in self.text:
            return False
        self.text[path] = content
        self.writes.append(("text", path))
        self.events.append(("create_text", path))
        return True

    def _download_text(self, path: str) -> str | None:
        return self.text.get(path)

    def _upload_text(self, path: str, content: str) -> None:
        self.text[path] = content
        self.writes.append(("text", path))
        self.events.append(("upload_text", path))

    def _delete_blob(self, path: str) -> None:
        self.text.pop(path, None)
        self.binary.pop(path, None)
        self.deletes.append(path)
        self.events.append(("delete_blob", path))

    def read_bytes(self, path: str) -> bytes | None:
        return self.binary.get(path)

    def upload_file_if_absent(self, path: str, filename: str) -> bool:
        if path in self.binary:
            return False
        self.binary[path] = Path(filename).read_bytes()
        self.events.append(("upload_file", path))
        return True

    def _list_paths(self, prefix: str) -> list[str]:
        self.list_calls.append(prefix)
        return sorted(path for path in self.binary if path.startswith(prefix))

    def download_blob(self, path: str, filename: str) -> bool:
        payload = self.binary.get(path)
        if payload is None:
            return False
        Path(filename).write_bytes(payload)
        return True


class MachineSchemaTests(unittest.TestCase):
    def test_normalized_job_has_only_the_versioned_schema_and_canonical_state(self) -> None:
        job = make_job(state="queue", instance_ref="provider-secret", submitted_by="alice")

        normalized = normalize_job(job)

        self.assertEqual(set(normalized), NORMALIZED_JOB_FIELDS)
        self.assertEqual(normalized["state"], "queued")
        self.assertNotIn("instance_ref", normalized)
        self.assertNotIn("submitted_by", normalized)

    def test_request_schema_rejects_missing_unknown_and_wrong_typed_fields(self) -> None:
        facade = MachineFacade(store=FakeStorage(), submitter=Mock())
        invalid_requests = {
            "missing command": {"client_request_id": "req-1"},
            "unknown field": {"client_request_id": "req-1", "command": "x", "secret": "no"},
            "path-like id": {"client_request_id": "../req", "command": "x"},
            "boolean vram": {"client_request_id": "req-1", "command": "x", "vram_gb": True},
            "bad packages": {"client_request_id": "req-1", "command": "x", "apt_packages": [""]},
        }
        for name, request in invalid_requests.items():
            with self.subTest(name):
                with self.assertRaises(MachineError) as raised:
                    facade.submit_request(request)
                self.assertEqual(raised.exception.code, "INVALID_REQUEST")
                self.assertFalse(raised.exception.retryable)

    def test_cli_success_is_one_exact_json_envelope_on_stdout(self) -> None:
        facade = Mock()
        facade.status.return_value = {"job": normalize_job(make_job())}

        with patch("stado.machine.MachineFacade", return_value=facade):
            result = CliRunner().invoke(machine, ["status", "job-123"])

        self.assertEqual(result.exit_code, 0)
        self.assertEqual(len(result.stdout.splitlines()), 1)
        self.assertEqual(result.stderr, "")
        self.assertEqual(
            json.loads(result.stdout),
            {"schema_version": 1, "ok": True, "result": facade.status.return_value},
        )

    def test_cli_machine_errors_are_truthful_json_and_exit_nonzero(self) -> None:
        facade = Mock()
        facade.status.side_effect = MachineError("NOT_FOUND", "job 'missing' was not found")

        with patch("stado.machine.MachineFacade", return_value=facade):
            result = CliRunner().invoke(machine, ["status", "missing"])

        self.assertNotEqual(result.exit_code, 0)
        self.assertEqual(len(result.stdout.splitlines()), 1)
        self.assertEqual(result.stderr, "")
        self.assertEqual(
            json.loads(result.stdout),
            {
                "schema_version": 1,
                "ok": False,
                "error": {
                    "code": "NOT_FOUND",
                    "message": "job 'missing' was not found",
                    "retryable": False,
                },
            },
        )

    def test_cli_unexpected_failures_keep_the_real_message_and_exit_nonzero(self) -> None:
        facade = Mock()
        facade.status.side_effect = RuntimeError("storage unavailable")

        with patch("stado.machine.MachineFacade", return_value=facade):
            result = CliRunner().invoke(machine, ["status", "job-123"])

        self.assertNotEqual(result.exit_code, 0)
        self.assertEqual(
            json.loads(result.stdout)["error"],
            {"code": "INTERNAL", "message": "storage unavailable", "retryable": False},
        )


class MachineSubmissionTests(unittest.TestCase):
    def test_exact_idempotent_replay_returns_identical_job_without_resubmitting(self) -> None:
        store = FakeStorage()
        submitter = Mock(return_value=make_job())
        facade = MachineFacade(store=store, submitter=submitter)
        request = {
            "client_request_id": "paper-run-1",
            "command": "python3 train.py",
            "provider": "gcp",
            "gpu_type": "nvidia-rtx-pro-6000",
            "vram_gb": 24,
            "max_cost_per_hour_usd": 0,
            "pin_to_provider": True,
            "priority": 7,
            "repo": "https://example.test/paper.git",
            "repo_workdir": "paper",
            "repo_extras": "train",
            "pre_command": "export MODE=validation",
            "apt_packages": ["git"],
            "output_uri": "gs://results/paper",
            "verify_command": "test -f result.json",
            "exclusive": True,
        }

        first = facade.submit_request(request)
        replay = facade.submit_request(dict(reversed(list(request.items()))))

        self.assertEqual(replay, first)
        submitter.assert_called_once_with(
            "python3 train.py",
            bucket="unit-test-bucket",
            provider="gcp",
            gpu_type="nvidia-rtx-pro-6000",
            vram_gb=24,
            max_cost_per_hour_usd=0.0,
            pin_to_provider=True,
            priority=7,
            repo="https://example.test/paper.git",
            repo_workdir="paper",
            repo_extras="train",
            pre_command="export MODE=validation",
            apt_packages=["git"],
            output_uri="gs://results/paper",
            verify_command="test -f result.json",
            exclusive=True,
        )
        record = json.loads(store.text["machine_requests/paper-run-1.json"])
        self.assertEqual(record["state"], "submitted")
        self.assertEqual(record["job"], first["job"])

    def test_same_id_with_changed_request_is_rejected_without_resubmitting(self) -> None:
        store = FakeStorage()
        submitter = Mock(return_value=make_job())
        facade = MachineFacade(store=store, submitter=submitter)
        facade.submit_request({"client_request_id": "same-id", "command": "first"})

        with self.assertRaises(MachineError) as raised:
            facade.submit_request({"client_request_id": "same-id", "command": "different"})

        self.assertEqual(raised.exception.code, "IDEMPOTENCY_CONFLICT")
        self.assertFalse(raised.exception.retryable)
        submitter.assert_called_once()

    def test_submitter_failure_is_non_silent_retryable_and_releases_reservation(self) -> None:
        store = FakeStorage()
        facade = MachineFacade(store=store, submitter=Mock(side_effect=RuntimeError("provider refused job")))

        with self.assertRaises(MachineError) as raised:
            facade.submit_request({"client_request_id": "retry-me", "command": "train"})

        self.assertEqual(raised.exception.code, "SUBMIT_FAILED")
        self.assertEqual(raised.exception.message, "provider refused job")
        self.assertTrue(raised.exception.retryable)
        self.assertNotIn("machine_requests/retry-me.json", store.text)


class MachineSourceArchiveTests(unittest.TestCase):
    def test_archive_upload_returns_sha_uri_and_bootstraps_the_original_command(self) -> None:
        store = FakeStorage()
        submitter = Mock(return_value=make_job())
        with tempfile.TemporaryDirectory() as directory:
            archive = Path(directory) / "paper.tar.gz"
            source_sha = write_source_archive(archive)
            archive_bytes = archive.read_bytes()

            result = MachineFacade(store=store, submitter=submitter).submit_request({
                "client_request_id": "snapshot-1",
                "command": "python3 experiments/run.py",
                "repo": "https://example.test/stale.git",
                "repo_workdir": "stale",
                "repo_extras": "train",
                "pre_command": "export VALIDATION=1",
                "source_archive_path": str(archive),
            })

        blob_path = f"machine_inputs/snapshot-1/{source_sha}.tar.gz"
        source_uri = f"gs://unit-test-bucket/{blob_path}"
        self.assertEqual(set(result), {"job", "source_archive_uri", "source_sha256"})
        self.assertEqual(result["source_sha256"], source_sha)
        self.assertEqual(result["source_archive_uri"], source_uri)
        self.assertEqual(store.binary[blob_path], archive_bytes)
        submitted_command, = submitter.call_args.args
        submitted_options = submitter.call_args.kwargs
        self.assertEqual(submitted_command, "python3 experiments/run.py")
        self.assertNotIn("source_archive_path", submitted_options)
        self.assertEqual(submitted_options["repo"], "")
        self.assertEqual(submitted_options["repo_workdir"], "")
        self.assertEqual(submitted_options["repo_extras"], "")
        bootstrap = submitted_options["pre_command"]
        self.assertIn(f"gsutil cp {source_uri}", bootstrap)
        self.assertIn(f"/tmp/stado-machine-source/snapshot-1-{source_sha}", bootstrap)
        self.assertIn("tar --extract --gzip", bootstrap)
        self.assertTrue(bootstrap.endswith("export VALIDATION=1"))
        record = json.loads(store.text["machine_requests/snapshot-1.json"])
        self.assertEqual(record["result"], result)
        self.assertEqual(record["source_archive_uri"], source_uri)
        self.assertEqual(record["source_sha256"], source_sha)

    def test_same_archive_bytes_at_another_path_are_an_exact_replay(self) -> None:
        store = FakeStorage()
        submitter = Mock(return_value=make_job())
        facade = MachineFacade(store=store, submitter=submitter)
        with tempfile.TemporaryDirectory() as directory:
            first_path = Path(directory) / "first.tar.gz"
            second_path = Path(directory) / "second.tar.gz"
            write_source_archive(first_path)
            second_path.write_bytes(first_path.read_bytes())
            request = {
                "client_request_id": "snapshot-replay",
                "command": "python3 experiments/run.py",
                "source_archive_path": str(first_path),
            }

            first = facade.submit_request(request)
            replay = facade.submit_request({**request, "source_archive_path": str(second_path)})

        self.assertEqual(replay, first)
        submitter.assert_called_once()

    def test_changed_archive_bytes_conflict_for_the_same_id(self) -> None:
        store = FakeStorage()
        submitter = Mock(return_value=make_job())
        facade = MachineFacade(store=store, submitter=submitter)
        with tempfile.TemporaryDirectory() as directory:
            archive = Path(directory) / "paper.tar.gz"
            write_source_archive(archive, b"first source\n")
            request = {
                "client_request_id": "snapshot-conflict",
                "command": "python3 experiments/run.py",
                "source_archive_path": str(archive),
            }
            facade.submit_request(request)
            write_source_archive(archive, b"changed source\n")

            with self.assertRaises(MachineError) as raised:
                facade.submit_request(request)

        self.assertEqual(raised.exception.code, "IDEMPOTENCY_CONFLICT")
        submitter.assert_called_once()

    def test_archive_path_must_be_a_nonempty_bounded_regular_file(self) -> None:
        store = FakeStorage()
        facade = MachineFacade(store=store, submitter=Mock())
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            empty = root / "empty.tar.gz"
            empty.touch()
            regular = root / "paper.tar.gz"
            write_source_archive(regular)
            cases: list[tuple[str, Path, object]] = [
                ("directory", root, None),
                ("empty", empty, None),
                ("bounded", regular, patch("stado.machine.MAX_SOURCE_ARCHIVE_BYTES", regular.stat().st_size - 1)),
            ]
            for name, source, boundary_patch in cases:
                with self.subTest(name=name):
                    context = boundary_patch if boundary_patch is not None else patch(
                        "stado.machine.MAX_SOURCE_ARCHIVE_BYTES", 512 * 1024 * 1024
                    )
                    with context:
                        with self.assertRaises(MachineError) as raised:
                            facade.submit_request({
                                "client_request_id": f"invalid-{name}",
                                "command": "run",
                                "source_archive_path": str(source),
                            })
                    self.assertEqual(raised.exception.code, "INVALID_SOURCE_ARCHIVE")

    def test_archive_path_symlink_and_symlink_member_are_rejected(self) -> None:
        store = FakeStorage()
        facade = MachineFacade(store=store, submitter=Mock())
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            regular = root / "regular.tar.gz"
            write_source_archive(regular)
            archive_link = root / "linked.tar.gz"
            archive_link.symlink_to(regular)
            member_link = root / "member-link.tar.gz"
            member = tarfile.TarInfo("paper/current")
            member.type = tarfile.SYMTYPE
            member.linkname = "experiment.py"
            write_archive_with_member(member_link, member)

            for name, source in (("archive path", archive_link), ("archive member", member_link)):
                with self.subTest(name=name):
                    with self.assertRaises(MachineError) as raised:
                        facade.submit_request({
                            "client_request_id": f"symlink-{name.replace(' ', '-')}",
                            "command": "run",
                            "source_archive_path": str(source),
                        })
                    self.assertEqual(raised.exception.code, "INVALID_SOURCE_ARCHIVE")
        facade.submitter.assert_not_called()

    def test_archive_member_traversal_is_rejected_before_upload(self) -> None:
        store = FakeStorage()
        submitter = Mock()
        with tempfile.TemporaryDirectory() as directory:
            archive = Path(directory) / "traversal.tar.gz"
            member = tarfile.TarInfo("../outside.py")
            member.size = 1
            write_archive_with_member(archive, member)

            with self.assertRaises(MachineError) as raised:
                MachineFacade(store=store, submitter=submitter).submit_request({
                    "client_request_id": "traversal",
                    "command": "run",
                    "source_archive_path": str(archive),
                })

        self.assertEqual(raised.exception.code, "INVALID_SOURCE_ARCHIVE")
        self.assertFalse(any(event[0] == "upload_file" for event in store.events))
        submitter.assert_not_called()


class MachineStatusAndLogTests(unittest.TestCase):
    def test_status_uses_direct_reads_and_finds_cancelled_tombstones(self) -> None:
        store = FakeStorage()
        store.jobs[("cancelled", "job-123")] = make_job(state="cancelled", error="cancelled")
        facade = MachineFacade(store=store)

        result = facade.status("job-123")

        self.assertEqual(result["job"]["state"], "cancelled")
        self.assertEqual(store.list_calls, [])
        self.assertEqual(
            [prefix for prefix, job_id in store.read_job_calls if job_id == "job-123"],
            ["queue", "running", "completed", "uploaded", "failed", "cancelled"],
        )

    def test_missing_live_log_is_an_empty_success_page(self) -> None:
        store = FakeStorage()
        store.jobs[("running", "job-123")] = make_job(state="running")

        page = MachineFacade(store=store).read_logs("job-123", cursor=0, limit=1024)

        self.assertEqual(
            page,
            {"job_id": "job-123", "cursor": 0, "next_cursor": 0, "eof": True, "text": ""},
        )

    def test_log_cursor_is_a_byte_offset_with_exact_end_boundary(self) -> None:
        store = FakeStorage()
        store.jobs[("running", "job-123")] = make_job(state="running")
        store.binary["status/job-123/output/command_output.log"] = "A€B".encode("utf-8")
        facade = MachineFacade(store=store)

        first = facade.read_logs("job-123", cursor=0, limit=1)
        end = facade.read_logs("job-123", cursor=5, limit=10)

        self.assertEqual(first["text"], "A")
        self.assertEqual(first["next_cursor"], 1)
        self.assertFalse(first["eof"])
        self.assertEqual(end["text"], "")
        self.assertEqual(end["next_cursor"], 5)
        self.assertTrue(end["eof"])

    def test_log_cursor_rejects_negative_and_past_end_offsets(self) -> None:
        store = FakeStorage()
        store.jobs[("running", "job-123")] = make_job(state="running")
        store.binary["status/job-123/output/command_output.log"] = b"abc"
        facade = MachineFacade(store=store)

        for cursor in (-1, 4):
            with self.subTest(cursor=cursor):
                with self.assertRaises(MachineError) as raised:
                    facade.read_logs("job-123", cursor=cursor, limit=1)
                self.assertEqual(raised.exception.code, "INVALID_CURSOR")
        with self.assertRaises(MachineError) as raised:
            facade.read_logs("job-123", cursor=0, limit=0)
        self.assertEqual(raised.exception.code, "INVALID_CURSOR")


class MachineCancellationTests(unittest.TestCase):
    def test_queued_cancel_persists_tombstone_before_removing_queue_entry(self) -> None:
        store = FakeStorage()
        store.jobs[("queue", "job-123")] = make_job(state="queued")

        result = MachineFacade(store=store).cancel_job("job-123")

        self.assertEqual(result["job"]["state"], "cancelled")
        self.assertEqual(result["job"]["error"], "cancelled")
        self.assertIn(("cancelled", "job-123"), store.jobs)
        self.assertNotIn(("queue", "job-123"), store.jobs)
        self.assertIn("cancellations/job-123.json", store.text)
        self.assertLess(
            store.events.index(("write_job", "cancelled/job-123.json")),
            store.events.index(("delete_job", "queue/job-123.json")),
        )

    def test_running_cancel_uses_provider_and_finishes_as_cancelled(self) -> None:
        store = FakeStorage()
        store.jobs[("running", "job-123")] = make_job(
            state="running", provider="gcp", instance_ref="instance-9"
        )
        provider = Mock()

        with patch("stado.providers.get_provider", return_value=provider):
            result = MachineFacade(store=store).cancel_job("job-123")

        provider.delete_instance.assert_called_once_with("instance-9")
        self.assertEqual(result["job"]["state"], "cancelled")
        self.assertNotIn(("running", "job-123"), store.jobs)
        self.assertIn(("cancelled", "job-123"), store.jobs)

    def test_terminal_cancel_is_idempotent_and_does_not_touch_provider_or_storage(self) -> None:
        for state in ("completed", "uploaded", "failed", "cancelled"):
            with self.subTest(state=state):
                store = FakeStorage()
                store.jobs[(state, "job-123")] = make_job(state=state)
                provider = Mock()
                with patch("stado.providers.get_provider", return_value=provider):
                    result = MachineFacade(store=store).cancel_job("job-123")
                self.assertEqual(result["job"]["state"], state)
                self.assertEqual(store.writes, [])
                self.assertEqual(store.deletes, [])
                provider.delete_instance.assert_not_called()

    def test_provider_cancellation_failure_is_truthful_and_keeps_running_job(self) -> None:
        store = FakeStorage()
        store.jobs[("running", "job-123")] = make_job(
            state="running", provider="gcp", instance_ref="instance-9"
        )
        provider = Mock()
        provider.delete_instance.side_effect = RuntimeError("permission denied")

        with patch("stado.providers.get_provider", return_value=provider):
            with self.assertRaises(MachineError) as raised:
                MachineFacade(store=store).cancel_job("job-123")

        self.assertEqual(raised.exception.code, "CANCEL_FAILED")
        self.assertEqual(raised.exception.message, "permission denied")
        self.assertTrue(raised.exception.retryable)
        self.assertIn(("running", "job-123"), store.jobs)
        self.assertNotIn(("cancelled", "job-123"), store.jobs)


class MachineArtifactTests(unittest.TestCase):
    def test_terminal_artifacts_are_downloaded_with_relative_sha256_manifest(self) -> None:
        store = FakeStorage()
        store.jobs[("completed", "job-123")] = make_job(state="completed")
        store.binary.update({
            "status/job-123/output/metrics.json": b'{"loss":0.1}\n',
            "status/job-123/output/nested/report.txt": b"validation passed\n",
        })

        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "artifacts"
            manifest = MachineFacade(store=store).download_artifacts("job-123", output)

            self.assertEqual(Path(manifest["output_dir"]), output.resolve())
            self.assertEqual(
                manifest["artifacts"],
                [
                    {
                        "relative_path": "metrics.json",
                        "size_bytes": len(b'{"loss":0.1}\n'),
                        "sha256": hashlib.sha256(b'{"loss":0.1}\n').hexdigest(),
                    },
                    {
                        "relative_path": "nested/report.txt",
                        "size_bytes": len(b"validation passed\n"),
                        "sha256": hashlib.sha256(b"validation passed\n").hexdigest(),
                    },
                ],
            )
            self.assertEqual((output / "metrics.json").read_bytes(), b'{"loss":0.1}\n')
            self.assertEqual((output / "nested" / "report.txt").read_bytes(), b"validation passed\n")

    def test_artifacts_require_terminal_job_and_nonempty_canonical_output(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            for state, code in (("running", "NOT_TERMINAL"), ("failed", "NO_ARTIFACTS")):
                with self.subTest(state=state):
                    store = FakeStorage()
                    store.jobs[(state, "job-123")] = make_job(state=state)
                    with self.assertRaises(MachineError) as raised:
                        MachineFacade(store=store).download_artifacts("job-123", directory)
                    self.assertEqual(raised.exception.code, code)

    def test_artifact_traversal_is_rejected_without_writing_outside_output(self) -> None:
        store = FakeStorage()
        store.jobs[("failed", "job-123")] = make_job(state="failed")
        store.binary["status/job-123/output/../escape.txt"] = b"escape"

        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "safe"
            with self.assertRaises(MachineError) as raised:
                MachineFacade(store=store).download_artifacts("job-123", output)

            self.assertEqual(raised.exception.code, "ARTIFACT_SECURITY")
            self.assertFalse((Path(directory) / "escape.txt").exists())

    def test_symlink_output_directory_is_rejected_before_download(self) -> None:
        store = FakeStorage()
        store.jobs[("uploaded", "job-123")] = make_job(state="uploaded")
        store.binary["status/job-123/output/result.txt"] = b"result"

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            target = root / "target"
            target.mkdir()
            link = root / "output-link"
            link.symlink_to(target, target_is_directory=True)
            with self.assertRaises(MachineError) as raised:
                MachineFacade(store=store).download_artifacts("job-123", link)

            self.assertEqual(raised.exception.code, "ARTIFACT_SECURITY")
            self.assertEqual(list(target.iterdir()), [])


if __name__ == "__main__":
    unittest.main()
