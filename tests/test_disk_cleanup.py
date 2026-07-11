"""Behavioral safety contracts for registry-authorized local disk cleanup."""
from __future__ import annotations

from contextlib import ExitStack
import fcntl
import json
import os
import tempfile
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

from wisent_compute import targets
from wisent_compute.providers import local_agent
from wisent_compute.providers.local import disk as local_disk
from wisent_compute.providers.local import slots as local_slots
from wisent_compute.providers.local import version_check
from wisent_compute.providers.local.disk import cleanup
from wisent_compute.targets import ComputeTarget, DiskCleanerPolicy, DiskCleanupPolicy


GIB = 1024 ** 3
MIB = 1024 ** 2


def policy(
    *,
    mode: str = "enforce",
    low_free_gb: int = 10,
    target_free_gb: int = 12,
    max_bytes_per_pass: int = 20 * GIB,
    max_items_per_pass: int = 10,
    max_scan_items: int = 100,
    cleaners: tuple[str, ...] = ("huggingface_cache",),
) -> DiskCleanupPolicy:
    return DiskCleanupPolicy(
        mode=mode,
        check_interval_seconds=300,
        low_free_gb=low_free_gb,
        target_free_gb=target_free_gb,
        max_bytes_per_pass=max_bytes_per_pass,
        max_items_per_pass=max_items_per_pass,
        max_scan_items=max_scan_items,
        cleaners={
            name: DiskCleanerPolicy(
                min_age_seconds=3600 if name == "huggingface_cache" else 86400
            )
            for name in cleaners
        },
    )


def target(cleanup_policy: DiskCleanupPolicy) -> ComputeTarget:
    return ComputeTarget(name="test-host", kind="local", disk_cleanup=cleanup_policy)


def make_hf_root(home: Path) -> Path:
    root = home / ".cache" / "huggingface" / "hub"
    root.mkdir(parents=True)
    (root / ".locks").mkdir()
    return root


class HfCacheLayout:
    """A real, minimal Hugging Face cache layout rooted in a temporary HOME."""

    def __init__(self, home: Path):
        self.root = make_hf_root(home)
        self.repo = self.root / "models--org--model"
        for name in ("blobs", "refs", "snapshots"):
            (self.repo / name).mkdir(parents=True)

    def blob(self, digest: str, size: int = MIB) -> Path:
        path = self.repo / "blobs" / digest
        path.write_bytes(b"x" * size)
        os.utime(path, (0, 0))
        return path

    def revision(
        self,
        commit: str,
        blobs: dict[str, Path],
        *,
        modified: float = 0,
        ref: str | None = None,
    ) -> Path:
        snapshot = self.repo / "snapshots" / commit
        snapshot.mkdir()
        for filename, blob in blobs.items():
            (snapshot / filename).symlink_to(Path("../../blobs") / blob.name)
        if ref is not None:
            ref_path = self.repo / "refs" / ref
            ref_path.parent.mkdir(parents=True, exist_ok=True)
            ref_path.write_text(commit + "\n", encoding="ascii")
            os.utime(ref_path, (modified, modified))
        for entry in snapshot.iterdir():
            os.utime(entry, (modified, modified), follow_symlinks=False)
        os.utime(snapshot, (modified, modified))
        return snapshot


class CanonicalRegistryFetchTests(unittest.TestCase):
    def test_reload_pins_download_to_exact_generation_and_parses_json(self) -> None:
        calls: list[object] = []
        blob = Mock()
        blob.generation = "4815162342"
        blob.reload.side_effect = lambda: calls.append("reload")

        def download(*, if_generation_match: int) -> str:
            calls.append(("download", if_generation_match))
            return '{"targets": [{"name": "worker-a"}]}'

        blob.download_as_text.side_effect = download
        bucket = Mock()
        bucket.blob.return_value = blob
        client = Mock()
        client.bucket.return_value = bucket

        with patch("google.cloud.storage.Client", return_value=client):
            registry = cleanup._fetch_canonical_registry()

        self.assertEqual(registry, {"targets": [{"name": "worker-a"}]})
        self.assertEqual(calls, ["reload", ("download", 4815162342)])
        generation = blob.download_as_text.call_args.kwargs["if_generation_match"]
        self.assertIs(type(generation), int)

    def test_missing_generation_propagates_before_policy_validation(self) -> None:
        blob = Mock()
        blob.generation = None
        bucket = Mock()
        bucket.blob.return_value = blob
        client = Mock()
        client.bucket.return_value = bucket

        with (
            patch("google.cloud.storage.Client", return_value=client),
            patch.object(cleanup, "validate_registry") as validate_registry,
        ):
            with self.assertRaisesRegex(
                OSError, "canonical registry generation unavailable"
            ):
                cleanup.resolve_canonical_policy("worker-a")

        blob.reload.assert_called_once_with()
        blob.download_as_text.assert_not_called()
        validate_registry.assert_not_called()

    def test_generation_mismatch_propagates_before_policy_validation(self) -> None:
        mismatch = RuntimeError("object generation changed")
        calls: list[str] = []
        blob = Mock()
        blob.generation = 73
        blob.reload.side_effect = lambda: calls.append("reload")

        def reject_download(*, if_generation_match: int) -> str:
            calls.append("download")
            self.assertEqual(if_generation_match, 73)
            raise mismatch

        blob.download_as_text.side_effect = reject_download
        bucket = Mock()
        bucket.blob.return_value = blob
        client = Mock()
        client.bucket.return_value = bucket

        with (
            patch("google.cloud.storage.Client", return_value=client),
            patch.object(cleanup, "validate_registry") as validate_registry,
        ):
            with self.assertRaises(RuntimeError) as raised:
                cleanup.resolve_canonical_policy("worker-a")

        self.assertIs(raised.exception, mismatch)
        self.assertEqual(calls, ["reload", "download"])
        validate_registry.assert_not_called()


class DiskCleanupSafetyTests(unittest.TestCase):
    def run_pass(
        self,
        home: Path,
        cleanup_policy: DiskCleanupPolicy,
        *,
        free_bytes: object,
        active_slots: int = 0,
        force: bool = True,
    ) -> dict:
        with (
            patch.object(cleanup, "_secure_home", return_value=home),
            patch.object(
                cleanup,
                "resolve_canonical_policy",
                return_value=(target(cleanup_policy), cleanup_policy, "digest"),
            ),
            patch.object(
                cleanup,
                "_free_bytes",
                side_effect=free_bytes if callable(free_bytes) else None,
                return_value=free_bytes if not callable(free_bytes) else None,
            ),
        ):
            return cleanup.run_cleanup_once(
                active_slot_count=active_slots,
                force=force,
                log_fn=lambda _message: None,
            )

    def test_canonical_policy_failure_is_fail_closed_and_report_is_sanitized(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            home = Path(directory).resolve()
            logs: list[str] = []
            secret = str(home / "must-not-leak")
            with (
                patch.object(cleanup, "_secure_home", return_value=home),
                patch.object(
                    cleanup,
                    "resolve_canonical_policy",
                    side_effect=RuntimeError(f"registry failed: {secret}"),
                ),
                patch.object(cleanup, "_run_hf", side_effect=AssertionError("deletion reached")),
            ):
                report = cleanup.run_cleanup_once(0, force=True, log_fn=logs.append)

        self.assertEqual(report["outcome"], "invalid_or_unavailable_policy")
        self.assertEqual(report["errors"], ["policy:RuntimeError"])
        self.assertEqual(report["cleaners"]["huggingface_cache"]["deleted_items"], 0)
        self.assertNotIn(secret, repr(report))
        self.assertNotIn(secret, "".join(logs))

    def test_off_and_report_leave_real_cache_bytes_unchanged(self) -> None:
        cases = {
            "off": (policy(mode="off"), "healthy_noop"),
            "report": (policy(mode="report"), "report_only"),
        }
        for name, (cleanup_policy, outcome) in cases.items():
            with self.subTest(name), tempfile.TemporaryDirectory() as directory:
                home = Path(directory).resolve()
                layout = HfCacheLayout(home)
                blob = layout.blob("a" * 64)
                snapshot = layout.revision("1" * 40, {"weights.bin": blob}, ref="main")
                before = {path.relative_to(layout.root): path.lstat()
                          for path in layout.root.rglob("*")}

                report = self.run_pass(home, cleanup_policy, free_bytes=0)

                after = {path.relative_to(layout.root): path.lstat()
                         for path in layout.root.rglob("*")}
                self.assertEqual(report["outcome"], outcome)
                self.assertEqual(report["cleaners"]["huggingface_cache"]["deleted_items"], 0)
                self.assertEqual(before.keys(), after.keys())
                self.assertTrue(snapshot.is_dir())
                self.assertEqual(blob.read_bytes(), b"x" * MIB)

    def test_shared_workload_lock_excludes_cleanup_even_when_slot_count_is_zero(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            home = Path(directory).resolve()
            with patch.object(cleanup, "_secure_home", return_value=home):
                workload_lock = cleanup.acquire_workload_lock()
                self.assertIsNotNone(workload_lock)
                try:
                    with patch.object(
                        cleanup,
                        "resolve_canonical_policy",
                        side_effect=AssertionError("policy reached while workload lock held"),
                    ):
                        report = cleanup.run_cleanup_once(0, force=True, log_fn=lambda _message: None)
                finally:
                    cleanup.release_workload_lock(workload_lock)  # type: ignore[arg-type]

        self.assertEqual(report["outcome"], "lock_busy")
        self.assertTrue(report["lock_busy"])
        self.assertEqual(report["active_slot_count"], 0)

    def test_active_slots_block_hf_scanning_and_deletion(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            home = Path(directory).resolve()
            report = self.run_pass(home, policy(), free_bytes=0, active_slots=2)

        hf = report["cleaners"]["huggingface_cache"]
        self.assertEqual(report["outcome"], "blocked_active")
        self.assertEqual(hf["scanned_items"], 0)
        self.assertEqual(hf["deleted_items"], 0)
        self.assertEqual(hf["skipped"], {"active_slots": 1})

    def test_oldest_complete_revision_is_removed_until_target_and_new_revision_survives(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            home = Path(directory).resolve()
            layout = HfCacheLayout(home)
            oldest_blob = layout.blob("a" * 64)
            middle_blob = layout.blob("b" * 64)
            newest_blob = layout.blob("c" * 64)
            oldest = layout.revision("1" * 40, {"weights.bin": oldest_blob}, modified=10)
            middle = layout.revision("2" * 40, {"weights.bin": middle_blob}, modified=20)
            newest = layout.revision(
                "3" * 40,
                {"weights.bin": newest_blob},
                modified=time.time(),
                ref="main",
            )

            def free_bytes(_home: Path) -> int:
                return 9 * GIB if oldest.exists() else 12 * GIB

            report = self.run_pass(home, policy(), free_bytes=free_bytes)

            self.assertEqual(report["outcome"], "reclaimed_target")
            self.assertEqual(report["cleaners"]["huggingface_cache"]["deleted_items"], 1)
            self.assertFalse(oldest.exists())
            self.assertTrue(middle.is_dir())
            self.assertTrue(newest.is_dir())

    def test_only_blobs_proven_exclusive_to_deleted_revision_are_unlinked(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            home = Path(directory).resolve()
            layout = HfCacheLayout(home)
            exclusive = layout.blob("a" * 64)
            shared = layout.blob("b" * 64)
            retained_only = layout.blob("c" * 64)
            old = layout.revision(
                "1" * 40,
                {"exclusive.bin": exclusive, "shared.bin": shared},
                modified=0,
            )
            retained = layout.revision(
                "2" * 40,
                {"shared.bin": shared, "retained.bin": retained_only},
                modified=time.time(),
                ref="main",
            )

            report = self.run_pass(home, policy(), free_bytes=0)

            self.assertEqual(report["cleaners"]["huggingface_cache"]["deleted_items"], 1)
            self.assertFalse(old.exists())
            self.assertFalse(exclusive.exists())
            self.assertTrue(shared.is_file())
            self.assertTrue(retained_only.is_file())
            self.assertTrue(retained.is_dir())

    def test_symlinked_cache_root_is_rejected_without_touching_outside_tree(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            home = Path(directory).resolve()
            outside = home / "outside"
            outside.mkdir()
            sentinel = outside / "sentinel"
            sentinel.write_bytes(b"never delete")
            huggingface = home / ".cache" / "huggingface"
            huggingface.mkdir(parents=True)
            (huggingface / "hub").symlink_to(outside, target_is_directory=True)

            report = self.run_pass(home, policy(), free_bytes=0)

            self.assertEqual(sentinel.read_bytes(), b"never delete")
            self.assertEqual(report["cleaners"]["huggingface_cache"]["deleted_items"], 0)
            self.assertEqual(report["errors"], ["huggingface_cache:OSError"])

    def test_ancestor_swap_to_outside_tree_aborts_and_preserves_sentinel(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            home = Path(directory).resolve()
            layout = HfCacheLayout(home)
            blob = layout.blob("a" * 64)
            old = layout.revision("1" * 40, {"weights.bin": blob}, modified=0)
            outside = home / "outside"
            outside.mkdir()
            sentinel = outside / "sentinel"
            sentinel.write_bytes(b"never delete")
            parked = layout.root / "parked-repository"
            original_snapshot_state = cleanup._hf_snapshot_state
            calls = 0

            def swap_before_recheck(*args: object, **kwargs: object):
                nonlocal calls
                calls += 1
                if calls == 2:
                    layout.repo.rename(parked)
                    layout.repo.symlink_to(outside, target_is_directory=True)
                return original_snapshot_state(*args, **kwargs)

            with patch.object(cleanup, "_hf_snapshot_state", side_effect=swap_before_recheck):
                report = self.run_pass(home, policy(), free_bytes=0)

            self.assertEqual(sentinel.read_bytes(), b"never delete")
            self.assertTrue((parked / "snapshots" / old.name).is_dir())
            self.assertEqual(report["cleaners"]["huggingface_cache"]["deleted_items"], 0)
            self.assertEqual(report["errors"], ["huggingface_recheck:NotADirectoryError"])

    def test_growth_during_scan_hits_scan_cap_without_partial_mutation(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            home = Path(directory).resolve()
            layout = HfCacheLayout(home)
            blob = layout.blob("a" * 64)
            old = layout.revision("1" * 40, {"weights.bin": blob}, modified=0)
            original_tick = cleanup._hf_tick
            grew = False

            def grow_then_tick(budget: dict, deadline: float, report: dict) -> None:
                nonlocal grew
                if not grew and report["cleaners"]["huggingface_cache"]["scanned_items"] >= 2:
                    grew = True
                    for index in range(20):
                        (layout.repo / "blobs" / f"growth-{index}").write_bytes(b"g")
                original_tick(budget, deadline, report)

            with patch.object(cleanup, "_hf_tick", side_effect=grow_then_tick):
                report = self.run_pass(
                    home,
                    policy(max_scan_items=8, max_items_per_pass=1),
                    free_bytes=0,
                )

            hf = report["cleaners"]["huggingface_cache"]
            self.assertTrue(grew)
            self.assertTrue(report["caps"]["scan"])
            self.assertLessEqual(hf["scanned_items"], 8)
            self.assertEqual(hf["deleted_items"], 0)
            self.assertTrue(old.is_dir())
            self.assertTrue(blob.is_file())

    def test_growth_after_scan_is_detected_by_recheck_and_aborts(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            home = Path(directory).resolve()
            layout = HfCacheLayout(home)
            blob = layout.blob("a" * 64)
            old = layout.revision("1" * 40, {"weights.bin": blob}, modified=0)
            original_scan = cleanup._hf_scan_cache

            def grow_after_scan(*args: object, **kwargs: object) -> list[dict]:
                candidates = original_scan(*args, **kwargs)
                (old / "arrived-after-scan.bin").write_bytes(b"untracked")
                return candidates

            with patch.object(cleanup, "_hf_scan_cache", side_effect=grow_after_scan):
                report = self.run_pass(home, policy(), free_bytes=0)

            self.assertTrue((old / "arrived-after-scan.bin").is_file())
            self.assertTrue(blob.is_file())
            self.assertEqual(report["cleaners"]["huggingface_cache"]["deleted_items"], 0)
            self.assertEqual(report["errors"], ["huggingface_recheck:OSError"])

    def test_expired_lock_file_is_safe_but_held_lock_blocks_scan_and_mutation(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            home = Path(directory).resolve()
            layout = HfCacheLayout(home)
            blob = layout.blob("a" * 64)
            old = layout.revision("1" * 40, {"weights.bin": blob}, modified=0)
            lock_path = layout.root / ".locks" / "download.lock"
            lock_path.touch()

            stale_report = self.run_pass(home, policy(mode="report"), free_bytes=0)
            self.assertGreater(stale_report["cleaners"]["huggingface_cache"]["scanned_items"], 0)
            self.assertNotIn("cache_locked", stale_report["cleaners"]["huggingface_cache"]["skipped"])

            descriptor = os.open(lock_path, os.O_RDWR)
            try:
                fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
                held_report = self.run_pass(home, policy(), free_bytes=0)
            finally:
                fcntl.flock(descriptor, fcntl.LOCK_UN)
                os.close(descriptor)

            self.assertEqual(held_report["cleaners"]["huggingface_cache"]["scanned_items"], 1)
            self.assertEqual(held_report["cleaners"]["huggingface_cache"]["deleted_items"], 0)
            self.assertEqual(held_report["cleaners"]["huggingface_cache"]["skipped"], {"cache_locked": 1})
            self.assertTrue(old.is_dir())
            self.assertTrue(blob.is_file())

    def test_cache_lock_remains_held_through_the_unlink_operation(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            home = Path(directory).resolve()
            layout = HfCacheLayout(home)
            blob = layout.blob("a" * 64)
            old = layout.revision("1" * 40, {"weights.bin": blob}, modified=0)
            lock_path = layout.root / ".locks" / "download.lock"
            lock_path.touch()
            original_execute = cleanup._hf_execute_candidate
            observed_held_lock = False

            def assert_lock_held(root_fd: int, candidate: dict) -> None:
                nonlocal observed_held_lock
                contender = os.open(lock_path, os.O_RDWR)
                try:
                    with self.assertRaises(BlockingIOError):
                        fcntl.flock(contender, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    observed_held_lock = True
                finally:
                    os.close(contender)
                original_execute(root_fd, candidate)

            with patch.object(cleanup, "_hf_execute_candidate", side_effect=assert_lock_held):
                report = self.run_pass(home, policy(), free_bytes=0)

            self.assertTrue(observed_held_lock)
            self.assertEqual(report["cleaners"]["huggingface_cache"]["deleted_items"], 1)
            self.assertFalse(old.exists())
            self.assertFalse(blob.exists())

    def test_new_lock_creation_is_denied_during_unlink_barrier(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            home = Path(directory).resolve()
            layout = HfCacheLayout(home)
            blob = layout.blob("a" * 64)
            old = layout.revision("1" * 40, {"weights.bin": blob}, modified=0)
            original_execute = cleanup._hf_execute_candidate
            writer_was_denied = False

            def contend_during_unlink(root_fd: int, candidate: dict) -> None:
                nonlocal writer_was_denied
                with self.assertRaises(PermissionError):
                    os.open(
                        layout.root / ".locks" / "arrived-during-delete.lock",
                        os.O_WRONLY | os.O_CREAT | os.O_EXCL,
                        0o600,
                    )
                writer_was_denied = True
                original_execute(root_fd, candidate)

            with patch.object(cleanup, "_hf_execute_candidate", side_effect=contend_during_unlink):
                report = self.run_pass(home, policy(), free_bytes=0)

            self.assertTrue(writer_was_denied)
            self.assertEqual(report["cleaners"]["huggingface_cache"]["deleted_items"], 1)
            self.assertFalse(old.exists())
            self.assertFalse(blob.exists())
            self.assertFalse((layout.root / ".locks" / "arrived-during-delete.lock").exists())

    def test_lock_created_after_barrier_mirror_aborts_and_restores_namespace(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            home = Path(directory).resolve()
            layout = HfCacheLayout(home)
            blob = layout.blob("a" * 64)
            old = layout.revision("1" * 40, {"weights.bin": blob}, modified=0)
            locks = layout.root / ".locks"
            original_identity = (locks.stat().st_dev, locks.stat().st_ino)
            late_lock = locks / "arrived-after-mirror.lock"
            original_prepare = cleanup._hf_prepare_lock_barrier
            writer_fd: int | None = None

            def create_held_lock_after_mirror(*args: object, **kwargs: object):
                nonlocal writer_fd
                barrier_identity = original_prepare(*args, **kwargs)
                writer_fd = os.open(late_lock, os.O_RDWR | os.O_CREAT | os.O_EXCL, 0o600)
                fcntl.flock(writer_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                return barrier_identity

            try:
                with patch.object(
                    cleanup,
                    "_hf_prepare_lock_barrier",
                    side_effect=create_held_lock_after_mirror,
                ):
                    report = self.run_pass(home, policy(), free_bytes=0)

                self.assertIsNotNone(writer_fd)
                self.assertEqual(report["cleaners"]["huggingface_cache"]["deleted_items"], 0)
                self.assertEqual((locks.stat().st_dev, locks.stat().st_ino), original_identity)
                self.assertTrue(old.is_dir())
                self.assertTrue(blob.is_file())
                self.assertTrue(late_lock.is_file())
                self.assertFalse((layout.root / cleanup._HF_BARRIER_NAME).exists())
                contender = os.open(late_lock, os.O_RDWR)
                try:
                    with self.assertRaises(BlockingIOError):
                        fcntl.flock(contender, fcntl.LOCK_EX | fcntl.LOCK_NB)
                finally:
                    os.close(contender)
            finally:
                if writer_fd is not None:
                    fcntl.flock(writer_fd, fcntl.LOCK_UN)
                    os.close(writer_fd)

            reusable = os.open(late_lock, os.O_RDWR)
            try:
                fcntl.flock(reusable, fcntl.LOCK_EX | fcntl.LOCK_NB)
                fcntl.flock(reusable, fcntl.LOCK_UN)
            finally:
                os.close(reusable)

    def test_unlink_failure_restores_writable_lock_namespace_and_cache_content(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            home = Path(directory).resolve()
            layout = HfCacheLayout(home)
            blob = layout.blob("a" * 64)
            old = layout.revision("1" * 40, {"weights.bin": blob}, modified=0)
            locks = layout.root / ".locks"
            original_identity = (locks.stat().st_dev, locks.stat().st_ino)

            with patch.object(
                cleanup,
                "_hf_execute_candidate",
                side_effect=OSError("injected unlink failure"),
            ):
                failed = self.run_pass(home, policy(), free_bytes=0)

            self.assertEqual(failed["errors"], ["huggingface_delete:OSError"])
            self.assertEqual(failed["cleaners"]["huggingface_cache"]["deleted_items"], 0)
            self.assertEqual((locks.stat().st_dev, locks.stat().st_ino), original_identity)
            self.assertTrue(old.is_dir())
            self.assertTrue(blob.is_file())
            post_failure_lock = locks / "created-after-failure.lock"
            post_failure_lock.touch()
            follow_up = self.run_pass(home, policy(mode="report"), free_bytes=0)
            self.assertGreater(follow_up["cleaners"]["huggingface_cache"]["scanned_items"], 0)
            self.assertEqual(follow_up["errors"], [])

    def test_reserved_incomplete_and_untracked_content_abort_the_entire_cache_pass(self) -> None:
        cases = {
            "work directory": lambda layout, old: (layout.repo / ".work").mkdir(),
            "incomplete blob": lambda layout, old: (layout.repo / "blobs" / "download.incomplete").write_bytes(b"partial"),
            "untracked snapshot file": lambda layout, old: (old / "unknown.bin").write_bytes(b"unknown"),
            "untracked root": lambda layout, old: (layout.root / "attacker-data").write_bytes(b"unknown"),
        }
        for name, add_unsafe_content in cases.items():
            with self.subTest(name), tempfile.TemporaryDirectory() as directory:
                home = Path(directory).resolve()
                layout = HfCacheLayout(home)
                blob = layout.blob("a" * 64)
                old = layout.revision("1" * 40, {"weights.bin": blob}, modified=0)
                add_unsafe_content(layout, old)

                report = self.run_pass(home, policy(), free_bytes=0)

                self.assertEqual(report["cleaners"]["huggingface_cache"]["deleted_items"], 0)
                self.assertNotEqual(report["errors"], [])
                self.assertTrue(old.is_dir())
                self.assertTrue(blob.is_file())

    def test_byte_item_and_target_watermarks_never_overshoot(self) -> None:
        cases = ("bytes", "items", "target")
        for cap in cases:
            with self.subTest(cap), tempfile.TemporaryDirectory() as directory:
                home = Path(directory).resolve()
                layout = HfCacheLayout(home)
                first_blob = layout.blob("a" * 64, 2 * MIB)
                second_blob = layout.blob("b" * 64, 2 * MIB)
                first = layout.revision("1" * 40, {"weights.bin": first_blob}, modified=10)
                second = layout.revision("2" * 40, {"weights.bin": second_blob}, modified=20)
                kwargs: dict[str, int] = {}
                if cap == "bytes":
                    kwargs["max_bytes_per_pass"] = MIB
                elif cap == "items":
                    kwargs["max_items_per_pass"] = 1

                def free_bytes(_home: Path) -> int:
                    if cap == "target" and not first.exists():
                        return 12 * GIB
                    return 0

                report = self.run_pass(home, policy(**kwargs), free_bytes=free_bytes)
                deleted = report["cleaners"]["huggingface_cache"]["deleted_items"]

                if cap == "bytes":
                    self.assertEqual(deleted, 0)
                    self.assertTrue(report["caps"]["bytes"])
                    self.assertTrue(first.is_dir())
                else:
                    self.assertEqual(deleted, 1)
                    self.assertFalse(first.exists())
                    self.assertTrue(second.is_dir())
                    self.assertLessEqual(report["cleaners"]["huggingface_cache"]["expected_bytes"], 3 * MIB)

    def test_deadline_aborts_grown_cache_without_mutation(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            home = Path(directory).resolve()
            layout = HfCacheLayout(home)
            blob = layout.blob("a" * 64)
            old = layout.revision("1" * 40, {"weights.bin": blob}, modified=0)
            for index in range(20):
                (layout.repo / "blobs" / f"growth-{index}").write_bytes(b"g")
            report = cleanup._base_report(0)

            cleanup._run_hf(home, policy(), 0, time.time(), 0.0, report)

            self.assertTrue(report["caps"]["deadline"])
            self.assertEqual(report["cleaners"]["huggingface_cache"]["deleted_items"], 0)
            self.assertTrue(old.is_dir())
            self.assertTrue(blob.is_file())

    def test_weles_old_runs_are_reported_but_never_removed(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            home = Path(directory).resolve()
            recordings = home / "weles" / "recordings"
            old_run = recordings / "run-001"
            old_run.mkdir(parents=True)
            os.utime(old_run, (0, 0))
            (recordings / "local").mkdir()
            (recordings / ".hidden").mkdir()
            (recordings / ".work").mkdir()
            report = cleanup._base_report(0)
            with (
                patch.object(os, "unlink", side_effect=AssertionError("Weles file unlinked")),
                patch.object(os, "rmdir", side_effect=AssertionError("Weles directory removed")),
            ):
                cleanup._scan_weles(
                    home,
                    policy(cleaners=("weles_recordings",)),
                    now=100000.0,
                    remaining_scan=10,
                    report=report,
                )

        weles = report["cleaners"]["weles_recordings"]
        self.assertEqual(weles["scanned_items"], 4)
        self.assertEqual(weles["deleted_items"], 0)
        self.assertEqual(weles["skipped"]["upload_proof_unavailable_v1"], 1)
        self.assertEqual(weles["skipped"]["reserved_or_hidden"], 3)

    def test_recent_state_suppresses_repeat_cleanup_pass(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            home = Path(directory).resolve()
            cleanup_policy = policy(mode="off")
            first = self.run_pass(home, cleanup_policy, free_bytes=0, force=False)
            second = self.run_pass(home, cleanup_policy, free_bytes=0, force=False)

        self.assertEqual(first["outcome"], "healthy_noop")
        self.assertEqual(second["outcome"], "interval_noop")
        self.assertEqual(second["last_success_at"], first["last_success_at"])


class _StopAgent(BaseException):
    pass


class LocalAgentDiskPolicyTests(unittest.TestCase):
    def run_one_policy_decision(
        self,
        cleanup_result: dict | BaseException,
        *,
        free_bytes: int,
        admit: bool,
    ) -> tuple[Mock, Mock, Mock]:
        job = SimpleNamespace(
            job_id="queued-job", priority=1, created_at=0.0, command="safe-task",
            gpu_mem_gb=1,
        )

        class FakeStore:
            def list_jobs_fitting(self, *_args: object, **_kwargs: object) -> list[object]:
                return [job]

        published = Mock()
        started = Mock(return_value={"job": job})
        raw_delete = Mock(side_effect=AssertionError("legacy recursive reaper invoked"))

        def stop_after_admission(*_args: object, **_kwargs: object) -> bool:
            raise _StopAgent()

        cleanup_effect: object
        if isinstance(cleanup_result, BaseException):
            cleanup_effect = cleanup_result
        else:
            cleanup_effect = None

        patches = (
            patch.object(local_agent, "_detect_local_vram_gb", return_value=24),
            patch.object(local_agent, "setup_agent_staging"),
            patch.object(local_agent, "JobStorage", return_value=FakeStore()),
            patch.object(local_agent, "_persisted_disk_low_bytes", return_value=None),
            patch.object(
                local_agent, "run_cleanup_once", side_effect=cleanup_effect,
                return_value=None if cleanup_effect is not None else cleanup_result,
            ),
            patch.object(local_agent.shutil, "disk_usage", return_value=SimpleNamespace(free=free_bytes)),
            patch.object(local_agent.shutil, "rmtree", raw_delete),
            patch.object(local_agent, "publish_capacity", published),
            patch.object(local_agent, "_vast_has_renter", return_value=False),
            patch.object(targets, "lookup_self", return_value=None),
            patch.object(local_disk, "gate_and_maybe_evict", return_value=(False, {})),
            patch.object(version_check, "maybe_drain_or_upgrade", return_value=False),
            patch.object(local_agent, "_smi_free_vram_gb", return_value=-1),
            patch.object(local_agent, "_vram_safety_buffer_gb", return_value=1),
            patch.object(local_agent, "_cuda_child_available", return_value=(True, "healthy")),
            patch.object(local_agent, "_build_capacity_dict", return_value={"nvidia-rtx": 1}),
            patch.object(local_agent, "_maybe_yield_for_priority", return_value=0),
            patch.object(local_agent, "_free_ram_gb", return_value=100),
            patch.object(local_agent, "_static_ram_reserve_gb", return_value=0),
            patch.object(local_agent, "_ram_safety_buffer_gb", return_value=0),
            patch.object(local_agent, "_job_eligible", return_value=True),
            patch.object(local_agent, "estimate_gpu_memory", return_value=0),
            patch.object(local_agent, "activation_extraction_must_share_gpu", return_value=False),
            patch.object(local_agent, "acquire_workload_lock", return_value=object()),
            patch.object(local_slots, "start_slot", started),
            patch.object(
                local_slots, "advance_slot",
                side_effect=stop_after_admission if admit else AssertionError("slot unexpectedly active"),
            ),
            patch.object(local_agent.time, "sleep", side_effect=_StopAgent()),
            patch.dict(os.environ, {"WC_LOCAL_SLOTS": "0", "WC_LOCAL_MAX_CLAIMS_PER_TICK": "0"}),
        )
        with ExitStack() as stack:
            for runtime_patch in patches:
                stack.enter_context(runtime_patch)
            with self.assertRaises(_StopAgent):
                local_agent.run_agent(gpu_type="nvidia-rtx", kind="local")

        return published, started, raw_delete

    def test_fresh_invalid_or_unavailable_policy_publishes_zero_and_refuses_claim(self) -> None:
        cases: dict[str, dict | BaseException] = {
            "invalid report": {
                "outcome": "invalid_or_unavailable_policy",
                "policy_digest": None,
                "low_bytes": None,
            },
            "cleanup unavailable": OSError("registry unavailable"),
        }
        for name, cleanup_result in cases.items():
            with self.subTest(name):
                published, started, raw_delete = self.run_one_policy_decision(
                    cleanup_result, free_bytes=100 * GIB, admit=False,
                )

                started.assert_not_called()
                raw_delete.assert_not_called()
                self.assertEqual(published.call_count, 1)
                capacity = published.call_args
                self.assertEqual(capacity.kwargs["free_vram_gb"], 0)
                self.assertEqual(capacity.args[3], {})
                self.assertFalse(capacity.kwargs["diag"]["disk_cleanup_policy_known"])
                self.assertTrue(capacity.kwargs["diag"]["disk_pressure_unresolved"])

    def test_disk_pressure_refuses_unknown_or_low_space_and_accepts_watermark(self) -> None:
        cases = {
            "unknown policy": (None, 100 * GIB, True),
            "below watermark": (10 * GIB, 10 * GIB - 1, True),
            "at watermark": (10 * GIB, 10 * GIB, False),
        }
        for name, (low_bytes, free_bytes, expected) in cases.items():
            with self.subTest(name):
                self.assertEqual(
                    local_agent._disk_pressure_unresolved(low_bytes, free_bytes),
                    expected,
                )

    def test_persisted_policy_watermark_loads_only_from_safe_state_file(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            home = Path(directory).resolve()
            state_dir = home / ".cache" / "wisent-compute"
            state_dir.mkdir(parents=True)
            state_path = state_dir / "disk-cleanup-state.json"
            state = {
                "version": 1,
                "report": {"policy_digest": "b" * 64, "low_bytes": 10 * GIB},
            }
            state_path.write_text(json.dumps(state), encoding="utf-8")
            with patch.dict(os.environ, {"HOME": str(home)}):
                self.assertEqual(local_agent._persisted_disk_low_bytes(), 10 * GIB)

            state_path.unlink()
            external = home / "attacker-state.json"
            external.write_text(json.dumps(state), encoding="utf-8")
            state_path.symlink_to(external)
            with patch.dict(os.environ, {"HOME": str(home)}):
                self.assertIsNone(local_agent._persisted_disk_low_bytes())

    def test_fresh_valid_policy_below_watermark_publishes_zero_and_refuses_claim(self) -> None:
        published, started, raw_delete = self.run_one_policy_decision(
            {
                "outcome": "reclaimed_partial",
                "policy_digest": "a" * 64,
                "low_bytes": 10 * GIB,
            },
            free_bytes=10 * GIB - 1,
            admit=False,
        )

        started.assert_not_called()
        raw_delete.assert_not_called()
        capacity = published.call_args
        self.assertEqual(capacity.kwargs["free_vram_gb"], 0)
        self.assertEqual(capacity.args[3], {})
        self.assertTrue(capacity.kwargs["diag"]["disk_cleanup_policy_known"])
        self.assertTrue(capacity.kwargs["diag"]["disk_pressure_unresolved"])

    def test_fresh_valid_healthy_policy_publishes_capacity_and_admits_isolatable_job(self) -> None:
        published, started, raw_delete = self.run_one_policy_decision(
            {
                "outcome": "healthy_noop",
                "policy_digest": "a" * 64,
                "low_bytes": 10 * GIB,
            },
            free_bytes=100 * GIB,
            admit=True,
        )

        raw_delete.assert_not_called()
        started.assert_called_once()
        positive_capacity = [
            call for call in published.call_args_list
            if call.kwargs["free_vram_gb"] > 0
        ]
        self.assertEqual(len(positive_capacity), 1)
        self.assertEqual(positive_capacity[0].args[3], {"nvidia-rtx": 1})
        self.assertTrue(positive_capacity[0].kwargs["diag"]["disk_cleanup_policy_known"])
        self.assertFalse(positive_capacity[0].kwargs["diag"]["disk_pressure_unresolved"])


if __name__ == "__main__":
    unittest.main()
