"""Black-box HTTP and state-file contracts for the Stado cleanup dashboard."""
from __future__ import annotations

import fcntl
import http.client
import json
import os
import tempfile
import threading
import unittest
from contextlib import contextmanager
from http.server import HTTPServer, ThreadingHTTPServer
from queue import Queue
from pathlib import Path
from typing import Iterator
from unittest.mock import patch

from wisent_compute import dashboard
from wisent_compute.providers.local.disk import cleanup


SAFE_REPORT = {
    "version": 1,
    "mode": "enforce",
    "check_interval_seconds": 300,
    "started_at": "2026-07-10T12:00:00+00:00",
    "duration_ms": 17,
    "outcome": "healthy_noop",
    "free_bytes_before": 40,
    "free_bytes_after": 41,
    "low_bytes": 20,
    "target_bytes": 30,
    "pressure_active": False,
    "cleaners": {
        "huggingface_cache": {
            "scanned_items": 2,
            "eligible_items": 1,
            "deleted_items": 0,
            "expected_bytes": 12,
            "actual_free_delta_bytes": 0,
            "skipped": {"too_young": 1},
        },
        "weles_recordings": {
            "scanned_items": 0,
            "eligible_items": 0,
            "deleted_items": 0,
            "expected_bytes": 0,
            "actual_free_delta_bytes": 0,
            "skipped": {},
        },
    },
    "caps": {"bytes": False, "items": False, "scan": False, "deadline": False},
    "lock_busy": False,
    "active_slot_count": 0,
    "last_success_at": "2026-07-10T11:59:00+00:00",
    "errors": ["policy:RuntimeError"],
}


class QuietHandler(dashboard._Handler):
    def log_message(self, _format: str, *args: object) -> None:
        pass


@contextmanager
def dashboard_server(
    server_class: type[HTTPServer] = HTTPServer,
) -> Iterator[tuple[str, int]]:
    server = server_class(("127.0.0.1", 0), QuietHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        host, port = server.server_address
        yield host, port
    finally:
        server.shutdown()
        thread.join(timeout=5)
        server.server_close()


def request(
    address: tuple[str, int],
    method: str,
    path: str,
    *,
    headers: dict[str, str] | None = None,
    body: bytes | None = None,
) -> tuple[int, dict[str, str], bytes]:
    connection = http.client.HTTPConnection(*address, timeout=5)
    try:
        connection.request(method, path, body=body, headers=headers or {})
        response = connection.getresponse()
        return response.status, dict(response.getheaders()), response.read()
    finally:
        connection.close()


def json_request(
    address: tuple[str, int],
    method: str,
    path: str,
    *,
    headers: dict[str, str] | None = None,
    body: bytes | None = None,
) -> tuple[int, dict]:
    status, response_headers, payload = request(
        address, method, path, headers=headers, body=body,
    )
    if response_headers.get("Content-Type") != "application/json":
        raise AssertionError(f"unexpected content type: {response_headers!r}")
    return status, json.loads(payload)


class TrustedRequestHostTests(unittest.TestCase):
    def test_accepts_ip_literal_hosts_with_optional_ports(self) -> None:
        accepted = {
            "IPv4": "127.0.0.1",
            "IPv4 with port": "192.0.2.1:8787",
            "IPv6": "[::1]",
            "IPv6 with port": "[2001:db8::1]:8787",
        }

        for name, host in accepted.items():
            with self.subTest(name=name, host=host):
                self.assertTrue(dashboard._trusted_request_host(host))

    def test_rejects_missing_malformed_and_named_hosts(self) -> None:
        rejected = {
            "missing": None,
            "empty": "",
            "userinfo": "operator@127.0.0.1",
            "bad port": "127.0.0.1:not-a-port",
            "path": "127.0.0.1/admin",
            "query": "127.0.0.1?admin=true",
            "localhost": "localhost",
            "unqualified": "worker-01",
            "mDNS": "worker.local",
            "tailnet": "worker.tail-name.ts.net",
            "public DNS": "dashboard.example.com",
            "single dot": ".",
            "double dot": "..",
        }

        for name, host in rejected.items():
            with self.subTest(name=name, host=host):
                self.assertFalse(dashboard._trusted_request_host(host))


class DashboardCompatibilityTests(unittest.TestCase):
    def test_root_and_state_json_remain_available(self) -> None:
        cached_state = {
            "ready": True,
            "now": "2026-07-10T12:34:56+00:00",
            "counts": {"queue": 7, "running": 2, "completed": 11, "failed": 3},
            "throughput": {"samples": 5},
        }
        with (
            patch.dict(dashboard._CACHE_STATE, cached_state, clear=True),
            patch.object(dashboard, "read_cleanup_state", return_value=SAFE_REPORT),
            dashboard_server() as address,
        ):
            state_status, state = json_request(address, "GET", "/api/state.json")
            root_status, root_headers, root = request(address, "GET", "/")

        self.assertEqual((state_status, state), (200, cached_state))
        self.assertEqual(root_status, 200)
        self.assertEqual(root_headers["Content-Type"], "text/html; charset=utf-8")
        self.assertIn(b"<title>Stado Control Center</title>", root)
        self.assertIn(b"Stado Disk Cleanup", root)
        self.assertIn(b"queued <strong>7</strong>", root)


class DashboardCleanupHttpTests(unittest.TestCase):
    def test_get_cleanup_returns_exact_sanitized_public_envelope(self) -> None:
        raw_report = {
            **SAFE_REPORT,
            "started_at": "2026-07-10T12:00:00Z",
            "last_success_at": "2026-07-10T11:59:00Z",
            "hostname": "secret-host.internal",
            "target_name": "private-target",
            "policy_digest": "deadbeef",
            "path": "/Users/operator/private-cache",
            "cleaners": {
                **SAFE_REPORT["cleaners"],
                "private_cleaner": {"path": "/secret"},
            },
            "errors": ["policy:RuntimeError", "delete:/Users/operator/private-cache"],
        }
        with tempfile.TemporaryDirectory() as temporary_home:
            home = Path(temporary_home)
            state_dir = home / ".cache" / "wisent-compute"
            state_dir.mkdir(parents=True)
            (state_dir / "disk-cleanup-state.json").write_text(
                json.dumps({"version": 1, "report": raw_report}), encoding="utf-8",
            )
            with (
                patch.object(cleanup.Path, "home", return_value=home),
                dashboard_server() as address,
            ):
                status, payload = json_request(address, "GET", "/api/cleanup.json")

        self.assertEqual(status, 200)
        self.assertEqual(payload, {"ok": True, "service": "ready", "report": SAFE_REPORT})
        encoded = json.dumps(payload, sort_keys=True)
        for secret in (
            "secret-host.internal", "private-target", "deadbeef",
            "/Users/operator/private-cache", "private_cleaner",
        ):
            with self.subTest(secret=secret):
                self.assertNotIn(secret, encoded)

    def test_missing_or_wrong_action_header_is_forbidden_without_cleanup(self) -> None:
        cases = {
            "missing": {},
            "wrong value": {"X-Stado-Action": "clean"},
            "wrong case in value": {"X-Stado-Action": "Cleanup"},
        }
        with patch.object(dashboard, "run_cleanup_once") as run_cleanup, dashboard_server() as address:
            for name, headers in cases.items():
                with self.subTest(name):
                    status, payload = json_request(
                        address, "POST", "/api/cleanup/run", headers=headers,
                    )
                    self.assertEqual(status, 403)
                    self.assertEqual(payload["service"], "error")
            run_cleanup.assert_not_called()

    def test_query_body_and_transfer_encoding_are_rejected_without_cleanup(self) -> None:
        cases = {
            "query": ("/api/cleanup/run?force=true", {"X-Stado-Action": "cleanup"}, None),
            "body": ("/api/cleanup/run", {"X-Stado-Action": "cleanup"}, b"{}"),
            "transfer encoding": (
                "/api/cleanup/run",
                {"X-Stado-Action": "cleanup", "Transfer-Encoding": "chunked"},
                None,
            ),
        }
        with patch.object(dashboard, "run_cleanup_once") as run_cleanup, dashboard_server() as address:
            for name, (path, headers, body) in cases.items():
                with self.subTest(name):
                    status, payload = json_request(
                        address, "POST", path, headers=headers, body=body,
                    )
                    self.assertEqual(status, 400)
                    self.assertEqual(payload["service"], "error")
            run_cleanup.assert_not_called()

    def test_hostile_host_is_forbidden_without_cleanup(self) -> None:
        with (
            patch.object(dashboard, "run_cleanup_once") as run_cleanup,
            dashboard_server() as address,
        ):
            status, payload = json_request(
                address,
                "POST",
                "/api/cleanup/run",
                headers={
                    "Host": "attacker.example.com",
                    "X-Stado-Action": "cleanup",
                },
            )

        self.assertEqual(status, 403)
        self.assertEqual(payload["ok"], False)
        self.assertEqual(payload["service"], "error")
        self.assertEqual(payload["report"]["outcome"], "invalid_or_unavailable_policy")
        run_cleanup.assert_not_called()

    def test_malformed_content_length_is_bad_request_without_cleanup(self) -> None:
        malformed = "not-a-byte-count"
        with (
            patch.object(dashboard, "run_cleanup_once") as run_cleanup,
            dashboard_server() as address,
        ):
            status, payload = json_request(
                address,
                "POST",
                "/api/cleanup/run",
                headers={
                    "X-Stado-Action": "cleanup",
                    "Content-Length": malformed,
                },
            )

        self.assertEqual(status, 400)
        self.assertEqual(payload["ok"], False)
        self.assertEqual(payload["service"], "error")
        self.assertEqual(payload["report"]["outcome"], "invalid_or_unavailable_policy")
        self.assertNotIn(malformed, json.dumps(payload, sort_keys=True))
        run_cleanup.assert_not_called()

    def test_get_remains_responsive_while_cleanup_post_is_blocked(self) -> None:
        cached_state = {"ready": True, "counts": {"queue": 3}}
        cleanup_entered = threading.Event()
        release_cleanup = threading.Event()
        post_result: Queue[tuple[int, dict] | BaseException] = Queue()

        def blocking_cleanup(**_kwargs: object) -> dict:
            cleanup_entered.set()
            release_cleanup.wait()
            return SAFE_REPORT

        def post_cleanup(address: tuple[str, int]) -> None:
            try:
                post_result.put(json_request(
                    address,
                    "POST",
                    "/api/cleanup/run",
                    headers={"X-Stado-Action": "cleanup"},
                ))
            except BaseException as error:
                post_result.put(error)

        with (
            patch.dict(dashboard._CACHE_STATE, cached_state, clear=True),
            patch.object(dashboard, "run_cleanup_once", side_effect=blocking_cleanup),
            dashboard_server(ThreadingHTTPServer) as address,
        ):
            post_thread = threading.Thread(target=post_cleanup, args=(address,), daemon=True)
            post_thread.start()
            try:
                self.assertTrue(
                    cleanup_entered.wait(timeout=5),
                    "cleanup POST did not reach the blocking cleanup",
                )
                get_status, state = json_request(address, "GET", "/api/state.json")
            finally:
                release_cleanup.set()
                post_thread.join(timeout=5)

        self.assertFalse(post_thread.is_alive(), "cleanup POST did not finish after release")
        self.assertEqual((get_status, state), (200, cached_state))
        outcome = post_result.get_nowait()
        if isinstance(outcome, BaseException):
            raise outcome
        self.assertEqual(outcome, (200, {"ok": True, "service": "ready", "report": SAFE_REPORT}))

    def test_valid_post_force_runs_parameterless_cleanup_once(self) -> None:
        raw_report = {
            **SAFE_REPORT,
            "hostname": "must-not-leak",
            "path": "/private/cache",
            "policy_digest": "digest-must-not-leak",
        }
        with (
            patch.object(dashboard, "run_cleanup_once", return_value=raw_report) as run_cleanup,
            dashboard_server() as address,
        ):
            status, payload = json_request(
                address,
                "POST",
                "/api/cleanup/run",
                headers={"X-Stado-Action": "cleanup"},
            )

        self.assertEqual(status, 200)
        self.assertEqual(payload, {"ok": True, "service": "ready", "report": SAFE_REPORT})
        run_cleanup.assert_called_once_with(active_slot_count=0, force=True)

    def test_lock_busy_maps_to_conflict_for_read_and_run(self) -> None:
        busy = {**SAFE_REPORT, "outcome": "lock_busy", "lock_busy": True}
        with (
            patch.object(dashboard, "read_cleanup_state", return_value=busy),
            patch.object(dashboard, "run_cleanup_once", return_value=busy),
            dashboard_server() as address,
        ):
            get_status, get_payload = json_request(address, "GET", "/api/cleanup.json")
            post_status, post_payload = json_request(
                address,
                "POST",
                "/api/cleanup/run",
                headers={"X-Stado-Action": "cleanup"},
            )

        expected = {"ok": False, "service": "busy", "report": busy}
        self.assertEqual((get_status, get_payload), (409, expected))
        self.assertEqual((post_status, post_payload), (409, expected))

    def test_internal_cleanup_exception_maps_to_sanitized_server_error(self) -> None:
        with (
            patch.object(
                dashboard,
                "run_cleanup_once",
                side_effect=RuntimeError("host=private.internal path=/Users/operator/cache"),
            ),
            dashboard_server() as address,
        ):
            status, payload = json_request(
                address,
                "POST",
                "/api/cleanup/run",
                headers={"X-Stado-Action": "cleanup"},
            )

        self.assertEqual(status, 500)
        self.assertEqual(payload["ok"], False)
        self.assertEqual(payload["service"], "error")
        self.assertEqual(payload["report"]["outcome"], "invalid_or_unavailable_policy")
        encoded = json.dumps(payload, sort_keys=True)
        self.assertNotIn("private.internal", encoded)
        self.assertNotIn("/Users/operator/cache", encoded)


class CleanupStateReaderSafetyTests(unittest.TestCase):
    def test_state_file_symlink_is_never_followed(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_home:
            home = Path(temporary_home)
            state_dir = home / ".cache" / "wisent-compute"
            state_dir.mkdir(parents=True)
            outside = home / "outside.json"
            outside.write_text(json.dumps({"report": SAFE_REPORT}), encoding="utf-8")
            (state_dir / "disk-cleanup-state.json").symlink_to(outside)

            with patch.object(cleanup.Path, "home", return_value=home):
                with self.assertRaises(OSError):
                    cleanup.read_cleanup_state()

    def test_owner_mismatch_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_home:
            home = Path(temporary_home)
            with (
                patch.object(cleanup.Path, "home", return_value=home),
                patch.object(cleanup.os, "geteuid", return_value=os.geteuid() + 1),
            ):
                with self.assertRaisesRegex(OSError, "owner mismatch"):
                    cleanup.read_cleanup_state()

    def test_busy_cleanup_lock_returns_busy_public_state(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_home:
            home = Path(temporary_home)
            with patch.object(cleanup.Path, "home", return_value=home):
                cleanup.read_cleanup_state()
                lock_path = home / ".cache" / "wisent-compute" / "disk-cleanup.lock"
                lock_fd = os.open(lock_path, os.O_RDWR)
                try:
                    fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    report = cleanup.read_cleanup_state()
                finally:
                    fcntl.flock(lock_fd, fcntl.LOCK_UN)
                    os.close(lock_fd)

        self.assertEqual(report["outcome"], "lock_busy")
        self.assertEqual(report["lock_busy"], True)
        self.assertEqual(report["errors"], [])


if __name__ == "__main__":
    unittest.main()
