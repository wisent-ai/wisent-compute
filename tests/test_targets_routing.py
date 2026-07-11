"""Focused contracts for registry-v2 Weles placement and host lookup."""
from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from wisent_compute.targets import lookup_self
from wisent_compute.targets.validation import RegistryValidationError, validate_registry


def local_target(**overrides: object) -> dict[str, object]:
    target: dict[str, object] = {
        "name": "mac-mini-a",
        "kind": "local",
        "hostnames": ["mac-mini-a.local"],
        "weles": {"enabled": True, "actions": ["generic_browser_task"]},
    }
    target.update(overrides)
    return target


def registry(*targets: dict[str, object]) -> dict[str, object]:
    return {"schema_version": 2, "targets": list(targets)}

def disk_cleanup_policy(**overrides: object) -> dict[str, object]:
    policy: dict[str, object] = {
        "mode": "enforce",
        "check_interval_seconds": 300,
        "low_free_gb": 10,
        "target_free_gb": 20,
        "max_bytes_per_pass": 1024 ** 3,
        "max_items_per_pass": 10,
        "max_scan_items": 100,
        "cleaners": {
            "huggingface_cache": {"min_age_seconds": 3600},
            "weles_recordings": {"min_age_seconds": 86400},
        },
    }
    policy.update(overrides)
    return policy



class RegistryValidationTests(unittest.TestCase):
    def test_accepts_exact_lowercase_actions_and_sole_wildcard(self) -> None:
        cases = {
            "exact lowercase actions": ["generic_browser_task", "github_star"],
            "sole wildcard": ["*"],
        }
        for name, actions in cases.items():
            with self.subTest(name):
                document = registry(local_target(
                    weles={"enabled": True, "actions": actions},
                ))
                self.assertIs(validate_registry(document), document)

    def test_rejects_actions_that_are_not_exact_or_have_ambiguous_wildcards(self) -> None:
        cases = {
            "uppercase": ["Generic_Browser_Task"],
            "surrounding whitespace": [" generic_browser_task"],
            "punctuation": ["generic-browser-task"],
            "duplicate": ["github_star", "github_star"],
            "wildcard mixed with exact": ["*", "github_star"],
        }
        for name, actions in cases.items():
            with self.subTest(name):
                with self.assertRaises(RegistryValidationError):
                    validate_registry(registry(local_target(
                        weles={"enabled": True, "actions": actions},
                    )))

    def test_rejects_invalid_registry_schema(self) -> None:
        cases = {
            "missing version": {"targets": []},
            "old version": {"schema_version": 1, "targets": []},
            "boolean is not version two": {"schema_version": True, "targets": []},
            "targets must be an array": {"schema_version": 2, "targets": {}},
            "root must be an object": [],
        }
        for name, document in cases.items():
            with self.subTest(name):
                with self.assertRaises(RegistryValidationError):
                    validate_registry(document)

    def test_rejects_weles_policy_on_non_local_targets(self) -> None:
        for kind in ("gcp", "vast"):
            with self.subTest(kind):
                with self.assertRaisesRegex(
                    RegistryValidationError,
                    "allowed only for kind='local'",
                ):
                    validate_registry(registry(local_target(kind=kind)))

    def test_rejects_aliases_not_in_canonical_hostname_form(self) -> None:
        cases = ("MAC-MINI-A.LOCAL", "mac-mini-a.local.", " mac-mini-a.local")
        for alias in cases:
            with self.subTest(alias):
                with self.assertRaisesRegex(RegistryValidationError, "must be normalized"):
                    validate_registry(registry(local_target(hostnames=[alias])))

    def test_rejects_duplicate_normalized_host_identity(self) -> None:
        cases = {
            "alias duplicates another name": [
                local_target(name="mac-mini-a", hostnames=[]),
                local_target(name="mac-mini-b", hostnames=["mac-mini-a"]),
            ],
            "SSH case and trailing dot duplicate an alias": [
                local_target(name="mac-mini-a", hostnames=["builder.example"]),
                local_target(
                    name="mac-mini-b",
                    hostnames=[],
                    ssh="weles@BUILDER.EXAMPLE.",
                ),
            ],
            "identity repeats within one target": [
                local_target(hostnames=["mac-mini-a"]),
            ],
        }
        for name, targets in cases.items():
            with self.subTest(name):
                with self.assertRaisesRegex(
                    RegistryValidationError,
                    "host identity .* is already declared",
                ):
                    validate_registry(registry(*targets))


    def test_accepts_cleanup_policy_modes_boundaries_and_cleaner_subsets(self) -> None:
        cases = {
            "off with no cleaners": disk_cleanup_policy(mode="off", cleaners={}),
            "report at numeric minima": disk_cleanup_policy(
                mode="report",
                check_interval_seconds=60,
                low_free_gb=1,
                target_free_gb=2,
                max_bytes_per_pass=1024 ** 2,
                max_items_per_pass=1,
                max_scan_items=1,
                cleaners={"huggingface_cache": {"min_age_seconds": 3600}},
            ),
            "enforce at bounded maxima": disk_cleanup_policy(
                check_interval_seconds=86400,
                max_bytes_per_pass=1024 ** 4,
                max_items_per_pass=10000,
                max_scan_items=100000,
                cleaners={"weles_recordings": {"min_age_seconds": 86400}},
            ),
        }
        for name, policy in cases.items():
            with self.subTest(name):
                document = registry(local_target(disk_cleanup=policy))
                self.assertIs(validate_registry(document), document)

    def test_rejects_cleanup_policy_type_range_and_closed_schema_violations(self) -> None:
        base = disk_cleanup_policy()
        cases = {
            "unknown mode": {**base, "mode": "delete"},
            "boolean interval": {**base, "check_interval_seconds": True},
            "interval below minimum": {**base, "check_interval_seconds": 59},
            "interval above maximum": {**base, "check_interval_seconds": 86401},
            "target not above low": {**base, "target_free_gb": 10},
            "byte cap below minimum": {**base, "max_bytes_per_pass": 1024 ** 2 - 1},
            "item cap above maximum": {**base, "max_items_per_pass": 10001},
            "scan cap below item cap": {**base, "max_scan_items": 9},
            "unknown policy key": {**base, "path": "/tmp"},
            "unknown cleaner": {**base, "cleaners": {"home": {"min_age_seconds": 3600}}},
            "cleaner command field": {
                **base,
                "cleaners": {"huggingface_cache": {"min_age_seconds": 3600, "command": "rm"}},
            },
            "boolean retention": {
                **base,
                "cleaners": {"huggingface_cache": {"min_age_seconds": True}},
            },
            "HF retention below minimum": {
                **base,
                "cleaners": {"huggingface_cache": {"min_age_seconds": 3599}},
            },
            "Weles retention below minimum": {
                **base,
                "cleaners": {"weles_recordings": {"min_age_seconds": 86399}},
            },
        }
        for name, policy in cases.items():
            with self.subTest(name):
                with self.assertRaises(RegistryValidationError):
                    validate_registry(registry(local_target(disk_cleanup=policy)))

    def test_rejects_cleanup_policy_on_non_local_targets(self) -> None:
        for kind in ("gcp", "vast"):
            with self.subTest(kind):
                non_local = local_target(
                    kind=kind,
                    disk_cleanup=disk_cleanup_policy(),
                )
                del non_local["weles"]
                with self.assertRaisesRegex(
                    RegistryValidationError,
                    "allowed only for kind='local'",
                ):
                    validate_registry(registry(non_local))

class RegistryLookupTests(unittest.TestCase):
    def test_lookup_self_normalizes_canonical_name_and_alias_inputs(self) -> None:
        document = registry(local_target())
        with tempfile.TemporaryDirectory() as directory:
            registry_path = Path(directory) / "registry.json"
            registry_path.write_text(json.dumps(document), encoding="utf-8")
            with patch("wisent_compute.targets.REGISTRY_PATH", registry_path):
                cases = {
                    "canonical name": " MAC-MINI-A. ",
                    "explicit alias": "MAC-MINI-A.LOCAL...",
                }
                for name, hostname in cases.items():
                    with self.subTest(name):
                        target = lookup_self(hostname, source="local")
                        self.assertIsNotNone(target)
                        self.assertEqual(target.name, "mac-mini-a")

    def test_lookup_self_returns_none_for_missing_or_empty_host_identity(self) -> None:
        document = registry(local_target())
        with tempfile.TemporaryDirectory() as directory:
            registry_path = Path(directory) / "registry.json"
            registry_path.write_text(json.dumps(document), encoding="utf-8")
            with patch("wisent_compute.targets.REGISTRY_PATH", registry_path):
                for hostname in ("other-host", " ... "):
                    with self.subTest(hostname):
                        self.assertIsNone(lookup_self(hostname, source="local"))


if __name__ == "__main__":
    unittest.main()
