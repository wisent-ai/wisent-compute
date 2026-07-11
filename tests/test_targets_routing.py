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
