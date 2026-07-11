"""Focused contracts for local service installation and executable resolution."""
from __future__ import annotations

import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from wisent_compute.deploy import local_install


class WcBinResolutionTests(unittest.TestCase):
    def resolve_wc(
        self,
        *,
        argv0: str,
        executable: str,
        which: str | None,
        files: set[Path],
    ) -> str:
        home = Path("/Users/tester")
        with (
            patch.object(local_install.sys, "argv", [argv0]),
            patch.object(local_install.sys, "executable", executable),
            patch.object(local_install.shutil, "which", return_value=which),
            patch.object(
                local_install.Path,
                "is_file",
                autospec=True,
                side_effect=lambda path: path in files,
            ),
            patch.object(local_install.Path, "home", return_value=home),
        ):
            return local_install._wc_bin()

    def test_invoked_venv_wc_takes_precedence(self) -> None:
        invoked = Path("/opt/invoked-venv/bin/wc")
        sibling = Path("/opt/current-venv/bin/wc")

        resolved = self.resolve_wc(
            argv0=str(invoked),
            executable=str(sibling.with_name("python")),
            which="/Users/tester/.local/bin/wc",
            files={invoked, sibling},
        )

        self.assertEqual(resolved, str(invoked))

    def test_current_python_sibling_wc_wins_when_argv_is_not_wc(self) -> None:
        sibling = Path("/opt/current-venv/bin/wc")

        resolved = self.resolve_wc(
            argv0="/opt/invoked-venv/bin/python",
            executable=str(sibling.with_name("python")),
            which="/Users/tester/.local/bin/wc",
            files={sibling},
        )

        self.assertEqual(resolved, str(sibling))

    def test_system_wc_is_rejected(self) -> None:
        resolved = self.resolve_wc(
            argv0="/usr/bin/wc",
            executable="/usr/bin/python3",
            which="/usr/bin/wc",
            files={Path("/usr/bin/wc")},
        )

        self.assertEqual(resolved, "wc")

    def test_legacy_user_bins_remain_fallbacks(self) -> None:
        home = Path("/Users/tester")
        library_bin = home / "Library" / "Python" / "3.12" / "bin" / "wc"
        local_bin = home / ".local" / "bin" / "wc"
        cases = {
            "macOS user bin": (library_bin, {library_bin, local_bin}),
            "POSIX user bin": (local_bin, {local_bin}),
        }

        for name, (expected, files) in cases.items():
            with self.subTest(name):
                resolved = self.resolve_wc(
                    argv0="/opt/tools/bootstrap",
                    executable="/usr/bin/python3",
                    which="/usr/bin/wc",
                    files=files,
                )
                self.assertEqual(resolved, str(expected))


class DiskCleanupInstallTests(unittest.TestCase):
    def test_project_precedence_is_rendered_without_dropping_adc(self) -> None:
        from wisent_compute import config

        cases = {
            "explicit Google project": (
                {"GOOGLE_CLOUD_PROJECT": "explicit-project", "GCP_PROJECT": "legacy-project"},
                "explicit-project",
            ),
            "legacy GCP project": ({"GCP_PROJECT": "legacy-project"}, "legacy-project"),
            "configured project": ({}, "configured-project"),
        }

        for name, (process_env, expected_project) in cases.items():
            with self.subTest(name):
                rendered: list[dict[str, str]] = []

                def capture_install(
                    label: str,
                    exec_args: list[str],
                    env: dict[str, str],
                    echo: object,
                ) -> None:
                    rendered.append(dict(env))

                with (
                    patch.dict(local_install.os.environ, process_env, clear=True),
                    patch.object(config, "PROJECT", "configured-project"),
                    patch.object(local_install, "_adc_path", return_value="/secure/adc.json"),
                    patch.object(local_install, "_exec_args_for", return_value=["wc", "disk-cleanup", "--watch"]),
                    patch.object(local_install.platform, "system", return_value="Darwin"),
                    patch.object(local_install, "_install_darwin", new=capture_install),
                ):
                    local_install.install_local(
                        SimpleNamespace(name="cleanup-host"),
                        "disk-cleanup",
                        False,
                        lambda message: None,
                    )

                self.assertEqual(rendered[0]["GOOGLE_CLOUD_PROJECT"], expected_project)
                self.assertEqual(
                    rendered[0]["GOOGLE_APPLICATION_CREDENTIALS"],
                    "/secure/adc.json",
                )


if __name__ == "__main__":
    unittest.main()
