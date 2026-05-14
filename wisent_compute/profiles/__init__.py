"""Job profiles — named bundles of wcomp submit flags for recurring workflows.

A profile is a JSON file under `wisent_compute/profiles/` (or under the
operator-supplied `$WC_PROFILES_DIR`). It declares the same fields that
wcomp submit takes as CLI flags — gpu_type, vram_gb, apt, pre_command,
repo, etc. — so a recurring workflow ("Z-Image LoRA training on
ai-toolkit", "lm-eval on vLLM", ...) can be invoked with a single
--profile NAME flag instead of a 20-line one-shot command.

Discovery order (first hit wins):

  1. $WC_PROFILES_DIR/<name>.json     operator-local profiles
  2. <package>/profiles/<name>.json   bundled with the wheel

CLI flags ALWAYS override profile values. The profile is a default
template, not a hard contract.

Schema (every field optional):

  {
    "name": "ai-toolkit-zimage",
    "description": "...",
    "gpu_type": "nvidia-l4",
    "vram_gb": 22,
    "machine_type": "",
    "apt": ["libgl1", "git-lfs"],
    "pre_command": "export FOO=bar",
    "repo": "https://github.com/...",
    "repo_workdir": "ai-toolkit",
    "repo_extras": "",
    "output_uri": "",
    "verify": "",
    "priority": 0,
    "spot": false,
    "max_cost_per_hour": 0.0
  }
"""
from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any


PACKAGE_PROFILES_DIR = Path(__file__).parent


# Maps profile JSON keys to wcomp submit kwarg names. Keeps the JSON
# user-friendly (e.g. `apt` instead of `apt_packages`) while the
# downstream code stays explicit.
_PROFILE_KEY_TO_KWARG = {
    "gpu_type": "gpu_type",
    "vram_gb": "vram_gb",
    "machine_type": "machine_type",
    "apt": "apt_packages",
    "pre_command": "pre_command",
    "repo": "repo",
    "repo_workdir": "repo_workdir",
    "repo_extras": "repo_extras",
    "output_uri": "output_uri",
    "verify": "verify_command",
    "priority": "priority",
    "spot": "preemptible",
    "max_cost_per_hour": "max_cost_per_hour_usd",
    "provider": "provider",
    "pin_provider": "pin_to_provider",
}


def _candidate_dirs() -> list[Path]:
    """Profile search path. Operator override first, package second."""
    dirs: list[Path] = []
    override = os.environ.get("WC_PROFILES_DIR", "").strip()
    if override:
        dirs.append(Path(override))
    dirs.append(PACKAGE_PROFILES_DIR)
    return dirs


def list_profiles() -> list[str]:
    """All profile names visible on the discovery path. De-duped, sorted."""
    seen: set[str] = set()
    out: list[str] = []
    for d in _candidate_dirs():
        if not d.is_dir():
            continue
        for p in sorted(d.glob("*.json")):
            name = p.stem
            if name in seen:
                continue
            seen.add(name)
            out.append(name)
    return out


def load_profile(name: str) -> dict[str, Any]:
    """Return the profile JSON as a dict. Raises FileNotFoundError if absent.

    The first match on the discovery path wins, so operator-local profiles
    in $WC_PROFILES_DIR override bundled ones with the same name.
    """
    for d in _candidate_dirs():
        path = d / f"{name}.json"
        if path.is_file():
            with path.open() as f:
                data = json.load(f)
            if not isinstance(data, dict):
                raise ValueError(f"profile {name}: expected JSON object, got {type(data).__name__}")
            data.setdefault("name", name)
            return data
    raise FileNotFoundError(
        f"profile '{name}' not found. Searched: "
        + ", ".join(str(d) for d in _candidate_dirs())
        + f". Available: {', '.join(list_profiles()) or '(none)'}"
    )


def merge_into_kwargs(profile: dict[str, Any], cli: dict[str, Any]) -> dict[str, Any]:
    """Return submit_job kwargs with profile values applied where the CLI
    didn't pass an explicit value.

    `cli` is the dict of caller-supplied kwargs from wcomp submit. A
    kwarg counts as "explicit" when its value differs from the wisent-
    compute defaults — empty string, 0, False, []. This means a user who
    passes `--gpu-type nvidia-l4` overrides the profile's gpu_type; a
    user who doesn't pass --gpu-type takes whatever the profile says
    (including nothing).
    """
    DEFAULTS = {
        "gpu_type": "",
        "vram_gb": 0,
        "machine_type": "",
        "apt_packages": [],
        "pre_command": "",
        "repo": "",
        "repo_workdir": "",
        "repo_extras": "train",  # historical default — keep
        "output_uri": "",
        "verify_command": "",
        "priority": 0,
        "preemptible": False,
        "max_cost_per_hour_usd": 0.0,
        "provider": "gcp",
        "pin_to_provider": False,
    }
    out = dict(cli)
    for pkey, kwarg in _PROFILE_KEY_TO_KWARG.items():
        if pkey not in profile:
            continue
        cli_val = cli.get(kwarg, DEFAULTS.get(kwarg))
        if cli_val == DEFAULTS.get(kwarg):
            # CLI didn't override — adopt the profile value.
            out[kwarg] = profile[pkey]
    return out
