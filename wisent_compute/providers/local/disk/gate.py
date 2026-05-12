"""Per-tick disk-space gate for the local agent loop.

Two structural problems this addresses:
  1. The agent's $HOME runs out of disk after a week of jobs because each
     job's HF dataset download accumulates in ~/.cache/huggingface/hub
     and is never reaped. Once $HOME is full, every subsequent job dies
     mid-pair-generation with [Errno 28] No space left on device.
  2. The old gate refused all slots when free < 30 GB and waited forever
     for the operator to clean up. The agent went silently idle on a
     full disk for ~17 hours on 2026-05-07 because no one was watching.

This module:
  - Reports current free-disk under $HOME (signal for capacity diag).
  - When low, tries one eviction of the HF cache before refusing slots.
  - Returns a single (refuse_this_tick, agent_diag_updates) tuple.
"""
from __future__ import annotations

import os
import shutil
from typing import Callable


MIN_FREE_DISK_GB = 15


def _free_gb(path: str) -> float:
    try:
        return shutil.disk_usage(path).free / (1024 ** 3)
    except OSError:
        return -1.0


def gate_and_maybe_evict(log_fn: Callable[[str], None]) -> tuple[bool, dict]:
    """Returns (refuse_slots_this_tick, diag_updates).

    refuse_slots_this_tick=True if free disk is still below
    MIN_FREE_DISK_GB AFTER attempting one HF-cache eviction.
    """
    home = os.path.expanduser("~")
    free_gb = _free_gb(home)
    diag: dict = {"free_disk_gb": round(free_gb, 1)}
    if 0 <= free_gb < MIN_FREE_DISK_GB:
        hf_hub = os.path.expanduser("~/.cache/huggingface/hub")
        if os.path.isdir(hf_hub):
            before = _free_gb(home)
            try:
                shutil.rmtree(hf_hub, ignore_errors=True)
            except Exception as exc:
                log_fn(f"HF cache eviction failed: {exc!r}")
            after = _free_gb(home)
            reclaimed = max(0.0, after - before)
            log_fn(
                f"HF cache evicted; reclaimed {reclaimed:.1f} GB "
                f"({before:.1f} -> {after:.1f})"
            )
            free_gb = after
            diag["free_disk_gb"] = round(free_gb, 1)
            diag["hf_cache_evicted_gb"] = round(reclaimed, 1)
    refuse = 0 <= free_gb < MIN_FREE_DISK_GB
    if refuse:
        log_fn(
            f"disk still low (~{free_gb:.1f} GB free in $HOME) after "
            f"eviction; refusing slots this tick"
        )
    return refuse, diag
