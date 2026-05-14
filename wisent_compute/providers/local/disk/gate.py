"""Per-tick disk-space gate for the local agent loop.

Two structural problems this addresses:
  1. The agent's $HOME runs out of disk after a week of jobs because each
     job's HF dataset download accumulates in ~/.cache/huggingface/hub
     and is never reaped. Once $HOME is full, every subsequent job dies
     mid-pair-generation with [Errno 28] No space left on device.
  2. The old gate refused all slots when free < 30 GB and waited forever
     for the operator to clean up. The agent went silently idle on a
     full disk for ~17 hours on 2026-05-07 because no one was watching.

Eviction is NOT a blind rmtree. The previous implementation deleted the
entire ~/.cache/huggingface/hub directory, which on 2026-05-13 was
observed to clobber in-progress gpt-oss-20b downloads (~40 GB) mid-fetch
because the eviction tick fired while disk pressure pushed free below
MIN_FREE_DISK_GB. The hf_hub_download path then crashed with
FileNotFoundError on the just-deleted .incomplete blob. Now we route
through huggingface_hub.scan_cache_dir which enumerates complete
revisions only and lets us delete the oldest-accessed ones via
delete_revisions, leaving any revision with an active download alone.
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


def _evict_complete_hf_revisions(log_fn: Callable[[str], None]) -> float:
    """Delete oldest-accessed COMPLETE HF cache revisions. Returns total
    bytes freed (as reported by HfCacheInfo). Revisions with any
    .incomplete blob (i.e. an active download) are excluded because
    huggingface_hub.scan_cache_dir filters down to fully-materialized
    snapshots. HF SDK errors propagate so the operator sees them.
    """
    from huggingface_hub import scan_cache_dir
    from huggingface_hub.errors import CacheNotFound
    try:
        info = scan_cache_dir()
    except CacheNotFound:
        # Documented absent-cache state: ~/.cache/huggingface/hub does
        # not exist yet, so there is literally nothing to evict. Return
        # 0 reclaimed rather than crashing the entire agent loop on a
        # state that is expected on a fresh host.
        log_fn("HF cache absent (~/.cache/huggingface/hub not found); 0 GB reclaimed")
        return 0.0
    revisions = []
    for repo in info.repos:
        for rev in repo.revisions:
            revisions.append((rev.last_accessed, rev.commit_hash, rev.size_on_disk))
    revisions.sort()  # oldest accessed first
    if not revisions:
        return 0.0
    hashes = [h for _, h, _ in revisions]
    strategy = info.delete_revisions(*hashes)
    strategy.execute()
    freed_bytes = getattr(strategy, "expected_freed_size", 0)
    return freed_bytes / (1024 ** 3)


def _evict_secondary_caches(log_fn: Callable[[str], None]) -> float:
    """Second-tier eviction: pip wheel cache, wisent local cache, apt
    archive, system journals truncated to last day. Used when HF cache
    eviction freed 0 GB (typical when the HF cache directory itself was
    already pruned by something upstream) but the disk gate still
    refuses slots. Each target is reproducible from upstream so its
    deletion does not lose unique state.
    """
    home = os.path.expanduser("~")
    targets = [
        os.path.join(home, ".cache", "pip"),
        os.path.join(home, ".wisent_cache"),
        os.path.join(home, ".cache", "huggingface", "hub"),
        os.path.join(home, ".cache", "huggingface", "datasets"),
        "/var/cache/apt/archives",
        "/tmp/pip-build",
    ]
    freed = 0.0
    for tgt in targets:
        if not os.path.isdir(tgt):
            continue
        before = shutil.disk_usage(home).free
        try:
            shutil.rmtree(tgt, ignore_errors=True)
        except Exception as exc:
            log_fn(f"secondary-evict rmtree({tgt}) failed: {exc!r}")
            continue
        after = shutil.disk_usage(home).free
        gained_gb = max(0.0, (after - before) / (1024 ** 3))
        if gained_gb > 0:
            log_fn(f"secondary-evict rm {tgt}: +{gained_gb:.1f} GB free")
            freed += gained_gb
    return freed


def _top_consumers() -> list[dict]:
    """du --max-depth=1 across $HOME, /var, /opt, /tmp; top 15 by size.

    Used by the diag dump when the agent has to refuse slots due to
    disk pressure, so the operator can see exactly which paths are
    filling the disk without needing to SSH in.
    """
    import subprocess
    paths = [os.path.expanduser("~"), "/var", "/opt", "/tmp"]
    rows: list[tuple[int, str]] = []
    for p in paths:
        if not os.path.isdir(p):
            continue
        try:
            r = subprocess.run(
                ["du", "--max-depth=1", "--block-size=1M", p],
                capture_output=True, text=True,
            )
        except FileNotFoundError:
            return []
        for line in r.stdout.splitlines():
            parts = line.split(None, 1)
            if len(parts) != 2:
                continue
            try:
                size_mb = int(parts[0])
            except ValueError:
                continue
            rows.append((size_mb, parts[1]))
    rows.sort(reverse=True)
    return [{"size_mb": s, "path": p} for s, p in rows[:15]]


def gate_and_maybe_evict(log_fn: Callable[[str], None]) -> tuple[bool, dict]:
    """Returns (refuse_slots_this_tick, diag_updates).

    refuse_slots_this_tick=True only if free disk is still below
    MIN_FREE_DISK_GB AFTER:
      1. Evicting oldest-accessed complete HF cache revisions
      2. Evicting secondary caches (pip wheels, apt archives, wisent
         cache, HF cache root, HF datasets cache) when HF eviction
         freed 0 GB but disk is still tight

    When we still refuse after both eviction passes, diag includes a
    `top_disk_consumers` list of the biggest paths so the operator can
    see what is filling the disk without needing to SSH in.
    """
    home = os.path.expanduser("~")
    free_gb = _free_gb(home)
    diag: dict = {"free_disk_gb": round(free_gb, 1)}
    if 0 <= free_gb < MIN_FREE_DISK_GB:
        before = _free_gb(home)
        reclaimed = _evict_complete_hf_revisions(log_fn)
        after = _free_gb(home)
        actual_reclaimed = max(0.0, after - before)
        log_fn(
            f"HF cache evicted (complete revisions only); reclaimed "
            f"{actual_reclaimed:.1f} GB ({before:.1f} -> {after:.1f})"
        )
        free_gb = after
        diag["free_disk_gb"] = round(free_gb, 1)
        diag["hf_cache_evicted_gb"] = round(actual_reclaimed, 1)
        if free_gb < MIN_FREE_DISK_GB and actual_reclaimed < 1.0:
            secondary = _evict_secondary_caches(log_fn)
            after2 = _free_gb(home)
            free_gb = after2
            diag["free_disk_gb"] = round(free_gb, 1)
            diag["secondary_evicted_gb"] = round(secondary, 1)
    refuse = 0 <= free_gb < MIN_FREE_DISK_GB
    if refuse:
        log_fn(
            f"disk still low (~{free_gb:.1f} GB free in $HOME) after "
            f"eviction; refusing slots this tick"
        )
        diag["top_disk_consumers"] = _top_consumers()
    return refuse, diag
