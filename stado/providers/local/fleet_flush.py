"""Asynchronous fleet-staging flush helpers for the local agent."""
from __future__ import annotations

import os
import shutil
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path


def _pid_live(pid: int) -> bool:
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


def _active_flush(lock_path: Path) -> bool:
    try:
        pid = int(lock_path.read_text().strip())
    except (OSError, ValueError):
        return False
    if _pid_live(pid):
        return True
    lock_path.unlink(missing_ok=True)
    return False


def _ensure_hf_token_from_cache() -> None:
    home = os.environ.get("HOME") or os.path.expanduser("~")
    candidates = [
        os.environ.get("HF_TOKEN_FILE", ""),
        os.path.join(home, ".cache", "huggingface", "token"),
        os.path.join(home, ".huggingface", "token"),
    ]
    for path in candidates:
        if not path:
            continue
        try:
            token = Path(path).read_text(encoding="utf-8").strip()
        except OSError:
            continue
        if token:
            os.environ["HF_TOKEN"] = token
            os.environ["HUGGINGFACE_HUB_TOKEN"] = token
            os.environ["HUGGING_FACE_HUB_TOKEN"] = token
            return


def spawn_fleet_flush(fleet_staging: str, log_fn) -> bool:
    """Rotate a staging dir and flush it in a background process.

    Returns True when a background flush is active or was started. The caller
    can keep admitting GPU jobs because new jobs write to a freshly recreated
    fleet_staging directory while the rotated directory uploads.
    """
    staging = Path(fleet_staging)
    flush_root = staging.with_name(f"{staging.name}.async_flushes")
    log_root = staging.with_name(f"{staging.name}.flush_logs")
    flush_root.mkdir(parents=True, exist_ok=True)
    log_root.mkdir(parents=True, exist_ok=True)
    lock_path = flush_root / ".active_pid"
    if _active_flush(lock_path):
        return True

    candidates = sorted(p for p in flush_root.iterdir() if p.is_dir())
    if candidates:
        flush_dir = candidates[0]
    else:
        if not staging.is_dir() or not any(staging.iterdir()):
            return False
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        flush_dir = flush_root / f"flush_{stamp}_{os.getpid()}"
        shutil.move(str(staging), str(flush_dir))
        staging.mkdir(parents=True, exist_ok=True)

    log_path = log_root / f"{flush_dir.name}.log"
    log_file = log_path.open("ab")
    proc = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "wisent_compute.providers.local.fleet_flush",
            "--run",
            str(flush_dir),
            str(lock_path),
        ],
        stdout=log_file,
        stderr=subprocess.STDOUT,
        start_new_session=True,
        close_fds=True,
    )
    log_file.close()
    lock_path.write_text(str(proc.pid))
    log_fn(f"started async fleet staging flush pid={proc.pid} dir={flush_dir}")
    return True


def run_flush(flush_dir: Path, lock_path: Path) -> int:
    try:
        _ensure_hf_token_from_cache()
        from wisent.core.reading.modules.utilities.data.sources.hf.hf_writers import (
            flush_staging_dir,
        )

        flush_staging_dir(str(flush_dir))
        shutil.rmtree(flush_dir)
        return 0
    except Exception as exc:
        print(f"fleet staging async flush failed: {exc}", flush=True)
        return 1
    finally:
        try:
            if int(lock_path.read_text().strip()) == os.getpid():
                lock_path.unlink(missing_ok=True)
        except (OSError, ValueError):
            pass


def main() -> int:
    if len(sys.argv) != 4 or sys.argv[1] != "--run":
        raise SystemExit("usage: python -m wisent_compute.providers.local.fleet_flush --run FLUSH_DIR LOCK_PATH")
    return run_flush(Path(sys.argv[2]), Path(sys.argv[3]))


if __name__ == "__main__":
    raise SystemExit(main())
