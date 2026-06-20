from __future__ import annotations

import argparse
import json
import os
import shutil
import socket
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path

DEFAULT_BUCKET = "wisent-compute"
DEFAULT_INTERVAL_S = 60
OUT_PREFIX = "box_diagnostics"
DEFAULT_COMMAND_TIMEOUT_SECONDS = 12
DEFAULT_OUTPUT_TAIL_CHARS = 12000
MIN_LOOP_INTERVAL_SECONDS = 10


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _run(
    name: str,
    cmd: list[str],
    timeout_s: int = DEFAULT_COMMAND_TIMEOUT_SECONDS,
    tail: int = DEFAULT_OUTPUT_TAIL_CHARS,
) -> dict:
    started = time.time()
    try:
        res = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout_s)
        return {
            "name": name,
            "cmd": cmd,
            "rc": res.returncode,
            "elapsed_s": round(time.time() - started, 3),
            "stdout_tail": (res.stdout or "")[-tail:],
            "stderr_tail": (res.stderr or "")[-tail:],
        }
    except subprocess.TimeoutExpired as exc:
        return {
            "name": name,
            "cmd": cmd,
            "rc": None,
            "elapsed_s": round(time.time() - started, 3),
            "timeout_s": timeout_s,
            "stdout_tail": (exc.stdout or "")[-tail:] if isinstance(exc.stdout, str) else "",
            "stderr_tail": (exc.stderr or "")[-tail:] if isinstance(exc.stderr, str) else "",
            "timed_out": True,
        }
    except Exception as exc:
        return {
            "name": name,
            "cmd": cmd,
            "rc": None,
            "elapsed_s": round(time.time() - started, 3),
            "error": f"{type(exc).__name__}: {exc}",
        }


def _disk(path: str) -> dict:
    try:
        du = shutil.disk_usage(path)
        return {
            "path": path,
            "total_gb": round(du.total / (1024 ** 3), 2),
            "used_gb": round(du.used / (1024 ** 3), 2),
            "free_gb": round(du.free / (1024 ** 3), 2),
            "used_pct": round((du.used / du.total) * 100, 2) if du.total else 0,
        }
    except Exception as exc:
        return {"path": path, "error": f"{type(exc).__name__}: {exc}"}


def _collect(bucket: str) -> dict:
    host = socket.gethostname()
    commands = [
        ("systemctl-agent", ["systemctl", "status", "wisent-agent.service", "--no-pager", "-l"], 12),
        ("systemctl-health", ["systemctl", "status", "wisent-host-health.timer", "--no-pager", "-l"], 12),
        ("journal-agent", ["journalctl", "-u", "wisent-agent.service", "-n", "240", "--no-pager"], 20),
        ("journal-health", ["journalctl", "-u", "wisent-host-health.service", "-n", "120", "--no-pager"], 20),
        ("ps", ["ps", "-eo", "pid,ppid,stat,pcpu,pmem,comm,args", "--sort=-%cpu"], 12),
        ("nvidia-smi", ["nvidia-smi"], 12),
        ("df", ["df", "-h"], 12),
        ("memory", ["free", "-h"], 12),
        ("capacity-list", ["gcloud", "--quiet", "storage", "ls", f"gs://{bucket}/capacity/"], 20),
    ]
    return {
        "schema": "wisent-box-diagnostics-v1",
        "reported_at": _now(),
        "host": host,
        "bucket": bucket,
        "pid": os.getpid(),
        "disk": [_disk("/"), _disk(os.path.expanduser("~")), _disk("/tmp")],
        "commands": {name: _run(name, cmd, timeout) for name, cmd, timeout in commands},
    }


def _upload(bucket: str, payload: dict) -> None:
    from google.cloud import storage
    client = storage.Client(project=os.environ.get("GOOGLE_CLOUD_PROJECT") or None)
    b = client.bucket(bucket)
    host = payload["host"]
    text = json.dumps(payload, indent=2, sort_keys=True)
    b.blob(f"{OUT_PREFIX}/{host}.json").upload_from_string(
        text, content_type="application/json", timeout=30,
    )
    b.blob(f"{OUT_PREFIX}/{host}/latest.json").upload_from_string(
        text, content_type="application/json", timeout=30,
    )


def _write_local(payload: dict) -> None:
    Path("/tmp/wisent_box_diagnostics_latest.json").write_text(
        json.dumps(payload, indent=2, sort_keys=True)
    )


def once(bucket: str) -> int:
    payload = _collect(bucket)
    try:
        _upload(bucket, payload)
        print(f"{payload['reported_at']} uploaded {OUT_PREFIX}/{payload['host']}.json")
        return 0
    except Exception as exc:
        payload["upload_error"] = f"{type(exc).__name__}: {exc}"
        _write_local(payload)
        print(f"{payload['reported_at']} upload failed; wrote /tmp/wisent_box_diagnostics_latest.json")
        return 1


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Upload workstation diagnostics to GCS.")
    p.add_argument("--bucket", default=os.environ.get("WC_BUCKET", DEFAULT_BUCKET))
    p.add_argument("--interval-s", type=int, default=DEFAULT_INTERVAL_S)
    p.add_argument("--once", action="store_true")
    args = p.parse_args(argv)
    if args.once:
        return once(args.bucket)
    while True:
        once(args.bucket)
        time.sleep(max(MIN_LOOP_INTERVAL_SECONDS, args.interval_s))


if __name__ == "__main__":
    raise SystemExit(main())
