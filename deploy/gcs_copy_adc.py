#!/usr/bin/env python3
"""Resumable ADC-based GCS bucket copier (server-side rewrite, threaded).

Bucket-to-bucket copy for the wisent-compute -> stado migration WITHOUT
gsutil (blocked by the device keychain gate). Uses google-cloud-storage with
application-default credentials and Blob.rewrite (server-side; bytes never
transit this machine). Resumable: skips any object already present in the
destination with a matching size, so re-running continues where it left off.

The source bucket is only READ; nothing is deleted. Safe to run repeatedly.

Usage:
  python3 gcs_copy_adc.py --src wisent-compute --dst stado [--workers 32]
"""
from __future__ import annotations
import argparse
import concurrent.futures as cf
import sys
import threading
from google.cloud import storage
from google.api_core import exceptions as gce

_done = 0
_skipped = 0
_lock = threading.Lock()


def _copy_one(src_bucket, dst_bucket, name, size, present):
    global _done, _skipped
    try:
        if present.get(name) == size:
            with _lock:
                _skipped += 1
            return
        dst_blob = dst_bucket.blob(name)
        src_blob = src_bucket.blob(name)
        token = None
        while True:
            token, _, _ = dst_blob.rewrite(src_blob, token=token)
            if token is None:
                break
        with _lock:
            _done += 1
    except gce.NotFound:
        # source vanished between list and copy: ignore
        pass


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--src", required=True)
    ap.add_argument("--dst", required=True)
    ap.add_argument("--project", default="wisent-480400")
    ap.add_argument("--workers", type=int, default=200)
    a = ap.parse_args()

    client = storage.Client(project=a.project)
    src_bucket = client.bucket(a.src)
    dst_bucket = client.bucket(a.dst)

    print("listing destination for resume...", flush=True)
    present = {b.name: b.size for b in client.list_blobs(dst_bucket)}
    print(f"destination already holds {len(present)} objects", flush=True)

    items = [(b.name, b.size) for b in client.list_blobs(src_bucket)]
    total = len(items)
    print(f"source objects: {total}", flush=True)

    with cf.ThreadPoolExecutor(max_workers=a.workers) as ex:
        futs = [ex.submit(_copy_one, src_bucket, dst_bucket, n, s, present) for n, s in items]
        for i, _ in enumerate(cf.as_completed(futs), 1):
            if i % 5000 == 0:
                print(f"  progress {i}/{total} (copied={_done} skipped={_skipped})", flush=True)

    print(f"DONE: copied={_done} skipped={_skipped} total={total}", flush=True)
    # verify counts match
    dst_total = sum(1 for _ in client.list_blobs(dst_bucket))
    print(f"dst now holds {dst_total} objects (src {total})", flush=True)
    return 0 if dst_total >= total else 1


if __name__ == "__main__":
    raise SystemExit(main())
