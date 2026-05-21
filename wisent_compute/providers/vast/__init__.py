"""Vast.ai marketplace host-listing bridge.

Wisent-compute is the renter on every other cloud (GCP/Azure/AWS).
On Vast.ai it is the HOST — we own the lab-box GPU and list it on
Vast's marketplace so external renters use the otherwise-idle
capacity when wisent-compute has nothing to dispatch.

The Vast.ai REST endpoints used here are verified against vast-cli
(github.com/vast-ai/vast-cli) — list_machine() at vast.py:8092
issues PUT /api/v0/machines/create_asks/ and unlist__machine() at
vast.py:8991 issues DELETE /api/v0/machines/{machine_id}/asks/.

Auth: VAST_API_KEY env var (https://cloud.vast.ai/manage-keys/).
Target machine: WC_VAST_MACHINE_ID env var (numeric machine id from
the Vast host UI; the host has exactly one per physical box).
Optional pricing knobs default to conservative values that the
caller can override via the CLI.

The auto-list loop polls (a) the wisent-compute queue count and
(b) the local-{hostname} capacity blob in GCS. When both indicate
idle for the configured cooldown window, the machine is listed.
When work appears, the machine is unlisted immediately. Existing
Vast rentals are NOT preempted — Vast pays them, wisent-compute
cannot kick them off. The toggle only controls whether NEW renters
can land on the box.
"""
from __future__ import annotations

import json
import os
import socket
import time
import urllib.error
import urllib.request


_VAST_BASE = "https://console.vast.ai/api/v0"


class VastConfigError(RuntimeError):
    pass


def _api_key() -> str:
    key = os.environ.get("VAST_API_KEY", "").strip()
    if not key:
        raise VastConfigError(
            "VAST_API_KEY not set. Get one at https://cloud.vast.ai/manage-keys/"
        )
    return key


def _machine_id() -> int:
    mid = os.environ.get("WC_VAST_MACHINE_ID", "").strip()
    if not mid:
        raise VastConfigError(
            "WC_VAST_MACHINE_ID not set. Find your numeric machine_id at "
            "https://cloud.vast.ai/host/machines/."
        )
    try:
        return int(mid)
    except ValueError as e:
        raise VastConfigError(f"WC_VAST_MACHINE_ID must be int: {mid}") from e


def _request(method: str, path: str, body: dict | None = None) -> dict:
    """Bearer-authenticated REST call against the Vast host API."""
    headers = {
        "Authorization": f"Bearer {_api_key()}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    data = json.dumps(body).encode("utf-8") if body is not None else None
    req = urllib.request.Request(
        f"{_VAST_BASE}{path}", method=method, headers=headers, data=data,
    )
    try:
        with urllib.request.urlopen(req) as resp:
            raw = resp.read().decode("utf-8") or "{}"
            return json.loads(raw)
    except urllib.error.HTTPError as e:
        body_txt = (e.read() or b"").decode("utf-8", errors="replace")
        raise RuntimeError(
            f"Vast.ai {method} {path} -> HTTP {e.code}: {body_txt[:280]}"
        ) from e


def list_machine(
    *,
    price_gpu: float = 0.50,
    price_disk: float = 0.05,
    price_inetu: float = 0.01,
    price_inetd: float = 0.01,
    price_min_bid: float | None = None,
    min_chunk: int = 1,
    duration: int | None = None,
) -> dict:
    """List the configured Vast.ai machine on the marketplace at the
    given prices. PUT /api/v0/machines/create_asks/ with the machine
    id from WC_VAST_MACHINE_ID. Returns the parsed API response."""
    body = {
        "machine": _machine_id(),
        "price_gpu": price_gpu,
        "price_disk": price_disk,
        "price_inetu": price_inetu,
        "price_inetd": price_inetd,
        "min_chunk": min_chunk,
    }
    if price_min_bid is not None:
        body["price_min_bid"] = price_min_bid
    if duration is not None:
        body["duration"] = duration
    return _request("PUT", "/machines/create_asks/", body)


def unlist_machine() -> dict:
    """Remove every active offer from the configured Vast.ai machine.
    DELETE /api/v0/machines/{machine_id}/asks/. Does NOT terminate
    existing rentals — those run until the renter releases them."""
    mid = _machine_id()
    return _request("DELETE", f"/machines/{mid}/asks/", None)


def machine_status() -> dict:
    """Return the current Vast.ai view of our machine (current_rentals,
    listed_status, etc.). GET /api/v0/machines/?owner=me filtered to
    our machine_id."""
    mid = _machine_id()
    resp = _request("GET", "/machines/?owner=me", None)
    machines = resp.get("machines") or resp.get("results") or []
    for m in machines:
        if int(m.get("id", -1)) == mid:
            return m
    return {"id": mid, "error": "not found in /machines/?owner=me response"}


def _is_wisent_compute_busy(bucket: str, hostname: str) -> dict:
    """Read GCS to decide if wisent-compute has any work for this box.
    Returns {queued, running_here, free_vram_gb, idle} so the caller
    can act and tell apart "queue has work and box is free" from
    "queue has work and box is renter-occupied (waiting it out)"."""
    from google.cloud import storage
    from google.api_core.exceptions import NotFound
    client = storage.Client()
    b = client.bucket(bucket)
    queued = sum(1 for _ in b.list_blobs(prefix="queue/", max_results=2))
    cap_blob = b.blob(f"capacity/local-{hostname}.json")
    running_here = 0
    free_vram_gb = None
    try:
        cap = json.loads(cap_blob.download_as_text())
        running_here = int(
            cap.get("diag", {}).get("claimed_this_loop", 0) or 0
        )
        free_vram_gb = cap.get("free_vram_gb")
    except NotFound:
        pass
    except Exception:
        pass
    return {
        "queued": queued, "running_here": running_here,
        "free_vram_gb": free_vram_gb,
        "idle": queued == 0 and running_here == 0,
    }


def auto_list_loop(
    *,
    bucket: str = "wisent-compute",
    hostname: str | None = None,
    idle_window_s: int = 300,
    poll_interval_s: int = 60,
    price_gpu: float = 0.50,
    duration_s: int | None = 3600,
    dry_run: bool = False,
    log_fn=None,
) -> None:
    """Daemon: poll wisent-compute state, toggle Vast.ai listing.
    Lists the machine when wisent-compute has been idle for
    idle_window_s consecutive seconds. Unlists the moment any work
    shows up. Existing Vast rentals are NOT touched.

    duration_s caps the maximum length of any rental Vast can hand
    out from this offer (PUT /machines/create_asks/ duration field,
    vast-cli vast.py:8092). With duration_s=3600 the worst-case wait
    for a wisent-compute job behind an active Vast rental is one
    hour; pass None to leave the offer open-ended."""
    if log_fn is None:
        log_fn = lambda m: print(m, flush=True)  # noqa: E731
    if hostname is None:
        hostname = socket.gethostname()
    idle_since: float | None = None
    listed = False
    while True:
        try:
            st = _is_wisent_compute_busy(bucket, hostname)
        except Exception as exc:
            log_fn(f"poll failed: {type(exc).__name__}: {exc}")
            time.sleep(poll_interval_s)
            continue
        now = time.time()
        if st["idle"]:
            idle_since = idle_since or now
            idle_dur = int(now - idle_since)
            if idle_dur >= idle_window_s and not listed:
                if dry_run:
                    log_fn(f"DRY-RUN would list (idle {idle_dur}s)")
                else:
                    try:
                        list_machine(price_gpu=price_gpu, duration=duration_s)
                        listed = True
                        log_fn(
                            f"LISTED on Vast (idle {idle_dur}s, "
                            f"gpu=${price_gpu}/h, max_duration={duration_s}s)"
                        )
                    except Exception as exc:
                        log_fn(f"list failed: {exc}")
            else:
                log_fn(f"idle {idle_dur}s/{idle_window_s}s (listed={listed})")
        else:
            idle_since = None
            if listed:
                if dry_run:
                    log_fn(
                        f"DRY-RUN would unlist (queued={st['queued']}, "
                        f"running_here={st['running_here']})"
                    )
                else:
                    try:
                        unlist_machine()
                        listed = False
                        log_fn(
                            f"UNLISTED (queued={st['queued']}, "
                            f"running_here={st['running_here']})"
                        )
                    except Exception as exc:
                        log_fn(f"unlist failed: {exc}")
            # Visibility for the "wait for renter to finish" path:
            # if the offer is already gone AND wisent-compute has
            # queued work AND the box has near-zero free VRAM, that
            # means a Vast rental is still on the GPU and the wisent-
            # compute claim loop is going to sit idle until the
            # renter releases (or hits the duration cap). Explicit
            # log so the operator can tell this state apart from a
            # plain dead-agent state.
            free_v = st.get("free_vram_gb")
            if (not listed
                    and st["queued"] > 0
                    and isinstance(free_v, (int, float))
                    and free_v < 10):
                log_fn(
                    f"waiting for Vast rental to finish "
                    f"(queued={st['queued']}, free_vram_gb={free_v}); "
                    f"wisent-compute jobs claim as soon as renter releases"
                )
            elif not listed:
                log_fn(
                    f"busy (queued={st['queued']}, "
                    f"running_here={st['running_here']}); not listed"
                )
        time.sleep(poll_interval_s)
