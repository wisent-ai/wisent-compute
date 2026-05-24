"""Vast.ai marketplace host-listing bridge.

Wisent-compute is the renter on GCP/Azure/AWS. On Vast.ai it is the
HOST — we own the lab-box GPU and list it on Vast so external
renters use the otherwise-idle capacity when wisent-compute has
nothing to dispatch.

Endpoints verified against vast-cli (github.com/vast-ai/vast-cli):
list_machine vast.py:8092 -> PUT /machines/create_asks/;
unlist__machine vast.py:8991 -> DELETE /machines/{id}/asks/.

Auth: VAST_API_KEY env var. Target machine: WC_VAST_MACHINE_ID (or
auto-discovered via /machines/?owner=me + hostname).

The auto-list loop polls wisent-compute queue + local-{hostname}
capacity blob; lists when idle, unlists when work appears. Existing
Vast rentals are NOT preempted — only NEW renters are blocked.
"""
from __future__ import annotations

import json
import os
import socket
import time
import urllib.error
import urllib.request


_VAST_BASE = "https://console.vast.ai/api/v0"
_AUTO_LIST_THREAD_RUNNING = False  # set True when auto_list_loop starts


class VastConfigError(RuntimeError):
    pass


def _api_key() -> str:
    from ._auth import resolve_vast_api_key
    key = resolve_vast_api_key()
    if not key:
        raise VastConfigError(
            "VAST_API_KEY missing in env and GCP Secret Manager vast-api-key")
    return key


def _machine_id() -> int:
    """Resolve the Vast.ai machine_id.

    Priority: WC_VAST_MACHINE_ID env (explicit) > auto-discovery via
    GET /machines/?owner=me + hostname match. VAST_API_KEY alone is
    enough to enumerate our own machines, so the lab box doesn't need
    the explicit env var — the existing _vast_has_renter helper
    already proves VAST_API_KEY is set on the lab box's agent."""
    mid = os.environ.get("WC_VAST_MACHINE_ID", "").strip()
    if mid:
        try:
            return int(mid)
        except ValueError as e:
            raise VastConfigError(f"WC_VAST_MACHINE_ID must be int: {mid}") from e
    # Auto-discover. _request() will raise VastConfigError if the API
    # key is missing — that's the right error to surface.
    me = _request("GET", "/machines/?owner=me", None)
    machines = me.get("machines") or me.get("results") or []
    if not machines:
        raise VastConfigError(
            "Vast.ai /machines/?owner=me returned no machines. "
            "Register the box at https://cloud.vast.ai/host/setup first."
        )
    host = socket.gethostname()
    for m in machines:
        if (m.get("hostname") or "").strip() == host:
            return int(m["id"])
    if len(machines) == 1:
        return int(machines[0]["id"])
    raise VastConfigError(
        f"Vast.ai returned {len(machines)} machines and hostname "
        f"'{host}' did not match any. Set WC_VAST_MACHINE_ID "
        f"explicitly. Candidates: "
        + ", ".join(f"{m.get('id')}={m.get('hostname')}" for m in machines)
    )


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
    """busy = queue not empty OR any running/ blob has instance_ref
    referencing this hostname. Earlier impl read claimed_this_loop
    (per-iter counter, ~always 0) and missed in-flight work."""
    from google.cloud import storage
    from google.api_core.exceptions import NotFound
    client = storage.Client()
    b = client.bucket(bucket)
    queued = sum(1 for _ in b.list_blobs(prefix="queue/", max_results=2))
    running_here = 0
    for blob in b.list_blobs(prefix="running/"):
        try:
            doc = json.loads(blob.download_as_text())
            if hostname and hostname in (doc.get("instance_ref") or ""):
                running_here += 1
        except (NotFound, Exception):
            continue
    free_vram_gb = None
    try:
        free_vram_gb = json.loads(
            b.blob(f"capacity/local-{hostname}.json").download_as_text()
        ).get("free_vram_gb")
    except (NotFound, Exception):
        pass
    return {"queued": queued, "running_here": running_here,
            "free_vram_gb": free_vram_gb,
            "idle": queued == 0 and running_here == 0}


def auto_list_loop(
    *,
    bucket: str = "wisent-compute",
    hostname: str | None = None,
    idle_window_s: int = 300,
    poll_interval_s: int = 10,
    price_gpu: float = 0.50,
    duration_s: int | None = 15768000,
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
    _ed = os.environ.get("WC_VAST_MAX_DURATION_S")  # env wins (cli.py uneditable)
    if _ed:
        duration_s = int(_ed)
    global _AUTO_LIST_THREAD_RUNNING; _AUTO_LIST_THREAD_RUNNING = True
    idle_since: float | None = None
    # Startup sync: take over any pre-existing listing (manual host-UI
    # placement or an earlier bridge run) so the loop can unlist it
    # when wisent-compute work arrives. Normalize price + duration to
    # the bridge's configured values.
    listed = False
    try:
        ms = machine_status()
        cur_price = ms.get("listed_gpu_cost")
        if isinstance(cur_price, (int, float)) and cur_price > 0:
            listed = True
            log_fn(f"startup: listed at ${cur_price}/h on machine_id={ms.get('id')}")
            if abs(cur_price - price_gpu) > 0.01 and not dry_run:
                try:
                    unlist_machine()
                    list_machine(price_gpu=price_gpu, duration=duration_s)
                    log_fn(f"normalized ${cur_price}/h -> ${price_gpu}/h")
                except Exception as exc:
                    log_fn(f"normalize failed: {exc}")
        else:
            log_fn("startup: not currently listed")
    except Exception as exc:
        log_fn(f"startup probe failed: {exc}")
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
                        log_fn(f"LISTED ({idle_dur}s idle, ${price_gpu}/h, dur={duration_s}s)")
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
