# Configuration

## Environment variables

| Var | Purpose | Read at |
|---|---|---|
| `GCP_PROJECT` | GCP project ID. | `config.py` (default `wisent-480400`) |
| `WC_BUCKET` | GCS bucket for queue + state. | `config.py` (default `wisent-compute`) |
| `GCP_REGION` | Region for VM creation + quota fetch. | `config.py`, `quota.py:_fetch_gcp_quotas` |
| `WC_ALERTS_TOPIC` | Pub/Sub topic for the alert publisher. | `config.py` |
| `HF_TOKEN` | HuggingFace token, baked into VM startup scripts. | `queue/submit.py`, agent template |
| `WC_LOCAL_SLOTS` | Optional hard cap on local-agent concurrency. `0` = uncapped (default). | `local_agent.py` |
| `COMPUTE_API_KEY` | If set, `wc submit` / `wc status` route through the `compute.wisent.com` HTTPS API instead of GCS. | `cli.py:_api_key` |
| `COMPUTE_API_URL` | Overrides the default `https://compute.wisent.com`. | `queue/submit.py` |
| `WC_SLACK_WEBHOOK`, `WC_TELEGRAM_BOT_TOKEN`, `WC_TELEGRAM_CHAT_ID`, `WC_SENDGRID_API_KEY`, `WC_EMAIL_TO`, `WC_EMAIL_FROM` | Alert sinks for `monitor/alerts.py`. | optional |

## Registry

`wisent_compute/targets/registry.example.json` is the template.
Operators ship their own `wisent_compute/targets/registry.json`
(gitignored) or `gsutil cp` it directly to
`gs://$WC_BUCKET/registry.json` — running agents re-fetch every poll
so edits propagate without restart.

Each target entry:

```jsonc
{
  "name": "my-workstation",
  "kind": "local",
  "ssh": "user@host-or-ip",       // used by `wc bootstrap` to install the agent
  "gpu_type": "nvidia-tesla-t4",  // SKU label the agent broadcasts
  "slots": 0,                     // 0 = no concurrency cap, pure VRAM admission
  "vram_gb": 96,                  // total GPU VRAM
  "env_overrides": { "WISENT_DTYPE": "auto" },
  "agent_args": ["--gpu-type", "nvidia-tesla-t4"]
}
```

A coordinator entry pins the scheduling-tick driver:

```jsonc
{
  "name": "gcp-cloud-function",
  "runtime": "gcp_cloud_function",
  "host": null,
  "interval_seconds": 180,
  "state_uri": "gs://$WC_BUCKET",
  "active": true
}
```

## Quotas

Live limits come from the GCP regions API
(`compute_v1.RegionsClient().get(project, region)`).
`gs://$WC_BUCKET/config/quotas.json` is *reservations only*:

```json
{
  "gcp": {
    "nvidia-tesla-a100": {"reserved": 4}
  }
}
```

means "subtract 4 A100s from the live limit before dispatching" —
useful when you want to reserve capacity for non-wisent workloads.
The `total` field is ignored; setting it has no effect.

The metric-name → internal accel mapping
(`wisent_compute/scheduler/quota.py:_GCP_METRIC_TO_ACCEL`):

```python
_GCP_METRIC_TO_ACCEL = {
    "PREEMPTIBLE_NVIDIA_T4_GPUS":      "nvidia-tesla-t4",
    "PREEMPTIBLE_NVIDIA_L4_GPUS":      "nvidia-l4",
    "PREEMPTIBLE_NVIDIA_A100_GPUS":    "nvidia-tesla-a100",
    "PREEMPTIBLE_NVIDIA_A100_80GB_GPUS": "nvidia-a100-80gb",
}
```

## GCP setup

`deploy/gcp_setup.sh` is a one-time bootstrap (creates the SA, grants
project roles, creates the bucket and the Pub/Sub topic). Re-runs are
idempotent. Operators set `GCP_PROJECT` and `GCP_REGION` env vars first.

`deploy/redeploy_function.sh` redeploys just the Cloud Function — used
by CI on every push to `main` (see `.github/workflows/deploy.yml`).

For workload-identity federation from GitHub Actions: edit the
`workload_identity_provider` and `service_account` lines in
`deploy.yml` to match your project's pool / SA.

## Per-machine-type zone rotation

`MACHINE_TYPE_ZONES` in `wisent_compute/config.py` is consulted by
`providers/gcp.py:create_instance` before the default
`ZONE_ROTATION`. It exists because some accelerator-optimized SKUs
(`a2-ultragpu-1g`, the A100-80GB single-GPU machine) only exist in a
subset of zones, and Spot capacity in `us-central1-a` is regularly
exhausted:

```python
MACHINE_TYPE_ZONES = {
    "a2-ultragpu-1g": [
        f"{REGION}-c", f"{REGION}-a",
        "us-east5-a", "us-east5-b", "us-east4-c",
        "europe-west4-a",
    ],
}
```

The provider iterates these in order and creates the instance in the
first zone that returns a non-`None` ref. 503 ZONE_RESOURCE_POOL_EXHAUSTED
or 400 "machine type does not exist" cause it to walk to the next zone.

## Pinned cloud-agent dependencies

`wisent_compute/templates/startup_gpu_agent.sh` pins the following
deps. Each pin has a known reason — don't relax them without reading
the comments in the template:

| Pin | Reason |
|---|---|
| `transformers>=4.55,<5.0` | transformers 5.x has a 0-indexed shard-name miscompute that fails on Llama-2-7b/Qwen3-8B/gpt-oss-20b. |
| `tokenizers>=0.20,<0.22` | matches `transformers<5.0`. |
| `datasets>=3.0,<4.0` | datasets 4.0 dropped support for dataset loading scripts (`flores.py` etc. raise `RuntimeError: Dataset scripts are no longer supported`). |
| `huggingface-hub>=0.34.0,<1.0` | hub 1.x violates `transformers<5.0`'s `huggingface-hub<1.0` constraint and the agent crashes at import time. |
| `numpy>=1.24,<2.3` | numba 0.61.x requires numpy < 2.3. |
| `NUMBA_NUM_THREADS=1` (env var) | wisent sets this in 8 modules but the import-order race lets numba init at the system cpu_count first; setting it in the agent's own env avoids `RuntimeError: Cannot set NUMBA_NUM_THREADS once threads have been launched`. |
| `HF_HUB_DOWNLOAD_TIMEOUT=120` (env var) | wisent fleet hits HF's 1000-req/5-min free-tier ceiling regularly; longer timeouts let the SDK back off and retry rather than fail the whole job. |
