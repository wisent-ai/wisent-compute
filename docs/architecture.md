# Architecture

## Data flow

```
        submit (CLI / API)
              │
              ▼
   gs://$WC_BUCKET/queue/<id>.json     ← the job ledger
              │
        ┌─────┴───────────────────────────────────┐
        ▼                                         ▼
  Cloud Function tick                     Local agent (long-lived)
  (every 3 min via Cloud Scheduler)       (workstation, 1 box per registry)
        │                                         │
        ▼                                         ▼
  schedule_queued_jobs                       _job_eligible + claim
        │                                         │
        ▼                                         ▼
  dispatch_agent_vms                       move_job(queue → running)
   (one VM per (accel,                     run subprocess
   machine_type) bucket,                          │
   sized to largest queued                         ▼
   job in the bucket,                       gs://$WC_BUCKET/
   uses startup_gpu_agent.sh                  status/<id>/output/
   which boots `wc agent                            │
   --idle-shutdown`)                                ▼
        │                                    move_job(running → completed)
        ▼
  GCE VM boots, pip-installs
  wisent + wisent-compute, runs
  `wc agent --idle-shutdown`,
  claims jobs by VRAM, self-
  deletes when queue exhausted.
```

## GCS layout

Job state lives under `gs://$WC_BUCKET/`:

| Prefix | Contents |
|---|---|
| `queue/<id>.json` | Pending jobs. The ledger. |
| `running/<id>.json` | In-flight jobs with `instance_ref`, `started_at` set. |
| `completed/<id>.json` | Finished successfully. `completed_at` set. |
| `failed/<id>.json` | Finished with rc != 0. `failed_at`, `error` set. |
| `status/<id>/status` | `RUNNING <ts>` / `COMPLETED` / `FAILED exit=N`. |
| `status/<id>/heartbeat` | `RUNNING <ts>` refreshed by the running job. |
| `status/<id>/output/...` | stdout, profile PNGs, anything `gsutil cp -r`'d at job end. |
| `capacity/<consumer-id>.json` | Per-consumer broadcast: `free_vram_gb`, `total_vram_gb`, `free_slots`. |
| `scripts/<id>.sh` | Per-job rendered startup script (legacy 1-VM-per-job path). |
| `config/quotas.json` | Reservation overlay (subtracts from live API limits). |
| `registry.json` | Live compute-target registry; agents re-fetch every poll. |

State transitions are atomic from the caller's POV:
`write_job(new_prefix)` then `delete_blob(old_prefix)`.

## Scheduling rules

The scheduler (`wisent_compute/scheduler/scheduler.py`) sorts queued jobs
by `(-priority, created_at)` and applies, in order:

1. **Per-tick listing cap** — `_dynamic_per_tick_cap(queue_depth) * 8`
   blobs are pulled from GCS oldest-first. Beyond that wouldn't dispatch
   anyway, and pulling a 28k+ queue every tick blew the function timeout.
2. **Per-accelerator fairness** — `per_accel_share = ceil(per_tick_cap /
   distinct_accels)` so a heterogeneous queue (T4 + A100-40 + A100-80)
   makes concurrent progress instead of one accel hogging every tick.
3. **Cost-optimal local pack** — a `(wall_seconds/3600) * $/hr / vram`
   knapsack picks queued jobs to *yield* to a free local consumer
   instead of paying for a fresh VM.
4. **Spot with on-demand escape** — after `max_preempts_before_ondemand`
   preemptions (default 3), the next attempt for that job dispatches
   on-demand instead of Spot.
5. **Per-machine-type zone rotation** — `MACHINE_TYPE_ZONES` in
   `config.py` lets a SKU walk to alternate regions when its primary
   zones are exhausted (e.g. `a2-ultragpu-1g` → us-east5, europe-west4
   when us-central1 spot is dry).
6. **Dispatch backoff** — failed `create_instance` calls escalate via
   `DISPATCH_BACKOFF_MINUTES = [0, 1, 5, 15, 30, 60, 120, 240]`
   minutes per attempt count.

The local agent (`wisent_compute/providers/local_agent.py`) walks the
queue FIFO and claims any job whose `gpu_mem_gb <= free_vram_gb` AND
passes `_job_eligible` (gpu_type-compat or pinned-local). No slot count
— pure VRAM admission.

## Cloud-agent VM lifecycle

- **Spawn** — scheduler tick calls `provider.create_instance(...)` with
  `wisent_compute/templates/startup_gpu_agent.sh` rendered into the VM's
  startup script (`HF_TOKEN` substituted from Secret Manager).
- **Boot** — VM runs `apt-get install python3-venv`, creates a venv,
  `pip install wisent wisent-compute wisent-extractors wisent-evaluators
  wisent-tools lm-eval` plus pinned `transformers<5.0`, `tokenizers`,
  `datasets<4.0`, `huggingface-hub<1.0`, `numpy<2.3`. Sets
  `NUMBA_NUM_THREADS=1` and `HF_HUB_DOWNLOAD_TIMEOUT=120` in env so the
  pinned versions and rate-limit-tolerant fetches are loaded before
  Python imports.
- **Run** — `wc agent --gpu-type "${ACCEL_TYPE}" --idle-shutdown` polls
  the queue, claims jobs, spawns subprocesses.
- **Self-shutdown** — when `len(slots) == 0 AND no_eligible_in_queue`,
  the agent exits cleanly. On GCE,
  `wisent_compute/providers/local/gcp_self.py` calls `gcloud compute
  instances delete --quiet` against the VM's own metadata-derived
  `(name, zone)` to release the Spot reservation back to the pool.
  No-op outside GCE.

## Consumer capacity broadcasts

Every consumer (local agent + each cloud agent) writes
`gs://$WC_BUCKET/capacity/<consumer-id>.json` every poll cycle:

```json
{
  "consumer_id": "local-ubuntu-server",
  "kind": "local",
  "free_vram_gb": 14,
  "total_vram_gb": 96,
  "free_slots": {"nvidia-tesla-t4": 6, "nvidia-l4": 4, "nvidia-tesla-a100": 2},
  "published_at": "2026-04-29T01:30:01.000000+00:00"
}
```

The cloud scheduler reads these to decide whether to *yield* a queued
job to a free local consumer instead of dispatching a paid VM. The
yield decision uses the cost-optimal knapsack: jobs with the highest
`$-saved-per-GB-of-local-VRAM` get marked for local pickup first.
