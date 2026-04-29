# CLI reference

All commands accept `--help` for the canonical option list. The package
installs a `wc` entry point.

## `wc submit`

Submit a job (or a batch) to the GCS-backed queue.

| Option | What it does |
|---|---|
| `wc submit COMMAND` | Submit one shell command as a job. |
| `wc submit --batch FILE ''` | Submit each line of `FILE` as a separate job, in parallel via a `ThreadPoolExecutor`. |
| `wc submit --priority N` | Higher `N` is dispatched before lower `N`. Default 0. Tie-break inside a priority bucket is FIFO on `created_at`. |
| `wc submit --spot --max-cost-per-hour 4.00` | Dispatch on Spot/Preemptible at most $4/hr per accel. Set to 0 for no cap. |
| `wc submit --any-provider` | Default. Any consumer with capacity may claim. |
| `wc submit --pin-provider` | Only the requested `--provider` may claim. |
| `wc submit --provider gcp\|local` | Hint for which provider should pick the job up. With `--any-provider` this is just a hint. |

Submitter-side env: `HF_TOKEN`, `GH_TOKEN` are read from the submitter's
environment and baked into the per-job startup script that the cloud
agent renders at boot. `COMPUTE_API_KEY` (if set) routes the submission
through the `compute.wisent.com` HTTPS API instead of writing GCS
directly.

## `wc status [filter]`

Tab-separated table of queue / running / completed / failed jobs. With
`COMPUTE_API_KEY` set, hits `compute.wisent.com`; otherwise reads GCS
directly. Optional filter narrows by job-id or batch-id substring.

## `wc cancel <job_id>`

Remove a queued job from `gs://$WC_BUCKET/queue/<id>.json`, or terminate
a running instance via the provider's `delete_instance(...)` and move
the job to `failed/` with `error="cancelled"`.

## `wc results <job_id> <dir>`

`gsutil -m cp -r 'gs://$WC_BUCKET/status/<job_id>/output/*' '<dir>/'`.

## `wc agent`

Run a long-lived GPU agent. Polls `gs://$WC_BUCKET/queue/`, claims any
job whose `gpu_mem_gb <= free_vram_gb` AND passes `_job_eligible`, spawns
the job as a subprocess, and tracks completion.

| Flag | Behavior |
|---|---|
| `wc agent --gpu-type X` | Override the broadcast SKU label (default: nvidia-smi auto-detect). |
| `wc agent --target NAME` | Pull `gpu_type` and `slots` from the registry by name. |
| `wc agent --auto` | Look up self in the registry by hostname. Re-fetches periodically so registry edits propagate without restarting the agent. |
| `wc agent --idle-shutdown` | Exit cleanly (and self-delete the GCE VM if running on one) when no slots active and no eligible queued job remains. Used by cloud-agent VMs. |

The agent broadcasts capacity to
`gs://$WC_BUCKET/capacity/<consumer-id>.json` every poll cycle. The
scheduler reads these broadcasts to decide whether to *yield* a job to
a free local consumer instead of paying for a fresh cloud VM.

## `wc coordinator`

Run the scheduling tick locally instead of as the Cloud Function.

| Flag | Behavior |
|---|---|
| `wc coordinator --target NAME` | Use the named coordinator entry from the registry. |
| `wc coordinator --once` | Run a single scheduling tick and exit (cron-friendly). |

Useful for development and for redundancy if the Cloud Function is
unavailable.

## `wc registry`

| Subcommand | Behavior |
|---|---|
| `wc registry push [path]` | Upload `wisent_compute/targets/registry.json` (or `path`) to `gs://$WC_BUCKET/registry.json`. |
| `wc registry pull` | Print the GCS registry to stdout. |

## `wc cost`

| Subcommand | Behavior |
|---|---|
| `wc cost report` | Per-target / per-model `$` spend computed from completed jobs (`started_at` → `completed_at` × spot or on-demand `$/hr` per accel). |
| `wc cost estimate <batch>` | Project total `$` for a batch file using observed per-job cost from completed-jobs history. |

## `wc bootstrap`

| Flag | Behavior |
|---|---|
| `wc bootstrap [--target NAME]` | SSH into the registry-named host and install + enable the agent as a systemd unit. |
| `wc bootstrap --local` | Install on this machine via launchd (macOS) or systemd-user (Linux) instead of via SSH. |
| `wc bootstrap --dry-run` | Print the unit/plist; do not enable. |
