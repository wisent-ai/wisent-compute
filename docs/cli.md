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
| `wc submit --gpu-type STR` | Pin the accelerator label (`nvidia-l4`, `nvidia-a100-80gb`, ...). Skips the `--model X.YB` regex inference. Machine type resolved from `GPU_SIZING` unless `--machine-type` is also passed. |
| `wc submit --vram-gb N` | Caller-declared VRAM requirement. Picks the smallest tier in `GPU_SIZING` whose memory >= N. Use this when the job's command has no `--model` substring (any non-wisent workload). |
| `wc submit --machine-type STR` | Pin the GCE/Azure machine type verbatim (`g2-standard-8`, `Standard_NC8ads_A10_v4`, ...). For SKUs not in the catalog. |
| `wc submit --pre-command STR` | Shell snippet placed before the command in the same bash shell — `export FOO=...` reaches the subprocess. Joined via `&&` so a non-zero exit in the prelude aborts the job. |
| `wc submit --apt PKG[,PKG...]` | Apt packages installed via `sudo -n apt-get install -y --no-install-recommends` before the subprocess spawns. Cloud-kind agents only; local-kind agents refuse the job. |
| `wc submit --output-uri gs://...` | Additional destination mirrored to after job completion. Additive — `status/<id>/output/` is always written too. |
| `wc submit --verify STR` | Post-success shell command. Non-zero exit reverses `COMPLETED → FAILED`. Catches silent-success failure modes (e.g. wisent's `extract_and_upload` reporting "5/7 strategies failed" but exiting 0). |

Submitter-side env: `HF_TOKEN`, `GH_TOKEN` are read from the submitter's
environment and baked into the per-job startup script that the cloud
agent renders at boot. `COMPUTE_API_KEY` (if set) routes the submission
through the `compute.wisent.com` HTTPS API instead of writing GCS
directly.

**Sizing precedence** (each layer overrides the previous):

1. `estimate_gpu_memory(command)` — model-name regex on the command, the wisent-eval default.
2. `--vram-gb N` — caller-declared VRAM, skips the regex.
3. `--gpu-type STR` — pinned accelerator, picks machine_type from the catalog.
4. `--machine-type STR` — pinned machine type verbatim.

If none of the GPU flags are set AND no `--model X.YB` matches, the job lands on `e2-standard-8` (CPU).

**Example** (Z-Image LoRA training on a fresh L4 with ai-toolkit deps):

```bash
wcomp submit \
  --gpu-type nvidia-l4 --vram-gb 22 \
  --apt libgl1,git-lfs,build-essential,libglib2.0-0 \
  --repo https://github.com/ostris/ai-toolkit.git --repo-workdir ai-toolkit --repo-extras "" \
  --pre-command 'TORCH_NVDIR=$(python3 -c "import os,nvidia; print(os.path.dirname(nvidia.__file__))"); export LD_LIBRARY_PATH=$(ls -d $TORCH_NVDIR/*/lib|paste -sd:):$LD_LIBRARY_PATH' \
  --output-uri "gs://wisent-images-bucket/Jakubs-lora/zimage_lora_run03_$(date +%F)/" \
  --verify 'gsutil -q stat gs://wisent-images-bucket/Jakubs-lora/zimage_lora_run03_*/checkpoints/zimage_lora_run03.safetensors' \
  "cd ai-toolkit && python run.py /opt/zimage-lora/configs/run.yaml"
```

Or, collapsed via a profile (see `wc profiles` below):

```bash
wcomp submit --profile ai_toolkit_zimage \
  --output-uri "gs://wisent-images-bucket/Jakubs-lora/zimage_lora_run03_$(date +%F)/" \
  "cd ai-toolkit && python run.py /opt/zimage-lora/configs/run.yaml"
```

## `wc profiles`

| Subcommand | Behavior |
|---|---|
| `wc profiles` | List available profiles with one-line descriptions. |
| `wc profiles NAME` | Print the profile's resolved JSON. |

A profile is a JSON file under `wisent_compute/profiles/` (bundled with
the wheel) or `$WC_PROFILES_DIR/` (operator-local override). It bundles
the `wc submit` flags for a recurring workflow — `gpu_type`, `vram_gb`,
`apt`, `pre_command`, `repo`, `repo_workdir`, `repo_extras`,
`output_uri`, `verify`, `priority`, `spot`, `max_cost_per_hour`,
`provider`, `pin_provider`. Every field is optional.

Discovery order:

1. `$WC_PROFILES_DIR/<name>.json` — operator-local; first hit wins.
2. `wisent_compute/profiles/<name>.json` — bundled with the package.

**Merge rule:** CLI flags always win. A kwarg that equals the
wisent-compute default (empty string / 0 / False / [] / "train" for
`repo_extras`) counts as "unspecified by CLI" and adopts the profile's
value. To override a profile field, pass the explicit flag.

**Bundled profiles:**

| Profile | What it sets up |
|---|---|
| `ai_toolkit_zimage` | Z-Image Turbo LoRA training via Ostris ai-toolkit. L4 (22 GB request), apt deps for cv2 + git-lfs + build tools, cu128/cu129 cuBLAS `LD_LIBRARY_PATH` fix as `pre_command`, ai-toolkit repo clone. |

To add a new bundled profile: drop a JSON file in `wisent_compute/profiles/` and bump the package version. To add an operator-local profile without a release: `WC_PROFILES_DIR=/path/to/profiles wcomp submit --profile mything ...`.

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
