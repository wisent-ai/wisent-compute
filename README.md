# wisent-compute

Job queue and compute management for Wisent GPU workloads.

`wisent-compute` runs a fleet of GPU workers — a long-lived workstation
("local agent") plus an auto-scaling pool of GCE Spot VMs ("cloud agents")
— against a single GCS-backed job queue. A Cloud Function tick picks
queued jobs, dispatches agent VMs sized for the work, and the agents
multi-tenant by VRAM. Includes priority queues, per-accelerator zone
rotation, HF pair-text caching, cost-aware dispatch, and a
condition-driven idle-shutdown for cloud VMs.

## Install

```bash
pip install wisent-compute
```

The package installs a `wc` CLI plus the `wisent_compute` Python
package. Cloud Function code lives at
`wisent_compute/cloud_function/main.py`.

## Quick start

```bash
# 1. Submit a single job (any GPU consumer with capacity will claim it)
wc submit "python -m wisent.scripts.activations.extract_and_upload \
  --task gsm8k --model 'meta-llama/Llama-3.2-1B-Instruct' \
  --device cuda --layers all --limit 32"

# 2. Submit a batch (one command per line)
wc submit --batch jobs.txt --spot --max-cost-per-hour 4.00 ''

# 3. Watch progress
wc status

# 4. Pull results from GCS once a job completes
wc results <job_id> ./out/

# 5. Run the local agent on a workstation (polls queue, claims jobs that
#    fit in nvidia-smi-detected VRAM)
wc agent --auto

# 6. Run a one-shot scheduling tick locally instead of the Cloud Function
wc coordinator --once
```

## Documentation

- [`docs/cli.md`](docs/cli.md) — full CLI reference (`wc submit`, `wc agent`, `wc coordinator`, `wc registry`, `wc cost`, `wc bootstrap`).
- [`docs/architecture.md`](docs/architecture.md) — data flow, scheduling rules, cloud-agent VM lifecycle, the GCS layout (`queue/`, `running/`, `completed/`, `failed/`, `capacity/`).
- [`docs/configuration.md`](docs/configuration.md) — every `WC_*` / `GCP_*` env var, the registry schema, the live-quota + reservation overlay, GCP one-time setup.
- [`docs/operations.md`](docs/operations.md) — common operator queries (failure breakdowns, fleet inspection, log paths) and release/publishing flow.

## Project layout

```
wisent_compute/
  cli.py                              # `wc` Click entry points
  config.py                           # GCP_PROJECT, ZONE_ROTATION, MACHINE_TYPE_ZONES, ...
  models.py                           # Job dataclass, GPU_SIZING, GPU_HOURLY_RATE_USD, SPOT_DISCOUNT
  queue/                              # GCS read/write, parallel list_jobs, capacity broadcasts
  scheduler/                          # tick body, live-quota, cost projector, agent dispatcher
  providers/                          # GCP provider + local agent + GCE self-terminate
  templates/                          # startup scripts (gpu_agent, gpu, cpu)
  cloud_function/                     # `monitor_jobs` HTTP entry point
  deploy/                             # `wc bootstrap` SSH+systemd installer
  monitor/                            # alert sinks (Slack/Telegram/SendGrid)
  targets/                            # ComputeTarget, registry.example.json
deploy/
  gcp_setup.sh                        # one-time bootstrap (SA, bucket, topic, IAM)
  redeploy_function.sh                # CI-friendly redeploy (just the Cloud Function)
.github/workflows/
  deploy.yml                          # push-to-main → redeploy CF
  registry-bootstrap.yml              # registry.json edits → SSH-install agents
```

## Contributing

Issues and PRs welcome. The repo has no test runner today — we rely on
the existing live deployment for integration testing and direct
inspection of `gs://$WC_BUCKET/{queue,running,completed,failed}/` for
correctness.

For a release: bump the version in `pyproject.toml`, commit, push to
`main`. CI redeploys the Cloud Function. PyPI publishing is currently
manual (`python -m build && twine upload dist/wisent_compute-*`).

## License

Apache-2.0. See [`LICENSE`](LICENSE).
