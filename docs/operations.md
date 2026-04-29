# Operations

## Common queries

### Hour-bucketed completion + failure histograms

```bash
gsutil ls -l gs://$WC_BUCKET/completed/ | grep -oE '202[0-9]-[0-9-]+T[0-9]+' \
  | sort | uniq -c
gsutil ls -l gs://$WC_BUCKET/failed/    | grep -oE '202[0-9]-[0-9-]+T[0-9]+' \
  | sort | uniq -c
```

### Latest scheduler decisions

```bash
gcloud functions logs read wisent-compute-tick --gen2 --region=$GCP_REGION \
  --project=$GCP_PROJECT 2>&1 \
  | grep -E "Available slots|Dispatched agent|Skip bucket|Yielding|Cost-optimal"
```

### Live quota usage

```bash
gcloud compute regions describe $GCP_REGION --project=$GCP_PROJECT \
  --format=json | grep -A1 PREEMPTIBLE_NVIDIA
```

### Local agent state

```bash
ssh root@<host> '
  systemctl is-active wisent-agent.service
  journalctl -u wisent-agent.service --since="5 minutes ago" --no-pager -o cat
  ps -eo pid,etime,cmd | grep extract_and_upload | grep -v grep
  nvidia-smi --query-gpu=memory.used,memory.free,utilization.gpu --format=csv,noheader
'
```

### Inspect one job end-to-end

```bash
JID=00255b9b
gsutil cat gs://$WC_BUCKET/{queue,running,completed,failed}/${JID}.json 2>/dev/null
gsutil cat gs://$WC_BUCKET/status/${JID}/output/command_output.log
```

## Failure mode quick-grep

The most common failure modes — search the per-job stdout for one of
these substrings to classify failures fast:

| Substring | Cause |
|---|---|
| `HfHubHTTPError: 429` | HF Hub rate limit (free tier 1000 req / 5 min). Retry path lives in `wisent.core.utils.infra_tools.infra.data.dataset_splits.get_all_docs_from_task` and the cache fast-path in `generate_pairs_from_task.py`. |
| `Couldn't find cache for` | datasets cache miss for a config that doesn't exist on the dataset's HF repo. lm-eval task config drift. |
| `OverflowError: int too big to convert` | tokenizer `model_max_length` was a sentinel (1e30) handed to the rust binding's u32. Capped at 4096 in `activations_collector.py`. |
| `Dataset scripts are no longer supported` | datasets 4.x dropped the script loader. Pinned `datasets<4.0` in the agent template. |
| `huggingface-hub>=0.34.0,<1.0 is required` | transformers 4.55.x dep-pin mismatch. Pinned `huggingface-hub<1.0` in the agent template. |
| `RuntimeError: Cannot set NUMBA_NUM_THREADS` | numba init happened before wisent's env-set. Set `NUMBA_NUM_THREADS=1` in the agent's env BEFORE Python starts. |
| `does not appear to have files named` | transformers shard-name miscompute on `gpt_oss` / 0-indexed safetensors. Fixed in `transformers>=4.57`. |
| `AttributeError: ... has no attribute 'transformer'` | wisent activation hook expected GPT-2 path on a model whose `model_type` contains `gpt`; gpt_oss uses Llama-style. Fixed in `transformer_analysis.py`. |
| `gated repo` / `401 Client Error` | `HF_TOKEN` missing or revoked. Rotate in Secret Manager: `gcloud secrets versions add wisent-hf-token --data-file=-`. |
| `Quota 'PREEMPTIBLE_NVIDIA_*_GPUS' exceeded` | Hit the regional preemptible quota. Either raise via GCP console or add zones to `MACHINE_TYPE_ZONES`. |

## Velocity / load script

A handy one-shot reporter (run from anywhere with ADC):

```python
# /tmp/wisent_velocity.py — see the README's Operations section
# Computes: completions per 5-min bucket, failures per 5-min bucket,
# concurrent in-flight count per bucket, average + peak load, per-model
# pass-rate, plus an ASCII bar chart for the last 24 buckets.
```

`pass-rate (last 60m)` and `pass-rate (last 15m)` are the two numbers
operators watch. Below ~30% sustained signals a regression — pull
`gsutil ls -l gs://$WC_BUCKET/failed/ | sort -k 2 -r | head` and grep
the first few jobs' stdouts to find the new failure class.

## Release / publishing

`wisent-compute` is published manually (no CI publish step yet):

```bash
# 1. bump version
sed -i '' 's/version = "X.Y.Z"/version = "X.Y.Z+1"/' pyproject.toml

# 2. commit + push  (CI redeploys the Cloud Function on push to main)
git add pyproject.toml
git commit -m "release: X.Y.Z+1"
git push origin main

# 3. build + upload the wheel
rm -rf build/ dist/ wisent_compute.egg-info/
python -m build
twine upload dist/wisent_compute-*

# 4. (optional) verify
curl -s https://pypi.org/pypi/wisent-compute/json | grep -oE '"version":"[^"]+"' | head -1
```

The GitHub Action `.github/workflows/deploy.yml` redeploys the Cloud
Function via `deploy/redeploy_function.sh` on every push to `main`.
The cloud agents pull `wisent-compute` and `wisent` via `pip install
--upgrade` in their startup script, so they pick up the new version
on next spawn — no per-VM upgrade needed.

To force the running fleet onto a new release immediately, delete the
running agent VMs (`gcloud compute instances delete ...` — the scheduler
respawns within one tick).

## Bringing up a new local box

```bash
# On the box (or via wc bootstrap from your laptop, see CLI reference):

#  Install package + ADC
pip install --user wisent-compute
gcloud auth application-default login

# Configure registry entry (also pushed to gs:// for cluster-wide visibility)
cat >> wisent_compute/targets/registry.json <<EOF
{"name": "$(hostname)", "kind": "local", "ssh": "user@$(hostname)",
 "gpu_type": "auto", "slots": 0, "vram_gb": 96}
EOF
wc registry push

# Install + enable as systemd
wc bootstrap --local
systemctl --user status wisent-agent.service
```
