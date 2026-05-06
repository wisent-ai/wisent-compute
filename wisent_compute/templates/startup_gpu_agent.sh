#!/bin/bash
# GCE startup template: install wisent-compute, start the agent in idle-shutdown
# mode. The agent reads its own VRAM via nvidia-smi and packs as many queued
# jobs as fit — no constant slot count. Self-deletes the VM when the queue
# stops yielding eligible work.
set -euxo pipefail
exec > /var/log/wisent-agent.log 2>&1

echo "Wisent agent VM start: $(date -u)"

# If /opt/wisent-agent/.venv is already populated by the baked image
# (wisent-agent family, built via deploy/bake_agent_image.sh), skip the
# install path entirely. Otherwise fall through to the legacy install path
# so VMs running on the deeplearning-platform-release base still work.
if [ ! -x /opt/wisent-agent/.venv/bin/wc ]; then
    while fuser /var/lib/dpkg/lock-frontend >/dev/null 2>&1 || fuser /var/lib/apt/lists/lock >/dev/null 2>&1; do
        echo "Waiting for apt lock..."
    done
    apt-get update
    apt-get install -y python3-venv python3-pip git ca-certificates curl gnupg

    WORK=/opt/wisent-agent
    rm -rf "$WORK"
    mkdir -p "$WORK"
    cd "$WORK"
    python3 -m venv .venv
    source .venv/bin/activate
    pip install --upgrade pip
    pip install --upgrade wisent-compute wisent wisent-extractors wisent-evaluators wisent-tools \
        lm-eval optuna matplotlib word2number evaluate
    pip install --upgrade --force-reinstall 'transformers>=4.55,<5.0' 'tokenizers>=0.20,<0.22'
    pip install --upgrade --force-reinstall 'datasets>=3.0,<4.0' 'huggingface-hub>=0.34.0,<1.0'
    pip uninstall -y hf-xet || true
else
    echo "wisent-agent venv already present (baked image); skipping install"
    cd /opt/wisent-agent
    source .venv/bin/activate
    # Self-update wisent-compute + wisent + wisent-tools to the latest PyPI
    # without touching the heavy pinned deps. Cheap (small wheels) and lets
    # us pick up scheduler/extractor changes between image bakes.
    pip install --upgrade wisent-compute wisent wisent-tools wisent-extractors wisent-evaluators
fi

export HF_TOKEN="${HF_TOKEN}"
export HUGGING_FACE_HUB_TOKEN="${HF_TOKEN}"
export WISENT_DTYPE=auto
export PYTORCH_CUDA_ALLOC_CONF=expandable_segments:True
export PYTHONUNBUFFERED=1
export WC_LOCAL_SLOTS=0
# Pin numba threads BEFORE numba is imported. wisent code sets this to 1 in
# 8 modules but the import order on the agent's subprocess path lets numba
# initialise with the system cpu count first, and the later os.environ
# rewrite raises 'Cannot set NUMBA_NUM_THREADS once threads have been
# launched'. Setting it in the agent's own env propagates to subprocesses.
export NUMBA_NUM_THREADS=1
# Slow HF API calls so we don't hit the 1000-requests-per-5min limit when
# 20+ cloud agents all hit ArabicMMLU/etc dataset metadata at once.
export HF_HUB_DOWNLOAD_TIMEOUT=120
# Kill telemetry/analytics pings — they count against the 1000/5min ceiling.
export HF_HUB_DISABLE_TELEMETRY=1
# NOTE: do NOT set HF_HUB_DISABLE_IMPLICIT_TOKEN here. Despite its name, that
# flag disables BOTH the on-disk HfFolder lookup AND the HF_TOKEN env-var
# resolution path inside huggingface_hub. We rely on env-var auth for gated
# meta-llama repos, so leaving it unset (or =0) is the correct setting.
# Confirmed live on 2026-04-30: setting this to 1 caused 49 of last 50
# failures to be GatedRepoError 401 on Llama-2-7b-chat-hf and
# Llama-3.2-1B-Instruct.
# When a file IS present in cache, transformers/huggingface_hub still issues
# a HEAD to refresh the etag and re-validate. With cache-first loading in
# wisent>=0.11.20 this normally won't fire, but if some path still bypasses
# local_files_only we want the etag check to fail fast (1s) and fall back
# to cache rather than block waiting on a rate-limited Hub.
export HF_HUB_ETAG_TIMEOUT=1

# Pre-warm the small auxiliary models so each claimed job skips the download.
huggingface-cli download cross-encoder/nli-deberta-v3-small || true
huggingface-cli download sentence-transformers/all-MiniLM-L6-v2 || true

# Run the agent. --idle-shutdown makes it exit + self-delete the VM when the
# queue holds nothing this VM can run. No timer, no slot constant — pure
# condition-driven on (slots empty AND no eligible queued job).
.venv/bin/wc agent --kind gcp --gpu-type "${ACCEL_TYPE}" --idle-shutdown
EXIT=$?
echo "Agent exited with $EXIT at $(date -u)"
