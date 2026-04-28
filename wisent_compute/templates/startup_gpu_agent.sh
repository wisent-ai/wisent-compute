#!/bin/bash
# GCE startup template: install wisent-compute, start the agent in idle-shutdown
# mode. The agent reads its own VRAM via nvidia-smi and packs as many queued
# jobs as fit — no constant slot count. Self-deletes the VM when the queue
# stops yielding eligible work.
set -euxo pipefail
exec > /var/log/wisent-agent.log 2>&1

echo "Wisent agent VM start: $(date -u)"

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
# Pin transformers to a 4.x version. transformers 5.6.2 raises
# 'does not appear to have files named (model-00000-of-...)'  with a
# zero-indexed range that mismatches HF's 1-indexed sharded
# safetensors (proven on Qwen/Qwen3-8B and openai/gpt-oss-20b). 4.55.x
# uses range(1, n+1) and loads them correctly.
pip install --upgrade --force-reinstall 'transformers>=4.55,<5.0' 'tokenizers>=0.20,<0.22'
pip uninstall -y hf-xet || true

export HF_TOKEN="${HF_TOKEN}"
export HUGGING_FACE_HUB_TOKEN="${HF_TOKEN}"
export WISENT_DTYPE=auto
export PYTORCH_CUDA_ALLOC_CONF=expandable_segments:True
export PYTHONUNBUFFERED=1
export WC_LOCAL_SLOTS=0

# Pre-warm the small auxiliary models so each claimed job skips the download.
huggingface-cli download cross-encoder/nli-deberta-v3-small || true
huggingface-cli download sentence-transformers/all-MiniLM-L6-v2 || true

# Run the agent. --idle-shutdown makes it exit + self-delete the VM when the
# queue holds nothing this VM can run. No timer, no slot constant — pure
# condition-driven on (slots empty AND no eligible queued job).
.venv/bin/wc agent --gpu-type "${ACCEL_TYPE}" --idle-shutdown
EXIT=$?
echo "Agent exited with $EXIT at $(date -u)"
