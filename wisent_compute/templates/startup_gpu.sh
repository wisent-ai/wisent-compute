#!/bin/bash
set -euxo pipefail
exec > /var/log/wisent-job.log 2>&1

echo "Wisent GPU startup: job=${JOB_ID} at $(date)"

while fuser /var/lib/dpkg/lock-frontend >/dev/null 2>&1 || fuser /var/lib/apt/lists/lock >/dev/null 2>&1; do
    echo "Waiting for apt lock..."
done

apt-get update
apt-get install -y python3-venv python3-pip git ca-certificates curl gnupg

WORK=/opt/wisent-run
rm -rf $WORK
mkdir -p $WORK
cd $WORK
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
# Install from PyPI to avoid editable-install + namespace-package interaction
# bugs (see commit history: editable finder doesn't map deeply-nested
# subpackages so wisent.core.utils etc. won't resolve under -e installs).
pip install --upgrade wisent wisent-extractors wisent-evaluators wisent-tools \
    lm-eval optuna matplotlib word2number evaluate
pip uninstall -y hf-xet || true

export HF_TOKEN="${HF_TOKEN}"
export HUGGING_FACE_HUB_TOKEN="${HF_TOKEN}"
huggingface-cli download meta-llama/Llama-3.2-1B-Instruct || true
huggingface-cli download cross-encoder/nli-deberta-v3-small || true
huggingface-cli download sentence-transformers/all-MiniLM-L6-v2 || true

mkdir -p /home/ubuntu/output

STATUS_BUCKET="wisent-compute"
echo "RUNNING $(date -u +%Y-%m-%dT%H:%M:%SZ)" | gsutil cp - "gs://${STATUS_BUCKET}/status/${JOB_ID}/status" || true

# Heartbeat every 5 min. Tolerate the case where cron isn't installed on
# the deeplearning-platform image — when crontab is missing, set -e
# would otherwise kill the entire startup script before the eval runs.
(crontab -l 2>/dev/null || true; echo "*/5 * * * * echo RUNNING \$(date -u +\%Y-\%m-\%dT\%H:\%M:\%SZ) | gsutil cp - gs://${STATUS_BUCKET}/status/${JOB_ID}/heartbeat 2>/dev/null") | crontab - || true

export WISENT_DTYPE=auto
export PYTORCH_CUDA_ALLOC_CONF=expandable_segments:True
export PYTHONUNBUFFERED=1
echo "Running: ${COMMAND}"
eval "${COMMAND}" 2>&1 | tee /home/ubuntu/output/command_output.log
EXIT_CODE=$?

crontab -r 2>/dev/null || true

if [ $EXIT_CODE -eq 0 ]; then
    echo "COMPLETED" > /tmp/wisent_job_status
else
    echo "FAILED exit=$EXIT_CODE" > /tmp/wisent_job_status
fi
gsutil cp /tmp/wisent_job_status "gs://${STATUS_BUCKET}/status/${JOB_ID}/status" || true
gsutil -m cp -r /home/ubuntu/output/* "gs://${STATUS_BUCKET}/status/${JOB_ID}/output/" || true

echo "Job finished with exit code $EXIT_CODE at $(date)"
