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
git clone https://${GH_TOKEN}@github.com/wisent-ai/wisent.git $WORK
cd $WORK
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -e . lm-eval optuna matplotlib word2number evaluate
pip uninstall -y hf-xet || true
ln -sf registry/hf_task_extractors wisent/extractors/hf/hf_task_extractors || true

export HF_TOKEN="${HF_TOKEN}"
export HUGGING_FACE_HUB_TOKEN="${HF_TOKEN}"
huggingface-cli download meta-llama/Llama-3.2-1B-Instruct || true
huggingface-cli download cross-encoder/nli-deberta-v3-small || true
huggingface-cli download sentence-transformers/all-MiniLM-L6-v2 || true

mkdir -p /home/ubuntu/output

STATUS_BUCKET="wisent-compute"
echo "RUNNING $(date -u +%Y-%m-%dT%H:%M:%SZ)" | gsutil cp - "gs://${STATUS_BUCKET}/status/${JOB_ID}/status" || true

# Heartbeat every 5 min
(crontab -l 2>/dev/null; echo "*/5 * * * * echo RUNNING \$(date -u +\%Y-\%m-\%dT\%H:\%M:\%SZ) | gsutil cp - gs://${STATUS_BUCKET}/status/${JOB_ID}/heartbeat 2>/dev/null") | crontab -

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
