#!/bin/bash
set -euxo pipefail
exec > /var/log/wisent-job.log 2>&1

echo "Wisent CPU startup: job=${JOB_ID} at $(date)"

while fuser /var/lib/dpkg/lock-frontend >/dev/null 2>&1 || fuser /var/lib/apt/lists/lock >/dev/null 2>&1; do
    echo "Waiting for apt lock..."
done

apt-get update
apt-get install -y python3-venv python3-pip git

python3 -m venv /opt/wisent-venv
source /opt/wisent-venv/bin/activate
pip install --upgrade pip
pip install --no-cache-dir wisent==${WISENT_VERSION} lm-eval optuna

export HF_TOKEN="${HF_TOKEN}"
export HUGGING_FACE_HUB_TOKEN="${HF_TOKEN}"
huggingface-cli login --token "${HF_TOKEN}" --add-to-git-credential || true

mkdir -p /home/ubuntu/output

STATUS_BUCKET="wisent-compute"
echo "RUNNING $(date -u +%Y-%m-%dT%H:%M:%SZ)" | gsutil cp - "gs://${STATUS_BUCKET}/status/${JOB_ID}/status" || true

(crontab -l 2>/dev/null; echo "*/5 * * * * echo RUNNING \$(date -u +\%Y-\%m-\%dT\%H:\%M:\%SZ) | gsutil cp - gs://${STATUS_BUCKET}/status/${JOB_ID}/heartbeat 2>/dev/null") | crontab -

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
