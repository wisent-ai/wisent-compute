#!/bin/bash
set -euo pipefail

# CI-friendly redeploy: only the steps the wisent-compute-sa is permitted
# to do. For one-time bootstrap (creating the SA, granting project roles,
# creating the bucket and pub/sub topic), use gcp_setup.sh instead.

PROJECT="${GCP_PROJECT:?GCP_PROJECT required}"
REGION="${GCP_REGION:-us-central1}"
BUCKET="wisent-compute"
SA_EMAIL="wisent-compute-sa@${PROJECT}.iam.gserviceaccount.com"
FUNCTION="wisent-compute-tick"
SCHEDULER="wisent-compute-cron"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(dirname "$SCRIPT_DIR")"

echo "=== Redeploying wisent-compute-tick ==="

# 1. Upload quotas (storage.admin)
gsutil cp "$REPO_DIR/config/quotas.json" "gs://${BUCKET}/config/quotas.json"
echo "Quotas refreshed at gs://${BUCKET}/config/quotas.json"

# 2. Stage Cloud Function source
STAGING="$(mktemp -d)"
cp -r "$REPO_DIR/wisent_compute" "$STAGING/"
cp "$REPO_DIR/wisent_compute/cloud_function/main.py" "$STAGING/main.py"
cp "$REPO_DIR/wisent_compute/cloud_function/requirements.txt" "$STAGING/requirements.txt"
cp "$REPO_DIR/pyproject.toml" "$STAGING/"

# 3. Deploy function (cloudfunctions.developer + iam.serviceAccountUser)
gcloud functions deploy "$FUNCTION" \
    --gen2 --runtime=python312 --region="$REGION" \
    --source="$STAGING" \
    --entry-point=monitor_jobs \
    --trigger-http --no-allow-unauthenticated \
    --service-account="$SA_EMAIL" \
    --memory=512Mi --cpu=1 \
    --set-env-vars="GCP_PROJECT=${PROJECT},WC_BUCKET=${BUCKET},WC_ALERTS_TOPIC=projects/${PROJECT}/topics/wisent-compute-alerts,WC_SLACK_WEBHOOK=${WC_SLACK_WEBHOOK:-},WC_TELEGRAM_BOT_TOKEN=${WC_TELEGRAM_BOT_TOKEN:-},WC_TELEGRAM_CHAT_ID=${WC_TELEGRAM_CHAT_ID:-},WC_SENDGRID_API_KEY=${WC_SENDGRID_API_KEY:-},WC_EMAIL_TO=${WC_EMAIL_TO:-},WC_EMAIL_FROM=${WC_EMAIL_FROM:-compute@wisent.ai}" \
    --project="$PROJECT" --quiet

rm -rf "$STAGING"
echo "Cloud Function $FUNCTION deployed"

# 4. Refresh scheduler URI (cloudscheduler.admin)
URL=$(gcloud functions describe "$FUNCTION" --gen2 --region="$REGION" --project="$PROJECT" --format='value(serviceConfig.uri)')
if gcloud scheduler jobs describe "$SCHEDULER" --location="$REGION" --project="$PROJECT" >/dev/null 2>&1; then
    gcloud scheduler jobs update http "$SCHEDULER" --location="$REGION" --project="$PROJECT" \
        --schedule="*/3 * * * *" --uri="$URL" \
        --oidc-service-account-email="$SA_EMAIL" --quiet
    echo "Scheduler $SCHEDULER updated"
else
    gcloud scheduler jobs create http "$SCHEDULER" --location="$REGION" --project="$PROJECT" \
        --schedule="*/3 * * * *" --uri="$URL" \
        --oidc-service-account-email="$SA_EMAIL" --quiet
    echo "Scheduler $SCHEDULER created"
fi

echo "=== Redeploy complete ==="
