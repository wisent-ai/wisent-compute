#!/bin/bash
set -euo pipefail

PROJECT="${GCP_PROJECT:?GCP_PROJECT required}"
REGION="${GCP_REGION:-us-central1}"
BUCKET="wisent-compute"
SA_NAME="wisent-compute-sa"
SA_EMAIL="${SA_NAME}@${PROJECT}.iam.gserviceaccount.com"
FUNCTION="wisent-compute-tick"
SCHEDULER="wisent-compute-cron"
TOPIC="wisent-compute-alerts"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(dirname "$SCRIPT_DIR")"

echo "=== Deploying wisent-compute ==="

# 1. GCS bucket
if ! gsutil ls -b "gs://${BUCKET}" >/dev/null 2>&1; then
    gsutil mb -l "$REGION" "gs://${BUCKET}"
fi
echo "Bucket: gs://${BUCKET}"

# 2. Upload quotas
gsutil cp "$REPO_DIR/config/quotas.json" "gs://${BUCKET}/config/quotas.json"
echo "Quotas uploaded"

# 3. Service account
if ! gcloud iam service-accounts describe "$SA_EMAIL" --project="$PROJECT" >/dev/null 2>&1; then
    gcloud iam service-accounts create "$SA_NAME" --project="$PROJECT" --display-name="Wisent Compute"
fi
for role in roles/compute.admin roles/storage.admin roles/pubsub.publisher roles/secretmanager.secretAccessor; do
    gcloud projects add-iam-policy-binding "$PROJECT" \
        --member="serviceAccount:${SA_EMAIL}" --role="$role" --quiet >/dev/null 2>&1
done
echo "Service account: $SA_EMAIL"

# 4. Pub/Sub
if ! gcloud pubsub topics describe "$TOPIC" --project="$PROJECT" >/dev/null 2>&1; then
    gcloud pubsub topics create "$TOPIC" --project="$PROJECT"
fi
echo "Alerts topic: $TOPIC"

# 5. Secrets
for secret in wisent-hf-token wisent-gh-token; do
    if ! gcloud secrets describe "$secret" --project="$PROJECT" >/dev/null 2>&1; then
        echo "Create secret: echo -n '\$TOKEN' | gcloud secrets create $secret --data-file=- --project=$PROJECT"
    fi
done

# 6. Deploy Cloud Function
# Package the entire wisent_compute module for the function
STAGING=$(mktemp -d)
cp -r "$REPO_DIR/wisent_compute" "$STAGING/"
cp "$REPO_DIR/wisent_compute/cloud_function/main.py" "$STAGING/main.py"
cp "$REPO_DIR/wisent_compute/cloud_function/requirements.txt" "$STAGING/requirements.txt"
cp "$REPO_DIR/pyproject.toml" "$STAGING/"

gcloud functions deploy "$FUNCTION" \
    --gen2 --runtime=python312 --region="$REGION" \
    --source="$STAGING" \
    --entry-point=monitor_jobs \
    --trigger-http --no-allow-unauthenticated \
    --service-account="$SA_EMAIL" \
    --memory=512Mi --cpu=1 \
    --set-env-vars="GCP_PROJECT=${PROJECT},WC_BUCKET=${BUCKET},WC_ALERTS_TOPIC=projects/${PROJECT}/topics/${TOPIC},WC_SLACK_WEBHOOK=${WC_SLACK_WEBHOOK:-},WC_TELEGRAM_BOT_TOKEN=${WC_TELEGRAM_BOT_TOKEN:-},WC_TELEGRAM_CHAT_ID=${WC_TELEGRAM_CHAT_ID:-},WC_SENDGRID_API_KEY=${WC_SENDGRID_API_KEY:-},WC_EMAIL_TO=${WC_EMAIL_TO:-},WC_EMAIL_FROM=${WC_EMAIL_FROM:-compute@example.com}" \
    --project="$PROJECT" --quiet
rm -rf "$STAGING"
echo "Cloud Function deployed"

# 7. Grant invoker
gcloud run services add-iam-policy-binding "$FUNCTION" \
    --region="$REGION" --project="$PROJECT" \
    --member="serviceAccount:${SA_EMAIL}" --role="roles/run.invoker" --quiet >/dev/null

# 8. Cloud Scheduler
URL=$(gcloud functions describe "$FUNCTION" --gen2 --region="$REGION" --project="$PROJECT" --format='value(serviceConfig.uri)')
if gcloud scheduler jobs describe "$SCHEDULER" --location="$REGION" --project="$PROJECT" >/dev/null 2>&1; then
    gcloud scheduler jobs update http "$SCHEDULER" --location="$REGION" --project="$PROJECT" \
        --schedule="*/3 * * * *" --uri="$URL" --oidc-service-account-email="$SA_EMAIL" --quiet
else
    gcloud scheduler jobs create http "$SCHEDULER" --location="$REGION" --project="$PROJECT" \
        --schedule="*/3 * * * *" --uri="$URL" --oidc-service-account-email="$SA_EMAIL" --quiet
fi
echo "Scheduler: $SCHEDULER (every 3 min)"

echo ""
echo "=== wisent-compute deployed ==="
echo "Bucket:    gs://${BUCKET}"
echo "Function:  $FUNCTION"
echo "Scheduler: $SCHEDULER"
echo "Alerts:    $TOPIC"
