#!/bin/bash
# =============================================================================
# Bake a custom GCE image for the wisent-compute agent.
#
# Why: today every dispatched VM runs a 5-10 minute pip install of
# wisent-compute + transformers + datasets + aux model warm-up before the
# agent claims its first job. Spot preemption restarts compound the waste.
# Per-VM utilization sits at ~12%. Pre-installing the entire stack into a
# GCE image drops boot time to ~30s, raising per-VM utilization headroom.
#
# What: launches a one-shot builder VM, runs install.sh inside it, snapshots
# the boot disk into image family `wisent-agent`. The dispatcher picks up
# the new image via Job.image / Job.image_project defaults in models.py.
#
# Usage: ./deploy/bake_agent_image.sh
# =============================================================================
set -euo pipefail

GCP_PROJECT="${GCP_PROJECT:-wisent-480400}"
GCP_ZONE="${GCP_ZONE:-us-central1-a}"
GCP_BUCKET="${WC_BUCKET:-wisent-compute}"
GCP_SERVICE_ACCOUNT="${GCP_SERVICE_ACCOUNT:-wisent-compute-sa@wisent-480400.iam.gserviceaccount.com}"

BASE_IMAGE_FAMILY="${BASE_IMAGE_FAMILY:-pytorch-2-9-cu129-ubuntu-2204-nvidia-580}"
BASE_IMAGE_PROJECT="${BASE_IMAGE_PROJECT:-deeplearning-platform-release}"
MACHINE_TYPE="${MACHINE_TYPE:-n1-standard-4}"

TIMESTAMP=$(date +%Y%m%d-%H%M%S)
INSTANCE_NAME="wisent-agent-builder-$TIMESTAMP"
IMAGE_NAME="wisent-agent-$TIMESTAMP"
IMAGE_FAMILY="wisent-agent"
STATUS_PREFIX="wisent-agent-image"

HF_TOKEN="${HF_TOKEN:-}"
[ -z "$HF_TOKEN" ] && {
  echo "Set HF_TOKEN env var (needed to pre-warm gated aux models)" >&2
  exit 1
}

echo "=========================================="
echo "Wisent-agent GCE image baker"
echo "  project    $GCP_PROJECT"
echo "  zone       $GCP_ZONE"
echo "  base image $BASE_IMAGE_PROJECT/$BASE_IMAGE_FAMILY"
echo "  builder    $INSTANCE_NAME"
echo "  image      $IMAGE_NAME (family=$IMAGE_FAMILY)"
echo "=========================================="

STARTUP_FILE=$(mktemp)
cat > "$STARTUP_FILE" <<EOF
#!/bin/bash
exec > /var/log/wisent-bake.log 2>&1
set -euxo pipefail

upload_log() { gcloud storage cp /var/log/wisent-bake.log "gs://$GCP_BUCKET/$STATUS_PREFIX/setup.log" || true; }
write_status() { echo -n "\$1" > /tmp/_status; gcloud storage cp /tmp/_status "gs://$GCP_BUCKET/$STATUS_PREFIX/status.txt" || true; }
trap 'upload_log; write_status FAIL' ERR

echo "Bake start: \$(date -u)"
write_status STARTING

while fuser /var/lib/dpkg/lock-frontend >/dev/null 2>&1 || fuser /var/lib/apt/lists/lock >/dev/null 2>&1; do
  echo "Waiting for apt lock..."
  sleep 5
done
apt-get update
apt-get install -y python3-venv python3-pip git ca-certificates curl gnupg

WORK=/opt/wisent-agent
rm -rf "\$WORK"
mkdir -p "\$WORK"
cd "\$WORK"
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install --upgrade wisent-compute wisent wisent-extractors wisent-evaluators wisent-tools \
  lm-eval optuna matplotlib word2number evaluate
pip install --upgrade --force-reinstall 'transformers>=4.55,<5.0' 'tokenizers>=0.20,<0.22'
pip install --upgrade --force-reinstall 'datasets>=2.18,<3.0' 'huggingface-hub>=0.34.0,<1.0'
pip uninstall -y hf-xet || true

export HF_TOKEN="$HF_TOKEN"
huggingface-cli download cross-encoder/nli-deberta-v3-small || true
huggingface-cli download sentence-transformers/all-MiniLM-L6-v2 || true

.venv/bin/python -c "import wisent_compute, wisent, transformers, datasets, lm_eval; print('imports OK')"

chown -R root:root "\$WORK"
chmod -R go+rX "\$WORK"

rm -rf /root/.cache/pip
apt-get clean
rm -rf /var/lib/apt/lists/*

upload_log
write_status COMPLETE
echo "Bake done: \$(date -u)"
EOF

echo
echo "Step 1/5: launching builder $INSTANCE_NAME"
gcloud compute instances create "$INSTANCE_NAME" \
  --project="$GCP_PROJECT" \
  --zone="$GCP_ZONE" \
  --machine-type="$MACHINE_TYPE" \
  --image-family="$BASE_IMAGE_FAMILY" \
  --image-project="$BASE_IMAGE_PROJECT" \
  --boot-disk-size=120GB \
  --boot-disk-type=pd-ssd \
  --scopes=cloud-platform \
  --service-account="$GCP_SERVICE_ACCOUNT" \
  --metadata-from-file=startup-script="$STARTUP_FILE"
rm -f "$STARTUP_FILE"

echo
echo "Step 2/5: waiting for install to complete (poll GCS status)"
gcloud storage rm "gs://$GCP_BUCKET/$STATUS_PREFIX/status.txt" 2>/dev/null || true

DEADLINE=$(( $(date +%s) + 1800 ))
while [ "$(date +%s)" -lt "$DEADLINE" ]; do
  STATUS=$(gcloud storage cat "gs://$GCP_BUCKET/$STATUS_PREFIX/status.txt" 2>/dev/null | tr -d '[:space:]') || STATUS=""
  case "$STATUS" in
    COMPLETE) echo "  install complete"; break ;;
    FAIL)
      echo "  install FAILED — see gs://$GCP_BUCKET/$STATUS_PREFIX/setup.log"
      gcloud storage cat "gs://$GCP_BUCKET/$STATUS_PREFIX/setup.log" | tail -40
      exit 1
      ;;
    *)
      TAIL=$(gcloud storage cat "gs://$GCP_BUCKET/$STATUS_PREFIX/setup.log" 2>/dev/null | tail -1) || TAIL="(no log yet)"
      echo "  status=$STATUS  log=$TAIL"
      sleep 30
      ;;
  esac
done

if [ "$STATUS" != "COMPLETE" ]; then
  echo "ERROR: install did not complete; builder $INSTANCE_NAME left for debugging" >&2
  exit 1
fi

echo
echo "Step 3/5: stopping builder"
gcloud compute instances stop "$INSTANCE_NAME" --zone="$GCP_ZONE" --project="$GCP_PROJECT" --quiet

echo
echo "Step 4/5: snapshotting boot disk into image $IMAGE_NAME"
gcloud compute images create "$IMAGE_NAME" \
  --source-disk="$INSTANCE_NAME" \
  --source-disk-zone="$GCP_ZONE" \
  --project="$GCP_PROJECT" \
  --family="$IMAGE_FAMILY" \
  --description="wisent-compute agent stack pre-installed; built $TIMESTAMP from $BASE_IMAGE_FAMILY"

echo
echo "Step 5/5: deleting builder"
gcloud compute instances delete "$INSTANCE_NAME" --zone="$GCP_ZONE" --project="$GCP_PROJECT" --quiet
gcloud storage rm "gs://$GCP_BUCKET/$STATUS_PREFIX/status.txt" 2>/dev/null || true
gcloud storage rm "gs://$GCP_BUCKET/$STATUS_PREFIX/setup.log" 2>/dev/null || true

echo
echo "=========================================="
echo "Image baked:"
echo "  name    $IMAGE_NAME"
echo "  family  $IMAGE_FAMILY"
echo "  project $GCP_PROJECT"
echo
echo "Update wisent_compute/models.py Job defaults (or pass --image at submit):"
echo "  image         = \"$IMAGE_NAME\""
echo "  image_project = \"$GCP_PROJECT\""
echo "Or use family='$IMAGE_FAMILY' for always-latest."
echo "=========================================="
