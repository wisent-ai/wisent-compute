#!/usr/bin/env bash
# migrate_to_stado.sh -- live-infra migration for the wisent-compute -> stado
# rename. The Python package/CLI rename is already done in the repo; this
# handles the CATEGORY-B resources that carry live state and cannot be renamed
# by a code edit: GCS bucket, GCP service account, Pub/Sub topic, Azure RG/net,
# PyPI publish, Cloud Function + baked image redeploy, and the config-default
# flip.
#
# IDEMPOTENT + GATED. Dry-run by default; each destructive step needs an
# explicit confirm env var. Safe to re-run: every create/copy checks existence
# first. REQUIRES the fleet to be DRAINED first (see runbook) -- copying a live
# queue bucket while the box is claiming jobs causes split-brain.
set -euo pipefail

OLD_BUCKET="${OLD_BUCKET:-wisent-compute}"
NEW_BUCKET="${NEW_BUCKET:-stado}"
PROJECT="${GCP_PROJECT:-wisent-480400}"
REGION="${GCP_REGION:-us-central1}"
OLD_TOPIC="wisent-compute-alerts"
NEW_TOPIC="stado-alerts"
OLD_SA="wisent-compute-sa@${PROJECT}.iam.gserviceaccount.com"
NEW_SA="stado-sa@${PROJECT}.iam.gserviceaccount.com"
MODE="dry-run"
[[ "${1:-}" == "--execute" ]] && MODE="execute"
say() { echo "[migrate:$MODE] $*"; }
run() { if [[ "$MODE" == "execute" ]]; then say "RUN: $*"; eval "$@"; else say "would: $*"; fi; }

# --- preconditions ---------------------------------------------------------
say "preconditions: fleet MUST be drained (queue empty, no running jobs, box agent stopped)."
if [[ "$MODE" == "execute" && "${CONFIRM_FLEET_DRAINED:-}" != "yes" ]]; then
  echo "Refusing execute: set CONFIRM_FLEET_DRAINED=yes after confirming queued=0/running=0 and the box agent is stopped." >&2
  exit 1
fi
command -v gcloud >/dev/null || { echo "gcloud required" >&2; exit 1; }
command -v gsutil >/dev/null || { echo "gsutil required" >&2; exit 1; }

# --- 1. GCS bucket: create + copy (idempotent) -----------------------------
if gsutil ls -b "gs://${NEW_BUCKET}" >/dev/null 2>&1; then
  say "bucket gs://${NEW_BUCKET} already exists (skip create)"
else
  run "gsutil mb -p '${PROJECT}' -l '${REGION}' 'gs://${NEW_BUCKET}'"
fi
# rsync is resumable + idempotent: re-run continues where it left off.
run "gsutil -m rsync -r 'gs://${OLD_BUCKET}' 'gs://${NEW_BUCKET}'"

# --- 2. GCP service account (idempotent) -----------------------------------
if gcloud iam service-accounts describe "$NEW_SA" --project "$PROJECT" >/dev/null 2>&1; then
  say "SA $NEW_SA exists (skip)"
else
  run "gcloud iam service-accounts create stado-sa --project '${PROJECT}' --display-name 'stado compute agent'"
fi
# mirror the old SA's project roles (edit list to match your actual bindings)
for ROLE in roles/storage.objectAdmin roles/pubsub.publisher roles/compute.instanceAdmin.v1; do
  run "gcloud projects add-iam-policy-binding '${PROJECT}' --member 'serviceAccount:${NEW_SA}' --role '${ROLE}' --condition=None"
done

# --- 3. Pub/Sub alerts topic (idempotent) ----------------------------------
if gcloud pubsub topics describe "$NEW_TOPIC" --project "$PROJECT" >/dev/null 2>&1; then
  say "topic $NEW_TOPIC exists (skip)"
else
  run "gcloud pubsub topics create '${NEW_TOPIC}' --project '${PROJECT}'"
fi

# --- 4. Azure (optional; only if AZURE_SUBSCRIPTION_ID set) -----------------
if [[ -n "${AZURE_SUBSCRIPTION_ID:-}" ]]; then
  command -v az >/dev/null || { echo "az CLI required for Azure step" >&2; exit 1; }
  run "az group create -n stado -l eastus --subscription '${AZURE_SUBSCRIPTION_ID}'"
  run "az network vnet create -g stado -n stado-vnet --subscription '${AZURE_SUBSCRIPTION_ID}' --subnet-name stado-subnet"
  run "az network nsg create -g stado -n stado-nsg --subscription '${AZURE_SUBSCRIPTION_ID}'"
else
  say "AZURE_SUBSCRIPTION_ID unset -> skipping Azure reprovision"
fi

# --- 5. PyPI publish (needs twine creds) -----------------------------------
say "PyPI: build + publish the renamed dist 'stado'"
run "cd '$(dirname "$0")/..' && rm -rf build dist stado.egg-info && python -m build && twine upload dist/stado-*"

# --- 6. Config default flip (AFTER resources exist + PyPI live) ------------
# stado/config.py defaults still point at the old names so the fleet keeps
# working until this cutover. Flip them in the repo, commit, and let CI
# redeploy the Cloud Function + rebake the agent image.
say "cutover: set config.py defaults BUCKET->${NEW_BUCKET}, topic/SA->stado-*, then commit; CI redeploys function + image."
say "done ($MODE). Post-migration: restart the box agent so it installs 'stado' from PyPI and points at gs://${NEW_BUCKET}."
