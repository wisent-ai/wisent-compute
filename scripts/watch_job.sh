#!/usr/bin/env bash
# Watch a wisent-compute job until it lands in completed/ or failed/.
# Emits a single state-change line per transition + a terminal report.
#
# Usage: GOOGLE_APPLICATION_CREDENTIALS=... watch_job.sh <job_id>

set -u
JOB="${1:?job_id required}"
PREV=""
while true; do
  LOC=""; STARTED=""; INSTANCE=""; ERR=""
  for p in queue running completed failed; do
    body=$(gsutil cat "gs://wisent-compute/${p}/${JOB}.json" 2>/dev/null)
    if [ -n "$body" ]; then
      LOC="$p"
      STARTED=$(echo "$body" | grep -oE '"started_at": "[^"]*"' | head -1)
      INSTANCE=$(echo "$body" | grep -oE '"instance_ref": "[^"]*"' | head -1)
      ERR=$(echo "$body" | grep -oE '"error": "[^"]*"' | head -1 | head -c 300)
      break
    fi
  done
  CUR="$LOC|$STARTED|$INSTANCE"
  if [ "$CUR" != "$PREV" ]; then
    echo "[$(date -u +%H:%M:%SZ)] $JOB state=$LOC $STARTED $INSTANCE"
    PREV="$CUR"
  fi
  if [ "$LOC" = "completed" ]; then
    HF=$(curl -s -o /dev/null -w "%{http_code}" \
      "https://huggingface.co/wisent-ai/llama-3.2-1b-kantbench-cheap-talk-pd-grpo")
    echo "[$(date -u +%H:%M:%SZ)] $JOB COMPLETED  hf_http=$HF"
    gh issue create -R wisent-ai/wisent-compute \
      --title "[watcher] job ${JOB} COMPLETED  hf_http=${HF}" \
      --body "Job ${JOB} (cheap_talk_pd full fine-tune) ended in completed/ at $(date -u). HF repo HTTP=${HF}. Watcher exit." \
      2>/dev/null || true
    exit 0
  fi
  if [ "$LOC" = "failed" ]; then
    echo "[$(date -u +%H:%M:%SZ)] $JOB FAILED  err_head=$ERR"
    gh issue create -R wisent-ai/wisent-compute \
      --title "[watcher] job ${JOB} FAILED" \
      --body "Job ${JOB} ended in failed/ at $(date -u). Error head: ${ERR}" \
      2>/dev/null || true
    exit 1
  fi
  sleep 60
done
