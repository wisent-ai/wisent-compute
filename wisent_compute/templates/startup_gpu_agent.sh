#!/bin/bash
# GCE startup template: install wisent-compute, start the agent in idle-shutdown
# mode. The agent reads its own VRAM via nvidia-smi and packs as many queued
# jobs as fit — no constant slot count. Self-deletes the VM when the queue
# stops yielding eligible work.
set -euxo pipefail
exec > /var/log/wisent-agent.log 2>&1

echo "Wisent agent VM start: $(date -u)"

# Ship the agent's own stdout+stderr to GCS continuously. /var/log/
# wisent-agent.log is VM-local; before this, an agent crash was
# invisible: the reaper deletes the VM (and this log) when capacity +
# job heartbeat go stale, and only the training subprocess's
# command_output.log survived (synced by the agent itself, which is
# gone once it crashes). That is why agent deaths never surfaced an
# error. Now: a background loop mirrors this log to
# gs://wisent-compute/agent_logs/<instance>.log every 20s, and an EXIT
# trap does a final flush so the crash traceback is captured even on
# abnormal exit. Best-effort: never let logging failure abort the VM.
_WC_INST="$(curl -s -H 'Metadata-Flavor: Google' \
  'http://metadata.google.internal/computeMetadata/v1/instance/name' \
  2>/dev/null || hostname)"
# CRITICAL: this function and the shipper loop MUST run with xtrace
# off. Line 6 sets `set -euxo pipefail` (xtrace global). Without the
# `local -; set +x` here and the `set +x` in the subshell below, the
# 20s shipper loop emits `+ _wc_ship_log / + gsutil -q cp ... /
# + sleep 20 / + true` into /var/log/wisent-agent.log — the very file
# it ships — every 20s. Over a multi-hour agent life that is tens of
# thousands of xtrace lines that bury the real agent stdout/stderr
# (job claims, training progress, death tracebacks): the exact signal
# this shipper exists to surface. `local -` scopes the set change to
# the function so the main agent path keeps xtrace; the subshell
# `set +x` silences the loop's while/sleep/true.
_wc_ship_log() {
  local -
  set +x
  gsutil -q cp /var/log/wisent-agent.log \
    "gs://wisent-compute/agent_logs/${_WC_INST}.log" 2>/dev/null \
  || gcloud storage cp /var/log/wisent-agent.log \
    "gs://wisent-compute/agent_logs/${_WC_INST}.log" 2>/dev/null || true
}
trap '_wc_ship_log' EXIT
( set +x; while true; do _wc_ship_log; sleep 20; done ) &

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
    pip install --upgrade --force-reinstall 'datasets>=2.18,<3.0' 'huggingface-hub>=0.34.0,<1.0'
    if pip show hf-xet >/dev/null 2>&1; then pip uninstall -y hf-xet; fi
else
    echo "wisent-agent venv already present (baked image); skipping install"
    cd /opt/wisent-agent
    source .venv/bin/activate
    # Self-update wisent-compute + wisent + wisent-tools to the latest PyPI.
    # Critical: re-pin transformers and datasets to the same versions the
    # bake used. Without this, pip's resolver upgrades datasets to 3.x
    # (which dropped dataset-loading scripts) when wisent's deps loosen,
    # then every script-loaded task (flores.py, gsm8k forks, basque_bench,
    # Hennara/aexams) crashes with 'Dataset scripts are no longer supported'.
    # Same for transformers 5.x's incompatible safetensors shard handling.
    pip install --upgrade wisent-compute wisent wisent-tools wisent-extractors wisent-evaluators
    pip install --force-reinstall 'transformers>=4.55,<5.0' 'tokenizers>=0.20,<0.22'
    pip install --force-reinstall 'datasets>=2.18,<3.0' 'huggingface-hub>=0.34.0,<1.0'
fi

export HF_TOKEN="${HF_TOKEN}"
export HUGGING_FACE_HUB_TOKEN="${HF_TOKEN}"
# Supabase Management API token so wisent-tools extract_and_upload can
# write Activation rows to the Wisent App project after each per-strategy
# HF upload. Without it the post-extraction supabase write step logs a
# skip message and the activation->pair mapping stays unpopulated.
# Coordinator-provided token. The placeholder is substituted by
# dispatch_agent_vms when the mac-mini env has SUPABASE_ACCESS_TOKEN set
# in the coordinator secrets dict. When unsubstituted (literal text
# remains), bash empty-default substitution keeps it empty so set -u
# does not crash; the post-extraction supabase write step then exits
# early when the token is empty.
export SUPABASE_ACCESS_TOKEN="${WC_SUPABASE_TOKEN:-}"
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
# Hard-fail on download error: a VM that can't fetch these will fail every
# claimed job anyway, so fail fast at startup and let the VM recycle.
huggingface-cli download cross-encoder/nli-deberta-v3-small
huggingface-cli download sentence-transformers/all-MiniLM-L6-v2

# Run the agent. --idle-shutdown makes it exit + self-delete the VM when the
# queue holds nothing this VM can run. No timer, no slot constant — pure
# condition-driven on (slots empty AND no eligible queued job).
.venv/bin/wc agent --kind gcp --gpu-type "${ACCEL_TYPE}" --idle-shutdown
EXIT=$?
echo "Agent exited with $EXIT at $(date -u)"

# Always recycle this VM after the agent exits, regardless of status. The
# agent's own self_terminate() (called from the idle-shutdown path and
# from the cloud-agent drift handler in version_check.maybe_drain_or_upgrade)
# normally handles delete cleanly, but if the agent crashed before reaching
# either path, the VM would stay RUNNING forever as a zombie — that's the
# exact failure pattern that produced 1778695548-{2,3,5} on 2026-05-13:
# capacity blob published once at boot, agent silent thereafter, VM still
# alive. By force-deleting here we make zombification structurally
# impossible: any path that ends the script ends the VM. The dispatcher
# creates fresh VMs from the latest PyPI version, so this is also the
# upgrade path for cloud agents (drift detected -> agent exits -> VM
# deleted -> dispatcher launches fresh VM with new code).
INSTANCE_NAME=$(curl -fs -H "Metadata-Flavor: Google" http://metadata.google.internal/computeMetadata/v1/instance/name)
INSTANCE_ZONE=$(curl -fs -H "Metadata-Flavor: Google" http://metadata.google.internal/computeMetadata/v1/instance/zone | awk -F/ '{print $NF}')
echo "Force-deleting self: $INSTANCE_NAME in $INSTANCE_ZONE (exit was $EXIT)"
gcloud compute instances delete "$INSTANCE_NAME" --zone="$INSTANCE_ZONE" --quiet &
exit $EXIT
