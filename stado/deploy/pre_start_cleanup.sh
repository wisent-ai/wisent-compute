#!/bin/bash
# Pre-start disk cleanup, invoked from wisent-agent.service ExecStartPre.
#
# This script lives in the wisent-compute package and is copied to
# $HOME/wisent_pre_start_cleanup.sh by install.sh. The systemd unit
# invokes the $HOME path so future pip-upgrades can refresh the
# cleanup logic without ever needing to re-render the .service file.
#
# Targets (only when home free disk is below the gate threshold):
#   - HF model cache: ~/.cache/huggingface/hub
#   - HF datasets cache: ~/.cache/huggingface/datasets
#   - pip wheel cache: ~/.cache/pip
#   - wisent local cache: ~/.wisent_cache
#   - apt archive cache: /var/cache/apt/archives (sudo only)
#   - stale wisent_* / wisent-* training dirs under $HOME whose newest
#     file mtime is older than 60 minutes (mtime proves no active writer)
#
# The script exits 0 unconditionally so a partial failure can't block
# the agent unit from starting. Failures are visible in stdout/stderr
# which the unit routes to /var/log/wisent-agent.log.

set +e  # never fail the unit on cleanup error

THRESHOLD_GB="${WISENT_CLEANUP_THRESHOLD_GB:-30}"

home="${HOME:-/home/ubuntu}"
avail_gb=$(/bin/df -BG --output=avail "$home" 2>/dev/null | /usr/bin/tail -1 | /usr/bin/tr -dc 0-9)

if [ -z "$avail_gb" ]; then
    echo "[pre_start_cleanup] cannot read disk free; skipping"
    exit 0
fi

if [ "$avail_gb" -ge "$THRESHOLD_GB" ]; then
    echo "[pre_start_cleanup] $home free=${avail_gb}G >= ${THRESHOLD_GB}G; no cleanup needed"
    exit 0
fi

echo "[pre_start_cleanup] $home free=${avail_gb}G < ${THRESHOLD_GB}G; running eviction"

before=$avail_gb

# 1. Standard reproducible caches.
for tgt in \
    "$home/.cache/huggingface/hub" \
    "$home/.cache/huggingface/datasets" \
    "$home/.cache/pip" \
    "$home/.wisent_cache"; do
    if [ -d "$tgt" ]; then
        sz=$(/usr/bin/du -sh "$tgt" 2>/dev/null | /usr/bin/cut -f1)
        /bin/rm -rf "$tgt" 2>/dev/null
        echo "[pre_start_cleanup] removed $tgt (was ${sz})"
    fi
done

# 2. apt cache (needs sudo; pre-passwordless or no-op).
if [ -d /var/cache/apt/archives ]; then
    sudo -n /bin/rm -rf /var/cache/apt/archives/*.deb 2>/dev/null && \
        echo "[pre_start_cleanup] cleared apt archives"
fi

# 3. Stale wisent_* / wisent-* training output dirs under $HOME.
#    mtime > 60 min proves no active writer is holding the directory.
/usr/bin/find "$home" -maxdepth 1 -type d \( -name "wisent_*" -o -name "wisent-*" \) -mmin +60 2>/dev/null | while read -r d; do
    # Defensive: never rm $home itself or anything outside it
    case "$d" in
        "$home"|"$home/") continue ;;
        "$home"/*) ;;
        *) continue ;;
    esac
    sz=$(/usr/bin/du -sh "$d" 2>/dev/null | /usr/bin/cut -f1)
    /bin/rm -rf "$d" 2>/dev/null
    echo "[pre_start_cleanup] removed stale training dir $d (was ${sz})"
done

after_gb=$(/bin/df -BG --output=avail "$home" 2>/dev/null | /usr/bin/tail -1 | /usr/bin/tr -dc 0-9)
freed=$(( (after_gb - before) ))
echo "[pre_start_cleanup] freed ${freed}G ($home now has ${after_gb}G free)"
exit 0
