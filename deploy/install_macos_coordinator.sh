#!/bin/bash
# Idempotent installer for the wisent-compute coordinator on the user's mac
# mini. Invoked by .github/workflows/deploy-coordinator-mac-mini.yml after
# the workflow has SSH'd into the mini over Tailscale. Re-running this is
# safe — it boots out the existing LaunchAgent, refreshes the plist, and
# bootstraps a fresh launchd handle.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LABEL="com.wisent.compute.coordinator"
PLIST="$HOME/Library/LaunchAgents/${LABEL}.plist"
LOG_DIR="$HOME/Library/Logs"
LOG_OUT="$LOG_DIR/wisent-compute-coordinator.out"
LOG_ERR="$LOG_DIR/wisent-compute-coordinator.err"
ADC_PATH="$HOME/.config/gcloud/application_default_credentials.json"

# Pick the newest Python ≥3.10 we can find. wisent-compute pyproject.toml
# requires-python = ">=3.10" so Apple's bundled /usr/bin/python3 (3.9 on
# macOS 26) is too old. Mac mini install path: /opt/homebrew/bin/python3.12.
PY=""
for cand in /opt/homebrew/bin/python3.13 /opt/homebrew/bin/python3.12 \
            /opt/homebrew/bin/python3.11 /opt/homebrew/bin/python3.10 \
            /usr/local/bin/python3.13 /usr/local/bin/python3.12 \
            /usr/local/bin/python3.11 /usr/local/bin/python3.10; do
    if [ -x "$cand" ]; then PY="$cand"; break; fi
done
if [ -z "$PY" ]; then
    PY=$(command -v python3 || true)
fi
if [ -z "$PY" ]; then
    echo "FATAL: no python3 found on $(hostname)" >&2
    exit 2
fi
PY_VER=$("$PY" -c 'import sys; print("%d.%d" % sys.version_info[:2])')
case "$PY_VER" in
    3.10|3.11|3.12|3.13) ;;
    *) echo "FATAL: $PY is Python $PY_VER but wisent-compute needs >=3.10" >&2; exit 2 ;;
esac
echo "Using Python: $PY ($PY_VER)"

if [ ! -f "$ADC_PATH" ]; then
    echo "FATAL: missing GCS application-default credentials at $ADC_PATH." >&2
    echo "The deploy step must scp the service-account JSON there before invoking this script." >&2
    exit 3
fi

# wisent-compute's queue/storage.py shells out to `gsutil` for ls/cat/cp/rm
# against gs://wisent-compute. Install Google Cloud SDK if it is not on PATH.
if ! command -v gsutil >/dev/null 2>&1; then
    BREW_BIN=""
    for cand in /opt/homebrew/bin/brew /usr/local/bin/brew; do
        if [ -x "$cand" ]; then BREW_BIN="$cand"; break; fi
    done
    if [ -z "$BREW_BIN" ]; then
        echo "FATAL: gsutil missing and no Homebrew install on this host." >&2
        exit 6
    fi
    echo "Installing google-cloud-sdk via Homebrew (this brings in gsutil + gcloud)"
    "$BREW_BIN" install --cask google-cloud-sdk
fi
GCLOUD_BIN_DIR=""
for cand in /opt/homebrew/share/google-cloud-sdk/bin /usr/local/share/google-cloud-sdk/bin \
            /opt/homebrew/Caskroom/google-cloud-sdk/latest/google-cloud-sdk/bin; do
    if [ -d "$cand" ]; then GCLOUD_BIN_DIR="$cand"; break; fi
done
if [ -z "$GCLOUD_BIN_DIR" ]; then
    echo "FATAL: cannot locate google-cloud-sdk bin/ after install." >&2
    exit 7
fi
echo "Using google-cloud-sdk bin: $GCLOUD_BIN_DIR"

cd "$REPO_ROOT"

# Use a dedicated venv to side-step PEP 668's externally-managed-environment
# guard on Homebrew Python installs. The venv lives at ~/.venvs/wisent-compute
# and is recreated only if missing, so re-deploys are fast.
VENV="$HOME/.venvs/wisent-compute"
if [ ! -d "$VENV" ]; then
    "$PY" -m venv "$VENV"
fi
"$VENV/bin/python" -m pip install --upgrade pip
"$VENV/bin/python" -m pip install -e .

WC_BIN="$VENV/bin/wc"
if [ ! -x "$WC_BIN" ]; then
    echo "FATAL: wc binary not found at $WC_BIN after pip install -e ." >&2
    exit 4
fi

mkdir -p "$LOG_DIR" "$HOME/Library/LaunchAgents"

# Compose the LaunchAgent plist. KeepAlive on Crashed=true so the daemon
# self-revives if it dies; SuccessfulExit=false means a clean exit (e.g.
# launchctl bootout) will not respawn it.
cat > "$PLIST" <<PLISTEOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>${LABEL}</string>
    <key>ProgramArguments</key>
    <array>
        <string>${WC_BIN}</string>
        <string>coordinator</string>
        <string>--target</string>
        <string>local-mac</string>
    </array>
    <key>EnvironmentVariables</key>
    <dict>
        <key>GOOGLE_APPLICATION_CREDENTIALS</key>
        <string>${ADC_PATH}</string>
        <key>GOOGLE_CLOUD_PROJECT</key>
        <string>wisent-480400</string>
        <key>WC_BUCKET</key>
        <string>wisent-compute</string>
        <key>HOME</key>
        <string>${HOME}</string>
        <key>PATH</key>
        <string>${GCLOUD_BIN_DIR}:/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:${VENV}/bin</string>
    </dict>
    <key>RunAtLoad</key>
    <true/>
    <key>KeepAlive</key>
    <dict>
        <key>SuccessfulExit</key>
        <false/>
        <key>Crashed</key>
        <true/>
    </dict>
    <key>StandardOutPath</key>
    <string>${LOG_OUT}</string>
    <key>StandardErrorPath</key>
    <string>${LOG_ERR}</string>
    <key>WorkingDirectory</key>
    <string>${REPO_ROOT}</string>
    <key>ProcessType</key>
    <string>Background</string>
</dict>
</plist>
PLISTEOF

GUI_DOMAIN="gui/$(id -u)"

# Reload the LaunchAgent. `launchctl bootstrap` after `bootout` sometimes
# returns 5: Input/output error on macOS 26 because the prior process is
# still tearing down. Fall back to the legacy unload/load pair which
# handles that race more gracefully.
reload_launchagent() {
    local plist_path="$1"; local lbl="$2"
    if launchctl bootstrap "${GUI_DOMAIN}" "$plist_path" 2>/dev/null; then return 0; fi
    launchctl unload "$plist_path" 2>/dev/null || true
    launchctl load "$plist_path"
}
launchctl bootout "${GUI_DOMAIN}/${LABEL}" 2>/dev/null || true
reload_launchagent "$PLIST" "$LABEL"
launchctl enable "${GUI_DOMAIN}/${LABEL}" 2>/dev/null || true
launchctl kickstart -k "${GUI_DOMAIN}/${LABEL}" 2>/dev/null || true

# Verify launchd has the service registered. `print` returns the live state.
if ! launchctl print "${GUI_DOMAIN}/${LABEL}" >/dev/null 2>&1; then
    echo "FATAL: launchctl did not register ${LABEL} after bootstrap" >&2
    exit 5
fi

PID=$(launchctl print "${GUI_DOMAIN}/${LABEL}" 2>/dev/null | awk '/^\s*pid =/ {print $3; exit}')
echo "wisent-compute-coordinator installed and running:"
echo "  label:      ${LABEL}"
echo "  pid:        ${PID:-?}"
echo "  wc binary:  ${WC_BIN}"
echo "  plist:      ${PLIST}"
echo "  stdout log: ${LOG_OUT}"
echo "  stderr log: ${LOG_ERR}"

# === Auto-deployer LaunchAgent ===
# Polls origin/main every 60 s and re-runs this installer when there are
# new commits. Skipped silently if the github PAT is missing — operator
# can drop one in later and the next ssh-driven install will set it up.
PAT_FILE="$HOME/.config/wisent/github_pat"
DEPLOY_REPO_DIR="$HOME/wisent-compute-deploy"
AD_LABEL="com.wisent.compute.auto-deployer"
AD_PLIST="$HOME/Library/LaunchAgents/${AD_LABEL}.plist"
AD_LOG_OUT="$LOG_DIR/wisent-compute-auto-deployer.out"
AD_LOG_ERR="$LOG_DIR/wisent-compute-auto-deployer.err"

if [ ! -f "$PAT_FILE" ]; then
    echo "Auto-deployer SKIPPED: $PAT_FILE missing. Bootstrap is incomplete." >&2
else
    if [ ! -d "$DEPLOY_REPO_DIR/.git" ]; then
        rm -rf "$DEPLOY_REPO_DIR"
        PAT_VAL=$(cat "$PAT_FILE")
        git clone --quiet \
            "https://x-access-token:${PAT_VAL}@github.com/wisent-ai/wisent-compute.git" \
            "$DEPLOY_REPO_DIR"
        unset PAT_VAL
    fi

    AD_TICK="$DEPLOY_REPO_DIR/deploy/auto_deploy_tick.sh"
    if [ ! -f "$AD_TICK" ]; then
        echo "FATAL: $AD_TICK missing in clone; auto-deployer cannot start" >&2
        exit 9
    fi
    chmod +x "$AD_TICK"

    cat > "$AD_PLIST" <<ADPLISTEOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>${AD_LABEL}</string>
    <key>ProgramArguments</key>
    <array>
        <string>/bin/bash</string>
        <string>${AD_TICK}</string>
    </array>
    <key>StartInterval</key>
    <integer>60</integer>
    <key>RunAtLoad</key>
    <true/>
    <key>StandardOutPath</key>
    <string>${AD_LOG_OUT}</string>
    <key>StandardErrorPath</key>
    <string>${AD_LOG_ERR}</string>
    <key>WorkingDirectory</key>
    <string>${DEPLOY_REPO_DIR}</string>
    <key>EnvironmentVariables</key>
    <dict>
        <key>HOME</key>
        <string>${HOME}</string>
        <key>PATH</key>
        <string>/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin</string>
        <key>REPO_DIR</key>
        <string>${DEPLOY_REPO_DIR}</string>
    </dict>
</dict>
</plist>
ADPLISTEOF

    # Self-eviction guard: when this script runs from inside the
    # auto-deployer's tick (the common case), `launchctl bootout` of
    # AD_LABEL kills our own process tree. The matching bootstrap
    # never runs, the agent unregisters, and no future tick fires.
    # Detect that case via XPC_SERVICE_NAME (set by launchd for the
    # current job) and, when self, only refresh the plist on disk —
    # the running agent keeps running with its current ProgramArguments
    # until the next reboot or an external bootstrap.
    if [ "${XPC_SERVICE_NAME:-}" = "${AD_LABEL}" ]; then
        echo "wisent-compute-auto-deployer plist refreshed (self-tick, skipping reload)"
    else
        launchctl bootout "${GUI_DOMAIN}/${AD_LABEL}" 2>/dev/null || true
        reload_launchagent "$AD_PLIST" "$AD_LABEL"
        launchctl enable "${GUI_DOMAIN}/${AD_LABEL}" 2>/dev/null || true
        echo "wisent-compute-auto-deployer installed:"
        echo "  label:      ${AD_LABEL}"
        echo "  tick:       ${AD_TICK}"
        echo "  repo:       ${DEPLOY_REPO_DIR}"
        echo "  interval:   60s"
        echo "  stdout log: ${AD_LOG_OUT}"
        echo "  stderr log: ${AD_LOG_ERR}"
    fi
fi

# === Dashboard LaunchAgent ===
DASH_LABEL="com.wisent.compute.dashboard"
DASH_PLIST="$HOME/Library/LaunchAgents/${DASH_LABEL}.plist"
DASH_LOG_OUT="$LOG_DIR/wisent-compute-dashboard.out"
DASH_LOG_ERR="$LOG_DIR/wisent-compute-dashboard.err"
DASH_BIND="${WC_DASHBOARD_BIND:-0.0.0.0}"
DASH_PORT="${WC_DASHBOARD_PORT:-8765}"

cat > "$DASH_PLIST" <<DASHPLISTEOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>${DASH_LABEL}</string>
    <key>ProgramArguments</key>
    <array>
        <string>${WC_BIN}</string>
        <string>dashboard</string>
        <string>--bind</string>
        <string>${DASH_BIND}</string>
        <string>--port</string>
        <string>${DASH_PORT}</string>
    </array>
    <key>EnvironmentVariables</key>
    <dict>
        <key>GOOGLE_APPLICATION_CREDENTIALS</key>
        <string>${ADC_PATH}</string>
        <key>GOOGLE_CLOUD_PROJECT</key>
        <string>wisent-480400</string>
        <key>WC_BUCKET</key>
        <string>wisent-compute</string>
        <key>HOME</key>
        <string>${HOME}</string>
        <key>PATH</key>
        <string>${GCLOUD_BIN_DIR}:/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:${VENV}/bin</string>
    </dict>
    <key>RunAtLoad</key>
    <true/>
    <key>KeepAlive</key>
    <dict>
        <key>SuccessfulExit</key>
        <false/>
        <key>Crashed</key>
        <true/>
    </dict>
    <key>StandardOutPath</key>
    <string>${DASH_LOG_OUT}</string>
    <key>StandardErrorPath</key>
    <string>${DASH_LOG_ERR}</string>
    <key>WorkingDirectory</key>
    <string>${REPO_ROOT}</string>
</dict>
</plist>
DASHPLISTEOF

launchctl bootout "${GUI_DOMAIN}/${DASH_LABEL}" 2>/dev/null || true
reload_launchagent "$DASH_PLIST" "$DASH_LABEL"
launchctl enable "${GUI_DOMAIN}/${DASH_LABEL}" 2>/dev/null || true
launchctl kickstart -k "${GUI_DOMAIN}/${DASH_LABEL}" 2>/dev/null || true

echo "wisent-compute-dashboard installed:"
echo "  label:      ${DASH_LABEL}"
echo "  url:        http://${DASH_BIND}:${DASH_PORT}/"
echo "  stdout log: ${DASH_LOG_OUT}"
echo "  stderr log: ${DASH_LOG_ERR}"

# Dashboard is bound to 0.0.0.0 so any tailnet member can reach it at
# http://<host>.<tailnet>:<port>/ directly. tailscale serve isn't used —
# the App Store Tailscale build does not support 'tailscale serve' on
# macOS, and the direct tailnet hostname:port path is sufficient.
echo "  tailnet:    http://$(hostname -s).tail6443b3.ts.net:${DASH_PORT}/"

# === wisent-enterprise HF refresh LaunchAgent ===
# Periodic walk of wisent-ai/activations on Hugging Face that upserts
# coverage + pair_counts rows into Supabase (read by console.wisent.com
# /api/jobs/hf-coverage and /api/jobs/hf-pair-counts). Lives on the
# mac mini, NOT in GitHub Actions (avoids GHA org-wide allowance and
# shared-IP rate-limit thrashing).
HFR_LABEL="com.wisent.hf-refresh"
HFR_PLIST="$HOME/Library/LaunchAgents/${HFR_LABEL}.plist"
HFR_LOG_OUT="$LOG_DIR/wisent-hf-refresh.out"
HFR_LOG_ERR="$LOG_DIR/wisent-hf-refresh.err"
HFR_REPO_DIR="$HOME/wisent-enterprise"

if [ ! -f "$PAT_FILE" ]; then
    echo "HF refresh SKIPPED: $PAT_FILE missing (needed to clone wisent-enterprise)." >&2
else
    if [ ! -d "$HFR_REPO_DIR/.git" ]; then
        rm -rf "$HFR_REPO_DIR"
        PAT_VAL=$(cat "$PAT_FILE")
        git clone --quiet \
            "https://x-access-token:${PAT_VAL}@github.com/wisent-ai/wisent-enterprise.git" \
            "$HFR_REPO_DIR"
        unset PAT_VAL
    else
        # Keep the clone fresh on every installer re-run; the wrapper
        # also pulls per-fire so this is belt-and-suspenders.
        (cd "$HFR_REPO_DIR" && git fetch --quiet origin main && git reset --quiet --hard origin/main) || true
    fi

    HFR_WRAPPER="$HFR_REPO_DIR/scripts/run-hf-refresh.sh"
    if [ ! -x "$HFR_WRAPPER" ]; then
        echo "FATAL: $HFR_WRAPPER missing or non-executable; HF refresh cannot start" >&2
        exit 10
    fi

    # Read both secrets from the same on-box config dir as $PAT_FILE.
    # gcloud CLI auth on the mac mini is keychain-backed and breaks in
    # non-interactive contexts (errSecInteractionNotAllowed -25308),
    # so we don't try to round-trip through GCP Secret Manager from
    # this script. Operator drops the secret files once; same pattern
    # as github_pat. Files are 0600 by convention.
    HFR_HF_TOKEN_FILE="$HOME/.config/wisent/hf_token"
    HFR_SBP_TOKEN_FILE="$HOME/.config/wisent/supabase_access_token"
    HFR_HF_TOKEN=""
    HFR_SBP_TOKEN=""
    [ -r "$HFR_HF_TOKEN_FILE" ]  && HFR_HF_TOKEN=$(cat "$HFR_HF_TOKEN_FILE")
    [ -r "$HFR_SBP_TOKEN_FILE" ] && HFR_SBP_TOKEN=$(cat "$HFR_SBP_TOKEN_FILE")
    if [ -z "$HFR_HF_TOKEN" ] || [ -z "$HFR_SBP_TOKEN" ]; then
        echo "HF refresh SKIPPED: could not fetch hf-token/supabase-access-token from GCP Secret Manager." >&2
    else
        # Find a node binary that ships with Apple's Homebrew layout.
        NODE_BIN=""
        for cand in /opt/homebrew/bin/node /usr/local/bin/node; do
            if [ -x "$cand" ]; then NODE_BIN="$cand"; break; fi
        done
        if [ -z "$NODE_BIN" ]; then
            echo "FATAL: no node binary found; HF refresh cannot start" >&2
            exit 11
        fi

        cat > "$HFR_PLIST" <<HFRPLISTEOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>${HFR_LABEL}</string>
    <key>ProgramArguments</key>
    <array>
        <string>/bin/bash</string>
        <string>${HFR_WRAPPER}</string>
    </array>
    <key>StartInterval</key>
    <integer>21600</integer>
    <key>RunAtLoad</key>
    <true/>
    <key>StandardOutPath</key>
    <string>${HFR_LOG_OUT}</string>
    <key>StandardErrorPath</key>
    <string>${HFR_LOG_ERR}</string>
    <key>WorkingDirectory</key>
    <string>${HFR_REPO_DIR}</string>
    <key>EnvironmentVariables</key>
    <dict>
        <key>HOME</key>
        <string>${HOME}</string>
        <key>PATH</key>
        <string>/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin</string>
        <key>NODE_BIN</key>
        <string>${NODE_BIN}</string>
        <key>REPO_DIR</key>
        <string>${HFR_REPO_DIR}</string>
        <key>HF_TOKEN</key>
        <string>${HFR_HF_TOKEN}</string>
        <key>SUPABASE_ACCESS_TOKEN</key>
        <string>${HFR_SBP_TOKEN}</string>
    </dict>
</dict>
</plist>
HFRPLISTEOF

        # Restrict the plist file mode so the embedded secrets aren't
        # world-readable.
        chmod 600 "$HFR_PLIST"

        launchctl bootout "${GUI_DOMAIN}/${HFR_LABEL}" 2>/dev/null || true
        reload_launchagent "$HFR_PLIST" "$HFR_LABEL"
        launchctl enable "${GUI_DOMAIN}/${HFR_LABEL}" 2>/dev/null || true
        launchctl kickstart -k "${GUI_DOMAIN}/${HFR_LABEL}" 2>/dev/null || true

        echo "wisent-hf-refresh installed:"
        echo "  label:      ${HFR_LABEL}"
        echo "  wrapper:    ${HFR_WRAPPER}"
        echo "  repo:       ${HFR_REPO_DIR}"
        echo "  interval:   21600s (6h)"
        echo "  stdout log: ${HFR_LOG_OUT}"
        echo "  stderr log: ${HFR_LOG_ERR}"
    fi
fi

# === Install status beacon ===
# Write a JSON beacon to GCS after every install so off-box observers
# can see whether the auto-deployer has actually picked up the latest
# wisent-compute commit and which LaunchAgents got registered. Reads
# the deploy clone's HEAD; LaunchAgent presence is a plain file-exists
# check.
HOST_SHORT=$(hostname -s)
DEPLOY_HEAD=$(cd "$DEPLOY_REPO_DIR" 2>/dev/null && git rev-parse --short HEAD 2>/dev/null || echo "?")
HFR_REPO_HEAD=$(cd "$HFR_REPO_DIR" 2>/dev/null && git rev-parse --short HEAD 2>/dev/null || echo "?")
BEACON_TMP=$(mktemp)
cat > "$BEACON_TMP" <<BEACONEOF
{
  "host": "${HOST_SHORT}",
  "installed_at": "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
  "wisent_compute_head": "${DEPLOY_HEAD}",
  "wisent_enterprise_head": "${HFR_REPO_HEAD}",
  "agents": {
    "coordinator":   "$([ -f \"$PLIST\" ] && echo yes || echo no)",
    "auto_deployer": "$([ -f \"$AD_PLIST\" ] && echo yes || echo no)",
    "dashboard":     "$([ -f \"$DASH_PLIST\" ] && echo yes || echo no)",
    "hf_refresh":    "$([ -f \"$HFR_PLIST\" ] && echo yes || echo no)"
  }
}
BEACONEOF
# wisent-compute already has the google-cloud-storage Python client
# installed in the venv (it's a wisent-compute dep). Use it for the
# beacon write — wisent-compute's storage layer handles ADC and
# doesn't trip the gcloud-CLI keychain interaction issue.
"$VENV/bin/python" -c "
import json, sys
from google.cloud import storage
data = open('$BEACON_TMP').read()
client = storage.Client(project='wisent-480400')
client.bucket('wisent-compute').blob('install_status/${HOST_SHORT}.json').upload_from_string(data, content_type='application/json')
" 2>>"$LOG_DIR/wisent-hf-refresh-install.err" \
    || echo "WARN: could not upload install beacon to GCS" >&2
rm -f "$BEACON_TMP"
