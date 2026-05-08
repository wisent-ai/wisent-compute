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

# launchctl bootout exits non-zero if the service is not loaded; absorb that.
launchctl bootout "${GUI_DOMAIN}/${LABEL}" 2>/dev/null || true

launchctl bootstrap "${GUI_DOMAIN}" "$PLIST"
launchctl enable "${GUI_DOMAIN}/${LABEL}"
launchctl kickstart -k "${GUI_DOMAIN}/${LABEL}"

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
