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

if ! command -v python3 >/dev/null 2>&1; then
    echo "FATAL: python3 not on PATH on $(hostname)" >&2
    exit 2
fi
PY=$(command -v python3)

if [ ! -f "$ADC_PATH" ]; then
    echo "FATAL: missing GCS application-default credentials at $ADC_PATH." >&2
    echo "Run 'gcloud auth application-default login' on this host once before deploy." >&2
    exit 3
fi

cd "$REPO_ROOT"
"$PY" -m pip install --user --upgrade pip
"$PY" -m pip install --user -e .

USER_BASE=$("$PY" -m site --user-base)
WC_BIN="${USER_BASE}/bin/wc"
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
        <key>WC_BUCKET</key>
        <string>wisent-compute</string>
        <key>HOME</key>
        <string>${HOME}</string>
        <key>PATH</key>
        <string>/usr/local/bin:/usr/bin:/bin:${USER_BASE}/bin</string>
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
