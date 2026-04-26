#!/usr/bin/env bash
# One-command onboarding for a new compute target.
#
# Usage (run on the box you want to add to the queue):
#
#   curl -fsSL https://raw.githubusercontent.com/wisent-ai/wisent-compute/main/install.sh | bash
#
# What it does:
#   1. pip install --user --upgrade wisent-compute (from PyPI)
#   2. Drops a launchd plist (Darwin) or systemd --user unit (Linux)
#      that runs `wc agent --auto` so the agent self-starts at boot
#      and self-recovers on crash.
#   3. Loads the service so the agent starts immediately.
#
# After this single command, any change to gs://wisent-compute/registry.json
# propagates to the agent on its next poll cycle. No further manual steps.
#
# Prerequisites the operator still needs once per box:
#   - GOOGLE_APPLICATION_CREDENTIALS pointing at an SA key with read access
#     to gs://wisent-compute/, OR a `gcloud auth application-default login`
#     session for the user the service runs as.
#   - The host's hostname (or the SSH user@host string) appears in
#     wisent_compute/targets/registry.json with kind=local. If absent,
#     the agent will still start but `wc agent --auto` will exit because
#     no entry matches its hostname.

set -euo pipefail

OS="$(uname -s)"
HOSTNAME_S="$(hostname -s 2>/dev/null || hostname)"
echo "[install] OS=${OS} hostname=${HOSTNAME_S}"

if ! command -v python3 >/dev/null 2>&1; then
    echo "[install] ERROR: python3 not found on PATH" >&2
    exit 1
fi

echo "[install] pip install --user --upgrade wisent-compute"
python3 -m pip install --user --upgrade wisent-compute

# Resolve wc binary that pip just installed (handles --user PATH variations).
WC_BIN="$(python3 -c 'import shutil, sys; sys.stdout.write(shutil.which("wc") or "")')"
if [ -z "$WC_BIN" ] || [ "$WC_BIN" = "/usr/bin/wc" ]; then
    for cand in \
        "$HOME/.local/bin/wc" \
        "$HOME/Library/Python/3.12/bin/wc" \
        "$HOME/Library/Python/3.11/bin/wc" \
        "$HOME/Library/Python/3.10/bin/wc" \
        ; do
        if [ -x "$cand" ]; then
            WC_BIN="$cand"
            break
        fi
    done
fi
if [ -z "$WC_BIN" ] || [ ! -x "$WC_BIN" ]; then
    echo "[install] ERROR: could not locate the wc binary after pip install" >&2
    exit 1
fi
echo "[install] wc binary: $WC_BIN"

# Find ADC path so the service inherits credentials. Prefer the user's
# gcloud-managed default; falls back to an explicit GOOGLE_APPLICATION_CREDENTIALS.
ADC_PATH="${GOOGLE_APPLICATION_CREDENTIALS:-}"
if [ -z "$ADC_PATH" ]; then
    for cand in "$HOME/.config/gcloud/legacy_credentials/"*"/adc.json"; do
        if [ -f "$cand" ]; then
            ADC_PATH="$cand"
            break
        fi
    done
fi
if [ -z "$ADC_PATH" ]; then
    echo "[install] WARNING: no Application Default Credentials found"
    echo "[install] WARNING: agent will start but cannot read GCS until ADC is set"
fi

case "$OS" in
    Darwin)
        LABEL="com.wisent.compute.agent"
        PLIST="$HOME/Library/LaunchAgents/${LABEL}.plist"
        mkdir -p "$HOME/Library/LaunchAgents"
        cat > "$PLIST" <<PLIST
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN"
  "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key><string>${LABEL}</string>
    <key>ProgramArguments</key>
    <array>
        <string>${WC_BIN}</string>
        <string>agent</string>
        <string>--auto</string>
    </array>
    <key>RunAtLoad</key><true/>
    <key>KeepAlive</key><true/>
    <key>StandardOutPath</key><string>/tmp/${LABEL}.log</string>
    <key>StandardErrorPath</key><string>/tmp/${LABEL}.log</string>
    <key>EnvironmentVariables</key>
    <dict>
        <key>PYTHONUNBUFFERED</key><string>1</string>
        <key>GOOGLE_APPLICATION_CREDENTIALS</key><string>${ADC_PATH}</string>
    </dict>
</dict>
</plist>
PLIST
        UID_VAL=$(id -u)
        launchctl bootout "gui/${UID_VAL}/${LABEL}" 2>/dev/null || true
        launchctl bootstrap "gui/${UID_VAL}" "$PLIST"
        launchctl kickstart -k "gui/${UID_VAL}/${LABEL}" || true
        echo "[install] launchd job ${LABEL} loaded; logs at /tmp/${LABEL}.log"
        ;;
    Linux)
        UNIT_DIR="$HOME/.config/systemd/user"
        mkdir -p "$UNIT_DIR"
        UNIT="$UNIT_DIR/wisent-compute-agent.service"
        cat > "$UNIT" <<UNIT
[Unit]
Description=Wisent Compute local GPU agent (auto)
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
Environment=PYTHONUNBUFFERED=1
Environment=GOOGLE_APPLICATION_CREDENTIALS=${ADC_PATH}
ExecStart=${WC_BIN} agent --auto
Restart=on-failure
RestartSec=30

[Install]
WantedBy=default.target
UNIT
        systemctl --user daemon-reload
        systemctl --user enable --now wisent-compute-agent.service
        echo "[install] systemd --user unit ${UNIT} enabled"
        echo "[install] follow logs with: journalctl --user -u wisent-compute-agent -f"
        ;;
    *)
        echo "[install] ERROR: unsupported OS: ${OS}" >&2
        exit 1
        ;;
esac

echo "[install] DONE. Agent is running and broadcasting capacity."
echo "[install] Verify on any other box with:"
echo "          GOOGLE_APPLICATION_CREDENTIALS=... wc status"
echo "[install] Hostname '${HOSTNAME_S}' must match a kind=local entry in"
echo "          wisent_compute/targets/registry.json (or its ssh field's host part)"
echo "          for the agent to claim its slot count from the registry."
