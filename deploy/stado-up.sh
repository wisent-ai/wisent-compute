#!/bin/sh
# stado-up: install (or remove) a persistent local stado agent via launchd.
# Usage: stado-up <target-name> [uninstall]
set -eu

TARGET="${1:?"usage: stado-up <target-name> [uninstall]"}"
LABEL="com.stado.agent.${TARGET}"
PLIST="${HOME}/Library/LaunchAgents/${LABEL}.plist"
PYTHON="$(command -v python3.12 || command -v python3)"
STADO_SRC="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
LOG_DIR="${HOME}/.stado/logs"

if [ "${2:-}" = "uninstall" ]; then
    launchctl bootout "gui/$(id -u)/${LABEL}" 2>/dev/null || true
    launchctl bootout "user/$(id -u)/${LABEL}" 2>/dev/null || true
    rm -f "$PLIST"
    echo "removed ${LABEL}"
    exit 0
fi

mkdir -p "$LOG_DIR"
cat > "$PLIST" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>${LABEL}</string>
    <key>ProgramArguments</key>
    <array>
        <string>${PYTHON}</string>
        <string>-c</string>
        <string>from stado.cli import main; main()</string>
        <string>agent</string>
        <string>--target</string>
        <string>${TARGET}</string>
    </array>
    <key>WorkingDirectory</key>
    <string>${STADO_SRC}</string>
    <key>EnvironmentVariables</key>
    <dict>
        <key>GCP_PROJECT</key>
        <string>wisent-480400</string>
        <key>PATH</key>
        <string>/usr/local/bin:/usr/bin:/bin:/opt/homebrew/bin</string>
    </dict>
    <key>RunAtLoad</key>
    <true/>
    <key>KeepAlive</key>
    <true/>
    <key>StandardOutPath</key>
    <string>${LOG_DIR}/${LABEL}.out.log</string>
    <key>StandardErrorPath</key>
    <string>${LOG_DIR}/${LABEL}.err.log</string>
</dict>
</plist>
EOF

launchctl bootout "gui/$(id -u)/${LABEL}" 2>/dev/null || true
launchctl bootout "user/$(id -u)/${LABEL}" 2>/dev/null || true
if launchctl print "gui/$(id -u)" >/dev/null 2>&1; then
    launchctl bootstrap "gui/$(id -u)" "$PLIST"
    echo "installed ${LABEL} into gui domain (logs: ${LOG_DIR})"
else
    # Headless box reached over SSH: no Aqua domain for this user, so
    # bootstrap into the per-user domain (persists while any session lives;
    # ~/Library/LaunchAgents reloads into the gui domain at console login).
    launchctl bootstrap "user/$(id -u)" "$PLIST"
    echo "installed ${LABEL} into user domain (headless; logs: ${LOG_DIR})"
fi
