#!/usr/bin/env bash
# One-command onboarding for a new Linux compute target.
#
# Usage (run on the box you want to add to the queue, as root or via sudo):
#
#   curl -fsSL https://raw.githubusercontent.com/wisent-ai/wisent-compute/main/install.sh | sudo bash
#   # or, if HF_TOKEN must land on the box for activation jobs:
#   curl -fsSL https://raw.githubusercontent.com/wisent-ai/wisent-compute/main/install.sh | sudo HF_TOKEN=hf_xxx bash
#
# What it does, in order:
#   1. python3 -m pip install --break-system-packages --user wisent-compute wisent
#      wisent-tools wisent-extractors wisent-evaluators (latest from PyPI)
#   2. Renders wisent-agent.service from the package's deploy template,
#      writes /etc/systemd/system/wisent-agent.service, daemon-reload,
#      enable --now -> the agent self-starts at boot, restarts on failure,
#      and reads gpu_type/slots/env_overrides/agent_args from the GCS-hosted
#      registry every poll.
#   3. Renders wisent-upgrade.service + wisent-upgrade.timer from templates,
#      enables the timer -> daily pip-upgrade of the wisent stack so registry
#      config + new releases reach the box without further SSH.
#
# After this single command, every subsequent change to
# gs://wisent-compute/registry.json propagates automatically. Picking the
# right hostname / kind=local entry is up to whoever maintains the registry.

set -euo pipefail

if [ "$(id -u)" -ne 0 ]; then
    echo "[install] ERROR: this script must run as root (use sudo)." >&2
    exit 1
fi

# Determine the unprivileged user the agent will run as. SUDO_USER is set by
# sudo; fall back to "ubuntu" which is the default on Ubuntu cloud images.
TARGET_USER="${SUDO_USER:-ubuntu}"
TARGET_HOME=$(getent passwd "$TARGET_USER" | cut -d: -f6)
if [ -z "$TARGET_HOME" ] || [ ! -d "$TARGET_HOME" ]; then
    echo "[install] ERROR: could not resolve home for user '$TARGET_USER'" >&2
    exit 1
fi
HOSTNAME_S="$(hostname -s 2>/dev/null || hostname)"
echo "[install] user=$TARGET_USER home=$TARGET_HOME hostname=$HOSTNAME_S"

if ! command -v python3 >/dev/null 2>&1; then
    echo "[install] ERROR: python3 not found on PATH" >&2
    exit 1
fi

echo "[install] pip install --user --break-system-packages --upgrade wisent stack"
sudo -u "$TARGET_USER" python3 -m pip install --user --break-system-packages --upgrade \
    wisent-compute wisent wisent-tools wisent-extractors wisent-evaluators

# Resolve wc binary that pip just installed.
WC_BIN="$TARGET_HOME/.local/bin/wc"
if [ ! -x "$WC_BIN" ]; then
    echo "[install] ERROR: wc binary not found at $WC_BIN after pip install" >&2
    exit 1
fi
echo "[install] wc binary: $WC_BIN"

# ADC discovery. Operator can pre-set GOOGLE_APPLICATION_CREDENTIALS; otherwise
# pick up whatever gcloud-managed legacy credentials sit under the user's home.
ADC_PATH="${GOOGLE_APPLICATION_CREDENTIALS:-}"
if [ -z "$ADC_PATH" ]; then
    for cand in "$TARGET_HOME/.config/gcloud/legacy_credentials/"*"/adc.json"; do
        if [ -f "$cand" ]; then
            ADC_PATH="$cand"
            break
        fi
    done
fi
if [ -z "$ADC_PATH" ]; then
    echo "[install] WARNING: no ADC found. Agent will start but cannot read GCS"
    echo "[install] until $TARGET_HOME/.config/gcloud/legacy_credentials/.../adc.json exists"
    ADC_PATH="$TARGET_HOME/.config/gcloud/adc.json"
fi
echo "[install] ADC path: $ADC_PATH"

# Optional HF token + GCP project + bucket override.
HF_TOKEN_VAL="${HF_TOKEN:-}"
GCP_PROJECT_VAL="${GCP_PROJECT:-wisent-480400}"
WC_BUCKET_VAL="${WC_BUCKET:-wisent-compute}"
PATH_VAL="$TARGET_HOME/google-cloud-sdk/bin:$TARGET_HOME/.local/bin:/usr/bin:/bin"

# Resolve the deploy templates shipped inside the installed package.
TEMPLATE_DIR=$(sudo -u "$TARGET_USER" python3 - <<'PY'
import os, wisent_compute
print(os.path.join(os.path.dirname(wisent_compute.__file__), "deploy", "templates"))
PY
)
if [ ! -d "$TEMPLATE_DIR" ]; then
    echo "[install] ERROR: deploy templates not found under installed package" >&2
    exit 1
fi
echo "[install] templates: $TEMPLATE_DIR"

render_template() {
    local src="$1"
    local dst="$2"
    sed -e "s|{USER}|$TARGET_USER|g" \
        -e "s|{HOME}|$TARGET_HOME|g" \
        -e "s|{PATH}|$PATH_VAL|g" \
        -e "s|{ADC_PATH}|$ADC_PATH|g" \
        -e "s|{GCP_PROJECT}|$GCP_PROJECT_VAL|g" \
        -e "s|{WC_BUCKET}|$WC_BUCKET_VAL|g" \
        -e "s|{HF_TOKEN}|$HF_TOKEN_VAL|g" \
        -e "s|{WC_BIN}|$WC_BIN|g" \
        "$src" > "$dst"
    chmod 644 "$dst"
}

echo "[install] rendering /etc/systemd/system/wisent-agent.service"
render_template "$TEMPLATE_DIR/wisent-agent.service.tmpl" /etc/systemd/system/wisent-agent.service

echo "[install] rendering /etc/systemd/system/wisent-upgrade.service"
render_template "$TEMPLATE_DIR/wisent-upgrade.service.tmpl" /etc/systemd/system/wisent-upgrade.service

echo "[install] rendering /etc/systemd/system/wisent-upgrade.timer"
render_template "$TEMPLATE_DIR/wisent-upgrade.timer.tmpl" /etc/systemd/system/wisent-upgrade.timer

# Host-health beacon (out-of-band, captures restart loops the
# capacity-publish heartbeat misses).
echo "[install] rendering /etc/systemd/system/wisent-host-health.{service,timer}"
render_template "$TEMPLATE_DIR/wisent-host-health.service.tmpl" /etc/systemd/system/wisent-host-health.service
render_template "$TEMPLATE_DIR/wisent-host-health.timer.tmpl"   /etc/systemd/system/wisent-host-health.timer

# Drop the beacon script next to the user home so the service unit
# above can find it. The script ships inside the wisent_compute Python
# package as deploy/host_health_beacon.sh (see pyproject.toml
# package-data).
HHB_SRC=$(sudo -u "$TARGET_USER" python3 -c '
import os, wisent_compute
print(os.path.join(os.path.dirname(wisent_compute.__file__), "deploy", "host_health_beacon.sh"))
')
if [ -f "$HHB_SRC" ]; then
    echo "[install] copying $HHB_SRC -> $TARGET_HOME/host_health_beacon.sh"
    install -m 0755 -o "$TARGET_USER" -g "$TARGET_USER" \
        "$HHB_SRC" "$TARGET_HOME/host_health_beacon.sh"
else
    echo "[install] WARN: host_health_beacon.sh not found at $HHB_SRC; the timer will fail"
fi

# Pre-start cleanup script. Lives at a fixed path under $HOME and is
# invoked by wisent-agent.service ExecStartPre. The script itself is
# replaced by the wisent-upgrade.service / wisent-upgrade.timer on
# every pip install --upgrade (see wisent-upgrade.service.tmpl), so
# improvements to the cleanup logic flow automatically without ever
# re-rendering the .service file.
PSC_SRC=$(sudo -u "$TARGET_USER" python3 -c '
import os, wisent_compute
print(os.path.join(os.path.dirname(wisent_compute.__file__), "deploy", "pre_start_cleanup.sh"))
')
if [ -f "$PSC_SRC" ]; then
    echo "[install] copying $PSC_SRC -> $TARGET_HOME/wisent_pre_start_cleanup.sh"
    install -m 0755 -o "$TARGET_USER" -g "$TARGET_USER" \
        "$PSC_SRC" "$TARGET_HOME/wisent_pre_start_cleanup.sh"
else
    echo "[install] WARN: pre_start_cleanup.sh not found at $PSC_SRC; agent will skip pre-start cleanup"
fi

touch /var/log/wisent-agent.log /var/log/wisent-upgrade.log
chown "$TARGET_USER:$TARGET_USER" /var/log/wisent-agent.log /var/log/wisent-upgrade.log

systemctl daemon-reload
systemctl enable --now wisent-agent.service
systemctl enable --now wisent-upgrade.timer
systemctl enable --now wisent-host-health.timer

sleep 4
echo
echo "=== wisent-agent ==="
systemctl is-active wisent-agent || true
tail -3 /var/log/wisent-agent.log 2>/dev/null || true
echo
echo "=== wisent-upgrade timer ==="
systemctl is-active wisent-upgrade.timer || true
systemctl list-timers wisent-upgrade.timer --no-pager 2>/dev/null | head -3 || true

echo
echo "[install] DONE. Hostname '$HOSTNAME_S' must match a kind=local entry in"
echo "          gs://wisent-compute/registry.json (or its ssh user@host part)"
echo "          for the agent to pick up its slot count + env_overrides."
echo "[install] Agent log: /var/log/wisent-agent.log"
echo "[install] Upgrade log: /var/log/wisent-upgrade.log"
