#!/bin/bash
# macOS variant of host_health_beacon.sh.
#
# Writes gs://wisent-compute/host_health/<host>.json every tick. Same
# schema as the Linux version so the Vercel HostHealthCard renders both
# uniformly. Differences:
#   - Uses `launchctl print gui/<uid>/<label>` for unit state instead of
#     systemctl.
#   - df parsing differs from Linux's (BSD df).
#
# Invoked from a LaunchAgent (rendered by install_macos_coordinator.sh)
# with StartInterval=60.

set -u

PROJECT="wisent-480400"
BUCKET="wisent-compute"
HOST_SLUG=$(/bin/hostname -s 2>/dev/null | /usr/bin/tr '[:upper:]' '[:lower:]')
ADC_PATH="${GOOGLE_APPLICATION_CREDENTIALS:-$HOME/.config/gcloud/application_default_credentials.json}"
# The macOS gcloud CLI tries to write OAuth tokens to the user keychain
# and crashes from non-interactive launchd contexts with
# errSecInteractionNotAllowed (-25308). Use the wisent-compute venv's
# google-cloud-storage Python client directly (it honors ADC via
# $GOOGLE_APPLICATION_CREDENTIALS without going through keychain).
VENV_PY="${WC_VENV_PYTHON:-$HOME/.venvs/wisent-compute/bin/python}"
[ -x "$VENV_PY" ] || exit 1

# Use the existing health schedule for a bounded, best-effort policy pass.
WC_BIN="${WC_BIN:-$HOME/.venvs/wisent-compute/bin/wc}"
if [ -x "$WC_BIN" ]; then
    GOOGLE_APPLICATION_CREDENTIALS="$ADC_PATH" "$VENV_PY" - "$WC_BIN" <<'CLEANUPPY' >/dev/null 2>&1
import subprocess
import sys

try:
    subprocess.run(
        [sys.argv[1], "disk-cleanup", "--once"],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        timeout=40,
        check=True,
    )
except (OSError, subprocess.SubprocessError):
    raise SystemExit(1)
CLEANUPPY
    if [ "$?" -ne 0 ]; then
        echo "host_health_beacon: wc disk-cleanup did not complete; leaving disk state unchanged" >&2
    fi
else
    echo "host_health_beacon: wc disk-cleanup unavailable; leaving disk state unchanged" >&2
fi

reported_at=$(/bin/date -u +%Y-%m-%dT%H:%M:%SZ)

# Root fs: BSD df. KB blocks via `df -k`.
disk_line=$(/bin/df -k / 2>/dev/null | /usr/bin/awk 'NR==2 {print $3, $4, $5}')
read -r disk_used_kb disk_avail_kb disk_pct_str <<<"$disk_line"
disk_pct="${disk_pct_str%%%}"
disk_avail_gb=$(( ${disk_avail_kb:-0} / 1024 / 1024 ))

# LaunchAgent labels we expect on the mac mini.
LABELS="${WC_HEALTH_UNITS:-com.wisent.compute.coordinator com.wisent.compute.auto-deployer com.wisent.compute.dashboard com.wisent.hf-refresh}"
GUI_DOMAIN="gui/$(/usr/bin/id -u)"

units_json=""
for lbl in $LABELS; do
    if /bin/launchctl print "${GUI_DOMAIN}/${lbl}" >/dev/null 2>&1; then
        # Pull "last exit code" + "state" + "pid" from the print
        # output. State "running" with last_exit=0 = active.
        info=$(/bin/launchctl print "${GUI_DOMAIN}/${lbl}" 2>/dev/null)
        state="active"
        last_exit=$(echo "$info" | /usr/bin/awk -F'=' '/last exit code/ {gsub(/[ \t]/,""); print $2; exit}')
        n_restarts="?"
        active_since=$(echo "$info" | /usr/bin/awk -F'=' '/spawn type/ {print "?"; exit}')
        if [ -n "$last_exit" ] && [ "$last_exit" != "0" ]; then
            state="failed"
        fi
    else
        state="inactive"
        last_exit="?"
        n_restarts="?"
        active_since="?"
    fi
    if [ -n "$units_json" ]; then units_json="$units_json,"; fi
    units_json="$units_json\"$lbl\":{\"state\":\"$state\",\"n_restarts\":\"$n_restarts\",\"active_since\":\"$active_since\"}"
done


tmpfile=$(/usr/bin/mktemp)
cat > "$tmpfile" <<EOF
{
  "host": "${HOST_SLUG}",
  "reported_at": "${reported_at}",
  "disk_pct": ${disk_pct:-0},
  "disk_avail_gb": ${disk_avail_gb:-0},
  "units": {${units_json}}
}
EOF

GOOGLE_APPLICATION_CREDENTIALS="$ADC_PATH" \
    "$VENV_PY" - "$tmpfile" "$BUCKET" "host_health/${HOST_SLUG}.json" "$PROJECT" <<'PYEOF' >/dev/null 2>&1
import sys
from google.cloud import storage
src, bucket, blob, project = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4]
data = open(src, "rb").read()
client = storage.Client(project=project)
client.bucket(bucket).blob(blob).upload_from_string(data, content_type="application/json")
PYEOF
/bin/rm -f "$tmpfile"
