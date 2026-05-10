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

# Discover gcloud (homebrew Cask path on macOS).
GCLOUD_BIN=""
for cand in /opt/homebrew/share/google-cloud-sdk/bin/gcloud \
            /usr/local/share/google-cloud-sdk/bin/gcloud \
            /opt/homebrew/bin/gcloud /usr/local/bin/gcloud \
            "$(/usr/bin/which gcloud 2>/dev/null)"; do
    if [ -n "$cand" ] && [ -x "$cand" ]; then GCLOUD_BIN="$cand"; break; fi
done
[ -z "$GCLOUD_BIN" ] && exit 1

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

# Last 4KB of any wisent-compute log (whichever exists, in priority order).
last_log='""'
for p in "$HOME/Library/Logs/wisent-compute-coordinator.err" \
         "$HOME/Library/Logs/wisent-compute-auto-deployer.err" \
         "$HOME/Library/Logs/wisent-hf-refresh.err"; do
    if [ -r "$p" ]; then
        last_log=$(/usr/bin/tail -c 4096 "$p" 2>/dev/null \
            | /usr/bin/python3 -c 'import sys,json; print(json.dumps(sys.stdin.read()))')
        break
    fi
done

tmpfile=$(/usr/bin/mktemp)
cat > "$tmpfile" <<EOF
{
  "host": "${HOST_SLUG}",
  "reported_at": "${reported_at}",
  "disk_pct": ${disk_pct:-0},
  "disk_avail_gb": ${disk_avail_gb:-0},
  "units": {${units_json}},
  "last_log": ${last_log}
}
EOF

GOOGLE_APPLICATION_CREDENTIALS="$ADC_PATH" \
    "$GCLOUD_BIN" --quiet --project="$PROJECT" storage cp \
    "$tmpfile" "gs://$BUCKET/host_health/${HOST_SLUG}.json" >/dev/null 2>&1
/bin/rm -f "$tmpfile"
