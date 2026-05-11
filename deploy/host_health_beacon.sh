#!/bin/bash
# Out-of-band host health beacon.
#
# Writes gs://wisent-compute/host_health/<host>.json every tick with the
# fields the wisent-enterprise /jobs page surfaces:
#   - host (short hostname)
#   - reported_at (ISO8601 UTC)
#   - disk_pct, disk_avail_gb (root filesystem)
#   - units: { <unit>: {state, restart_counter, since} }
#   - last_log: tail of the agent log (truncated)
#
# Why it exists: the wisent-agent itself only publishes capacity to
# gs://wisent-compute/capacity/ AFTER it successfully starts. A unit
# stuck in a systemd restart loop (the RTX workstation went 30+ hours
# in 3,645 restart attempts on 2026-05-09 because pip self-upgrade hit
# a full disk) never publishes capacity, so the dashboard's "stale
# agents" check misses it. This beacon runs out-of-band so the
# dashboard can show the failure.
#
# Run via systemd timer (Linux) or launchd LaunchAgent (macOS); the
# tick interval should be ~60s.

set -u

PROJECT="wisent-480400"
BUCKET="wisent-compute"
UNITS_TO_WATCH="${WC_HEALTH_UNITS:-wisent-agent.service}"
LOG_PATHS="${WC_HEALTH_LOGS:-/var/log/wisent-agent.log}"
HOST_SLUG=$(/bin/hostname -s 2>/dev/null | /usr/bin/tr '[:upper:]' '[:lower:]')

# Discover gcloud (the GCP SDK is at different paths on Linux/macOS).
GCLOUD_BIN=""
for cand in /opt/homebrew/share/google-cloud-sdk/bin/gcloud \
            /usr/local/share/google-cloud-sdk/bin/gcloud \
            /home/ubuntu/google-cloud-sdk/bin/gcloud \
            "$(command -v gcloud 2>/dev/null)"; do
    if [ -n "$cand" ] && [ -x "$cand" ]; then GCLOUD_BIN="$cand"; break; fi
done
if [ -z "$GCLOUD_BIN" ]; then
    echo "host_health_beacon: no gcloud found; aborting" >&2
    exit 1
fi

reported_at=$(/bin/date -u +%Y-%m-%dT%H:%M:%SZ)

# Root fs usage
disk_line=$(/bin/df -k / 2>/dev/null | /usr/bin/awk 'NR==2 {print $3, $4, $5}')
read -r disk_used_kb disk_avail_kb disk_pct_str <<<"$disk_line"
disk_pct="${disk_pct_str%%%}"
# Avail in GB (rounded down).
disk_avail_gb=$(( ${disk_avail_kb:-0} / 1024 / 1024 ))

# Self-heal: when disk is 90%+ full AND the wisent-agent unit has been
# stuck for at least one restart, evict caches before reporting. Without
# this the agent restart-loops forever on a full disk (the workstation
# spent 30+ hours in a 3,645-restart loop on 2026-05-09 with pip-upgrade
# failing on a 46GB HF cache). The eviction targets are non-canonical
# state: HF datasets/snapshots, wheel cache, wisent corrupt pair-text
# cache. All are reproducible from the upstream source on next access.
SELF_HEAL_DISK_PCT_THRESHOLD=90
HOME_DIR="${HOME:-/home/ubuntu}"
if [ "${disk_pct:-0}" -ge "$SELF_HEAL_DISK_PCT_THRESHOLD" ]; then
    # Only run if wisent-agent is NOT actively serving (state != active).
    primary_unit="${UNITS_TO_WATCH%%,*}"
    if ! /usr/bin/systemctl is-active "$primary_unit" >/dev/null 2>&1; then
        for tgt in "$HOME_DIR/.cache/huggingface/hub" \
                   "$HOME_DIR/.cache/pip" \
                   "$HOME_DIR/.wisent_cache" \
                   /root/.cache/huggingface/hub \
                   /root/.cache/pip; do
            [ -d "$tgt" ] && /bin/rm -rf "$tgt" 2>/dev/null || true
        done
        # Trigger a fresh systemd restart so the agent's pip ExecStartPre
        # runs on the now-cleaned disk.
        /usr/bin/systemctl restart "$primary_unit" >/dev/null 2>&1 || true
        # Re-read disk after eviction so the same beacon tick reports
        # the post-heal state.
        disk_line=$(/bin/df -k / 2>/dev/null | /usr/bin/awk 'NR==2 {print $3, $4, $5}')
        read -r disk_used_kb disk_avail_kb disk_pct_str <<<"$disk_line"
        disk_pct="${disk_pct_str%%%}"
        disk_avail_gb=$(( ${disk_avail_kb:-0} / 1024 / 1024 ))
    fi
fi

# systemctl unit states (one entry per UNITS_TO_WATCH item, comma-sep).
units_json=""
for unit in ${UNITS_TO_WATCH//,/ }; do
    if /usr/bin/systemctl is-active "$unit" >/dev/null 2>&1; then
        state="active"
    elif /usr/bin/systemctl is-failed "$unit" >/dev/null 2>&1; then
        state="failed"
    else
        state="inactive"
    fi
    # Restart counter: parse from `systemctl show -p NRestarts`.
    n_restarts=$(/usr/bin/systemctl show -p NRestarts --value "$unit" 2>/dev/null || echo "?")
    since=$(/usr/bin/systemctl show -p ActiveEnterTimestamp --value "$unit" 2>/dev/null || echo "?")
    if [ -n "$units_json" ]; then units_json="$units_json,"; fi
    units_json="$units_json\"$unit\":{\"state\":\"$state\",\"n_restarts\":\"$n_restarts\",\"active_since\":\"$since\"}"
done

# Last log lines (truncate to 4 KB).
last_log=""
for p in ${LOG_PATHS//,/ }; do
    if [ -r "$p" ]; then
        last_log=$(/usr/bin/tail -c 4096 "$p" 2>/dev/null \
            | /usr/bin/python3 -c 'import sys,json; print(json.dumps(sys.stdin.read()))')
        break
    fi
done
[ -z "$last_log" ] && last_log='""'

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

"$GCLOUD_BIN" --quiet --project="$PROJECT" storage cp \
    "$tmpfile" "gs://$BUCKET/host_health/${HOST_SLUG}.json" >/dev/null 2>&1
rm -f "$tmpfile"
