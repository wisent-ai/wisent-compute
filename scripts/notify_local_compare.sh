#!/usr/bin/env bash
# Sidecar: wait for the local cheap_talk_pd self-play comparison process
# to exit (PID passed as $1), then file a GitHub issue with the result so
# the user gets emailed regardless of whether their Claude session is alive.
#
# Without this the local bash pipeline writes its DONE marker to disk and
# nothing reads it; the user has to come back and ask. With this, GitHub's
# email-on-issue flow delivers the notification asynchronously.

set -u
PID="${1:?pid required}"
LOG="/Users/lukaszbartoszcze/Documents/CodingProjects/Wisent/backends/wisent-compute/scripts/local_compare.log"

# Wait for the target process to die. kill -0 succeeds while pid is alive.
while kill -0 "$PID" 2>/dev/null; do
  sleep 30
done

if grep -q "=== DONE ===" "$LOG" 2>/dev/null; then
  TITLE="[local] cheap_talk_pd self-play compare DONE (pid=${PID})"
  BODY=$(printf '%s\n```\n%s\n```\n' "Local Mac MPS self-play comparison completed at $(date -u)." "$(tail -120 "$LOG")")
else
  TITLE="[local] cheap_talk_pd self-play compare CRASHED (pid=${PID})"
  BODY=$(printf '%s\n```\n%s\n```\n' "Process exited without writing DONE marker." "$(tail -50 "$LOG")")
fi

gh issue create -R wisent-ai/wisent-compute --title "$TITLE" --body "$BODY"
