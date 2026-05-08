#!/bin/bash
# Per-minute deploy tick. Runs as launchd LaunchAgent
# com.wisent.compute.auto-deployer with StartInterval=60. Fetches
# origin/main, and when local HEAD diverges, pulls + re-runs the
# coordinator installer. Idempotent: a tick that finds no new commits
# exits in well under a second.
#
# The repo is expected to live at $REPO_DIR (default
# ~/wisent-compute-deploy) as a real git clone. install_macos_coordinator.sh
# bootstraps the clone on first run using the PAT at $PAT_FILE
# (default ~/.config/wisent/github_pat).

set -euo pipefail

REPO_DIR="${REPO_DIR:-$HOME/wisent-compute-deploy}"

if [ ! -d "$REPO_DIR/.git" ]; then
    echo "FATAL: $REPO_DIR is not a git clone; bootstrap is incomplete." >&2
    exit 2
fi

cd "$REPO_DIR"

# git fetch is the only network call we make per tick. Failures here are
# transient (Wi-Fi blip, GitHub flake) and should not abort the daemon.
if ! git fetch --quiet origin main 2>/dev/null; then
    echo "tick: git fetch failed; skipping" >&2
    exit 0
fi

LOCAL=$(git rev-parse HEAD 2>/dev/null || echo "")
REMOTE=$(git rev-parse origin/main 2>/dev/null || echo "")

if [ -z "$REMOTE" ]; then
    echo "tick: could not resolve origin/main" >&2
    exit 0
fi
if [ "$LOCAL" = "$REMOTE" ]; then
    # No-op: nothing changed.
    exit 0
fi

echo "tick: local=${LOCAL:0:7} remote=${REMOTE:0:7} — deploying"
git pull --ff-only origin main
bash "$REPO_DIR/deploy/install_macos_coordinator.sh"
