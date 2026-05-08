#!/bin/bash
# Idempotent installer for a GitHub Actions self-hosted runner on the
# user's mac mini. Once installed, the runner polls GitHub over outbound
# HTTPS, picks up jobs scoped to runs-on: [self-hosted, mac-mini-coordinator],
# and runs them locally. The deploy-coordinator-mac-mini workflow uses
# this runner so its rsync/ssh steps become local, dropping the dependency
# on a Tailscale OAuth client.
#
# Inputs:
#   REGISTRATION_TOKEN   short-lived token from
#       gh api -X POST /repos/wisent-ai/wisent-compute/actions/runners/registration-token
#   RUNNER_VERSION       (optional) actions-runner version, default tracks
#                        the latest release determined at install time.

set -euo pipefail

if [ -z "${REGISTRATION_TOKEN:-}" ]; then
    echo "FATAL: REGISTRATION_TOKEN env var required (mint via gh api)." >&2
    exit 2
fi

REPO_URL="https://github.com/wisent-ai/wisent-compute"
RUNNER_DIR="$HOME/actions-runner-wisent-compute"
RUNNER_NAME="mac-mini-coordinator"
RUNNER_LABELS="self-hosted,mac-mini-coordinator"

# Resolve runner version. The latest GitHub-published runner (v2.334.0
# at time of writing) fails to start on macOS 26 (Tahoe) with
# "Failed to create CoreCLR, HRESULT: 0x8007000C" — an internal .NET
# self-host error. v2.319.1 boots cleanly on the same host. Pin to that
# version unless the caller overrides via RUNNER_VERSION env.
if [ -z "${RUNNER_VERSION:-}" ]; then
    RUNNER_VERSION="2.319.1"
fi
if [ -z "${RUNNER_VERSION:-}" ]; then
    echo "FATAL: could not resolve actions-runner latest version." >&2
    exit 3
fi
echo "Using actions-runner version: $RUNNER_VERSION"

ARCH="osx-arm64"
TARBALL="actions-runner-${ARCH}-${RUNNER_VERSION}.tar.gz"
TARBALL_URL="https://github.com/actions/runner/releases/download/v${RUNNER_VERSION}/${TARBALL}"

# If a runner is already configured at RUNNER_DIR, remove it cleanly so we
# do not stack a second registration on top of the first.
if [ -f "$RUNNER_DIR/.runner" ]; then
    echo "Existing runner found; removing before re-register"
    if [ -f "$RUNNER_DIR/svc.sh" ]; then
        ( cd "$RUNNER_DIR" && ./svc.sh stop 2>/dev/null || true )
        ( cd "$RUNNER_DIR" && ./svc.sh uninstall 2>/dev/null || true )
    fi
    if [ -f "$RUNNER_DIR/config.sh" ]; then
        ( cd "$RUNNER_DIR" && ./config.sh remove --token "$REGISTRATION_TOKEN" 2>/dev/null || true )
    fi
fi

mkdir -p "$RUNNER_DIR"
cd "$RUNNER_DIR"

# Re-download the tarball every install so we stay current with the
# pinned RUNNER_VERSION; the runner is small and bandwidth is cheap.
/usr/bin/curl -fsSL -o "$TARBALL" "$TARBALL_URL"
/usr/bin/tar xzf "$TARBALL"
rm -f "$TARBALL"

./config.sh \
    --url "$REPO_URL" \
    --token "$REGISTRATION_TOKEN" \
    --name "$RUNNER_NAME" \
    --labels "$RUNNER_LABELS" \
    --work _work \
    --replace \
    --unattended

# svc.sh registers a launchd LaunchAgent under
# ~/Library/LaunchAgents/actions.runner.<owner-repo>.<name>.plist that
# survives reboots and crashes. Reinstall is idempotent: stop+uninstall,
# then install+start.
./svc.sh stop 2>/dev/null || true
./svc.sh uninstall 2>/dev/null || true
./svc.sh install
./svc.sh start
./svc.sh status | head -10

echo "GitHub Actions self-hosted runner installed:"
echo "  dir:    $RUNNER_DIR"
echo "  name:   $RUNNER_NAME"
echo "  labels: $RUNNER_LABELS"
echo "  repo:   $REPO_URL"
