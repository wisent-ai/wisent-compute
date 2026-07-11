#!/bin/bash
# Best-effort, bounded registry-authorized cleanup before agent/upgrade start.

set +e

WC_BIN="${WC_BIN:-${HOME:-/home/ubuntu}/.local/bin/wc}"
if [ ! -x "$WC_BIN" ]; then
    echo "[pre_start_cleanup] wc disk-cleanup unavailable; leaving disk state unchanged" >&2
    exit 0
fi

/usr/bin/timeout 40s "$WC_BIN" disk-cleanup --once >/dev/null 2>&1
status=$?
if [ "$status" -ne 0 ]; then
    echo "[pre_start_cleanup] wc disk-cleanup did not complete (status=$status); leaving disk state unchanged" >&2
fi
exit 0
