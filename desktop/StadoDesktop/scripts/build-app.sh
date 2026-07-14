#!/bin/sh
set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
DESKTOP_ROOT=$(CDPATH= cd -- "$SCRIPT_DIR/.." && pwd)
APP_BUNDLE="$DESKTOP_ROOT/.build/Stado.app"
CONTENTS="$APP_BUNDLE/Contents"
MACOS="$CONTENTS/MacOS"

swift build --package-path "$DESKTOP_ROOT" --configuration release --product Stado
SWIFT_BIN_DIR=$(swift build --package-path "$DESKTOP_ROOT" --configuration release --show-bin-path)

rm -rf "$APP_BUNDLE"
mkdir -p "$MACOS"
install -m 0644 "$DESKTOP_ROOT/Resources/Info.plist" "$CONTENTS/Info.plist"
install -m 0755 "$SWIFT_BIN_DIR/Stado" "$MACOS/Stado"

if command -v codesign >/dev/null 2>&1; then
    codesign --force --deep --sign - "$APP_BUNDLE"
fi

printf 'Built %s\n' "$APP_BUNDLE"
