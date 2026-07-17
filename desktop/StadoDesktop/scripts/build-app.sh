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

CODESIGN_IDENTITY=${WISENT_CODESIGN_IDENTITY:-}
if [ -z "$CODESIGN_IDENTITY" ]; then
    CODESIGN_IDENTITY=$(security find-identity -v -p codesigning 2>/dev/null \
        | awk -F '"' '/Apple Development:/ { print $2; exit }')
fi
if [ -z "$CODESIGN_IDENTITY" ] || [ "$CODESIGN_IDENTITY" = "-" ]; then
    printf '%s\n' "Stable Apple Development signing identity is required; refusing ad-hoc signing." >&2
    exit 1
fi
codesign --force --deep --sign "$CODESIGN_IDENTITY" --timestamp=none "$APP_BUNDLE"
codesign --verify --strict --deep "$APP_BUNDLE"

printf 'Built %s\n' "$APP_BUNDLE"
