#!/bin/zsh
# Build, sign, and install the Stado menu-bar app.
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
cd "$ROOT"

PRODUCT="Stado"
BUNDLE="$ROOT/.build/Stado.app"
INSTALLED_BUNDLE="${STADO_INSTALL_APP_PATH:-$HOME/Applications/Stado.app}"
EXECUTABLE="$ROOT/.build/release/$PRODUCT"

print "→ building release"
swift build -c release --product "$PRODUCT"

if [[ ! -x "$EXECUTABLE" ]]; then
    print -u2 "build did not produce $EXECUTABLE"
    exit 1
fi

print "→ assembling $BUNDLE"
rm -rf "$BUNDLE"
mkdir -p "$BUNDLE/Contents/MacOS" "$BUNDLE/Contents/Resources"
cp "$ROOT/Resources/Info.plist" "$BUNDLE/Contents/Info.plist"
cp "$EXECUTABLE" "$BUNDLE/Contents/MacOS/Stado"
chmod +x "$BUNDLE/Contents/MacOS/Stado"

IDENTITY="${STADO_SIGN_IDENTITY:-}"
if [[ -z "$IDENTITY" ]]; then
    IDENTITY=$(security find-identity -v -p codesigning \
        | awk -F '"' '/Developer ID Application/{print $2; exit}')
fi
if [[ -z "$IDENTITY" && "${STADO_ALLOW_DEVELOPMENT_SIGNING:-}" == "1" ]]; then
    IDENTITY=$(security find-identity -v -p codesigning \
        | awk -F '"' '/Apple Development/{print $2; exit}')
fi
if [[ -z "$IDENTITY" && "${STADO_ALLOW_ADHOC_SIGNING:-}" == "1" ]]; then
    IDENTITY="-"
fi
if [[ -z "$IDENTITY" ]]; then
    print -u2 "No Developer ID Application signing identity found."
    print -u2 "Set STADO_SIGN_IDENTITY, STADO_ALLOW_DEVELOPMENT_SIGNING=1, or explicitly set STADO_ALLOW_ADHOC_SIGNING=1 for same-Mac use."
    exit 1
fi

SIGN_ARGS=(--force --sign "$IDENTITY")
if [[ "$IDENTITY" == Developer\ ID\ Application:* ]]; then
    SIGN_ARGS+=(--options runtime --timestamp)
else
    SIGN_ARGS+=(--timestamp=none)
fi

print "→ signing with $IDENTITY"
codesign "${SIGN_ARGS[@]}" "$BUNDLE/Contents/MacOS/Stado"
codesign "${SIGN_ARGS[@]}" "$BUNDLE"
codesign --verify --strict --deep --verbose=2 "$BUNDLE"

print "→ installing $INSTALLED_BUNDLE"
rm -rf "$INSTALLED_BUNDLE"
mkdir -p "$(dirname "$INSTALLED_BUNDLE")"
ditto "$BUNDLE" "$INSTALLED_BUNDLE"
codesign --verify --strict --deep --verbose=2 "$INSTALLED_BUNDLE"
print "✓ $INSTALLED_BUNDLE"

if [[ "${1:-}" == "--open" ]]; then
    open "$INSTALLED_BUNDLE"
fi
