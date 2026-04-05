#!/usr/bin/env bash
# install-arrow-extension.sh
#
# Downloads the DuckDB Arrow community extension for the current platform and
# installs it into ~/.duckdb/extensions/ so that "LOAD arrow" works without
# network access (air-gapped environments).
#
# Usage:
#   ./install-arrow-extension.sh                  # install to ~/.duckdb/extensions/
#   DUCKDB_EXTENSION_DIR=/custom/path \
#     ./install-arrow-extension.sh                # install to custom directory
#
# After running this script once you can execute tests normally:
#   MAVEN_OPTS="--add-opens=java.base/java.nio=ALL-UNNAMED -Xmx2g" \
#     mvn test -pl pgw-ingestor,pgw-validator
#
# For air-gapped machines: run this script on a machine with internet access,
# then copy ~/.duckdb/extensions/v1.4.4/<platform>/arrow.duckdb_extension
# to the same path on the air-gapped machine (or to $DUCKDB_EXTENSION_DIR/).

set -euo pipefail

# ── Configuration ─────────────────────────────────────────────────────────────

DUCKDB_VERSION="v1.4.4"
EXTENSION_NAME="arrow"

# Base URL for DuckDB community extensions
COMMUNITY_BASE="https://community-extensions.duckdb.org"

# ── Detect platform ────────────────────────────────────────────────────────────

OS=$(uname -s)
ARCH=$(uname -m)

case "$OS" in
  Linux)
    case "$ARCH" in
      x86_64)  PLATFORM="linux_amd64" ;;
      aarch64) PLATFORM="linux_arm64" ;;
      *)       echo "ERROR: Unsupported Linux architecture: $ARCH" >&2; exit 1 ;;
    esac
    ;;
  Darwin)
    case "$ARCH" in
      x86_64) PLATFORM="osx_amd64" ;;
      arm64)  PLATFORM="osx_arm64" ;;
      *)      echo "ERROR: Unsupported macOS architecture: $ARCH" >&2; exit 1 ;;
    esac
    ;;
  MINGW*|CYGWIN*|MSYS*)
    PLATFORM="windows_amd64"
    ;;
  *)
    echo "ERROR: Unsupported OS: $OS" >&2
    exit 1
    ;;
esac

# ── Resolve install directory ──────────────────────────────────────────────────

if [ -n "${DUCKDB_EXTENSION_DIR:-}" ]; then
  INSTALL_DIR="$DUCKDB_EXTENSION_DIR"
else
  INSTALL_DIR="$HOME/.duckdb/extensions/${DUCKDB_VERSION}/${PLATFORM}"
fi

EXTENSION_FILE="${INSTALL_DIR}/${EXTENSION_NAME}.duckdb_extension"
DOWNLOAD_URL="${COMMUNITY_BASE}/${DUCKDB_VERSION}/${PLATFORM}/${EXTENSION_NAME}.duckdb_extension.gz"

# ── Already installed? ─────────────────────────────────────────────────────────

if [ -f "$EXTENSION_FILE" ]; then
  echo "✓ Arrow extension already installed: $EXTENSION_FILE"
  exit 0
fi

# ── Download ───────────────────────────────────────────────────────────────────

echo "Installing DuckDB Arrow extension"
echo "  DuckDB version : $DUCKDB_VERSION"
echo "  Platform       : $PLATFORM"
echo "  Source         : $DOWNLOAD_URL"
echo "  Destination    : $EXTENSION_FILE"
echo ""

mkdir -p "$INSTALL_DIR"

TMP_GZ=$(mktemp /tmp/arrow_ext_XXXXXX.gz)
trap 'rm -f "$TMP_GZ"' EXIT

if command -v curl &>/dev/null; then
  curl -fsSL --progress-bar "$DOWNLOAD_URL" -o "$TMP_GZ"
elif command -v wget &>/dev/null; then
  wget -q --show-progress "$DOWNLOAD_URL" -O "$TMP_GZ"
else
  echo "ERROR: Neither curl nor wget found. Install one and retry." >&2
  exit 1
fi

gunzip -c "$TMP_GZ" > "$EXTENSION_FILE"

echo ""
echo "✓ Arrow extension installed: $EXTENSION_FILE"
echo ""
echo "Run tests with:"
echo "  MAVEN_OPTS=\"--add-opens=java.base/java.nio=ALL-UNNAMED -Xmx2g\" \\"
echo "    mvn test -pl pgw-ingestor,pgw-validator"
echo ""
echo "Or with a custom extension directory:"
echo "  DUCKDB_EXTENSION_DIR=$INSTALL_DIR \\"
echo "    MAVEN_OPTS=\"--add-opens=java.base/java.nio=ALL-UNNAMED -Xmx2g\" \\"
echo "    mvn test -pl pgw-ingestor,pgw-validator"
