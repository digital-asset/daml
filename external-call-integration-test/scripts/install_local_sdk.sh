#!/bin/bash
#
# Build and install local Daml SDK
#
# This script builds the Daml SDK from the local source tree and installs it
# so that `daml` commands use the local version with all recent changes
# (including external call feature).
#
# Usage:
#   ./install_local_sdk.sh           # Build and install
#   ./install_local_sdk.sh --restore # Restore previous default version
#   ./install_local_sdk.sh --status  # Show current status
#
# The script:
# 1. Builds //release:sdk-release-tarball-ce using bazel
# 2. Extracts it to ~/.daml/sdk/local-dev
# 3. Updates ~/.daml/bin/daml symlink to use local version
# 4. Saves the previous version for easy restoration

set -e

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# Directories
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TEST_DIR="$(dirname "$SCRIPT_DIR")"
REPO_ROOT="$(dirname "$TEST_DIR")"
SDK_DIR="$REPO_ROOT/sdk"

DAML_HOME="${DAML_HOME:-$HOME/.daml}"
LOCAL_SDK_NAME="local-dev"
LOCAL_SDK_DIR="$DAML_HOME/sdk/$LOCAL_SDK_NAME"
DAML_BIN="$DAML_HOME/bin/daml"
PREV_VERSION_FILE="$DAML_HOME/.local-sdk-previous-version"

# ============================================================
# Helper Functions
# ============================================================

get_current_version() {
  if [ -L "$DAML_BIN" ]; then
    local target=$(readlink "$DAML_BIN")
    # Extract version from path like ../sdk/2.12.0/daml/daml
    echo "$target" | sed -n 's|.*/sdk/\([^/]*\)/.*|\1|p'
  else
    echo "unknown"
  fi
}

show_status() {
  echo "============================================================"
  echo "Daml SDK Status"
  echo "============================================================"
  echo ""
  echo "DAML_HOME: $DAML_HOME"
  echo ""

  if [ -L "$DAML_BIN" ]; then
    local current=$(get_current_version)
    echo -e "Current version: ${GREEN}$current${NC}"
    echo "Symlink target:  $(readlink "$DAML_BIN")"
  else
    echo -e "Current version: ${YELLOW}not installed${NC}"
  fi
  echo ""

  if [ -f "$PREV_VERSION_FILE" ]; then
    echo "Previous version: $(cat "$PREV_VERSION_FILE")"
  fi
  echo ""

  echo "Installed SDKs:"
  if [ -d "$DAML_HOME/sdk" ]; then
    ls -1 "$DAML_HOME/sdk" 2>/dev/null | while read sdk; do
      if [ "$sdk" = "$(get_current_version)" ]; then
        echo -e "  ${GREEN}* $sdk${NC} (active)"
      else
        echo "    $sdk"
      fi
    done
  else
    echo "  (none)"
  fi
  echo ""

  if [ -d "$LOCAL_SDK_DIR" ]; then
    echo -e "Local SDK: ${GREEN}installed${NC} at $LOCAL_SDK_DIR"
  else
    echo -e "Local SDK: ${YELLOW}not installed${NC}"
  fi
}

restore_previous() {
  echo "============================================================"
  echo "Restoring Previous SDK Version"
  echo "============================================================"
  echo ""

  if [ ! -f "$PREV_VERSION_FILE" ]; then
    echo -e "${RED}ERROR: No previous version recorded${NC}"
    echo "Cannot restore - previous version file not found"
    exit 1
  fi

  local prev_version=$(cat "$PREV_VERSION_FILE")
  local prev_sdk_dir="$DAML_HOME/sdk/$prev_version"

  if [ ! -d "$prev_sdk_dir" ]; then
    echo -e "${RED}ERROR: Previous SDK not found at $prev_sdk_dir${NC}"
    exit 1
  fi

  echo "Restoring version: $prev_version"

  # Update symlink
  rm -f "$DAML_BIN"
  ln -s "../sdk/$prev_version/daml/daml" "$DAML_BIN"

  echo -e "${GREEN}Restored to $prev_version${NC}"
  echo ""

  # Verify
  echo "Verification:"
  "$DAML_BIN" version 2>/dev/null | head -3 || echo "  (daml version command not available)"
}

# ============================================================
# Parse Arguments
# ============================================================

case "${1:-}" in
  --status|-s)
    show_status
    exit 0
    ;;
  --restore|-r)
    restore_previous
    exit 0
    ;;
  --help|-h)
    echo "Build and install local Daml SDK"
    echo ""
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Options:"
    echo "  (none)      Build and install local SDK"
    echo "  --status    Show current SDK status"
    echo "  --restore   Restore previous default version"
    echo "  --help      Show this help"
    exit 0
    ;;
esac

# ============================================================
# Build Local SDK
# ============================================================

echo "============================================================"
echo "Building Local Daml SDK"
echo "============================================================"
echo ""
echo "SDK source: $SDK_DIR"
echo "Target:     $LOCAL_SDK_DIR"
echo ""

# Check SDK directory exists
if [ ! -d "$SDK_DIR" ]; then
  echo -e "${RED}ERROR: SDK directory not found at $SDK_DIR${NC}"
  exit 1
fi

# Ensure DAML_HOME directories exist
mkdir -p "$DAML_HOME/bin"
mkdir -p "$DAML_HOME/sdk"

# Save current version before changing
current_version=$(get_current_version)
if [ "$current_version" != "unknown" ] && [ "$current_version" != "$LOCAL_SDK_NAME" ]; then
  echo "$current_version" > "$PREV_VERSION_FILE"
  echo "Saved previous version: $current_version"
fi

# Build the SDK tarball
echo ""
echo -e "${BLUE}[1/4] Building SDK tarball...${NC}"
echo "      This may take a while..."
echo ""

cd "$SDK_DIR"
./dev-env/bin/bazel build //release:sdk-release-tarball-ce 2>&1 | tail -20

# Find the built tarball
TARBALL=$(find bazel-bin/release -name "daml-sdk-*-linux-intel.tar.gz" -o -name "daml-sdk-*.tar.gz" -o -name "sdk-release-tarball-ce.tar.gz" 2>/dev/null | head -1)

if [ -z "$TARBALL" ] || [ ! -f "$TARBALL" ]; then
  echo -e "${RED}ERROR: SDK tarball not found after build${NC}"
  echo "Expected in bazel-bin/release/"
  ls -la bazel-bin/release/ 2>/dev/null | head -20
  exit 1
fi

echo ""
echo -e "${GREEN}Built: $TARBALL${NC}"

# Extract SDK version from tarball name
SDK_VERSION=$(basename "$TARBALL" | sed 's/daml-sdk-\(.*\)-linux.*/\1/' | sed 's/daml-sdk-\(.*\)\.tar\.gz/\1/')
echo "SDK Version: $SDK_VERSION"

# Remove old local SDK if exists
echo ""
echo -e "${BLUE}[2/4] Preparing installation directory...${NC}"
if [ -d "$LOCAL_SDK_DIR" ]; then
  echo "      Removing old local SDK..."
  rm -rf "$LOCAL_SDK_DIR"
fi
mkdir -p "$LOCAL_SDK_DIR"

# Extract tarball
echo ""
echo -e "${BLUE}[3/4] Extracting SDK...${NC}"
tar -xzf "$TARBALL" -C "$LOCAL_SDK_DIR" --strip-components=1

# Verify extraction
if [ ! -f "$LOCAL_SDK_DIR/daml/daml" ]; then
  echo -e "${RED}ERROR: Extraction failed - daml binary not found${NC}"
  ls -la "$LOCAL_SDK_DIR/"
  exit 1
fi

echo "      Extracted to $LOCAL_SDK_DIR"

# Update symlink
echo ""
echo -e "${BLUE}[4/4] Updating symlink...${NC}"
rm -f "$DAML_BIN"
ln -s "../sdk/$LOCAL_SDK_NAME/daml/daml" "$DAML_BIN"
echo "      $DAML_BIN -> ../sdk/$LOCAL_SDK_NAME/daml/daml"

# ============================================================
# Verification
# ============================================================

echo ""
echo "============================================================"
echo -e "${GREEN}Local SDK Installed Successfully${NC}"
echo "============================================================"
echo ""
echo "SDK Version: $SDK_VERSION"
echo "Location:    $LOCAL_SDK_DIR"
echo ""

# Verify daml command works
echo "Verification:"
if "$DAML_BIN" version 2>/dev/null; then
  echo ""
  echo -e "${GREEN}SUCCESS: Local SDK is now the default${NC}"
else
  echo -e "${YELLOW}WARNING: daml version command failed${NC}"
  echo "         The SDK may still work for building DARs"
fi

echo ""
echo "To restore previous version:"
echo "  $0 --restore"
echo ""
echo "To check status:"
echo "  $0 --status"
