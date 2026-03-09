#!/usr/bin/env bash
set -euo pipefail

if [ -z "${BUILD_WORKSPACE_DIRECTORY:-}" ]; then
  echo "This script must be executed with 'bazel run'." >&2
  exit 1
fi

GENERATED_FILE="$1"
DEST_PATH="$BUILD_WORKSPACE_DIRECTORY/$2"

cp -f "$GENERATED_FILE" "$DEST_PATH"
echo "✅ Successfully updated $2"
