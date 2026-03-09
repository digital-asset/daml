#!/usr/bin/env sh
set -euo pipefail

GOLDEN_FILE="$1"
GENERATED_FILE="$2"

if ! diff -u "$GOLDEN_FILE" "$GENERATED_FILE"; then
  echo ""
  echo "❌ ERROR: daml-lf-versions.json is out of date!"
  echo "👉 To fix this, run: bazel run //sdk/daml-lf:update-daml-lf-versions"
  echo ""
  exit 1
fi
