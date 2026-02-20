#!/usr/bin/env bash
# Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

usage() {
  cat <<'USAGE'
Usage: generate-daml-prim-json.sh [output-json]

Builds //compiler/damlc:daml-prim-json-docs and prints the path to daml-prim.json.
When output-json is provided, copies the generated file to that location.
USAGE
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

if [[ "$#" -gt 1 ]]; then
  usage >&2
  exit 1
fi

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"
SDK_DIR="$(cd -- "$SCRIPT_DIR/../.." && pwd)"

cd "$SDK_DIR"

if [[ -x "$SDK_DIR/dev-env/bin/bazelisk" ]]; then
  BAZEL_CMD="$SDK_DIR/dev-env/bin/bazelisk"
elif command -v bazelisk >/dev/null 2>&1; then
  BAZEL_CMD="bazelisk"
else
  BAZEL_CMD="bazel"
fi

"$BAZEL_CMD" build //compiler/damlc:daml-prim-json-docs

GENERATED_JSON="$SDK_DIR/bazel-bin/compiler/damlc/daml-prim.json"
if [[ ! -f "$GENERATED_JSON" ]]; then
  echo "Expected generated file not found: $GENERATED_JSON" >&2
  exit 1
fi

if [[ "$#" -eq 1 ]]; then
  DEST="$1"
  mkdir -p "$(dirname -- "$DEST")"
  cp "$GENERATED_JSON" "$DEST"
  echo "$DEST"
else
  echo "$GENERATED_JSON"
fi
