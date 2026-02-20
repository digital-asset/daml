#!/usr/bin/env bash
# Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

usage() {
  cat <<'USAGE'
Usage: run-shared-daml-docs-json-to-mdx.sh --docs-repo /path/to/docs [options] [converter args]

Thin wrapper around the shared converter in the docs repository.

Options:
  --docs-repo PATH   Path to a checkout of digital-asset/docs.
  --input-json PATH  Path to damlc docs JSON (default: bazel-bin/compiler/damlc/daml-prim.json).
  -h, --help         Show this help.

Environment:
  DAML_DOCS_TOOL_REPO_PATH  Default value for --docs-repo.
  DAML_DOCS_TOOL_SCRIPT     Converter script path within docs repo.
                            Default: scripts/daml_docs_json_to_mdx.py

Example:
  ./sdk/docs/scripts/run-shared-daml-docs-json-to-mdx.sh \
    --docs-repo /path/to/docs \
    --output-dir /path/to/docs/docs-main/appdev/reference/daml-prim-api \
    --docs-json /path/to/docs/docs.json
USAGE
}

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"
SDK_DIR="$(cd -- "$SCRIPT_DIR/../.." && pwd)"

DOCS_REPO="${DAML_DOCS_TOOL_REPO_PATH:-}"
INPUT_JSON="$SDK_DIR/bazel-bin/compiler/damlc/daml-prim.json"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --docs-repo)
      DOCS_REPO="$2"
      shift 2
      ;;
    --input-json)
      INPUT_JSON="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      break
      ;;
  esac
done

if [[ -z "$DOCS_REPO" ]]; then
  echo "--docs-repo (or DAML_DOCS_TOOL_REPO_PATH) is required." >&2
  usage >&2
  exit 1
fi

CONVERTER_REL="${DAML_DOCS_TOOL_SCRIPT:-scripts/daml_docs_json_to_mdx.py}"
CONVERTER_SCRIPT="$DOCS_REPO/$CONVERTER_REL"

if [[ ! -f "$CONVERTER_SCRIPT" ]]; then
  echo "Converter script not found: $CONVERTER_SCRIPT" >&2
  exit 1
fi

if [[ ! -f "$INPUT_JSON" ]]; then
  echo "Input JSON not found: $INPUT_JSON" >&2
  echo "Run ./sdk/docs/scripts/generate-daml-prim-json.sh first, or pass --input-json." >&2
  exit 1
fi

exec python3 "$CONVERTER_SCRIPT" --input-json "$INPUT_JSON" "$@"
