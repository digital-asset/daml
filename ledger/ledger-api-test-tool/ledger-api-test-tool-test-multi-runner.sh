#!/usr/bin/env bash

set -Eeuo pipefail

# --- begin runfiles.bash initialization ---
# Copy-pasted from Bazel's Bash runfiles library (tools/bash/runfiles/runfiles.bash).
if [[ ! -d "${RUNFILES_DIR:-/dev/null}" && ! -f "${RUNFILES_MANIFEST_FILE:-/dev/null}" ]]; then
  if [[ -f "$0.runfiles_manifest" ]]; then
    export RUNFILES_MANIFEST_FILE="$0.runfiles_manifest"
  elif [[ -f "$0.runfiles/MANIFEST" ]]; then
    export RUNFILES_MANIFEST_FILE="$0.runfiles/MANIFEST"
  elif [[ -f "$0.runfiles/bazel_tools/tools/bash/runfiles/runfiles.bash" ]]; then
    export RUNFILES_DIR="$0.runfiles"
  fi
fi
if [[ -f "${RUNFILES_DIR:-/dev/null}/bazel_tools/tools/bash/runfiles/runfiles.bash" ]]; then
  source "${RUNFILES_DIR}/bazel_tools/tools/bash/runfiles/runfiles.bash"
elif [[ -f "${RUNFILES_MANIFEST_FILE:-/dev/null}" ]]; then
  source "$(grep -m1 "^bazel_tools/tools/bash/runfiles/runfiles.bash " \
            "$RUNFILES_MANIFEST_FILE" | cut -d ' ' -f 2-)"
else
  echo >&2 "ERROR: cannot find @bazel_tools//tools/bash/runfiles:runfiles.bash"
  exit 1
fi
# --- end runfiles.bash initialization ---

# This script is called with args like the following (formatted for easier reading):
#
#     --target-port 42749 \
#     --executables ledger/ledger-api-test-tool/ledger-api-test-tool ledger/ledger-api-test-tool/ledger-api-test-tool.jar \
#     --timeout-scale-factor=10 --command-submission-ttl-scale-factor=2 --include=SemanticTests

#

shift # drop --target-port
PORT=$1
shift # drop port
shift # drop --executables
CLIENT=$1
shift # drop wrapper script for jar
shift # drop jar
echo "Port: $PORT"
echo "Client: $CLIENT"
echo "Other:"
echo $CLIENT $@ --mapping:Alice=localhost:$PORT --mapping:Bank=localhost:$PORT --mapping:Peggy=localhost:$PORT --include=SemanticTests
exec $CLIENT $@ --mapping:Alice=localhost:$PORT --mapping:Bank=localhost:$PORT --mapping:Peggy=localhost:$PORT --include=SemanticTests
