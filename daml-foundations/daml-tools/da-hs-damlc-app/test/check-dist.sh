#!/usr/bin/env bash
# Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -eu

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

DAMLC_TAR=$(rlocation "$TEST_WORKSPACE/$1")
MAIN_DAML=$(rlocation "$TEST_WORKSPACE/$2")
TAR_PATH=$(rlocation "$TEST_WORKSPACE/$3")
GZIP_PATH=$(rlocation "$TEST_WORKSPACE/$4")

if [[ "${OSTYPE}" =~ msys* ]]; then
  DAMLC_PATH="da-hs-damlc-app.exe/da-hs-damlc-app.exe"
  DAMLC_TAR=$(cygpath $DAMLC_TAR)
else
  DAMLC_PATH="da-hs-damlc-app/da-hs-damlc-app"
fi

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
TEST_TMP="$(mktemp -d)"
function cleanup() {
  rm -rf "$TEST_TMP"
}
trap cleanup EXIT

export PATH=$(dirname $TAR_PATH):$(dirname $GZIP_PATH):$PATH

tar xzf "$DAMLC_TAR" -C "$TEST_TMP"

"$TEST_TMP/$DAMLC_PATH" \
  compile "$MAIN_DAML" \
  -o $TEST_TMP/out
