#!/usr/bin/env bash
# Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# Copy-pasted from the Bazel Bash runfiles library v2.
set -uo pipefail; f=bazel_tools/tools/bash/runfiles/runfiles.bash
source "${RUNFILES_DIR:-/dev/null}/$f" 2>/dev/null || \
  source "$(grep -sm1 "^$f " "${RUNFILES_MANIFEST_FILE:-/dev/null}" | cut -f2- -d' ')" 2>/dev/null || \
  source "$0.runfiles/$f" 2>/dev/null || \
  source "$(grep -sm1 "^$f " "$0.runfiles_manifest" | cut -f2- -d' ')" 2>/dev/null || \
  source "$(grep -sm1 "^$f " "$0.exe.runfiles_manifest" | cut -f2- -d' ')" 2>/dev/null || \
  { echo>&2 "ERROR: cannot find $f"; exit 1; }; f=; set -e
# --- end runfiles.bash initialization v2 ---
set -euo pipefail

canonicalize_rlocation() {
    # Bazel will add a . at the beginning of locations in the root package
    # which breaks rlocation.
    rlocation $(realpath -L -s -m --relative-to=$PWD $TEST_WORKSPACE/$1)
}

RUNNER="$(rlocation "$TEST_WORKSPACE/$1")"
export DAML="$(rlocation "$TEST_WORKSPACE/$2")"
# These things are only used in the jest tests so rather
# than adding a lot of boilerplate to the Haskell code
# to parse them only to pass them on, we simply set them here.
export DAML_SANDBOX="$(rlocation "$TEST_WORKSPACE/$3")"
export SANDBOX_VERSION="${4}"
export DAML_JSON_API="$(rlocation "$TEST_WORKSPACE/$5")"
export JSON_API_VERSION="${6}"
export REACT_APP_LEDGER_ID="sandbox"
DAML_TYPES="$(rlocation "$TEST_WORKSPACE/$7")"
DAML_LEDGER="$(rlocation "$TEST_WORKSPACE/$8")"
DAML_REACT="$(rlocation "$TEST_WORKSPACE/$9")"
MESSAGING_PATCH="$(rlocation "$TEST_WORKSPACE/${10}")"
NPM="$(rlocation "$TEST_WORKSPACE/${11}")"
NODE="$(rlocation "$TEST_WORKSPACE/${12}")"
PATCH="$(rlocation "$TEST_WORKSPACE/${13}")"
TEST_DEPS="$(rlocation "$TEST_WORKSPACE/${14}")"
TEST_TS="$(rlocation "$TEST_WORKSPACE/${15}")"
CODEGEN_OUTPUT="$(canonicalize_rlocation "${16}")"
export DAR_PATH="$(canonicalize_rlocation "${17}")"

"$RUNNER" \
  --daml "$DAML" \
  --daml-types "$DAML_TYPES" \
  --daml-ledger "$DAML_LEDGER" \
  --daml-react "$DAML_REACT" \
  --messaging-patch "$MESSAGING_PATCH" \
  --npm "$NPM" \
  --node "$NODE" \
  --patch "$PATCH" \
  --test-deps "$TEST_DEPS" \
  --test-ts "$TEST_TS" \
  --codegen "$CODEGEN_OUTPUT" \
