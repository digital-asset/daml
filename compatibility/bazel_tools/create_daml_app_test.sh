#!/usr/bin/env bash
# Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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
set -euox pipefail

RUNNER="$(rlocation "$TEST_WORKSPACE/$1")"
DAML="$(rlocation "$TEST_WORKSPACE/$2")"
SANDBOX="$(rlocation "$TEST_WORKSPACE/$3")"
JSON_API="$(rlocation "$TEST_WORKSPACE/$4")"
DAML_TYPES="$(rlocation "$TEST_WORKSPACE/$5")"
DAML_LEDGER="$(rlocation "$TEST_WORKSPACE/$6")"
DAML_REACT="$(rlocation "$TEST_WORKSPACE/$7")"
MESSAGING_PATCH="$(rlocation "$TEST_WORKSPACE/$8")"
YARN="$(rlocation "$TEST_WORKSPACE/$9")"
PATCH="$(rlocation "$TEST_WORKSPACE/${10}")"

"$RUNNER" \
  --daml "$DAML" \
  --sandbox "$SANDBOX" \
  --json-api "$JSON_API" \
  --daml-types "$DAML_TYPES" \
  --daml-ledger "$DAML_LEDGER" \
  --daml-react "$DAML_REACT" \
  --messaging-patch "$MESSAGING_PATCH" \
  --yarn "$YARN" \
  --patch "$PATCH" \
  "${@:11}"
