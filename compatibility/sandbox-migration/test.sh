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

RUNNER="$(rlocation $TEST_WORKSPACE/sandbox-migration/sandbox-migration-runner)"
SCRIPT_DAR="$(rlocation $TEST_WORKSPACE/sandbox-migration/migration-script.dar)"
MODEL_DAR="$(rlocation $TEST_WORKSPACE/sandbox-migration/migration-model.dar)"
SCRIPT_ASSISTANT="$(rlocation daml-sdk-0.0.0/daml)"
SANDBOX_ARGS=""
for PLATFORM in $@; do
    SANDBOX_ARGS="$SANDBOX_ARGS $(rlocation daml-sdk-$PLATFORM/daml)"
done
$RUNNER \
    --script-dar $SCRIPT_DAR \
    --model-dar $MODEL_DAR \
    --script-assistant $SCRIPT_ASSISTANT \
    $SANDBOX_ARGS

