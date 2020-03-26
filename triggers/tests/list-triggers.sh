# Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -eou pipefail

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

TRIGGER_EXE=$(rlocation "$TEST_WORKSPACE/$1")
DAR=$(rlocation "$TEST_WORKSPACE/$2")
OUTPUT="$($TRIGGER_EXE list --dar $DAR | tail -n '+2' | tr -d '\r')"
EXPECTED="\
  CommandId:test
  PendingSet:booTrigger
  CopyTrigger:copyTrigger
  ExerciseByKey:exerciseByKeyTrigger
  Heartbeat:test
  Numeric:test
  Retry:retryTrigger
  ACS:test
  Time:test
  TemplateIdFilter:testOne
  TemplateIdFilter:testTwo\
"

if [ "$OUTPUT" != "$EXPECTED" ]; then
    echo "Unexpected output:"
    echo "$OUTPUT"
    echo "Expected:"
    echo "$EXPECTED"
    exit 1
fi

