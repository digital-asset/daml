#!/usr/bin/env bash
# Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# --- begin runfiles.bash initialization v2 ---
# Copy-pasted from the Bazel Bash runfiles library v2.
set -uo pipefail; f=bazel_tools/tools/bash/runfiles/runfiles.bash
source "${RUNFILES_DIR:-/dev/null}/$f" 2>/dev/null || \
  source "$(grep -sm1 "^$f " "${RUNFILES_MANIFEST_FILE:-/dev/null}" | cut -f2- -d' ')" 2>/dev/null || \
  source "$0.runfiles/$f" 2>/dev/null || \
  source "$(grep -sm1 "^$f " "$0.runfiles_manifest" | cut -f2- -d' ')" 2>/dev/null || \
  source "$(grep -sm1 "^$f " "$0.exe.runfiles_manifest" | cut -f2- -d' ')" 2>/dev/null || \
  { echo>&2 "ERROR: cannot find $f"; exit 1; }; f=; set -e
# --- end runfiles.bash initialization v2 ---

set -eou pipefail

PROFILE=$(rlocation "$TEST_WORKSPACE/$1")
EXPORT=$(rlocation "$TEST_WORKSPACE/$2")
DAR=$(rlocation "$TEST_WORKSPACE/$3")
JQ=$(rlocation "$TEST_WORKSPACE/$4")

PROFILE_DIR=$(mktemp -d)
trap "rm -rf $PROFILE_DIR" EXIT

$PROFILE --choice "Iou:Iou:Iou_Transfer" --choice-index 0 --dar $DAR --export $EXPORT --profile-dir $PROFILE_DIR --adapt
$JQ -e '.shared.frames | map(.name) | contains(["exercise @Iou:Iou Iou_Transfer"])' < $PROFILE_DIR/*.json
