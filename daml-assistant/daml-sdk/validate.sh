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

JAVA=$(rlocation "$TEST_WORKSPACE/$1")
SDK_CE=$(rlocation "$TEST_WORKSPACE/$2")
SDK_EE=$(rlocation "$TEST_WORKSPACE/$3")

for cmd in sandbox sandbox-classic; do
    ret=0
    $JAVA -jar $SDK_CE $cmd --help | grep -q profile-dir || ret=$?
    if [[ $ret -eq 0 ]]; then
        echo "Unexpected profile-dir option in CE"
        exit 1
    fi
done

for cmd in sandbox sandbox-classic; do
    $JAVA -jar $SDK_EE $cmd --help | grep -q profile-dir
done

if ! ($JAVA -jar $SDK_EE trigger-service --help | grep -q oracle); then
  exit 1
fi
if $JAVA -jar $SDK_CE trigger-service --help | grep -q oracle; then
    exit 1
fi
