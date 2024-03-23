#!/usr/bin/env bash
# Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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

DAMLC="$(rlocation "$TEST_WORKSPACE/$1")"
GENERATE="$(rlocation "$TEST_WORKSPACE/$2")"

DIR=$(mktemp -d)
trap "rm -rf $DIR" EXIT

$GENERATE "$DIR" 2000 2000

$DAMLC build --project-root $DIR/dep

# Note that we don’t seem to hit the limits here since the slowness
# does not come with larger memomry or stack usage.  However, the slow
# down is so big that this times out the 300s Bazel timeout whereas
# the fixed version runs in about 30s.
$DAMLC build --project-root $DIR/main +RTS -s -M150M -K1M -N1
