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

LABEL=$1
GENERATED=$(rlocation "$TEST_WORKSPACE/$2")
GOLDEN=$(rlocation "$TEST_WORKSPACE/$3")
DIFF=$4
ACCEPT=${5:-}

info() {
  echo "diff ($DIFF) failed with exit status: $1"
  echo "To accept the changes, run"
  echo "    bazel run $LABEL -- --accept"
  exit $1
}

case "$ACCEPT" in
  -a | --accept)
      echo Accepting changes, copying "$GENERATED" to "$3"
      cp "$GENERATED" "$3"
      ;;
  *)
      trap 'info $?' ERR
      $DIFF $GENERATED $GOLDEN
      ;;
esac
