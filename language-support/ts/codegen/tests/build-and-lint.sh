# Copyright (c) 2019 The DAML Authors. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

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

JAVA=$(rlocation "$TEST_WORKSPACE/$1")
YARN=$(rlocation "$TEST_WORKSPACE/$2")
DAML2TS=$(rlocation "$TEST_WORKSPACE/$3")
SANDBOX=$(rlocation "$TEST_WORKSPACE/$4")
JSON_API=$(rlocation "$TEST_WORKSPACE/$5")
DAR=$(rlocation "$TEST_WORKSPACE/$6")
PACKAGE_JSON=$(rlocation "$TEST_WORKSPACE/$7")
TS_DIR=$(dirname $PACKAGE_JSON)

TMP_DIR=$(mktemp -d)
cleanup() {
  rm -rf $TMP_DIR
}
trap cleanup EXIT
echo "TMP_DIR = $TMP_DIR"

cp -rL $TS_DIR/* $TMP_DIR
cd $TMP_DIR

$DAML2TS -o generated/src/daml --main-package-name daml-tests $DAR
$YARN install --frozen-lockfile
$YARN workspaces run build
$YARN workspaces run lint
cd generated
JAVA=$JAVA SANDBOX=$SANDBOX JSON_API=$JSON_API DAR=$DAR $YARN test
