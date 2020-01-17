#!/bin/bash
# Copyright (c) 2020 The DAML Authors. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

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

JAVA=$(rlocation "$TEST_WORKSPACE/$1")
YARN=$(rlocation "$TEST_WORKSPACE/$2")
DAML2TS=$(rlocation "$TEST_WORKSPACE/$3")
SANDBOX=$(rlocation "$TEST_WORKSPACE/$4")
JSON_API=$(rlocation "$TEST_WORKSPACE/$5")
DAR=$(rlocation "$TEST_WORKSPACE/$6")
PACKAGE_JSON=$(rlocation "$TEST_WORKSPACE/$7")
TS_DIR=$(dirname $PACKAGE_JSON)
DAML_JSON_TYPES=$(rlocation "$TEST_WORKSPACE/$8")
DAML_LEDGER_FETCH=$(rlocation "$TEST_WORKSPACE/$9")

TMP_DIR=$(mktemp -d)
TMP_DAML_JSON_TYPES=$TMP_DIR/daml-json-types
TMP_DAML_LEDGER_FETCH=$TMP_DIR/daml-ledger-fetch
cleanup() {
  cd /
  rm -rf $TMP_DIR
}
trap cleanup EXIT
echo "TMP_DIR = $TMP_DIR"
mkdir -p $TMP_DAML_JSON_TYPES
mkdir -p  $TMP_DAML_LEDGER_FETCH

cp -rL $TS_DIR/* $TMP_DIR
cp -rL $DAML_JSON_TYPES/* $TMP_DAML_JSON_TYPES
cp -rL $DAML_LEDGER_FETCH/* $TMP_DAML_LEDGER_FETCH

cd $TMP_DIR

$DAML2TS -o generated/src/daml --main-package-name daml-tests $DAR
$YARN install --frozen-lockfile
$YARN workspaces run build
$YARN workspaces run lint
cd generated
JAVA=$JAVA SANDBOX=$SANDBOX JSON_API=$JSON_API DAR=$DAR $YARN test
