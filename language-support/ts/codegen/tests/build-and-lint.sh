#!/bin/bash
# Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -eou pipefail

# It's sometimes useful to be able to run this test in a filesystem
# that one can inspect.
if [[ -z "${BUILD_AND_LINT_TMP_DIR:-}" ]];
then
    TMP_DIR=$(mktemp -d)
    cleanup() {
        cd /
        rm -rf $TMP_DIR
    }
    trap cleanup EXIT
else
    TMP_DIR="$BUILD_AND_LINT_TMP_DIR"
    rm -rf $TMP_DIR && mkdir -p $TMP_DIR
fi
echo "Temp directory : $TMP_DIR"

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
# language-support/ts/codegen/tests/daml/.daml/dist/daml-1.0.0.dar
DAR=$(rlocation "$TEST_WORKSPACE/$6")
# language-support/ts/codegen/tests/ts/package.json
PACKAGE_JSON=$(rlocation "$TEST_WORKSPACE/$7")
# language-support/ts/codegen/tests/ts
TS_DIR=$(dirname $PACKAGE_JSON)
DAML_TYPES=$(rlocation "$TEST_WORKSPACE/$8")
DAML_LEDGER=$(rlocation "$TEST_WORKSPACE/$9")
SDK_VERSION=${10}

TMP_DAML_TYPES=$TMP_DIR/daml-types
TMP_DAML_LEDGER=$TMP_DIR/daml-ledger

mkdir -p $TMP_DAML_TYPES
mkdir -p $TMP_DAML_LEDGER

cp -rL $TS_DIR/* $TMP_DIR
cp -rL $DAML_TYPES/* $TMP_DAML_TYPES
cp -rL $DAML_LEDGER/* $TMP_DAML_LEDGER

cd $TMP_DIR

# Call daml2js.
PATH=`dirname $YARN`:$PATH $DAML2TS -o daml2js $DAR

# Build, lint, test.
cd build-and-lint-test
$YARN install --pure-lockfile > /dev/null
$YARN run build && $YARN run lint
# Invoke 'yarn test'. Control is thereby passed to
# 'language-support/ts/codegen/tests/ts/build-and-lint-test/src/__tests__/test.ts'.
JAVA=$JAVA SANDBOX=$SANDBOX JSON_API=$JSON_API DAR=$DAR $YARN test
