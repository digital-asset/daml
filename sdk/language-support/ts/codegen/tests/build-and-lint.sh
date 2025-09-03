#!/usr/bin/env bash
# Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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
export YARN_CACHE_FOLDER=$TMP_DIR/yarn
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
CANTON=$(rlocation "$TEST_WORKSPACE/$4")
# language-support/ts/codegen/tests/daml/.daml/dist/daml-1.0.0.dar
DAR=$(rlocation "$TEST_WORKSPACE/$5")
# language-support/ts/codegen/tests/ts/package.json
PACKAGE_JSON=$(rlocation "$TEST_WORKSPACE/$6")
# language-support/ts/codegen/tests/ts
TS_DIR=$(dirname $PACKAGE_JSON)
DAML_TYPES=$(rlocation "$TEST_WORKSPACE/$7")
SDK_VERSION=${8}
UPLOAD_DAR=$(rlocation "$TEST_WORKSPACE/${9}")
HIDDEN_DAR=$(rlocation "$TEST_WORKSPACE/${10}")
GRPCURL=$(rlocation "$TEST_WORKSPACE/${11}" | xargs dirname)
DIFF="${12}"

TMP_DAML_TYPES=$TMP_DIR/daml-types

mkdir -p $TMP_DAML_TYPES

cp -rL $TS_DIR/* $TMP_DIR
cp -rL $DAML_TYPES/* $TMP_DAML_TYPES

cd $TMP_DIR

# Call daml2js.
PATH=`dirname $YARN`:$PATH $DAML2TS -o daml2js $DAR
PATH=$PATH:$GRPCURL

# yarn.lock includes local paths and hashes for daml.js; remove them
# before grepping
hide_changing_paths() {
    sed -Ee 's!^("@daml.js/)([0-9a-zA-Z\._-]+)@file:daml2js/\2":!\1...": # elided for diff!' \
        -e 's!( +"@daml.js/)[0-9a-zA-Z\._-]+" "file:.*"!\1..." "file:..." # elided for diff!' "$1"
}

# Build, lint, test.
cd build-and-lint-test
$YARN install > /dev/null
# when testing 0.0.0 only, simulate what
# yarn install --frozen-lockfile is supposed to do, because
# --frozen-lockfile appears to behave exactly like --pure-lockfile
# (see #14873)
if grep -qE '^    "@daml/types" "0.0.0"$' $TMP_DIR/yarn.lock && \
        ! "$DIFF" -du <(hide_changing_paths $TS_DIR/yarn.lock) <(hide_changing_paths $TMP_DIR/yarn.lock); then
    echo "FAIL: $TS_DIR/yarn.lock could not satisfy $TS_DIR/build-and-lint-test/package.json" 1>&2
    echo "FAIL: yarn.lock requires all of the above changes" 1>&2
    exit 1
fi
$YARN run build
$YARN run lint
# Invoke 'yarn test'. Control is thereby passed to
# 'language-support/ts/codegen/tests/ts/build-and-lint-test/src/__tests__/test.ts'.
JAVA=$JAVA CANTON=$CANTON DAR=$DAR UPLOAD_DAR=$UPLOAD_DAR HIDDEN_DAR=$HIDDEN_DAR $YARN test -t "${BUILD_AND_LINT_TEST_NAME_PATTERN:-.*}"
