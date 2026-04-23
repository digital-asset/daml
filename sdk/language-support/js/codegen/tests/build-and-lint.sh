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
export PNPM_STORE_DIR=$TMP_DIR/pnpm-store
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
PNPM=$(rlocation "$TEST_WORKSPACE/$2")
CODEGEN_JS=$(rlocation "$TEST_WORKSPACE/$3")
CANTON=$(rlocation "$TEST_WORKSPACE/$4")
# language-support/js/codegen/tests/daml/.daml/dist/daml-1.0.0.dar
DAR=$(rlocation "$TEST_WORKSPACE/$5")
# language-support/js/codegen/tests/ts/package.json
PACKAGE_JSON=$(rlocation "$TEST_WORKSPACE/$6")
# language-support/js/codegen/tests/ts
TS_DIR=$(dirname $PACKAGE_JSON)
DAML_TYPES=$(rlocation "$TEST_WORKSPACE/$7")
SDK_VERSION=${8}
UPLOAD_DAR=$(rlocation "$TEST_WORKSPACE/${9}")
HIDDEN_DAR=$(rlocation "$TEST_WORKSPACE/${10}")
GRPCURL=$(rlocation "$TEST_WORKSPACE/${11}" | xargs dirname)

TMP_DAML_TYPES=$TMP_DIR/daml-types

mkdir -p $TMP_DAML_TYPES

cp -rL $TS_DIR/* $TMP_DIR
cp -rL $DAML_TYPES/* $TMP_DAML_TYPES

cd $TMP_DIR

# Call codegen-js.
$CODEGEN_JS -o daml2js -V 2 $DAR
PATH=$PATH:$GRPCURL

# Build, lint, test.
cd build-and-lint-test
$PNPM install --no-frozen-lockfile > /dev/null
$PNPM run build
$PNPM run lint
# Invoke 'pnpm test'. Control is thereby passed to
# 'language-support/js/codegen/tests/ts/build-and-lint-test/src/__tests__/test.ts'.
JAVA=$JAVA CANTON=$CANTON DAR=$DAR UPLOAD_DAR=$UPLOAD_DAR HIDDEN_DAR=$HIDDEN_DAR $PNPM test -t "${BUILD_AND_LINT_TEST_NAME_PATTERN:-.*}"
