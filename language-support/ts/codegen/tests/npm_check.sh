#!/usr/bin/env bash
# Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# This test checks that the package-lock.json file gets updated by npm when a generated dependency
# changes.

set -eou pipefail

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

DAMLJS=$(rlocation "$TEST_WORKSPACE/$1")
NPM=$(rlocation "$TEST_WORKSPACE/$2")
JQ=$(rlocation "$TEST_WORKSPACE/$3")
DAR1=$(rlocation "$TEST_WORKSPACE/$4")
DAR2=$(rlocation "$TEST_WORKSPACE/$5")
TMP_DIR=$(mktemp -d)
mkdir -p "$TMP_DIR/.npm"
export npm_config_cache="$TMP_DIR/.npm"

cleanup() {
    rm -rf "$TMP_DIR"
}
trap cleanup EXIT

cat <<EOF > "$TMP_DIR"/package.json
{
  "name": "test",
  "version": "0.1.0",
  "private": true,
  "dependencies": {
    "@daml.js/npmcheck": "file:daml.js/npmcheck1-1.0.0"
  }
}
EOF

$DAMLJS "$DAR1" -o "$TMP_DIR"/daml.js
cd "$TMP_DIR"
$NPM install
PACKAGELOCK1=$($JQ '.dependencies | length' < "$TMP_DIR"/package-lock.json)

rm "$TMP_DIR"/package-lock.json
rm "$TMP_DIR"/package.json

cat <<EOF > "$TMP_DIR"/package.json
{
  "name": "test",
  "version": "0.1.0",
  "private": true,
  "dependencies": {
    "@daml.js/npmcheck": "file:daml.js/npmcheck2-1.0.0"
  }
}
EOF

$DAMLJS "$DAR2" -o "$TMP_DIR"/daml.js
$NPM install
PACKAGELOCK2=$($JQ '.dependencies | length' < "$TMP_DIR"/package-lock.json)

RES=$((PACKAGELOCK2 - PACKAGELOCK1))

if [ "$RES" == 1 ]; then
  echo Passed
  exit 0
else
  echo Failed: package-lock.json did not get updated.
  exit 1
fi
