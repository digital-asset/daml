# Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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

set -euo pipefail

DAMLC=$(rlocation $TEST_WORKSPACE/$1)
JQ=$(rlocation $TEST_WORKSPACE/$2)
TEST_DAR=$(rlocation $TEST_WORKSPACE/$3)
MAIN_PKG_ID=$($DAMLC inspect-dar $TEST_DAR | sed '0,/^DAR archive contains the following packages:$/d' | sed -n 's/^model-tests-1.0.0-[^"]*"\(.*\)"$/\1/p')
JSON_MAIN_PKG_ID=$($DAMLC inspect-dar --json $TEST_DAR | $JQ -r '.main_package_id')
if [[ "$MAIN_PKG_ID" != "$JSON_MAIN_PKG_ID" ]]; then
    echo "Mismatch in package ids:"
    echo "$MAIN_PKG_ID"
    echo "$JSON_MAIN_PKG_ID"
    exit 1
fi
OUT=$($DAMLC inspect-dar --json $TEST_DAR | $JQ ".packages.\"$MAIN_PKG_ID\"")
KEYS="$($JQ -c 'keys' <<<"$OUT")"
if [[ "$KEYS" != '["name","path","version"]' ]]; then
    echo "Unexpected keys:"
    echo "$KEYS"
    exit 1
fi
NAME=$($JQ -r '.name' <<<"$OUT")
if [[ "$NAME" != "model-tests" ]]; then
    echo "Unexpected name:"
    echo "$NAME"
    exit 1
fi
VERSION=$($JQ -r '.version' <<<"$OUT")
if [[ "$VERSION" != "1.0.0" ]]; then
    echo "Unexpected version:"
    echo "$VERSION"
    exit 1
fi
PATH=$($JQ -r '.path' <<<"$OUT")
if [[ "$PATH" != "model-tests-1.0.0-$MAIN_PKG_ID/model-tests-1.0.0-$MAIN_PKG_ID.dalf" ]]; then
    echo "Unexpected path:"
    echo "$PATH"
    exit 1
fi
