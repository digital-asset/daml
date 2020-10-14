#!/usr/bin/env bash
# Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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

TEST_RUNNER=$(rlocation $TEST_WORKSPACE/$1)
DAR_FILE=$(rlocation $TEST_WORKSPACE/$2)
DIFF=$3
GREP=$4
SORT=$5

set +e
TEST_OUTPUT="$($TEST_RUNNER --dar=$DAR_FILE --max-inbound-message-size 41943040 2>&1)"
TEST_RESULT=$?
set -e

echo "-- Runner Output -----------------------" >&2
echo "$TEST_OUTPUT" >&2
echo "----------------------------------------" >&2

FAIL=

if [[ $TEST_RESULT = 0 ]]; then
  FAIL=1
  echo "Expected non-zero exit-code." >&2
fi

EXPECTED="$($SORT <<'EOF'
MultiTest:listKnownPartiesTest SUCCESS
MultiTest:multiTest SUCCESS
MultiTest:partyIdHintTest SUCCESS
ScriptExample:test SUCCESS
ScriptTest:failingTest FAILURE (com.daml.lf.speedy.SError$DamlEUserError)
ScriptTest:listKnownPartiesTest SUCCESS
ScriptTest:test0 SUCCESS
ScriptTest:test1 SUCCESS
ScriptTest:test3 SUCCESS
ScriptTest:test4 SUCCESS
ScriptTest:testCreateAndExercise SUCCESS
ScriptTest:testKey SUCCESS
ScriptTest:testGetTime SUCCESS
ScriptTest:testSetTime SUCCESS
ScriptTest:traceOrder SUCCESS
ScriptTest:partyIdHintTest SUCCESS
ScriptTest:sleepTest SUCCESS
ScriptExample:initializeFixed SUCCESS
ScriptTest:testStack SUCCESS
ScriptTest:testMaxInboundMessageSize SUCCESS
ScriptTest:testQueryContractId SUCCESS
ScriptTest:testQueryContractKey SUCCESS
ScriptTest:testMultiPartyQueries SUCCESS
EOF
)"

ACTUAL="$(echo -n "$TEST_OUTPUT" | $GREP "SUCCESS\|FAILURE" | $SORT)"

if ! $DIFF -du0 --label expected <(echo -n "$EXPECTED") --label actual <(echo -n "$ACTUAL") >&2; then
  FAIL=1
fi

if [[ $FAIL = 1 ]]; then
  exit 1
fi
