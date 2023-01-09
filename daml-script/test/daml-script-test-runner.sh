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

TEST_RUNNER=$(rlocation $TEST_WORKSPACE/$1)
DAR_FILE=$(rlocation $TEST_WORKSPACE/$2)
DIFF=$3
GREP=$4
SED=$5
SANDBOX=$(rlocation $TEST_WORKSPACE/$6)

PORTFILE_DIR=$(mktemp -d)
PORTFILE=$PORTFILE_DIR/port-file
$SANDBOX run-legacy-cli-config --participant=participant-id=example,port=0,port-file=$PORTFILE --stack-traces=true --static-time --enable-user-management=true &
SANDBOX_PID=$!

cleanup() {
    kill $SANDBOX_PID || true
    rm -rf $PORTFILE_DIR
}

trap cleanup EXIT

while [ ! -f $PORTFILE ]
do
  sleep 0.5
done

PORT=$(cat $PORTFILE)

set +e
TEST_OUTPUT="$($TEST_RUNNER --all --static-time --dar=$DAR_FILE --max-inbound-message-size 41943040 --ledger-host localhost --ledger-port $PORT 2>&1)"
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

EXPECTED="$(cat <<'EOF'
MultiTest:listKnownPartiesTest SUCCESS
MultiTest:multiTest SUCCESS
MultiTest:partyIdHintTest SUCCESS
ScriptExample:allocateParties SUCCESS
ScriptExample:initializeFixed SUCCESS
ScriptExample:initializeUser SUCCESS
ScriptExample:test SUCCESS
ScriptTest:clearUsers SUCCESS
ScriptTest:failingTest FAILURE (com.daml.lf.engine.script.ScriptF$FailedCmd: Command submit failed: FAILED_PRECONDITION: DAML_INTERPRETATION_ERROR(9,XXXXXXXX): Interpretation error: Error: Unhandled Daml exception: DA.Exception.AssertionFailed:AssertionFailed@3f4deaf1{ message = "Assertion failed" }. Details: Last location: [DA.Internal.Exception:168], partial transaction:
ScriptTest:listKnownPartiesTest SUCCESS
ScriptTest:multiPartySubmission SUCCESS
ScriptTest:partyIdHintTest SUCCESS
ScriptTest:sleepTest SUCCESS
ScriptTest:stackTrace FAILURE (com.daml.lf.engine.script.ScriptF$FailedCmd: Command submit failed: FAILED_PRECONDITION: DAML_INTERPRETATION_ERROR(9,XXXXXXXX): Interpretation error: Error: Unhandled Daml exception: DA.Exception.AssertionFailed:AssertionFailed@3f4deaf1{ message = "Assertion failed" }. Details: Last location: [DA.Internal.Exception:168], partial transaction:
ScriptTest:test0 SUCCESS
ScriptTest:test1 SUCCESS
ScriptTest:test3 SUCCESS
ScriptTest:test4 SUCCESS
ScriptTest:testCreateAndExercise SUCCESS
ScriptTest:testGetTime SUCCESS
ScriptTest:testKey SUCCESS
ScriptTest:testMaxInboundMessageSize SUCCESS
ScriptTest:testMultiPartyQueries SUCCESS
ScriptTest:testQueryContractId SUCCESS
ScriptTest:testQueryContractKey SUCCESS
ScriptTest:testSetTime SUCCESS
ScriptTest:testStack SUCCESS
ScriptTest:testUserListPagination SUCCESS
ScriptTest:testUserManagement SUCCESS
ScriptTest:testUserRightManagement SUCCESS
ScriptTest:traceOrder SUCCESS
ScriptTest:tree SUCCESS
ScriptTest:tupleKey SUCCESS
EOF
)"

# We strip away the actual partial transaction since contract ids are not deterministic.
ACTUAL="$(echo -n "$TEST_OUTPUT" | $GREP "SUCCESS\|FAILURE" | $SED 's/partial transaction: .*$/partial transaction:/g; s/INTERPRETATION_ERROR(\([[:digit:]]\),.\{8\})/INTERPRETATION_ERROR(\1,XXXXXXXX)/g')"

if ! $DIFF -du0 --label expected <(echo -n "$EXPECTED") --label actual <(echo -n "$ACTUAL") >&2; then
  FAIL=1
fi

if [[ $FAIL = 1 ]]; then
  exit 1
fi
