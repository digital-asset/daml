#!/usr/bin/env bash
# Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

#
# Golden scenario tests
#

set -eu

export LC_ALL="en_US.UTF-8"

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

REPL=$(rlocation "$TEST_WORKSPACE/$1")
DAMLC=$(rlocation "$TEST_WORKSPACE/$2")
TESTMAIN=$(rlocation "$TEST_WORKSPACE/$3")
DIFF="$4"
if [ "$5" = "true" ] ; then
  DEVFLAG="--dev"
  TARGETFLAG="--target 1.dev"
else
  DEVFLAG=""
  TARGETFLAG=""
fi
TESTDIR="$(dirname $TESTMAIN)"
TESTDAR="$TESTDIR/Main.dar"

REGEX_HIDE_HASHES="s,@[a-z0-9]{8},@XXXXXXXX,g"

$DAMLC package --enable-scenarios=yes $TARGETFLAG --debug $TESTMAIN 'main' -o $TESTDAR

$REPL $DEVFLAG test Test:run $TESTDAR | sed -E "$REGEX_HIDE_HASHES" > ${TESTDIR}/ACTUAL.ledger

$DIFF -u --strip-trailing-cr ${TESTDIR}/EXPECTED.ledger ${TESTDIR}/ACTUAL.ledger
