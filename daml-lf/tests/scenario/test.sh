#!/usr/bin/env bash
# Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

#
# Golden scenario tests
#

set -eu

export LC_ALL="en_US.UTF-8"

REPL=$1
DAMLI=$2
TESTMAIN=$3
TESTDIR="$(dirname $TESTMAIN)"
TESTDALF="$TESTDIR/Main.dalf"

GHC_PRIM_DALF=${4:-""}
TARGET="1.3"

REGEX_HIDE_HASHES="s,@[a-z0-9]{8},@XXXXXXXX,g"

$DAMLI compile --debug --target $TARGET $TESTMAIN -o $TESTDALF

$REPL test Test:run $TESTDALF $GHC_PRIM_DALF | sed '1d' | sed -E "$REGEX_HIDE_HASHES" > ${TESTDIR}/ACTUAL.ledger

diff ${PWD}/${TESTDIR}/ACTUAL.ledger ${PWD}/${TESTDIR}/EXPECTED.ledger
