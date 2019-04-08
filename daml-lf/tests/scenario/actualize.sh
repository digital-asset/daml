#!/usr/bin/env bash
# Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

#
# Actualize Golden scenario tests expected ledger
#
# Note: Use absolute path for the target DAML file as bazel seems to set the current directory...
#
# Usage: actualize.sh <path-to-daml-file>
#

set -eux

TESTMAIN=$1
TESTDIR="$(dirname $TESTMAIN)"
TESTDALF="$TESTDIR/Main.dalf"
BAZEL_BIN="$(bazel info bazel-bin)"
GHC_PRIM_DALF=${BAZEL_BIN}/daml-foundations/daml-ghc/package-database/deprecated/daml-prim-1.3.dalf
REGEX_HIDE_HASHES="s,@[a-z0-9]{8},@XXXXXXXX,g"

bazel build //daml-foundations/daml-tools/da-hs-damlc-app:da-hs-damlc-app
../../../bazel-bin/daml-foundations/daml-tools/da-hs-damlc-app/da-hs-damlc-app compile --target 1.3 $TESTMAIN -o $TESTDALF

bazel build //daml-lf/repl:repl
../../../bazel-bin/daml-lf/repl/repl test Test:run $TESTDALF $GHC_PRIM_DALF | sed -E "$REGEX_HIDE_HASHES" | tee ${TESTDIR}/EXPECTED.ledger.new

