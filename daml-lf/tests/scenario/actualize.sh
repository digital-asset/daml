#!/usr/bin/env bash
# Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

#
# Actualize Golden scenario tests expected ledger
#
# Note: Use absolute path for the target DAML file as bazel seems to set the current directory...
#
# Usage: actualize.sh <path-to-daml-file>
#

set -eux

export LC_ALL="en_US.UTF-8"

TESTMAIN=$1
TESTDIR="$(dirname $TESTMAIN)"
TESTDAR="$TESTDIR/Main.dar"
BAZEL_BIN="$(bazel info bazel-bin)"
REGEX_HIDE_HASHES="s,@[a-z0-9]{8},@XXXXXXXX,g"

bazel build //compiler/damlc:damlc
../../../bazel-bin/compiler/damlc/damlc package --debug --target 1.dev $TESTMAIN main -o $TESTDAR

bazel build //daml-lf/repl:repl
../../../bazel-bin/daml-lf/repl/repl test Test:run $TESTDAR | sed '1d' | sed -E "$REGEX_HIDE_HASHES" | tee ${TESTDIR}/EXPECTED.ledger.new

