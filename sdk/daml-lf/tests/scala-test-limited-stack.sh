#!/usr/bin/env sh
# Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# Arguments:
#   $1   = the classpath of the tests
#   $2   = the location of the test jar

set -e
set -x

CLASSPATH="$1"
TESTJAR="$2"

echo "Running test $TESTJAR..."
java -Xms1m -Xmx2048m -cp "$CLASSPATH" \
  org.scalatest.tools.Runner \
  -R "$TESTJAR" \
  -eF
