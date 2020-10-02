#!/usr/bin/env bash
# Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0


set -euxo pipefail

LIMIT_BYTES=$1

SIZE_BYTES=`find sandbox -name "sandbox*.tgz" -exec wc -c {} \; | awk '{print $1}'`

echo "Checking Sandbox tarball size..."
if [ "${LIMIT_BYTES}" -lt "${SIZE_BYTES}" ]
then
    echo "ERROR: Sandbox tarball size (${SIZE_BYTES} bytes) is above accepted threshold ${LIMIT_BYTES} bytes."
    echo "Please check module dependencies to ensure that no redundant artifacts are compile time dependencies for the module."
    exit -1
else
    echo "Sandbox tarball size (${SIZE_BYTES} bytes) is acceptable."
fi
