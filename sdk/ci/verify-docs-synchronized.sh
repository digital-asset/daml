#!/usr/bin/env bash
# Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# Checks if the sharable docs are synchronized (consistent with the manual and auto-generated docs)
# If they're not, automatically synchronizes them

set -euo pipefail

DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

DIR_TO_CHECK=$DIR/../docs/sharable
TEMP_DIR=$(mktemp -d)

cp -r $DIR_TO_CHECK $TEMP_DIR

$DIR/synchronize-docs.sh

echo "Comparing the docs, expecting no diff:"
diff -r $DIR_TO_CHECK $TEMP_DIR/sharable # If there's any diff, the diff will return 1 and fail the script

echo "SUCCESS"
