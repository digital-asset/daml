#!/usr/bin/env bash
# Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# Checks if the sharable docs are synchronized (consistent with the manual and auto-generated docs)
# If they're not, automatically synchronizes them

set -euo pipefail

DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

TMP_DIR=$(mktemp -d)
cp -r $DIR/../docs/sharable $TMP_DIR

NEW_SHARABLE=$(mktemp -d)

$DIR/synchronize-docs.sh $NEW_SHARABLE

echo "Comparing the docs, expecting no diff:"
diff -r $NEW_SHARABLE $TMP_DIR/sharable # If there's any diff, the diff will return 1 and fail the script

echo "SUCCESS"
