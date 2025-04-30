#!/usr/bin/env bash
# Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# Checks if the sharable docs are synchronized (consistent with the manual and auto-generated docs)
# If they're not, automatically synchronizes them

set -euo pipefail

DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

OLD_SHARABLE=$DIR/../docs/sharable
NEW_SHARABLE=$(mktemp -d)
trap "rm -rf $NEW_SHARABLE" EXIT

$DIR/synchronize-docs.sh $NEW_SHARABLE

echo "Comparing the docs, expecting no diff:"
diff -r $NEW_SHARABLE $OLD_SHARABLE # If there's any diff, the diff will return 1 and fail the script

echo "SUCCESS"
