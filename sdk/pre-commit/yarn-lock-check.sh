#!/usr/bin/env bash
# Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

TMP_DIR=$(mktemp -d)
cp package.json yarn.lock $TMP_DIR

(cd $TMP_DIR; yarn install --silent > /dev/null)

if ! diff yarn.lock $TMP_DIR/yarn.lock; then
    echo "FAIL: yarn.lock could not satisfy package.json" 1>&2
    echo "FAIL: yarn.lock requires all of the above changes" 1>&2
    exit 1
fi
