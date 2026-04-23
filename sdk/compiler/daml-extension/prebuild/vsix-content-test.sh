#!/usr/bin/env bash
# Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -exuo pipefail

VSIX_PATH="$1"
PREBUILD_PATH="$2"
DIFF="$3"

HASH1=$(mktemp hash1.XXXXXXXX.txt)
HASH2=$(mktemp hash2.XXXXXXXX.txt)

unzip -v $VSIX_PATH | awk '{print $8, $7}' | sort > "$HASH1"
unzip -v $PREBUILD_PATH | awk '{print $8, $7}' | sort > "$HASH2"
$DIFF -u "$HASH1" "$HASH2"
