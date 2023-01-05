#!/usr/bin/env bash
# Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
set -euo pipefail

RELEASE_TAG=$1
SOURCE_DIR=$2
OUTPUT_DIR=$3

mkdir -p $OUTPUT_DIR/github

for file in $SOURCE_DIR/github/*; do
    # Copy as a placeholder for potential tweaks we might want to do here.
    cp $file $OUTPUT_DIR/github
done

cp -r $SOURCE_DIR/split-release $OUTPUT_DIR/split-release
