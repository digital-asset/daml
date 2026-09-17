#!/usr/bin/env bash
# Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
set -euo pipefail

RELEASE_TAG=$1
SOURCE_DIR=$2
OUTPUT_DIR=$3

cp -r $SOURCE_DIR/split-release $OUTPUT_DIR/split-release
