#!/usr/bin/env bash
# Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
set -eou pipefail

RELEASE_TAG=$1
OUTPUT_DIR=$2
SPLIT_RELEASE=${3:-false}

mkdir -p $OUTPUT_DIR/github
mkdir -p $OUTPUT_DIR/artifactory
mkdir -p $OUTPUT_DIR/split-release

TARBALL=daml-sdk-$RELEASE_TAG-windows.tar.gz
EE_TARBALL=daml-sdk-$RELEASE_TAG-windows-ee.tar.gz
cp bazel-bin/release/sdk-release-tarball-ce.tar.gz "$OUTPUT_DIR/github/$TARBALL"
# Used for the non-split release process.
cp bazel-bin/release/sdk-release-tarball-ee.tar.gz "$OUTPUT_DIR/artifactory/$EE_TARBALL"
# Used for the split release process.
cp bazel-bin/release/sdk-release-tarball-ee.tar.gz "$OUTPUT_DIR/split-release/$EE_TARBALL"
