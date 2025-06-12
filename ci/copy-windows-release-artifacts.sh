#!/usr/bin/env bash
# Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
set -eou pipefail

DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
cd $DIR/../sdk

RELEASE_TAG=$1
OUTPUT_DIR=$2
SPLIT_RELEASE=${3:-false}

mkdir -p $OUTPUT_DIR/github
mkdir -p $OUTPUT_DIR/artifactory
mkdir -p $OUTPUT_DIR/split-release

TARBALL=daml-sdk-$RELEASE_TAG-windows.tar.gz
EE_TARBALL=daml-sdk-$RELEASE_TAG-windows-ee.tar.gz
echo "Copyring SDK tarball CE to $OUTPUT_DIR/github/$TARBALL"
cp -v bazel-bin/release/sdk-release-tarball-ce.tar.gz "$OUTPUT_DIR/github/$TARBALL"
# Used for the non-split release process.
echo "Copying SDK tarball EE to $OUTPUT_DIR/artifactory/$TARBALL"
cp -v bazel-bin/release/sdk-release-tarball-ee.tar.gz "$OUTPUT_DIR/artifactory/$EE_TARBALL"
# Used for the split release process.
echo "Copying SDK tarball EE to $OUTPUT_DIR/split-release/$EE_TARBALL"
cp -v bazel-bin/release/sdk-release-tarball-ee.tar.gz "$OUTPUT_DIR/split-release/$EE_TARBALL"

DAMLC=damlc-$RELEASE_TAG-windows.tar.gz
echo "Copying damlc to $OUTPUT_DIR/split-release/$DAMLC"
cp -v bazel-bin/compiler/damlc/damlc-dist.tar.gz "$OUTPUT_DIR/split-release/$DAMLC"

DAML2JS=daml2js-$RELEASE_TAG-windows.tar.gz
echo "Copying daml2js to $OUTPUT_DIR/split-release/$DAML2JS"
cp -v bazel-bin/language-support/ts/codegen/daml2js-dist.tar.gz "$OUTPUT_DIR/split-release/$DAML2JS"

