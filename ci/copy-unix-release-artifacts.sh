#!/usr/bin/env bash
# Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
set -euo pipefail

DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
cd $DIR/../sdk

RELEASE_TAG=$1
NAME=$2
OUTPUT_DIR=$3

mkdir -p $OUTPUT_DIR/github
mkdir -p $OUTPUT_DIR/artifactory
# Artifacts that we only use in the split-release process
mkdir -p $OUTPUT_DIR/split-release
mkdir -p $OUTPUT_DIR/oci


TARBALL=daml-sdk-$RELEASE_TAG-$NAME.tar.gz
EE_TARBALL=daml-sdk-$RELEASE_TAG-$NAME-ee.tar.gz
cp bazel-bin/release/sdk-release-tarball-ce.tar.gz $OUTPUT_DIR/github/$TARBALL
# Used for the non-split release process.
cp bazel-bin/release/sdk-release-tarball-ee.tar.gz $OUTPUT_DIR/artifactory/$EE_TARBALL
# Used for the split release process.
cp bazel-bin/release/sdk-release-tarball-ee.tar.gz $OUTPUT_DIR/split-release/$EE_TARBALL

DAMLC=damlc-$RELEASE_TAG-$NAME.tar.gz
cp bazel-bin/compiler/damlc/damlc-dist.tar.gz $OUTPUT_DIR/split-release/$DAMLC

DAML2JS=daml2js-$RELEASE_TAG-$NAME.tar.gz
cp bazel-bin/language-support/ts/codegen/daml2js-dist.tar.gz $OUTPUT_DIR/oci/$DAML2JS


# Platform independent artifacts are only built on Linux.
if [[ "$NAME" == "linux-intel" ]]; then
    PROTOS_ZIP=protobufs-$RELEASE_TAG.zip
    cp bazel-bin/release/protobufs.zip $OUTPUT_DIR/github/$PROTOS_ZIP

    SCRIPT=daml-script-$RELEASE_TAG.jar
    cp bazel-bin/daml-script/runner/daml-script-binary_distribute.jar $OUTPUT_DIR/artifactory/$SCRIPT

    CODEGEN=codegen-$RELEASE_TAG.jar
    cp bazel-bin/language-support/java/codegen/binary.jar $OUTPUT_DIR/oci/$CODEGEN
    
    BUNDLED_VSIX=daml-bundled-$RELEASE_TAG.vsix
    cp bazel-bin/compiler/daml-extension/daml-bundled.vsix $OUTPUT_DIR/oci/$BUNDLED_VSIX
    cp bazel-bin/compiler/daml-extension/webview-stylesheet.css $OUTPUT_DIR/oci/webview-stylesheet.css


    mkdir -p $OUTPUT_DIR/split-release/daml-libs/daml-script
    cp bazel-bin/daml-script/daml/*.dar $OUTPUT_DIR/split-release/daml-libs/daml-script/

    mkdir -p $OUTPUT_DIR/split-release/docs

    cp bazel-bin/docs/sphinx-source-tree.tar.gz $OUTPUT_DIR/split-release/docs/sphinx-source-tree-$RELEASE_TAG.tar.gz
    cp bazel-bin/docs/sphinx-source-tree-deps.tar.gz $OUTPUT_DIR/split-release/docs/sphinx-source-tree-deps-$RELEASE_TAG.tar.gz
    cp bazel-bin/docs/pdf-fonts-tar.tar.gz $OUTPUT_DIR/split-release/docs/pdf-fonts-$RELEASE_TAG.tar.gz
    cp bazel-bin/docs/non-sphinx-html-docs.tar.gz $OUTPUT_DIR/split-release/docs/non-sphinx-html-docs-$RELEASE_TAG.tar.gz

    cp bazel-bin/test-evidence/daml-security-test-evidence.csv $OUTPUT_DIR/github/daml-security-test-evidence-$RELEASE_TAG.csv
    cp bazel-bin/test-evidence/daml-security-test-evidence.json $OUTPUT_DIR/github/daml-security-test-evidence-$RELEASE_TAG.json
fi
