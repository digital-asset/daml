#!/usr/bin/env bash
# Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -eou pipefail

SCRIPT_DIR=$PWD/$(dirname "$0")
BAZEL_BIN="$SCRIPT_DIR/../../bazel-bin"
SDK_VERSION=${DAML_SDK_RELEASE_VERSION:-0.0.0}

if [ "$#" -ne 1 ]; then
    echo "Expected exactly one argument to specify the target directory"
    exit 1
fi

bazel build //docs:sphinx-source-tree //docs:pdf-fonts-tar //docs:non-sphinx-html-docs

cp -f $BAZEL_BIN/docs/sphinx-source-tree.tar.gz $1/sphinx-source-tree-$SDK_VERSION.tar.gz
cp -f $BAZEL_BIN/docs/non-sphinx-html-docs.tar.gz $1/non-sphinx-html-docs-$SDK_VERSION.tar.gz
cp -f $BAZEL_BIN/docs/pdf-fonts-tar.tar.gz $1/pdf-fonts-$SDK_VERSION.tar.gz
