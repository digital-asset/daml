#!/usr/bin/env bash
# Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
SHARABLE_DIR="$DIR/../docs/sharable"
MANUAL_DIR="$DIR/../docs/manually-written"

cd $DIR/..

bazel build //docs:sharable-docs

rm -Rf $SHARABLE_DIR
mkdir -p $SHARABLE_DIR

tar -zxf $DIR/../bazel-bin/docs/sharable-docs.tar.gz -C $SHARABLE_DIR --strip-components=2


if [ -d "$MANUAL_DIR" ] && [ "$(ls $MANUAL_DIR)" ]; then
  cp -a $MANUAL_DIR/* $SHARABLE_DIR/
fi

rm -f $SHARABLE_DIR/LICENSE
rm -f $SHARABLE_DIR/NOTICES

# Quick, temporary fix: remove the file after generation until we properly remove the unnecessary logic of creating it
# TODO: Remove it after completing https://github.com/digital-asset/daml/issues/20813
rm -Rf $SHARABLE_DIR/app-dev/grpc/proto-docs.rst
