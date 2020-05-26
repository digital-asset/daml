#!/usr/bin/env bash
# Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0


SCRIPT_DIR=$(dirname "$0")
cd $SCRIPT_DIR
BUILD_DIR=$(cd ..; pwd)/build

trap cleanup 1 2 3 6

cleanup()
{
  echo "Caught Signal ... cleaning up."
  rm -rf $BUILD_DIR
  echo "Done cleanup ... quitting."
  exit 1
}

rm -rf $BUILD_DIR
mkdir $BUILD_DIR

bazel build //docs:docs
tar -zxf ../../bazel-bin/docs/html.tar.gz -C $BUILD_DIR
cd $BUILD_DIR/html
python -m http.server 8000
