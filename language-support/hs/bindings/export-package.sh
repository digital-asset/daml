#!/usr/bin/env bash
# Copyright (c) 2019 The DAML Authors. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -eou pipefail

if [ "$#" -ne 1 ]; then
    echo "Expected exactly one argument."
    echo "Usage: ./build_packages.sh TARGET_DIR"
    exit 1
fi

TARGET_DIR=$1

bazel build //ledger-api/grpc-definitions:google-protobuf-haskellpb-sources //ledger-api/grpc-definitions:google-rpc-haskellpb-sources //ledger-api/grpc-definitions:ledger-api-haskellpb-sources //ledger-api/grpc-definitions:ledger-api-haskellpb-sources-admin //ledger-api/grpc-definitions:ledger-api-haskellpb-sources-testing

BAZEL_BIN=$(bazel info bazel-bin)

rm -rf gen
mkdir gen
mkdir gen/src

cp -r $BAZEL_BIN/ledger-api/grpc-definitions/Google gen/src/Google
cp -r $BAZEL_BIN/ledger-api/grpc-definitions/Com gen/src/Com

stack sdist --tar-dir $TARGET_DIR
