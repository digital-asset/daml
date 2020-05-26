#!/usr/bin/env bash
# Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -eou pipefail

if [ "$#" -ne 1 ]; then
    echo "Expected exactly one argument."
    echo "Usage: ${BASH_SOURCE[0]} TARGET_DIR"
    exit 1
fi

TARGET_DIR=$(realpath $1)

cd "$(dirname ${BASH_SOURCE[0]})"

bazel build //ledger-api/grpc-definitions:all-ledger-api-haskellpb-sources

BAZEL_BIN=$(bazel info bazel-bin)

rm -rf gen
mkdir gen
mkdir gen/src

cp -rp $BAZEL_BIN/ledger-api/grpc-definitions/Google gen/src/Google
cp -rp $BAZEL_BIN/ledger-api/grpc-definitions/Com gen/src/Com

stack sdist --tar-dir $TARGET_DIR
