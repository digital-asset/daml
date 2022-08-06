#!/usr/bin/env bash
# Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -eou pipefail

if [ "$#" -ne 1 ]; then
    echo "Expected exactly one argument."
    echo "Usage: ./extract-ts-libs.sh path/to/canton-coin/nix/vendored"
    exit 1
fi

bazel run //language-support/ts/daml-types:npm_package.pack -- --pack-destination $1
bazel run //language-support/ts/daml-ledger:npm_package.pack -- --pack-destination $1/
bazel run //language-support/ts/daml-lapi:npm_package.pack -- --pack-destination $1/
