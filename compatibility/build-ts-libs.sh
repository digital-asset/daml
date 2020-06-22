#!/usr/bin/env bash
# Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -eou pipefail

cd "$(dirname "$0")/.."

eval "$(./dev-env/bin/dade-assist)"

# We allow overwriting this since on CI we build this in a separate step and upload it first
# before fetching it in another step.
HEAD_TARGET_DIR=${1:-compatibility/head_sdk}

VERSION=${DAML_SDK_RELEASE_VERSION:-0.0.0}

function cleanup {
  rm -rf "daml-types-${VERSION}.tgz" "daml-ledger-${VERSION}.tgz" "daml-react-${VERSION}.tgz"
}

trap cleanup EXIT

bazel run //language-support/ts/daml-types:npm_package.pack
bazel run //language-support/ts/daml-ledger:npm_package.pack
bazel run //language-support/ts/daml-react:npm_package.pack


#cp -f daml-types-0.0.0.tgz daml-react-0.0.0.tgz daml-ledger-0.0.0.tgz "$HEAD_TARGET_DIR"
cp -f daml-types-${VERSION}.tgz daml-react-${VERSION}.tgz daml-ledger-${VERSION}.tgz "$HEAD_TARGET_DIR"
