#!/usr/bin/env bash
# Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

## Functions

step() {
  echo "step: $*" >&2
}

## Main

cd "$(dirname "$0")"/..

step "loading dev-env"

eval "$(dev-env/bin/dade assist)"

# build
step "build release script"
bazel build //release:release

if [[ "${BUILD_SOURCEBRANCHNAME:-}" == "master" ]]; then
    # set up bintray credentials
    mkdir -p ~/.jfrog
    echo "$JFROG_CONFIG_CONTENT" > ~/.jfrog/jfrog-cli.conf
    unset JFROG_CONFIG_CONTENT

    step "run release script (with --upload)"
    ./bazel-bin/release/release --artifacts release/artifacts.yaml --upload --log-level debug --release-dir "$(mktemp -d)"
else
    step "run release script (dry run)"
    ./bazel-bin/release/release --artifacts release/artifacts.yaml --log-level debug --release-dir "$(mktemp -d)"
fi
