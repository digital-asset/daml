#!/usr/bin/env bash
# Copyright (c) 2020 The DAML Authors. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

## Functions

step() {
  echo "step: $*" >&2
}

## Main

cd "$(dirname "$0")"/..

if [[ "$(uname)" == "Darwin" ]]; then
    echo "Simulating release step..."
    echo "##vso[task.setvariable variable=has_released;isOutput=true]true"
    echo "##vso[task.setvariable variable=release_tag]$(cat VERSION)"
    exit 0
fi


step "loading dev-env"

eval "$(dev-env/bin/dade assist)"

# build
step "build release script"
bazel build //release:release

# set up temp location
step "set up temporary location"
release_dir="$(mktemp -d)"
step "temporary release directory is ${release_dir}"

if [[ "${BUILD_SOURCEBRANCHNAME:-}" == "master" ]]; then
    # set up bintray credentials
    mkdir -p ~/.jfrog
    cleanup() {
        rm -f ~/.jfrog/jfrog-cli.conf
    }
    trap cleanup EXIT
    echo "$JFROG_CONFIG_CONTENT" > ~/.jfrog/jfrog-cli.conf
    unset JFROG_CONFIG_CONTENT

    step "run release script (with --upload)"
    ./bazel-bin/release/release --artifacts release/artifacts.yaml --upload --log-level debug --release-dir "${release_dir}"
else
    step "run release script (dry run)"
    ./bazel-bin/release/release --artifacts release/artifacts.yaml --log-level debug --release-dir "${release_dir}"
    step "release artifacts got stored in ${release_dir}"
fi
