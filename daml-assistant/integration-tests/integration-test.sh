#!/bin/bash
# Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
set -euxo pipefail
if [[ $(uname -s) != "Linux" ]]; then
    # Outside of Linux this is going to try to use a release tarball
    # not meant for Linux in a Linux docker container which is going
    # to go wrong.

    # There does not seem to be a nice way to disable an sh_test on certain platforms
    echo "Skipping integration test since it only runs on Linux"
    exit 0
fi
DOCKER="$1"
DOCKERFILE="$2"
TARBALL="$3"
TEST_SCRIPT="$4"

export PATH="$(cd $(dirname $DOCKER); pwd -P)":$PATH
BUILD_DIR="$(mktemp -d)"

cp "$TEST_SCRIPT" "$TARBALL" "$BUILD_DIR"
docker build --ulimit nofile=1024 -t daml-integration-tests -f "$DOCKERFILE" "$BUILD_DIR/"
docker run --ulimit nofile=1024 -t --rm daml-integration-tests "/data/run-test.sh"
