#!/usr/bin/env bash
# Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0


# Build the release artifacts required for running the compatibility
# tests against HEAD. At the moment this includes the SDK release tarball.

set -eou pipefail

cd "$(dirname "$0")/.."

eval "$(./dev-env/bin/dade-assist)"

# We allow overwriting this since on CI we build this in a separate step and upload it first
# before fetching it in another step.
HEAD_TARGET_DIR=${1:-compatibility/head_sdk}

git clean -fxd -e 'daml-*.tgz' $HEAD_TARGET_DIR

bazel build \
  --java_language_version=11 \
  --java_runtime_version=nixpkgs_java_11 \
  --tool_java_runtime_version=nixpkgs_java_11 \
  --tool_java_language_version=11 \
  //release:sdk-release-tarball \
  //daml-assistant:daml

cp -f bazel-bin/release/sdk-release-tarball-ce.tar.gz "$HEAD_TARGET_DIR"
cp -f bazel-bin/daml-assistant/daml "$HEAD_TARGET_DIR"
cp -f templates/create-daml-app-test-resources/messaging.patch "$HEAD_TARGET_DIR"
