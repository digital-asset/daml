#!/usr/bin/env bash
# Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

cd "$(dirname "$0")"
eval "$(../dev-env/bin/dade-assist)"

# Note: not using Bazel run so we actually have access to local files to run
# `git tag`
bazel run --run_under "cd $PWD &&" //versions:update-versions -- -o $PWD/versions.bzl

# We refer to latest_stable_version so this might need changing.
bazel run @unpinned_maven//:pin
