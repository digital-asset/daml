#!/usr/bin/env bash
# Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0


cd "$(dirname "$0")"
eval "$(../dev-env/bin/dade-assist)"

bazel run //versions:update-versions -- -o $PWD/versions.bzl

# We refer to latest_stable_version so this might need changing.
bazel run @unpinned_maven//:pin
