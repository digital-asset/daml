#!/usr/bin/env bash
# Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# run a dry release, binaries deployed to a local directory, see `release_dir` definition below
# this script tested on Linux, don't know if it works on macOS. Feel free to make corresponding changes

set -eux

release_dir=/var/tmp/daml-bintray-release

rm -rf "$release_dir" && bazel build //release:release && ./bazel-out/k8-fastbuild/bin/release/release --artifacts release/artifacts.yaml --release-dir "$release_dir"
