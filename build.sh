#!/usr/bin/env bash
# Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

eval "$("$(dirname "$0")/dev-env/bin/dade-assist")"

execution_log_postfix=${1:-}

export LC_ALL=en_US.UTF-8

ARTIFACT_DIRS="${BUILD_ARTIFACTSTAGINGDIRECTORY:-$PWD}"

tag_filter=""
if [[ "$execution_log_postfix" == "_Darwin" ]]; then
  tag_filter="-dont-run-on-darwin,-scaladoc,-pdfdocs"
fi

df -h

du -h .bazel-cache --summarize
du -h bazel-s/ --summarize
du -h compatibility/.bazel-cache --summarize
du -h compatibility/bazel-compatibility/ --summarize
