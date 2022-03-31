#!/usr/bin/env bash
# Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

# This file is run by Bazel to create `bazel-out/stable-status.txt`, which individual
# rules can depend on.

echo "STABLE_SCALATEST_UTILS_VERSION $(cd broken-off; ./version.sh art/scalatest-utils)"

# This is used for building the sitemap, so we want the date of publication,
# not the date of the release commit itself, hence we don't need to look into
# LATEST to find the date of the referred commit.
echo "STABLE_VERSION_DATE $(TZ=UTC git log -n1 -s --format=%cd --date=short -- LATEST)"
