#!/usr/bin/env bash
# Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

SHA=$(git log -n1 --format=%H)

revert_sha () {
    git checkout $SHA
}
trap revert_sha EXIT

cd "$(dirname "$0")"/../..

eval "$(dev-env/bin/dade assist)"

bazel build //ci/cron:cron

./bazel-bin/ci/cron/cron
