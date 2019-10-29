#!/usr/bin/env bash
# Copyright (c) 2019 The DAML Authors. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

cd "$(dirname "$0")"/../..

eval "$(dev-env/bin/dade assist)"

bazel build //ci/cron:cron

./bazel-bin/ci/cron/cron
