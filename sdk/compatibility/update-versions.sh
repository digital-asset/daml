#!/usr/bin/env bash
# Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

cd "$(dirname "$0")/.."

bazelisk run --run_under "cd $PWD &&" //compatibility/versions:update-versions -- -o "$PWD/compatibility/versions.bzl"
