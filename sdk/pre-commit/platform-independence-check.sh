#!/usr/bin/env bash
# Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

if [ "$RUN_PLATFORM_INDEPENDENCE_CHECK" = "yes" ]; then
  bazel test //compiler/damlc/tests:platform-independence-dar-hash-file-matches
fi
