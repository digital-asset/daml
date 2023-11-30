#!/usr/bin/env bash
# Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

if [ -z "${RUN_PLATFORM_INDEPENDENCE_CHECK:-}" ]; then
  echo "Warning: platform-independence-check will be skipped."
  echo "To silence this warning, set 'RUN_PLATFORM_INDEPENDENCE_CHECK' by "
  echo "running the following line:"
  echo "    echo export RUN_PLATFORM_INDEPENDENCE_CHECK=no >> $(pwd)/.envrc.private"
elif [ "$RUN_PLATFORM_INDEPENDENCE_CHECK" = "yes" ]; then
  bazel test //compiler/damlc/tests:platform-independence-dar-hash-file-matches
fi
