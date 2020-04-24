#!/usr/bin/env bash
# Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0


# Build the release artifacts required for running the compatibility
# tests against HEAD. At the moment this includes the SDK release tarball
# and the ledger-api-test-tool fat JAR.

set -eou pipefail

cd "$(dirname "$0")"

eval "$(../dev-env/bin/dade-assist)"

bazel build //...
if [ "${1:-}" = "--quick" ]; then
    bazel test //:head-quick
else
    bazel test //...
fi

