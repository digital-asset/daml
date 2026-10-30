#!/usr/bin/env bash
# Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

cd "$(dirname "$0")/.."

if command -v lsof >/dev/null; then
  for pid in $(lsof -ti tcp:6865 || true); do
    kill "$pid"
  done
fi

test_suite=//compatibility
if [ "${1:-}" = "--quick" ]; then
  test_suite=//compatibility:quick
  shift
fi

for sdk_target in $(bazelisk query 'kind(alias, @compat_sdks//:all)'); do
  bazelisk build "$sdk_target" "$@"
done

tag_filter=""
if [[ "$(uname)" == "Darwin" ]]; then
  tag_filter="-dont-run-on-darwin"
fi

bazelisk build "$test_suite" "$@"
bazelisk test "$test_suite" --test_tag_filters "$tag_filter" "$@"
