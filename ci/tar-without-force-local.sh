#!/bin/sh
# Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
# TODO: remove once release-trigger's upload-bazel-metrics.yml stops passing the GNU-only --force-local to tar on macOS
for arg do
  shift
  [ "$arg" = "--force-local" ] || set -- "$@" "$arg"
done
exec "$(dirname "$0")/bsdtar" "$@"
