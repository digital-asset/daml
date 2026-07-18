#!/bin/bash
# Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# Usage: ./query-tags.sh <bazel query pattern>
# Example: ./query-tags.sh '//compiler/damlc/tests:all'
#          ./query-tags.sh 'filter("integration-v", //compiler/damlc/tests:*)'

set -euo pipefail

cd "$(dirname "$0")"

QUERY="${1:?Usage: $0 '<bazel query expression>'}"

# Get all targets and their build output in one call, then extract name+tags
bazel query "$QUERY" --output=build 2>/dev/null | awk '
/^[a-z_]*\(/ { rule=$0 }
/name = "/ { name=$0; gsub(/.*name = "|".*/, "", name) }
/tags = / { tags=$0; gsub(/.*tags = /, "", tags); gsub(/,$/, "", tags) }
/^\)/ { if (name != "") printf "%-60s %s\n", name, tags; name=""; tags="[]" }
'


