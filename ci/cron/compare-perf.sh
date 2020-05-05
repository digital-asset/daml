# Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

#!/usr/bin/env bash

set -euo pipefail

BASELINE=$1

measure() {
    local treeish=$1
    local out=$(mktemp -d)/out.json
    git checkout $treeish >&2
    bazel run daml-lf/scenario-interpreter:scenario-perf -- -rf json -rff $out >&2
    cat $out | jq '.[0].primaryMetric.score'
}

main() {
  local current=$(git rev-parse HEAD)

  local baseline_perf=$(measure $BASELINE)
  local current_perf=$(measure $current)

  git checkout $current >&2

  local factor=$(echo "$current_perf / $baseline_perf" | bc -l)
  local speedup=$(printf "%.1f" $(echo "($factor - 1) * 100" | bc -l))
  local progress=$(printf "%05.2f%%" $(echo "100 / (l(5) / l($factor))" | bc -l))

  echo '{"current-perf": '$current_perf', "baseline-perf": '$baseline_perf', "improvement": '$speedup', "progress_towards_5x": "'$progress'", "current-sha": "'$current'", "baseline-sha": "'$BASELINE'"}'
}

main
