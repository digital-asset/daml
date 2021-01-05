# Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

#!/usr/bin/env bash

set -euox pipefail

BASELINE=$1

measure() {
    local out=$(mktemp -d)/out.json
    bazel run daml-lf/scenario-interpreter:scenario-perf -- -rf json -rff $out >&2
    cat $out | jq '.[0].primaryMetric.score'
}

main() {
  local current=$(git rev-parse HEAD)

  git checkout $BASELINE >&2
  git show ${current}:ci/cron/perf/CollectAuthority.scala.patch | git apply
  local baseline_perf=$(measure)
  if [ "" = "$baseline_perf" ]; then exit 1; fi

  # undo patch
  git reset --hard >&2
  git checkout $current >&2
  local current_perf=$(measure)
  if [ "" = "$current_perf" ]; then exit 1; fi

  local speedup=$(printf "%.2f" $(echo "$baseline_perf / $current_perf" | bc -l))
  local progress_5x=$(printf "%05.2f%%" $(echo "100 * l($speedup) / l(5)" | bc -l))
  local progress_10x=$(printf "%05.2f%%" $(echo "100 * l($speedup) / l(10)" | bc -l))

  echo '{"current-perf": '$current_perf', "baseline-perf": '$baseline_perf', "speedup": "'$speedup'x", "progress_towards_5x": "'$progress_5x'", "progress_towards_10x": "'$progress_10x'", "current-sha": "'$current'", "baseline-sha": "'$BASELINE'"}'
}

main
