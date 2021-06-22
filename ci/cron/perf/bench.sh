# Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

#!/usr/bin/env bash

set -euox pipefail

measure() {
    local out=$(mktemp -d)/out.json
    bazel run daml-lf/scenario-interpreter:scenario-perf -- -rf json -rff $out >&2
    cat $out | jq '.[0].primaryMetric.score'
}

main() {
  local current=$(git rev-parse HEAD)

  local current_perf=$(measure)
  if [ "" = "$current_perf" ]; then exit 1; fi

  echo '{"current-perf": '$current_perf', "current-sha": "'$current'"}'
}

main
