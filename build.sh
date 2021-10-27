#!/usr/bin/env bash
# Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

eval "$("$(dirname "$0")/dev-env/bin/dade-assist")"

execution_log_postfix=${1:-}

export LC_ALL=en_US.UTF-8

ARTIFACT_DIRS="${BUILD_ARTIFACTSTAGINGDIRECTORY:-$PWD}"
mkdir -p "${ARTIFACT_DIRS}/logs"

tag_filter=""
if [[ "$(uname)" == "Darwin" ]]; then
  tag_filter="-dont-run-on-darwin,-scaladoc,-pdfdocs"
fi

# Occasionally we end up with a stale sandbox process for a hardcoded
# port number. Not quite sure how we end up with a stale process
# but it happens sufficiently rarely that just killing it here is
# a cheaper solution than having to reset the node.
# Note that lsof returns a non-zero exit code if there is no match.
SANDBOX_PID="$(lsof -ti tcp:6865 || true)"
if [ -n "$SANDBOX_PID" ]; then
    echo $SANDBOX_PID | xargs kill
fi

bazel test //daml-script/export/integration-tests/reproduces-transactions:reproduces-transactions_test_suite_test_suite_scala_com_daml_script_export_EmptyACS.scala --runs_per_test=100
