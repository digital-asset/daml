#!/usr/bin/env bash
# Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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

if [ "${1:-}" = "_m1" ]; then
    bazel="arch -arm64 bazel"
else
    bazel=bazel
fi

# Bazel test only builds targets that are dependencies of a test suite so do a full build first.
$bazel build //... \
  --build_tag_filters "$tag_filter" \
  --profile build-profile.json \
  --experimental_profile_include_target_label \
  --build_event_json_file build-events.json \
  --build_event_publish_all_actions \
  --experimental_execution_log_file "$ARTIFACT_DIRS/logs/build_execution${execution_log_postfix}.log"

# Set up a shared PostgreSQL instance.
export POSTGRESQL_ROOT_DIR="${TMPDIR:-/tmp}/daml/postgresql"
export POSTGRESQL_DATA_DIR="${POSTGRESQL_ROOT_DIR}/data"
export POSTGRESQL_LOG_FILE="${POSTGRESQL_ROOT_DIR}/postgresql.log"
export POSTGRESQL_HOST='localhost'
export POSTGRESQL_PORT=54321
export POSTGRESQL_USERNAME='test'
export POSTGRESQL_PASSWORD=''
function start_postgresql() {
  mkdir -p "$POSTGRESQL_DATA_DIR"
  bazel run -- @postgresql_dev_env//:initdb --auth=trust --encoding=UNICODE --locale=en_US.UTF-8 --username="$POSTGRESQL_USERNAME" "$POSTGRESQL_DATA_DIR"
  eval "echo \"$(cat ci/postgresql.conf)\"" > "$POSTGRESQL_DATA_DIR/postgresql.conf"
  bazel run -- @postgresql_dev_env//:pg_ctl -w --pgdata="$POSTGRESQL_DATA_DIR" --log="$POSTGRESQL_LOG_FILE" start || {
    if [[ -f "$POSTGRESQL_LOG_FILE" ]]; then
      echo >&2 'PostgreSQL logs:'
      cat >&2 "$POSTGRESQL_LOG_FILE"
    fi
    return 1
  }
}
function stop_postgresql() {
  if [[ -e "$POSTGRESQL_DATA_DIR" ]]; then
    bazel run -- @postgresql_dev_env//:pg_ctl -w --pgdata="$POSTGRESQL_DATA_DIR" --mode=immediate stop || :
    rm -rf "$POSTGRESQL_ROOT_DIR"
  fi
}
trap stop_postgresql EXIT
stop_postgresql # in case it's running from a previous build
start_postgresql

# Run the tests.
$bazel test //... \
  --build_tag_filters "$tag_filter" \
  --test_tag_filters "$tag_filter" \
  --test_env "POSTGRESQL_HOST=${POSTGRESQL_HOST}" \
  --test_env "POSTGRESQL_PORT=${POSTGRESQL_PORT}" \
  --test_env "POSTGRESQL_USERNAME=${POSTGRESQL_USERNAME}" \
  --test_env "POSTGRESQL_PASSWORD=${POSTGRESQL_PASSWORD}" \
  --profile test-profile.json \
  --experimental_profile_include_target_label \
  --build_event_json_file test-events.json \
  --build_event_publish_all_actions \
  --experimental_execution_log_file "$ARTIFACT_DIRS/logs/test_execution${execution_log_postfix}.log"

# Make sure that Bazel query works.
$bazel query 'deps(//...)' >/dev/null

# Check that we can load damlc in ghci
# Disabled on darwin since it sometimes seem to hang and this only
# tests our dev setup rather than our code so issues are not critical.
if [[ "$(uname)" != "Darwin" ]]; then
  da-ghci --data yes //compiler/damlc:damlc -e ':main --help'
fi

# Test that hls at least builds, we don’t run it since it
# adds 2-5 minutes to each CI run with relatively little benefit. If
# you want to test it manually on upgrades, run
# da-hls compiler/damlc/exe/Main.hs.
da-hls --help
