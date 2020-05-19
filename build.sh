#!/usr/bin/env bash
# Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

eval "$("$(dirname "$0")/dev-env/bin/dade-assist")"

execution_log_postfix=${1:-}

export LC_ALL=en_US.UTF-8

ARTIFACT_DIRS="${BUILD_ARTIFACTSTAGINGDIRECTORY:-$PWD}"

tag_filter=""
if [[ "$execution_log_postfix" == "_Darwin" ]]; then
  tag_filter="-dont-run-on-darwin,-scaladoc,-pdfdocs"
fi

# Bazel test only builds targets that are dependencies of a test suite so do a full build first.
bazel build //... --build_tag_filters "$tag_filter"

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
  envsubst -no-unset -i ci/postgresql.conf -o "$POSTGRESQL_DATA_DIR/postgresql.conf"
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
bazel test //... \
  --build_tag_filters "$tag_filter" \
  --test_tag_filters "$tag_filter" \
  --test_env "POSTGRESQL_HOST=${POSTGRESQL_HOST}" \
  --test_env "POSTGRESQL_PORT=${POSTGRESQL_PORT}" \
  --test_env "POSTGRESQL_USERNAME=${POSTGRESQL_USERNAME}" \
  --test_env "POSTGRESQL_PASSWORD=${POSTGRESQL_PASSWORD}" \
  --experimental_execution_log_file "$ARTIFACT_DIRS/test_execution${execution_log_postfix}.log"

# Make sure that Bazel query works.
bazel query 'deps(//...)' >/dev/null

# Check that we can load damlc in ghci
# Disabled on darwin since it sometimes seem to hang and this only
# tests our dev setup rather than our code so issues are not critical.
if [[ "$(uname)" != "Darwin" ]]; then
  da-ghci --data yes //compiler/damlc:damlc -e ':main --help'
fi

# Check that our IDE works on our codebase
ghcide compiler/damlc/exe/Main.hs 2>&1 | tee ide-log
grep -q "1 file worked, 0 files failed" ide-log
