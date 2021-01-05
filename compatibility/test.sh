#!/usr/bin/env bash
# Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0


# Build the release artifacts required for running the compatibility
# tests against HEAD. At the moment this includes the SDK release tarball
# and the ledger-api-test-tool fat JAR.

set -eou pipefail

cd "$(dirname "$0")"

eval "$(../dev-env/bin/dade-assist)"

# Git, symlinks and windows do not play well together
# so we have to copy over the Bazel config. We just do
# it unconditionally since it should be cheap enough.
cp ../.bazelrc .bazelrc

# Occasionally we end up with a stale sandbox process for a hardcoded
# port number. Not quite sure how we end up with a stale process
# but it happens sufficiently rarely that just killing it here is
# a cheaper solution than having to reset the node.
# Note that lsof returns a non-zero exit code if there is no match.
SANDBOX_PID="$(lsof -ti tcp:6865 || true)"
if [ -n "$SANDBOX_PID" ]; then
    kill "$SANDBOX_PID"
fi

# Set up a shared PostgreSQL instance. This is the same code as in //:build.sh.
export POSTGRESQL_ROOT_DIR="${TMPDIR:-/tmp}/daml/postgresql"
export POSTGRESQL_DATA_DIR="${POSTGRESQL_ROOT_DIR}/data"
export POSTGRESQL_LOG_FILE="${POSTGRESQL_ROOT_DIR}/postgresql.log"
export POSTGRESQL_HOST='localhost'
export POSTGRESQL_PORT=54321
export POSTGRESQL_USERNAME='test'
function start_postgresql() {
  mkdir -p "$POSTGRESQL_DATA_DIR"
  bazel run -- @postgresql_nix//:bin/initdb --auth=trust --encoding=UNICODE --locale=en_US.UTF-8 --username="$POSTGRESQL_USERNAME" "$POSTGRESQL_DATA_DIR"
  eval "echo \"$(cat ../ci/postgresql.conf)\"" > "$POSTGRESQL_DATA_DIR/postgresql.conf"
  bazel run -- @postgresql_nix//:bin/pg_ctl -w --pgdata="$POSTGRESQL_DATA_DIR" --log="$POSTGRESQL_LOG_FILE" start || {
    if [[ -f "$POSTGRESQL_LOG_FILE" ]]; then
      echo >&2 'PostgreSQL logs:'
      cat >&2 "$POSTGRESQL_LOG_FILE"
    fi
    return 1
  }
}
function stop_postgresql() {
  if [[ -e "$POSTGRESQL_DATA_DIR" ]]; then
    bazel run -- @postgresql_nix//:bin/pg_ctl -w --pgdata="$POSTGRESQL_DATA_DIR" --mode=immediate stop || :
    rm -rf "$POSTGRESQL_ROOT_DIR"
  fi
}
trap stop_postgresql EXIT
stop_postgresql # in case it's running from a previous build
start_postgresql

bazel build //...

BAZEL_ARGS=""
if [ "${1:-}" = "--quick" ]; then
    BAZEL_ARGS="--test_tag_filters +head-quick"
fi

bazel test //... \
  --test_env "POSTGRESQL_HOST=${POSTGRESQL_HOST}" \
  --test_env "POSTGRESQL_PORT=${POSTGRESQL_PORT}" \
  --test_env "POSTGRESQL_USERNAME=${POSTGRESQL_USERNAME}" \
  $BAZEL_ARGS
