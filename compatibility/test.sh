#!/usr/bin/env bash
# Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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
## Even more rarely we end up with 2 pids returned by lsof, so we account for
## that with a for loop.
for pid in $(lsof -ti tcp:6865); do
    kill $pid
done

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

# Set up a shared Oracle instance
# Note: this is code duplicated from ../ci/build.yml
function start_oracle() {
  # Only log in automatically if DOCKER_LOGIN is set (as is in our CI).
  # When running this script locally, the developer is expected to have docker running and authenticated.
  if [ -z "${DOCKER_LOGIN:-}" ]; then
    echo "DOCKER_LOGIN is not set, skipping docker login"
  else
    docker login --username "$DOCKER_LOGIN" --password "$DOCKER_PASSWORD"
  fi
  IMAGE=$(cat ../ci/oracle_image)
  docker pull $IMAGE
  # Cleanup stray containers that might still be running from
  # another build that didn’t get shut down cleanly.
  docker rm -f oracle || true
  if [ "${OSTYPE:0:6}" = "darwin" ]; then
    # macOS: Oracle does not like if you use the host network to connect to it if it’s running in the container.
    echo "Starting oracle docker with port mapping"
    docker run -d --rm --name oracle -p $ORACLE_PORT:$ORACLE_PORT -e ORACLE_PWD=$ORACLE_PWD $IMAGE
  else
    # Unix: Oracle does not like if you connect to it via localhost if it’s running in the container.
    # Interestingly it works if you use the external IP of the host so the issue is
    # not the host it is listening on (it claims for that to be 0.0.0.0).
    # --network host is a cheap escape hatch for this.
    echo "Starting oracle docker with host network"
    docker run -d --rm --name oracle --network host -e ORACLE_PWD=$ORACLE_PWD $IMAGE
  fi
}
function stop_oracle() {
  docker rm -f oracle
}
function test_oracle_connection() {
  docker exec oracle bash -c 'sqlplus -L '"$ORACLE_USERNAME"'/'"$ORACLE_PWD"'@//localhost:'"$ORACLE_PORT"'/ORCLPDB1 <<< "select * from dba_users;"; exit $?' >/dev/null
}

# Pass the path to the docker executable to the tests.
# This is because the tests need to interact with the docker container started above.
ORACLE_DOCKER_PATH=$(which docker) || true

if [ -z "${ORACLE_DOCKER_PATH:-}" ]; then
  echo "Not starting Oracle because there is no docker"
else
  trap stop_oracle EXIT
  start_oracle
  until test_oracle_connection
  do
    echo "Could not connect to Oracle, trying again..."
    sleep 1
  done
fi

bazel build //...


tag_filter=""
if [[ "$(uname)" == "Darwin" ]]; then
  tag_filter="${tag_filter}-dont-run-on-darwin,"
fi
if [ "${1:-}" = "--quick" ]; then
  tag_filter="${tag_filter}+head-quick,"
fi

bazel test //... \
  --test_env "POSTGRESQL_HOST=${POSTGRESQL_HOST}" \
  --test_env "POSTGRESQL_PORT=${POSTGRESQL_PORT}" \
  --test_env "POSTGRESQL_USERNAME=${POSTGRESQL_USERNAME}" \
  --test_env "ORACLE_PORT=${ORACLE_PORT}" \
  --test_env "ORACLE_USERNAME=${ORACLE_USERNAME}" \
  --test_env "ORACLE_PWD=${ORACLE_PWD}" \
  --test_env "ORACLE_DOCKER_PATH=${ORACLE_DOCKER_PATH}" \
  --test_tag_filters "${tag_filter%,}"
