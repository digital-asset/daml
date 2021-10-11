#!/usr/bin/env bash
# Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -e
set -u
set -o pipefail

# Canton requires JDK11 in production to avoid ForkJoinPool deadlocks on older JDKs so we run it with JDK11.
JAVA="$(rlocation jdk11_nix/bin/java)"

CANTON_COMMAND=(
  "$(rlocation com_github_digital_asset_daml/ledger/ledger-api-test-tool-on-canton/canton_deploy.jar)"
  daemon
  "--config=$(rlocation com_github_digital_asset_daml/ledger/ledger-api-test-tool-on-canton/canton.conf)"
  "--bootstrap=$(rlocation com_github_digital_asset_daml/ledger/ledger-api-test-tool-on-canton/bootstrap.canton)"
)

PARTICIPANT_1_HOST=localhost
PARTICIPANT_1_MONITORING_PORT=7000

TIMEOUT=60

function wait_until() {
  local start

  start="$(date +%s)"
  while true; do
    if [[ "$(("$(date +%s)" - start))" -gt "$TIMEOUT" ]]; then
      echo >&2 "Timed out after ${TIMEOUT} seconds."
      return 1
    fi

    if "$@" >&/dev/null; then
      return 0
    fi

    sleep 1
  done
}

command=("${CANTON_COMMAND[@]}" "$@")

export UNIQUE_CONTRACT_KEYS="$(rlocation com_github_digital_asset_daml/ledger/ledger-api-test-tool-on-canton/unique-contract-keys.conf)"
if [[ -f ${UNIQUE_CONTRACT_KEYS} ]]; then
  command+=("--config=${UNIQUE_CONTRACT_KEYS}")
fi

export ENABLE_FASTER_PRUNING="$(rlocation com_github_digital_asset_daml/ledger/ledger-api-test-tool-on-canton/enable-faster-pruning.conf)"
if [[ -f ${ENABLE_FASTER_PRUNING} ]]; then
  command+=("--config=${ENABLE_FASTER_PRUNING}")
fi

# Change HOME since Canton uses ammonite in the default configuration, which tries to write to
# ~/.ammonite/cache, which is read-only when sandboxing is enabled.
HOME="$(mktemp -d)"
export HOME
# ammonite calls `System.getProperty('user.home')` which does not read $HOME.
JVM_FLAGS=(-Duser.home="$HOME" -Dlogback.configurationFile="$(rlocation com_github_digital_asset_daml/ledger/ledger-api-test-tool-on-canton/logback-debug.xml)")

echo >&2 'Starting Canton...'
$JAVA "${JVM_FLAGS[@]}" -jar "${command[@]}" &
pid="$!"

sleep 1
if ! kill -0 "$pid" 2>/dev/null; then
  echo >&2 'Failed to start Canton.'
  exit 1
fi

function stop() {
  local status
  status=$?
  kill -INT "$pid" || :
  rm -rf "$HOME" || :
  exit "$status"
}

trap stop EXIT INT TERM

wait_until curl -fsS "http://${PARTICIPANT_1_HOST}:${PARTICIPANT_1_MONITORING_PORT}/health"

echo >&2 'Canton is up and running.'

wait "$pid"
