#!/usr/bin/env bash
# Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -e
set -u
set -o pipefail

CANTON_COMMAND=(
  "$(rlocation com_github_digital_asset_daml/ledger/ledger-api-test-tool-on-canton/canton)"
  daemon
  "--config=$(rlocation com_github_digital_asset_daml/ledger/ledger-api-test-tool-on-canton/canton.conf)"
  "--bootstrap=$(rlocation com_github_digital_asset_daml/ledger/ledger-api-test-tool-on-canton/bootstrap.canton)"
)

PARTICIPANT_1_HOST=localhost
PARTICIPANT_1_LEDGER_API_PORT=5011
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

command=("${CANTON_COMMAND[@]}")
port_file=''
while (($#)); do
  # Extract the port file.
  if [[ "$1" == '--port-file' ]]; then
    port_file="$2"
    shift
    shift
  else
    command+=("$1")
    shift
  fi
done

if [[ -z "$port_file" ]]; then
  # shellcheck disable=SC2016
  echo >&2 'You must specify a port file with the `--port-file` switch.'
  exit 2
fi

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
command+=("--wrapper_script_flag=--jvm_flag=-Duser.home=$HOME")
command+=("--wrapper_script_flag=--jvm_flag=-Dlogback.configurationFile=$(rlocation com_github_digital_asset_daml/ledger/ledger-api-test-tool-on-canton/logback-debug.xml)")

echo >&2 'Starting Canton...'
"${command[@]}" &
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
  rm -f "$port_file" || :
  rm -rf "$HOME" || :
  exit "$status"
}

trap stop EXIT INT TERM

wait_until curl -fsS "http://${PARTICIPANT_1_HOST}:${PARTICIPANT_1_MONITORING_PORT}/health"

# This should write two ports, but the runner doesn't support that.
echo "$PARTICIPANT_1_LEDGER_API_PORT" >"$port_file"

echo >&2 'Canton is up and running.'

wait "$pid"
