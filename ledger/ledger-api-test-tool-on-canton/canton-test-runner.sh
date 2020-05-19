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
PARTICIPANT_2_HOST=localhost
PARTICIPANT_2_LEDGER_API_PORT=5021
PARTICIPANTS=(
  "${PARTICIPANT_1_HOST}:${PARTICIPANT_1_LEDGER_API_PORT}"
  "${PARTICIPANT_2_HOST}:${PARTICIPANT_2_LEDGER_API_PORT}"
)

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
dars=()
port_file=''
while (($#)); do
  # extract the port file
  if [[ "$1" == '--port-file' ]]; then
    port_file="$2"
    shift
    shift
  # upload DARs with the DAML assistant
  elif [[ "$1" =~ \.dar$ ]]; then
    dars+=("$1")
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

# Change HOME since Canton uses ammonite in the default configuration
# which tries to write to ~/.ammonite/cache which is read-only
# when sandboxing is enabled.
export HOME=$(mktemp -d)
# ammonite calls System.getProperty('user.home') which does not
# read $HOME.
command+=("--wrapper_script_flag=--jvm_flag=-Duser.home=$HOME")

# Redirect the Canton logs to a file for now, because they're really, really noisy.
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

for participant in "${PARTICIPANTS[@]}"; do
  for dar in "${dars[@]}"; do
    base64 "$dar" |
      jq -R --slurp '{"darFile": .}' |
      grpcurl \
        -plaintext \
        -d @ \
        "$participant" \
        com.daml.ledger.api.v1.admin.PackageManagementService.UploadDarFile \
        >/dev/null
  done
done

# This should write two ports, but the runner doesn't support that.
echo "$PARTICIPANT_1_LEDGER_API_PORT" >"$port_file"

echo >&2 'Canton is up and running.'

wait "$pid"
