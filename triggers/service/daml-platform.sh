#!/bin/bash
# Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -euxo pipefail
set -m

LEDGER_HOST=localhost
LEDGER_PORT=6865
LEDGER_ID="TRIGGER_SERVICE_TEST"

bazel run //ledger/sandbox:sandbox-binary -- \
  -a $LEDGER_HOST -p $LEDGER_PORT --ledgerid $LEDGER_ID -w &
SANDBOX_PID=$!
kill_sandbox() {
  kill $SANDBOX_PID || true
}
trap kill_sandbox EXIT

sleep 1
until nc -z $LEDGER_HOST $LEDGER_PORT; do
  echo "Waiting for sandbox."
  sleep 1
done
echo "Connected to sandbox."

bazel run //triggers/service:trigger-service-binary -- \
  --ledger-host $LEDGER_HOST --ledger-port $LEDGER_PORT --wall-clock-time &
TRIGGER_SERVICE_PID=$!

# A smoke test:
#  curl -X GET \
#    -H "Content-type: application/json" -H "Accept: application/json" \
#    "http://localhost:8080/test"

kill_everything() {
  kill $TRIGGER_SERVICE_PID || true
  kill $SANDBOX_PID || true
}
trap kill_everything EXIT

echo "Everything started. Press Ctrl-C to exit."
fg %1
