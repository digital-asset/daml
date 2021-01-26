#!/bin/bash
# Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -euxo pipefail
set -m

LEDGER_HOST=localhost
LEDGER_PORT=6865
LEDGER_ID="TRIGGER_SERVICE_TEST"
TRIGGER_SERVICE_HTTP_PORT=8088

bazel run //ledger/sandbox-classic:sandbox-classic-binary -- \
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
    --http-port $TRIGGER_SERVICE_HTTP_PORT --ledger-host $LEDGER_HOST --ledger-port $LEDGER_PORT --no-secret-key
$LEDGER_TRIGGER_SERVICE_PID=$!

# Requests to the trigger service need an HTTP basic authorization
# header.
#
# If 'alice' has password, '&alC2l3SDS*V' then her token is
# 'alice:&alC2l3SDS*V' which base64 encoded is
# 'YWxpY2U6JmFsQzJsM1NEUypW' and an example request is:
#
#  curl -X GET localhost:$TRIGGER_SERVICE_HTTP_PORT/v1/health \
#     -H "Authorization: Basic YWxpY2U6JmFsQzJsM1NEUypW"
#
# For example, to start a trigger:
#
#  curl -X POST localhost:$TRIGGER_SERVICE_HTTP_PORT/v1/start \
#    -H "Content-type: application/json" -H "Accept: application/json" \
#    -H "Authorization: Basic YWxpY2U6JmFsQzJsM1NEUypW" \
#    -d '{"triggerName":"d2c239382d4875c65d03435ec9da5a349cfd7055dbf348527acfe34ca99f5eb1:TestTrigger:trigger"}'
#
# You can get the package ID from a .dar with `damlc
# -inspect-dar`. You can "seed" the trigger service with a DAR using
# the `--dar` argument.

kill_everything() {
  kill $TRIGGER_SERVICE_PID || true
  kill $SANDBOX_PID || true
}
trap kill_everything EXIT

echo "Everything started. Press Ctrl-C to exit."
fg %1
