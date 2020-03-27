#!/bin/bash

trap cleanup SIGINT SIGTERM
cleanup() {
  kill $SANDBOX
  exit 0
}

scripts/daml-start-backend.sh &
SANDBOX=$!
sleep 10

scripts/restart.sh scripts/build.sh daml
