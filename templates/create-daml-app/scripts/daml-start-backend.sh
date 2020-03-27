#!/bin/bash
daml sandbox -w --ledgerid create-daml-app-sandbox &
sleep 5
daml json-api --ledger-host localhost --ledger-port 6865 --http-port 7575 --application-id create-daml-app-sandbox
