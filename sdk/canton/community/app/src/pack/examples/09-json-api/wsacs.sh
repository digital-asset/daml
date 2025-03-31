#!/bin/bash
# Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# This script shows how to get active contracts using websocket
# See also acs.sh

participant=$1
party=$2
ledgerEnd=$3


message='{"filter":{"filtersByParty":{},"filtersForAnyParty":{"cumulative":[{"identifierFilter":{"WildcardFilter":{"value":{"includeCreatedEventBlob":true}}}}]}},"verbose":false,"activeAtOffset":'$ledgerEnd',"eventFormat":null}'

output=$( echo "${message}" | websocat "ws://${participant}:7575/v2/state/active-contracts"  --protocol "daml.ws.auth" )


echo $output
