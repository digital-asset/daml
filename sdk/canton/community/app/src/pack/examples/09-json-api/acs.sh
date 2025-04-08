#!/bin/bash
# Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# This script shows how to get active contracts using HTTP Post
# See also wsacs.sh

participant=$1
party=$2
ledgerEnd=$3

source ./utils.sh

message='{"filter":{"filtersByParty":{},"filtersForAnyParty":{"cumulative":[{"identifierFilter":{"WildcardFilter":{"value":{"includeCreatedEventBlob":true}}}}]}},"verbose":false,"activeAtOffset":'$ledgerEnd',"eventFormat":null}'

output=$( curl_check  "http://$participant:7575/v2/state/active-contracts"  "application/json" \
  --data-raw $message
)

echo $output
