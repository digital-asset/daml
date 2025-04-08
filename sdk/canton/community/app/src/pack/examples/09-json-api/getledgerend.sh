#!/bin/bash
# Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -eo pipefail

source ./utils.sh

participant="localhost"

result=$(curl_check "http://$participant:7575/v2/state/ledger-end" "application/json")

echo "$result" | jq ".offset"
