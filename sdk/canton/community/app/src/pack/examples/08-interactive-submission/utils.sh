#!/usr/bin/env bash

# Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail  # Exit on error, prevent unset vars, fail pipeline on first error

# Source the transaction utility script
source "$(dirname "$0")/../../scripts/topology/topology_util.sh"

# [start make_rpc_call fn]
# Make an RPC call with the given request.
# Arguments:
#   $1 - JSON request string
#   $2 - RPC endpoint URL
make_rpc_call() {
  local request=$1
  local rpc=$2
  echo -n "$request" | buf curl --protocol grpc --http2-prior-knowledge -d @- "$rpc" 2>&1
}
# [end make_rpc_call fn]

# Handle RPC errors by extracting and decoding error details if available
# Arguments:
#   $1 - JSON response from RPC call
# [start handle_rpc_error fn]
handle_rpc_error() {
  local response="$1"
  local details
  local type

  echo "Request failed"
  # Extract the first element from the details field using jq
  details=$(echo "$response" | jq -r '.details[0].value // empty')
  type=$(echo "$response" | jq -r '.details[0].type // empty')

  if [ -n "$details" ] && [ "$type" = "google.rpc.ErrorInfo" ]; then
    # Decode the base64 value and save it to a file
    echo "$details" | base64 -d > error_info.bin

    # Download the error info proto if it doesn't exist
    if [ ! -f "google/rpc/error_details.proto" ]; then
      mkdir -p "google/rpc"
      curl -s "https://raw.githubusercontent.com/googleapis/googleapis/9415ba048aa587b1b2df2b96fc00aa009c831597/google/rpc/error_details.proto" -o "google/rpc/error_details.proto"
    fi

    # Deserialize the protobuf message using buf convert
    buf convert google/rpc/error_details.proto --from error_info.bin --to - --type google.rpc.ErrorInfo | jq .
  else
    echo "No details available in the response or type is not google.rpc.ErrorInfo."
  fi
}
# [end handle_rpc_error fn]
