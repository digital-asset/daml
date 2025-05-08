#!/usr/bin/env bash

# Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail  # Exit on error, prevent unset vars, fail pipeline on first error

# Setup
# [start-docs-entry: set buf image path]
CURRENT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
BUF_PROTO_IMAGE="$CURRENT_DIR/interactive_topology_buf_image.json.gz"
# [end-docs-entry: set buf image path]
# If we're in the release artifact
if [ -d "$CURRENT_DIR/../../protobuf/community" ]; then
  echo "Building buf image from artifact"
    # [start-docs-entry: set artifact root path]
    ROOT_PATH="../../protobuf"
    # [end-docs-entry: set artifact root path]
else
  echo "Building buf image from repo"
  # If the buf image is not there, assume we're in the git repo
  ROOT_PATH=$(git rev-parse --show-toplevel)
fi
# [start-docs-entry: build buf image]
(
  cd "$ROOT_PATH" &&
  buf build \
      --type "com.digitalasset.canton.protocol.v30.TopologyTransaction" \
      --type "com.digitalasset.canton.version.v1.UntypedVersionedMessage" \
      --type "com.digitalasset.canton.protocol.v30.SignedTopologyTransaction" \
      -o "$BUF_PROTO_IMAGE"
)
export BUF_PROTO_IMAGE
# [start-docs-entry: build buf image]

# Source the utility script
source "$(dirname "$0")/utils.sh"

# Read GRPC_ENDPOINT and SYNCHRONIZER_ID from arguments
GRPC_ENDPOINT="${1:-"localhost:$(jq -r .participant1.adminApi canton_ports.json)"}"
SYNCHRONIZER_ID="${2:-}"

# Read SYNCHRONIZER_ID from the environment or from the file if not provided as an argument
if [ -z "$SYNCHRONIZER_ID" ]; then
  if [ -f "synchronizer_id" ]; then
    SYNCHRONIZER_ID=$(<synchronizer_id)
  else
    echo "Error: SYNCHRONIZER_ID is not set and synchronizer_id file is not found."
    exit 1
  fi
fi

# [start generate keys]
# Generate an ECDSA private key and extract its public key
openssl ecparam -name prime256v1 -genkey -noout -outform DER -out namespace_private_key.der
openssl ec -inform der -in namespace_private_key.der -pubout -outform der -out namespace_public_key.der 2> /dev/null
# [end generate keys]

# [start compute fingerprint]
# Compute the fingerprint of the public key
fingerprint=$(compute_canton_fingerprint < namespace_public_key.der)
# [end compute fingerprint]
echo "Fingerprint: $fingerprint"

# [start create mapping]
# Base64 encoded public key: grpcurl expects protobuf bytes value to be Base64 encoded in the JSON representation
public_key_base64=$(openssl enc -base64 -A -in namespace_public_key.der)
# This is a root delegation and therefore can sign all mappings
mapping=$(build_namespace_mapping "$fingerprint" "CRYPTO_KEY_FORMAT_DER" "$public_key_base64" "SIGNING_KEY_SPEC_EC_P256" '"can_sign_all_mappings": {}')
# [end create mapping]

# [start build transaction]
# Serial = 1 as the expectation is that there is no existing root namespace with this key already
serial=1
transaction=$(build_topology_transaction "$mapping" "$serial")
# [end build transaction]

# [start build versioned transaction]
serialized_versioned_transaction_file="versioned_topology_transaction.binpb"
serialize_topology_transaction "$transaction" > "$serialized_versioned_transaction_file"
# [end build versioned transaction]

# [start compute transaction hash]
topology_transaction_hash_file="topology_transaction_hash.bin"
compute_topology_transaction_hash < $serialized_versioned_transaction_file > $topology_transaction_hash_file
# [end compute transaction hash]

# [start sign hash]
signature=$(sign_hash namespace_private_key.der $topology_transaction_hash_file)
canton_signature=$(build_canton_signature "SIGNATURE_FORMAT_DER" "$signature" "$fingerprint" "SIGNING_ALGORITHM_SPEC_EC_DSA_SHA_256")
# [end sign hash]

# [start submit transaction]
versioned_transaction_base64=$(openssl enc -base64 -A -in $serialized_versioned_transaction_file)
canton_signatures=("$canton_signature")

signed_transaction=$(build_signed_transaction "false" "$versioned_transaction_base64" "${canton_signatures[@]}")
signed_transactions=("$signed_transaction")
add_transactions_request=$(build_add_transactions_request "$SYNCHRONIZER_ID" "${signed_transactions[@]}")

rpc_status=0
response=$(make_rpc_call "$add_transactions_request" "http://$GRPC_ENDPOINT/com.digitalasset.canton.topology.admin.v30.TopologyManagerWriteService/AddTransactions") || rpc_status=$?
echo $response
if [ $rpc_status -eq 0 ]; then
  echo "Transaction submitted successfully"
else
  echo "Transaction submission failed"
  handle_rpc_error "$response"
  exit $rpc_status
fi
# [end submit transaction]

# [start observe transaction]
list_namespace_delegations_request=$(build_list_namespace_delegations_request "$SYNCHRONIZER_ID" "$fingerprint")

# Topology transaction submission is asynchronous, so we may need to wait a bit before observing the delegation in the topology state
while true; do
  rpc_status=0
  response=$(make_rpc_call "$list_namespace_delegations_request" "http://$GRPC_ENDPOINT/com.digitalasset.canton.topology.admin.v30.TopologyManagerReadService/ListNamespaceDelegation") || rpc_status=$?
  if [ $rpc_status -ne 0 ]; then
    handle_rpc_error "$response"
    exit $rpc_status
  elif [ "$response" != "{}" ]; then
      echo "Namespace delegation is now active"
      break
  fi
  sleep 1
done
# [end observe transaction]