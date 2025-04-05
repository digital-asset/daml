#!/usr/bin/env bash

# Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail  # Exit on error, prevent unset vars, fail pipeline on first error

# Compute a Canton hash with multi-hash encoding and a hash purpose.
# Arguments:
#   $1 - Hash purpose (integer)
# Reads input from stdin and outputs a hash prefixed with multi-hash encoding.
# [start compute_canton_hash fn]
compute_canton_hash() {
  # The hash purpose integer must be prefixed to the content to be hashed as a 4 bytes big endian
  (printf "\\x00\\x00\\x00\\x$(printf '%02X' "$1")"; cat - <(cat)) | \
  # Then hash with sha256
  openssl dgst -sha256 -binary | \
  # And finally prefix with 0x12 (The multicodec code for SHA256 https://github.com/multiformats/multicodec/blob/master/table.csv#L9)
  # and 0x20, the length of the hash (32 bytes)
  ( printf '\x12\x20'; cat - )
}
# [end compute_canton_hash fn]

# Compute a Canton fingerprint from the public key bytes.
# Reads input from stdin and outputs the fingerprint in hexadecimal format.
# [start compute_canton_fingerprint fn]
compute_canton_fingerprint() {
  # 12 is the hash purpose for public key fingerprints
  # https://github.com/digital-asset/canton/blob/main/community/base/src/main/scala/com/digitalasset/canton/crypto/HashPurpose.scala
  compute_canton_hash 12 | xxd -ps -c 0
}
# [end compute_canton_fingerprint fn]

# Compute a Canton fingerprint from the public key bytes encoded in base64 format.
# Reads input from stdin and outputs the fingerprint in hexadecimal format.
# [start compute_canton_fingerprint_from_base64 fn]
compute_canton_fingerprint_from_base64() {
  local public_key_base64="$1"
  echo "$public_key_base64" | openssl base64 -d | compute_canton_fingerprint
}
# [end compute_canton_fingerprint_from_base64 fn]

# Compute a Canton topology transaction hash from the serialized transaction bytes.
# Reads input from stdin and outputs the topology transaction hash as bytes.
# [start compute_topology_transaction_hash fn]
compute_topology_transaction_hash() {
  # 11 is the hash purpose for topology transaction signatures
  # https://github.com/digital-asset/canton/blob/main/community/base/src/main/scala/com/digitalasset/canton/crypto/HashPurpose.scala
  compute_canton_hash 11
}
# [end compute_topology_transaction_hash fn]

# Convert JSON to binary using buf.
# Arguments:
#   $1 - Path to the .proto file
#   $2 - Type to convert to
#   $3 - Output file path
# [start convert_json_to_bin fn]
convert_json_to_bin() {
    local proto_file="$1"
    local type="$2"
    local output_file="$3"
    buf convert "$proto_file" --validate --type="$type" --to="$output_file" --from -#format=json
}
# [end convert_json_to_bin fn]

# Build a JSON object representing namespace delegation.
# Arguments:
#   $1 - Namespace
#   $2 - Key format
#   $3 - Public key
#   $4 - Key spec
#   $5 - Root delegation flag (true/false)
# [start build_namespace_mapping fn]
build_namespace_mapping() {
  local namespace="$1"
  local format="$2"
  local public_key="$3"
  local spec="$4"
  local is_root="$5"
    cat <<EOF
{
  "namespace_delegation": {
    "namespace": "$namespace",
    "target_key": {
      "format": "$format",
      "public_key": "$public_key",
      "usage": ["SIGNING_KEY_USAGE_NAMESPACE"],
      "key_spec": "$spec"
    },
    "is_root_delegation": $is_root
  }
}
EOF
}
# [end build_namespace_mapping fn]

# Build a topology transaction JSON object.
# Arguments:
#   $1 - Mapping object JSON string
#   $2 - Serial
# [start build_topology_transaction fn]
build_topology_transaction() {
    local mapping="$1"
    local serial="$2"
    cat <<EOF
{
  "operation": "TOPOLOGY_CHANGE_OP_ADD_REPLACE",
  "serial": $serial,
  "mapping": $mapping
}
EOF
}
# [end build_topology_transaction fn]

# Build a versioned transaction JSON.
# Arguments:
#   $1 - Base64 encoded transaction string
# [start build_versioned_transaction fn]
build_versioned_transaction() {
  local data="$1"
    cat <<EOF
{
  "data": "$data",
  "version": "30"
}
EOF
}
# [end build_versioned_transaction fn]

# Build a request to add transactions.
# Arguments:
#   $1 - Synchronizer ID
#   $2... - Transactions JSON strings
# [start build_add_transactions_request fn]
build_add_transactions_request() {
    local synchronizer_id="$1"
    shift
    local transactions=("$@")
    local transactions_json=""

    # Construct JSON array properly
    for transaction in "${transactions[@]}"; do
        transactions_json+="$transaction,"
    done

    # Remove the trailing comma and wrap in brackets
    transactions_json="[${transactions_json%,}]"

    cat <<EOF
{
  "transactions": $transactions_json,
  "store": {
    "synchronizer": {
      "id": "$synchronizer_id"
    }
  }
}
EOF
}
# [end build_add_transactions_request fn]

# Build a Canton signature JSON object.
# Arguments:
#   $1 - Key format
#   $2 - Signature
#   $3 - Signed by entity
#   $4 - Signing algorithm specification
# [start build_canton_signature fn]
build_canton_signature() {
  local format="$1"
  local signature="$2"
  local signed_by="$3"
  local spec="$4"
    cat <<EOF
{
  "format": "$format",
  "signature": "$signature",
  "signed_by": "$signed_by",
  "signing_algorithm_spec": "$spec"
}
EOF
}
# [end build_canton_signature fn]

# Build a signed transaction JSON object.
# Arguments:
#   $1 - Proposal JSON string
#   $2 - Transaction JSON string
#   $3... - Signatures JSON strings
# [start build_signed_transaction fn]
build_signed_transaction() {
    local proposal="$1"
    local transaction="$2"
    shift 2
    local signatures=("$@")
    local signatures_json=""

    for signature in "${signatures[@]}"; do
        signatures_json+="$signature,"
    done

    signatures_json="[${signatures_json%,}]"

    cat <<EOF
{
  "transaction": "$transaction",
  "signatures": $signatures_json,
  "proposal": $proposal
}
EOF
}
# [end build_signed_transaction fn]

# Sign a hash using a private key.
# Arguments:
#   $1 - Private key file path
#   $2 - Transaction hash file path
# [start sign_hash fn]
sign_hash() {
  local private_key_file="$1"
  local transaction_hash_file="$2"
  openssl pkeyutl -rawin -inkey "$private_key_file" -keyform DER -sign < "$transaction_hash_file" | openssl base64 -e -A | tr -d '\n'
}
# [end sign_hash fn]

# Serialize a topology transaction and encode it in base64.
# Arguments:
#   $1 - Mapping JSON string
#   $2 - Synchronizer ID
#   $3 - Output file path
# [start serialize_transaction_base64 fn]
serialize_transaction_base64() {
  local mapping="$1"
  local serial="$2"
  local serialized_transaction_file
  serialized_transaction_file=$(mktemp)
  local transaction
  transaction=$(build_topology_transaction "$mapping" "$serial")
  echo "$transaction" | convert_json_to_bin \
      "$TOPOLOGY_PROTO" \
      "com.digitalasset.canton.protocol.v30.TopologyTransaction" \
      "$serialized_transaction_file"
  openssl enc -base64 -A -in "$serialized_transaction_file"
}
# [end serialize_transaction_base64 fn]

# Serialize a base64-encoded transaction into a versioned binary format
# Arguments:
#   $1 - Base64-encoded transaction
#   $2 - Output file path for the serialized versioned transaction
# [start serialize_versioned_transaction fn]
serialize_versioned_transaction() {
  local transaction_base64=$1
  local serialized_versioned_transaction_file="$2"
  local versioned_transaction
  versioned_transaction=$(build_versioned_transaction "$transaction_base64")
  # Serialize it to binary
  echo "$versioned_transaction" | convert_json_to_bin \
      "$VERSION_WRAPPER_PROTO" \
      "com.digitalasset.canton.version.v1.UntypedVersionedMessage" \
      "$serialized_versioned_transaction_file"
}
# [end serialize_versioned_transaction fn]

# Build a JSON request to list namespace delegations
# Arguments:
#   $1 - Synchronizer ID
#   $2 - Namespace to filter delegations for
# [start list_namespace_delegations fn]
build_list_namespace_delegations_request() {
  local synchronizer_id="$1"
  local namespace="$2"
    cat <<EOF
{
  "base_query": {
    "store": {
      "synchronizer": {
        "id": "$synchronizer_id"
      }
    },
    "head_state": {}
  },
  "filter_namespace": "$namespace"
}
EOF
}
# [end list_namespace_delegations fn]

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
