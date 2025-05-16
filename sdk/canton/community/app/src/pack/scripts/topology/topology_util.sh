#!/usr/bin/env bash

# Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail  # Exit on error, prevent unset vars, fail pipeline on first error

# Requirements
# BUF_PROTO_IMAGE should point to a buf image containing at least the topology.proto and untyped_versioned_message.proto

################################################
# Low level utility function to manipulate bytes
################################################

# [start byte utility functions]
# Encode bytes read from stdin to base64
encode_to_base64() {
  openssl base64 -e -A
}

# Decode base64 string to bytes
decode_from_base64() {
  openssl base64 -d
}

# Encode bytes read from stdin to hexadecimal
encode_to_hex() {
  xxd -ps -c 0
}
# [end byte utility functions]

###################################
# Canton specific utility functions
###################################

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
  compute_canton_hash 12 | encode_to_hex
}
# [end compute_canton_fingerprint fn]

# Compute a Canton fingerprint from the public key bytes encoded in base64 format.
# Reads input from stdin and outputs the fingerprint in hexadecimal format.
# [start compute_canton_fingerprint_from_base64 fn]
compute_canton_fingerprint_from_base64() {
  local public_key_base64="$1"
  echo "$public_key_base64" | decode_from_base64 | compute_canton_fingerprint
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
# Takes json input on stdin
# [start convert_json_to_bin fn]
convert_json_to_bin() {
    local buf_image="$1"
    local type="$2"
    buf convert "$buf_image" --validate --type="$type" --from -#format=json
}
# [end convert_json_to_bin fn]

# Convert binary to JSON using buf.
# Arguments:
#   $1 - Path to the .proto file
#   $2 - Type to convert to
# Takes binary input on stdin
# [start convert_bin_to_json fn]
convert_bin_to_json() {
    local buf_image="$1"
    local type="$2"
    buf convert "$buf_image" --validate --type="$type" --from -#format=binpb
}
# [end convert_bin_to_json fn]

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
  local restrictions="$5"
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
    $restrictions
  }
}
EOF
}
# [end build_namespace_mapping fn]

build_namespace_mapping_from_signing_key() {
  local namespace="$1"
  local signing_key="$2"
  local restrictions="$3"
    cat <<EOF
{
  "namespace_delegation": {
    "namespace": "$namespace",
    "target_key": $signing_key,
    $restrictions
  }
}
EOF
}

# Build a topology transaction JSON object.
# Arguments:
#   $1 - Mapping object JSON string
#   $2 - Serial
# [start build_topology_transaction fn]
build_topology_transaction() {
    local mapping="$1"
    local serial="$2"
    local operation="${3:-TOPOLOGY_CHANGE_OP_ADD_REPLACE}"
    cat <<EOF
{
  "operation": "$operation",
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
  openssl pkeyutl -rawin -inkey "$private_key_file" -keyform DER -sign < "$transaction_hash_file" | encode_to_base64
}
# [end sign_hash fn]

# Serialize a topology transaction to a versioned message in binary protobuf format.
# Arguments:
#   $1 - Mapping JSON string
#   $2 - serial number
# [start serialize_topology_transaction_from_mapping_and_serial fn]
serialize_topology_transaction_from_mapping_and_serial() {
  local mapping="$1"
  local serial="$2"
  local transaction
  transaction=$(build_topology_transaction "$mapping" "$serial")
  json_to_serialized_versioned_message "$transaction" "$BUF_PROTO_IMAGE" "com.digitalasset.canton.protocol.v30.TopologyTransaction"
}
# [end serialize_topology_transaction_from_mapping_and_serial fn]

# Serialize a topology transaction to a versioned message in binary protobuf format.
# Arguments:
#   $1 - transaction as JSON
# [start serialize_topology_transaction fn]
serialize_topology_transaction() {
  local transaction="$1"
  json_to_serialized_versioned_message "$transaction" "$BUF_PROTO_IMAGE" "com.digitalasset.canton.protocol.v30.TopologyTransaction"
}
# [end serialize_topology_transaction fn]

# Unwraps a serialized versioned message to JSON.
# Arguments:
#   $1 - proto file containing the type of the inner message
#   $2 - proto message type of the inner message
# Takes the serialized versioned message in binary format from stdin
# [start serialized_versioned_message_to_json fn]
serialized_versioned_message_to_json() {
  local proto=$1
  local message_type=$2

  WRAPPER=$(convert_bin_to_json "$BUF_PROTO_IMAGE" "com.digitalasset.canton.version.v1.UntypedVersionedMessage")
  echo "$WRAPPER" | jq -r .data | decode_from_base64 | convert_bin_to_json "$proto" "$message_type"
}
# [end serialized_versioned_message_to_json fn]

# Serializes a proto message in JSON representation to a versioned message.
# Arguments:
#   $1 - proto file containing the type of the inner message
#   $2 - proto message type of the inner message
#   $3 - json message
# [start json_to_serialized_versioned_message fn]
json_to_serialized_versioned_message() {
  local json=$1
  local proto=$2
  local message_type=$3
  # Serialize it to binary
  SERIALIZED_JSON_BASE64=$(echo "$json" | convert_json_to_bin "$proto"  "$message_type" | encode_to_base64)
  versioned_transaction=$(build_versioned_transaction "$SERIALIZED_JSON_BASE64")
  echo "$versioned_transaction" | convert_json_to_bin \
        "$BUF_PROTO_IMAGE" \
        "com.digitalasset.canton.version.v1.UntypedVersionedMessage"
}
# [end json_to_serialized_versioned_message fn]

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
