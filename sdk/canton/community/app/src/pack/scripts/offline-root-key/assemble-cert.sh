#!/usr/bin/env bash

# Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail  # Exit on error, prevent unset vars, fail pipeline on first error

source "$(dirname "$0")/utils.sh"

# Function to display usage information
usage() {
  echo "Usage: $0 --prepared-transaction <path> --signature <path> --signature-algorithm <ed25519|ecdsa256|ecdsa384> --output <prefix> [--fingerprint <value>]"
  echo
  echo "Arguments:"
  echo "  --prepared-transaction <path>   Path to the prepared transaction file (required)."
  echo "  --signature <path>              Path to the transaction signature file (required)."
  echo "  --signature-algorithm <value>   Signature algorithm to use. Valid values are:"
  echo "                                   - ed25519 # The signature must be in the format described at https://datatracker.ietf.org/doc/html/rfc8032#section-3.3"
  echo "                                   - ecdsa256 # The signature must be in the DER format following https://datatracker.ietf.org/doc/html/rfc3279#section-2.2.3"
  echo "                                   - ecdsa384 # The signature must be in the DER format following https://datatracker.ietf.org/doc/html/rfc3279#section-2.2.3"
  echo "  Note that for ed25519 the format is different from the format defined in IEEE P1363, which uses concatenation in big-endian form."
  echo "                                   (required)."
  echo "  --output <prefix>               Output prefix for the generated files (required)."
  echo
  echo "Description:"
  echo "  This script processes a prepared transaction and its signature to generate a Canton-compatible signature."
  exit 1
}

# Initialize variables
PREPARED_TRANSACTION=""
TRANSACTION_SIGNATURE=""
SIGNATURE_ALGORITHM_SPEC=""
SIGNATURE_FORMAT=""
OUTPUT=""

# Parse arguments
while [[ $# -gt 0 ]]; do
  case "$1" in
    --prepared-transaction)
      PREPARED_TRANSACTION="$2"
      if [[ ! -f "$PREPARED_TRANSACTION" ]]; then
        echo "Error: --prepared-transaction file '$PREPARED_TRANSACTION' does not exist."
        exit 1
      fi
      shift 2
      ;;
    --signature)
      TRANSACTION_SIGNATURE="$2"
      if [[ ! -f "$TRANSACTION_SIGNATURE" ]]; then
        echo "Error: --signature file '$TRANSACTION_SIGNATURE' does not exist."
        exit 1
      fi
      shift 2
      ;;
    --signature-algorithm)
      SIGNATURE_ALGORITHM_SPEC="$2"
      case "$SIGNATURE_ALGORITHM_SPEC" in
        ed25519)
          SIGNATURE_ALGORITHM_SPEC="SIGNING_ALGORITHM_SPEC_ED25519"
          SIGNATURE_FORMAT="SIGNATURE_FORMAT_CONCAT"
          ;;
        ecdsa256)
          SIGNATURE_ALGORITHM_SPEC="SIGNING_ALGORITHM_SPEC_EC_DSA_SHA_256"
          SIGNATURE_FORMAT="SIGNATURE_FORMAT_DER"
          ;;
        ecdsa384)
          SIGNATURE_ALGORITHM_SPEC="SIGNING_ALGORITHM_SPEC_EC_DSA_SHA_384"
          SIGNATURE_FORMAT="SIGNATURE_FORMAT_DER"
          ;;
        *)
          echo "Error: Invalid value for --signature-algorithm. Valid values are: ed25519, ecdsa256, ecdsa384."
          exit 1
          ;;
      esac
      shift 2
      ;;
    --output)
      OUTPUT="$2"
      if [[ -z "$OUTPUT" ]]; then
        echo "Error: --output cannot be empty."
        exit 1
      fi
      shift 2
      ;;
    *)
      echo "Error: Unknown argument '$1'."
      usage
      ;;
  esac
done

# Validate required arguments
if [[ -z "$PREPARED_TRANSACTION" || -z "$TRANSACTION_SIGNATURE" || -z "$SIGNATURE_ALGORITHM_SPEC" || -z "$OUTPUT" ]]; then
  echo "Error: Missing required arguments."
  usage
fi

echo "== Tool Versions =="
check_tool "openssl" "openssl version"
check_tool "buf" "buf --version"
check_tool "jq" "jq --version"
echo ""

echo "== Assembling Certificate =="
# Extract the fingerprint from the transaction
WRAPPED_TRANSACTION=$(convert_bin_to_json "$VERSION_WRAPPER_PROTO" "com.digitalasset.canton.version.v1.UntypedVersionedMessage" < "$PREPARED_TRANSACTION")
FINGERPRINT=$(echo "$WRAPPED_TRANSACTION" | jq -r .data | decode_from_base64 | convert_bin_to_json "$TOPOLOGY_PROTO" "com.digitalasset.canton.protocol.v30.TopologyTransaction" | jq -r .mapping.namespaceDelegation.namespace)
# Wrap the signature into Canton's protobuf Signature message
TRANSACTION_SIGNATURE_BASE64=$(encode_to_base64 < "$TRANSACTION_SIGNATURE")
CANTON_SIGNATURE=$(build_canton_signature "$SIGNATURE_FORMAT" "$TRANSACTION_SIGNATURE_BASE64" "$FINGERPRINT" "$SIGNATURE_ALGORITHM_SPEC")
# Encode the transaction to base64
PREPARED_TRANSACTION_BASE64=$(encode_to_base64 < "$PREPARED_TRANSACTION")
CANTON_SIGNATURES=("$CANTON_SIGNATURE")
PROPOSAL="false" # Proposal = false means the transaction must be fully authorized with the provided signatures
SIGNED_TRANSACTION=$(build_signed_transaction "$PROPOSAL" "$PREPARED_TRANSACTION_BASE64" "${CANTON_SIGNATURES[@]}")
echo "Certificate: $SIGNED_TRANSACTION"
json_to_serialized_versioned_message "$SIGNED_TRANSACTION" "$TOPOLOGY_PROTO" "com.digitalasset.canton.protocol.v30.SignedTopologyTransaction" > "$OUTPUT"
echo "Certificate written to $OUTPUT"
