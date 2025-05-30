#!/usr/bin/env bash

# Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail  # Exit on error, prevent unset vars, fail pipeline on first error

source "$(dirname "$0")/utils.sh"

echo "== Tool Versions =="
check_tool "openssl" "openssl version"
check_tool "buf" "buf --version"
check_tool "xxd" "xxd --version"
check_tool "gunzip" "gunzip --version"
check_tool "jq" "jq --version"
echo ""

# Extract and transform the list of allowed signing restrictions
ALLOWED_DELEGATION_RESTRICTIONS=($(gunzip -c "$BUF_PROTO_IMAGE" | jq '.file[]
    | select(.name == "com/digitalasset/canton/protocol/v30/topology.proto")
    | .messageType[]
    | select(.name == "Enums")
    | .enumType[]
    | select(.name == "TopologyMappingCode")
    | .value[]
    | select(.name != "TOPOLOGY_MAPPING_CODE_UNSPECIFIED")
    | .name' -r | sed -E 's/^TOPOLOGY_MAPPING_CODE_//'))

# Function to display usage information
usage() {
  echo "Usage: $0 [--root-delegation] [--delegation-restrictions <values>] --root-pub-key <path> --target-pub-key <path> --target-key-spec <ed25519|ecp256|ecp384|secp256k1> --output <prefix>"
  echo
  echo "Arguments:"
  echo "  --root-delegation              Optional. Set if the delegation is a self signed root delegation."
  echo "  --intermediate-delegation      Optional. Set if the delegation is an intermediate delegation."
  echo "  --delegation-restrictions <list>  Optional. Comma-separated list of topology mappings the delegation is restricted to authorize. Cannot be set if --root-delegation or --intermediate-delegation are set"
  echo "                                 Use to restrict the topology mappings to be authorized by the delegation."
  echo "                                 Allowed values:"
  for restriction in "${ALLOWED_DELEGATION_RESTRICTIONS[@]}"; do
    echo "                                   - $restriction"
  done
  echo "  --root-pub-key <path>          Required. Path to the root public key file in DER format."
  echo "  --canton-target-pub-key <path> Required. Path to the target public key file generated by a Canton node."
  echo "  --target-pub-key <path>        Required. Path to the target public key file in DER format."
  echo "                                 Either canton-target-pub-key or target-pub-key must be set, but not both"
  echo "  --target-key-spec <value>      Optional. Key specification for the target public key. Valid values are:"
  echo "                                   - ed25519: Curve25519-based key."
  echo "                                   - ecp256: P-256 elliptic curve key."
  echo "                                   - ecp384: P-384 elliptic curve key."
  echo "                                   - secp256k1: SECP256k1 elliptic curve key."
  echo "                                 The script will attempt to automatically detect the key specification"
  echo "                                 from the public key. This flag can be used to override it or provide it if"
  echo "                                 automatic detection fails."
  echo "  --output <prefix>              Required. Output prefix for the generated files."
  echo
  echo "Description:"
  echo "  This script prepares namespace delegation certificates, generates"
  echo "  the serialized transaction and its hash and writes them to files."
  exit 1
}

# Initialize variables
ROOT_DELEGATION="false"
INTERMEDIATE_DELEGATION="false"
ROOT_PUB_KEY=""
TARGET_PUB_KEY=""
CANTON_TARGET_PUB_KEY=""
OUTPUT=""
TARGET_KEY_SPEC=""
DELEGATION_RESTRICTIONS=""

# Parse arguments
while [[ $# -gt 0 ]]; do
  case "$1" in
    --root-delegation)
      ROOT_DELEGATION="true"
      shift
      ;;
    --intermediate-delegation)
      INTERMEDIATE_DELEGATION="true"
      shift
      ;;
    --delegation-restrictions)
      DELEGATION_RESTRICTIONS="$2"
      IFS=',' read -r -a RESTRICTIONS_ARRAY <<< "$DELEGATION_RESTRICTIONS"
      for restriction in "${RESTRICTIONS_ARRAY[@]}"; do
        if [[ ! " ${ALLOWED_DELEGATION_RESTRICTIONS[*]} " =~ " $restriction " ]]; then
          err "Error: Invalid delegation restriction '$restriction'. Allowed values are: ${ALLOWED_DELEGATION_RESTRICTIONS[*]}."
          exit 1
        fi
      done
      shift 2
      ;;
    --root-pub-key)
      ROOT_PUB_KEY="$2"
      if [[ ! -f "$ROOT_PUB_KEY" ]]; then
        err "Error: --root-pub-key file '$ROOT_PUB_KEY' does not exist."
        exit 1
      fi
      shift 2
      ;;
    --canton-target-pub-key)
      CANTON_TARGET_PUB_KEY="$2"
      if [[ ! -f "$CANTON_TARGET_PUB_KEY" ]]; then
        err "Error: --canton-target-pub-key '$CANTON_TARGET_PUB_KEY' does not exist."
        exit 1
      fi
      shift 2
      ;;
    --target-pub-key)
      TARGET_PUB_KEY="$2"
      if [[ ! -f "$TARGET_PUB_KEY" ]]; then
        err "Error: --target-pub-key file '$TARGET_PUB_KEY' does not exist."
        exit 1
      fi
      shift 2
      ;;
    --target-key-spec)
      TARGET_KEY_SPEC="$2"
      case "$TARGET_KEY_SPEC" in
        ed25519)
          TARGET_KEY_SPEC="SIGNING_KEY_SPEC_EC_CURVE25519"
          ;;
        ecp256)
          TARGET_KEY_SPEC="SIGNING_KEY_SPEC_EC_P256"
          ;;
        ecp384)
          TARGET_KEY_SPEC="SIGNING_KEY_SPEC_EC_P384"
          ;;
        secp256k1)
          TARGET_KEY_SPEC="SIGNING_KEY_SPEC_EC_SECP256K1"
          ;;
        *)
          err "Error: Invalid value for --key-spec. Valid values are: ed25519, ecp256, ecp384, secp256k1."
          exit 1
          ;;
      esac
      shift 2
      ;;
    --output)
      OUTPUT="$2"
      if [[ -z "$OUTPUT" ]]; then
        err "Error: --output cannot be empty."
        exit 1
      fi
      shift 2
      ;;
    *)
      err "Error: Unknown argument '$1'."
      usage
      ;;
  esac
done

# Ensure only one of --root-delegation or --delegation-restrictions is set
if [[ "$ROOT_DELEGATION" == "true" && -n "$DELEGATION_RESTRICTIONS" ]]; then
  err "Error: Only one of --root-delegation or --signing-restrictions can be set, not both."
  exit 1
fi

# Ensure only one of --intermediate-delegation or --delegation-restrictions is set
if [[ "$INTERMEDIATE_DELEGATION" == "true" && -n "$DELEGATION_RESTRICTIONS" ]]; then
  err "Error: Only one of --intermediate-delegation or --delegation-restrictions can be set, not both."
  exit 1
fi

# Ensure only one of --root-delegation or --intermediate-delegation is set
if [[ "$ROOT_DELEGATION" == "true" && "$INTERMEDIATE_DELEGATION" == "true" ]]; then
  err "Error: Only one of --root-delegation or --intermediate-delegation can be set, not both."
  exit 1
fi

# Ensure only one of --canton-target-pub-key or --target-pub-key is set
if [[ -n "$CANTON_TARGET_PUB_KEY" && -n "$TARGET_PUB_KEY" ]]; then
  err "Error: Only one of --canton-target-pub-key or --target-pub-key can be set, not both."
  exit 1
fi

# Ensure at least one of them is set
if [[ -z "$CANTON_TARGET_PUB_KEY" && -z "$TARGET_PUB_KEY" ]]; then
  err "Error: At least one of --canton-target-pub-key or --target-pub-key must be set."
  usage
fi

# Validate required arguments
if [[ -z "$ROOT_DELEGATION" || -z "$ROOT_PUB_KEY" || -z "$OUTPUT" ]]; then
  err "Error: Missing required arguments."
  usage
fi

# Additional security checks
if [[ "$ROOT_DELEGATION" == "true" && "$ROOT_PUB_KEY" != "$TARGET_PUB_KEY" ]]; then
  echo "Error: --target-pub-key must be equal to --root-pub-key when --root-delegation is 'true'."
  exit 1
fi

# Ensure the root public key file is in DER format
if ! openssl asn1parse -inform DER -in "$ROOT_PUB_KEY" &>/dev/null; then
  echo "Error: --root-pub-key is not a valid DER file." >&2
  exit 1
fi

echo "== Preparing Certificate =="
ROOT_NAMESPACE_FINGERPRINT=$(compute_canton_fingerprint < "$ROOT_PUB_KEY")
echo "Root namespace fingerprint: $ROOT_NAMESPACE_FINGERPRINT"

# Defaults to "can sign all mappings"
RESTRICTIONS='"can_sign_all_mappings": {}'

if [[ "$INTERMEDIATE_DELEGATION" == "true" ]]; then
  RESTRICTIONS='"can_sign_all_but_namespace_delegations": {}'
# If restrictions are set, restrict to those mappings
elif [[ -n "$DELEGATION_RESTRICTIONS" ]]; then
  # Convert restrictions to upper case snake case and prefix with TOPOLOGY_MAPPING_CODE_ expected by te proto enum
  RESTRICTIONS_LIST=()
  for restriction in "${RESTRICTIONS_ARRAY[@]}"; do
    RESTRICTIONS_LIST+=("TOPOLOGY_MAPPING_CODE_$restriction")
  done

  # Join the restrictions into a JSON array
  RESTRICTIONS=$(cat <<EOF
"can_sign_specific_mapings": {
  "mappings": [
    $(IFS=','; echo "\"${RESTRICTIONS_LIST[*]}\"" | sed 's/,/","/g')
  ]
}
EOF
)
fi

NAMESPACE_MAPPING=""
# Target pub key is a DER file
if [[ -n "$TARGET_PUB_KEY" ]]; then
  # Verify the key is in DER format
  if ! openssl asn1parse -inform DER -in "$TARGET_PUB_KEY" &>/dev/null; then
    echo "Error: --target-pub-key is not a valid DER file."
    exit 1
  fi

  # If the target key spec is not set, try to detect it
  if [[ -z "$TARGET_KEY_SPEC" ]]; then
      # Try to detect the key spec automatically, falling back to the provided spec if detection fails
      DETECTED_SPEC=$(detect_key_spec "$TARGET_PUB_KEY")
      if [[ "$DETECTED_SPEC" != "UNKNOWN" ]]; then
        TARGET_KEY_SPEC="$DETECTED_SPEC"
      else
        err "Error: Could not detect key spec and TARGET_KEY_SPEC is not set."
        exit 1
      fi
  fi

  # Encode the public key in base64
  NAMESPACE_PUBLIC_KEY_BASE64=$(encode_to_base64 < "$TARGET_PUB_KEY")
  # Build the namespace mapping
  NAMESPACE_MAPPING=$(build_namespace_mapping "$ROOT_NAMESPACE_FINGERPRINT" "CRYPTO_KEY_FORMAT_DER" "$NAMESPACE_PUBLIC_KEY_BASE64" "$TARGET_KEY_SPEC" "$RESTRICTIONS")
else
  # Target pub key is a serialized "SigningPublicKey"
  if ! NAMESPACE_DELEGATION_KEY=$(serialized_versioned_message_to_json "$BUF_PROTO_IMAGE" "com.digitalasset.canton.crypto.v30.SigningPublicKey" < "$CANTON_TARGET_PUB_KEY"); then
    echo "Error: Failed to import canton target key: $CANTON_TARGET_PUB_KEY"
    echo "Please ensure this is a valid signing key obtained from a canton node."
    exit 1
  fi
  NAMESPACE_MAPPING=$(build_namespace_mapping_from_signing_key "$ROOT_NAMESPACE_FINGERPRINT" "$NAMESPACE_DELEGATION_KEY" "$RESTRICTIONS" | jq .)
fi

# Serial of the transaction is 1 as there must not be any existing namespace for that key
SERIAL=1
NAMESPACE_TRANSACTION=$(build_topology_transaction "$NAMESPACE_MAPPING" "$SERIAL")
echo "Namespace Transaction: $NAMESPACE_TRANSACTION"
# Serialize transaction and output it to a file
serialize_topology_transaction_from_mapping_and_serial "$NAMESPACE_MAPPING" "$SERIAL" > "$OUTPUT.prep"
echo "Namespace Delegation Transaction written to $OUTPUT.prep"
# Compute the transaction hash and output it to a file
compute_topology_transaction_hash < "$OUTPUT.prep" > "$OUTPUT.hash"
echo "Namespace Delegation Transaction Hash written to $OUTPUT.hash"
