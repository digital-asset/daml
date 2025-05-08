#!/usr/bin/env bash

# Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail  # Exit on error, prevent unset vars, fail pipeline on first error

CURRENT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

BUF_PROTO_IMAGE=${BUF_PROTO_IMAGE:="${CURRENT_DIR}/root_namespace_buf_image.json.gz"}
if [[ -n "$BUF_PROTO_IMAGE" && -f "$BUF_PROTO_IMAGE" ]]; then
  echo "Using buf image from $BUF_PROTO_IMAGE"
else
  echo "Building buf image from source"
  # If the buf image is not there, assume we're in the git repo and build it
  export BUF_PROTO_IMAGE="$CURRENT_DIR/root_namespace_buf_image.json.gz"
  ROOT_PATH=$(git rev-parse --show-toplevel)
  (
    cd "$ROOT_PATH" &&
    buf build \
        --type "com.digitalasset.canton.protocol.v30.TopologyTransaction" \
        --type "com.digitalasset.canton.version.v1.UntypedVersionedMessage" \
        --type "com.digitalasset.canton.protocol.v30.SignedTopologyTransaction" \
        --type "com.digitalasset.canton.crypto.v30.SigningPublicKey" \
        -o "$BUF_PROTO_IMAGE"
  )
fi

# Source the transaction utility script
source "$CURRENT_DIR/../topology/topology_util.sh"

# Check for required tools and print their versions
check_tool() {
  local tool="$1"
  local version_command="$2"
  if ! command -v "$tool" &>/dev/null; then
    echo "Error: Required tool '$tool' is not installed."
    exit 1
  fi
  version=$(eval "$version_command" 2>&1 | head -n 1)
  echo "$tool version: $version"
}

# Function to detect key spec automatically from the public key
detect_key_spec() {
  local der_file="$1"
  local curve

  curve=$(openssl pkey -pubin -inform DER -in "$der_file" -text -noout 2>/dev/null | grep 'ASN1 OID' | awk -F': ' '{print $2}')

  case "$curve" in
    prime256v1)
      echo "SIGNING_KEY_SPEC_EC_P256"
      ;;
    secp384r1)
      echo "SIGNING_KEY_SPEC_EC_P384"
      ;;
    secp256k1)
      echo "SIGNING_KEY_SPEC_EC_SECP256K1"
      ;;
    ED25519) # this won't appear under ASN1 OID, handled below
      echo "SIGNING_KEY_SPEC_EC_CURVE25519"
      ;;
    "")
      # Handle Ed25519 or fallback when ASN1 OID is missing
      curve=$(openssl pkey -pubin -inform DER -in "$der_file" -text -noout 2>/dev/null | grep -o 'ED25519')
      if [[ "$curve" == "ED25519" ]]; then
        echo "SIGNING_KEY_SPEC_EC_CURVE25519"
      else
        echo "UNKNOWN"
      fi
      ;;
    *)
      echo "UNKNOWN"
      ;;
  esac
}

# Writes to stderr
err() {
  echo "$1" >&2
}
