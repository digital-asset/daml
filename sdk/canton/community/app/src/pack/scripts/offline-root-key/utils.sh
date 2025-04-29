#!/usr/bin/env bash

# Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail  # Exit on error, prevent unset vars, fail pipeline on first error

# Source the transaction utility script
source "$(dirname "$0")/../topology/topology_util.sh"

# Check for required tools and print their versions
check_tool() {
  local tool="$1"
  local version_command="$2"
  if ! command -v "$tool" &>/dev/null; then
    echo "Error: Required tool '$tool' is not installed."
    exit 1
  fi
  version=$(eval "$version_command" 2>&1)
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
