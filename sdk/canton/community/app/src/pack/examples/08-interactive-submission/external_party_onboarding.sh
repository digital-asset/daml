#!/usr/bin/env bash

# Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail  # Exit on error, prevent unset vars, fail pipeline on first error

PRIVATE_KEY_FILE="private_key.der"
HTTP_ENDPOINT="${1:-"localhost:$(jq -r .participant1.jsonApi canton_ports.json)"}"

# Determine synchronizer id from participant
echo "Connecting to ${HTTP_ENDPOINT}/v2/state/connected-synchronizers"
SYNCHRONIZER_ID=$(curl -s -L ${HTTP_ENDPOINT}/v2/state/connected-synchronizers | jq .connectedSynchronizers.[0].synchronizerId)
echo "Detected synchronizer-id ${SYNCHRONIZER_ID}"
# Generate an ed25519 private key and extract its public key
openssl genpkey -algorithm ed25519 -outform DER -out private_key.der
# Extract the public key from the private key
openssl pkey -in private_key.der -pubout -outform DER -out public_key.der 2> /dev/null
# Convert public key to base64
PUBLIC_KEY_BASE64=$(base64 -w 0 -i public_key.der)

# Create the JSON payload to generate the onboarding transaction
GENERATE=$(cat << EOF
{
  "synchronizer" : $SYNCHRONIZER_ID,
  "partyHint" : "MyParty",
  "publicKey" : {
    "format" : "CRYPTO_KEY_FORMAT_DER_X509_SUBJECT_PUBLIC_KEY_INFO",
    "keyData": "$PUBLIC_KEY_BASE64",
    "keySpec" : "SIGNING_KEY_SPEC_EC_CURVE25519"
  }
}
EOF
)

# Submit it to the JSON API
ONBOARDING_TX=$(curl -s -d "$GENERATE" -H "Content-Type: application/json" \
  -X POST ${HTTP_ENDPOINT}/v2/parties/external/generate-topology)

# Extract results
PARTY_ID=$(echo $ONBOARDING_TX | jq -r .partyId)
TRANSACTIONS=$(echo $ONBOARDING_TX | jq '.topologyTransactions | map({ transaction : .})')
PUBLIC_KEY_FINGERPRINT=$(echo $ONBOARDING_TX | jq -r .publicKeyFingerprint)
MULTI_HASH=$(echo -n $ONBOARDING_TX | jq -r .multiHash)

# Sign the multi-hash using the private key
echo "Signing hash ${MULTI_HASH} for ${PARTY_ID} using ED25519"
echo -n $MULTI_HASH | base64 --decode > hash_binary.bin
openssl pkeyutl -sign -inkey $PRIVATE_KEY_FILE -rawin -in hash_binary.bin -out signature.bin -keyform DER
SIGNATURE=$(base64 -w 0 < signature.bin)
rm signature.bin hash_binary.bin

# Submit the onboarding transaction to the JSON API
ALLOCATE=$(cat << EOF
{
  "synchronizer" : $SYNCHRONIZER_ID,
  "onboardingTransactions": $TRANSACTIONS,
  "multiHashSignatures": [{
     "format" : "SIGNATURE_FORMAT_CONCAT",
     "signature": "$SIGNATURE",
     "signedBy" : "$PUBLIC_KEY_FINGERPRINT",
     "signingAlgorithmSpec" : "SIGNING_ALGORITHM_SPEC_ED25519"
  }]
}
EOF
)

RESULT=$(curl -s -d "$ALLOCATE" -H "Content-Type: application/json" \
  -X POST ${HTTP_ENDPOINT}/v2/parties/external/allocate)

echo "Onboarded party $(echo $RESULT | jq .partyId)"
