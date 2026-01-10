#!/usr/bin/env bash

# Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail  # Exit on error, prevent unset vars, fail pipeline on first error

PARTY_NAME="MyParty"
MULTI_HOSTED=false
PARTICIPANT1=""
PARTICIPANT2=""
PRIVATE_KEY_FILE="private_key.der"

# Parse command line arguments
while [[ "$#" -gt 0 ]]; do
    case "$1" in
        -n|--name)
            PARTY_NAME="$2"
            shift
            ;;
        -m|--multi-hosted)
            MULTI_HOSTED=true
            ;;
        -p1|--participant1)
            PARTICIPANT1="$2"
            shift
            ;;
        -p2|--participant2)
            PARTICIPANT2="$2"
            shift
            ;;
        -k|--private-key)
            PRIVATE_KEY_FILE="$2"
            shift
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
    # Shift to the next argument
    shift
done

if [[ $PARTICIPANT1 == "" ]]; then
  PARTICIPANT1="localhost:"$(jq -r .participant1.jsonApi canton_ports.json)
fi
if [[ $PARTICIPANT2 == "" ]]; then
  PARTICIPANT2="localhost:"$(jq -r .participant2.jsonApi canton_ports.json)
fi
OTHER_PARTICIPANT_UIDS=""
if [[ $MULTI_HOSTED == true ]]; then
  echo "Fetching participant2 id"
# [begin-external-party-grab-other-participant-uid]
  OTHER_PARTICIPANT_UIDS="\"$(curl -f -s ${PARTICIPANT2}/v2/parties/participant-id | jq -r .participantId)\""
# [end-external-party-grab-other-participant-uid]
fi

# Determine synchronizer id from participant
echo "Fetching ${PARTICIPANT1}/v2/state/connected-synchronizers"
# [begin-external-party-find-synchronizer-id]
SYNCHRONIZER_ID=$(curl -f -s -L ${PARTICIPANT1}/v2/state/connected-synchronizers | jq .connectedSynchronizers.[0].synchronizerId)
# [end-external-party-find-synchronizer-id]
echo "Detected synchronizer-id ${SYNCHRONIZER_ID}"
if [[ ! -e $PRIVATE_KEY_FILE ]]; then
  # [begin-external-party-generate-keys]
  # Generate an ed25519 private key and extract its public key
  openssl genpkey -algorithm ed25519 -outform DER -out $PRIVATE_KEY_FILE
  # Extract the public key from the private key
  openssl pkey -in private_key.der -pubout -outform DER -out public_key.der 2> /dev/null
  # Convert public key to base64
  PUBLIC_KEY_BASE64=$(base64 -w 0 -i public_key.der)
  # [end-external-party-generate-keys]
else
  PUBLIC_KEY_BASE64=$(base64 -w 0 -i public_key.der)
fi
echo $PUBLIC_KEY_BASE64

echo "Requesting generate topology transactions"
# [begin-external-party-generate-onboarding-transaction]
# Create the JSON payload to generate the onboarding transaction
# Note: otherConfirmingParticipantUids is optional but can be used to add other participants
# as confirming nodes. confirmationThreshold allows to configure the number of required confirmations.
# If not set, all confirming nodes must confirm.
GENERATE=$(cat << EOF
{
  "synchronizer" : $SYNCHRONIZER_ID,
  "partyHint" : "$PARTY_NAME",
  "publicKey" : {
    "format" : "CRYPTO_KEY_FORMAT_DER_X509_SUBJECT_PUBLIC_KEY_INFO",
    "keyData": "$PUBLIC_KEY_BASE64",
    "keySpec" : "SIGNING_KEY_SPEC_EC_CURVE25519"
  },
  "otherConfirmingParticipantUids" : [$OTHER_PARTICIPANT_UIDS]
}
EOF
)

# Submit it to the JSON API
ONBOARDING_TX=$(curl -f -s -d "$GENERATE" -H "Content-Type: application/json" \
  -X POST ${PARTICIPANT1}/v2/parties/external/generate-topology)
# [end-external-party-generate-onboarding-transaction]

# Extract results
# [begin-external-party-extract-results]
PARTY_ID=$(echo $ONBOARDING_TX | jq -r .partyId)
TRANSACTIONS=$(echo $ONBOARDING_TX | jq '.topologyTransactions | map({ transaction : .})')
PUBLIC_KEY_FINGERPRINT=$(echo $ONBOARDING_TX | jq -r .publicKeyFingerprint)
MULTI_HASH=$(echo -n $ONBOARDING_TX | jq -r .multiHash)
# [end-external-party-extract-results]

# Sign the multi-hash using the private key
# [begin-external-party-sign-multi-hash]
echo "Signing hash ${MULTI_HASH} for ${PARTY_ID} using ED25519"
echo -n $MULTI_HASH | base64 --decode > hash_binary.bin
openssl pkeyutl -sign -inkey $PRIVATE_KEY_FILE -rawin -in hash_binary.bin -out signature.bin -keyform DER
SIGNATURE=$(base64 -w 0 < signature.bin)
# [end-external-party-sign-multi-hash]
rm signature.bin hash_binary.bin

# Submit the onboarding transaction to the JSON API
echo "Submitting onboarding transaction to participant1"
# [begin-external-party-submit-onboarding-transaction]
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

RESULT=$(curl -f -s -d "$ALLOCATE" -H "Content-Type: application/json" \
  -X POST ${PARTICIPANT1}/v2/parties/external/allocate)
# [end-external-party-submit-onboarding-transaction]

if [[ $MULTI_HOSTED == true ]]; then
  echo "Submitting onboarding transaction to participant2"
  # [begin-external-party-submit-multi-hosted]
  ALLOCATE=$(cat << EOF
  {
    "synchronizer" : $SYNCHRONIZER_ID,
    "onboardingTransactions": $TRANSACTIONS
  }
EOF
  )
  RESULT=$(curl -f -s -d "$ALLOCATE" -H "Content-Type: application/json" \
    -X POST ${PARTICIPANT2}/v2/parties/external/allocate)
  # [end-external-party-submit-multi-hosted]
fi

echo "Onboarded party $(echo $RESULT | jq .partyId)"
