#!/bin/bash
# Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -eo pipefail

# Change DEBUG to true in order to see content of request and responses bodies, and other details
export DEBUG=false
source ./utils.sh

# Random id to be added as suffix for commandId - this script can be started multiple times
uniqueId="$RANDOM"

# host on which participant node is running
participant="localhost"

# Function to create an Iou contract
create_contract() {
  local appUserParty=$1
  curl_check "http://$participant:7575/v2/commands/submit-and-wait" "application/json" \
    --data-raw '{
        "commands" : [
           {"CreateCommand": {
                   "createArguments": {
                     "observers": [],
                     "issuer": "'$appUserParty'",
                     "amount": "999.99",
                     "currency": "USD",
                     "owner": "'$appUserParty'"
                   },
                   "templateId": "#model-tests:Iou:Iou"
                 }
            }
        ],
        "applicationId": "ledger-api-user",
        "commandId": "example-app-create-'${uniqueId}'",
        "actAs": ["'$appUserParty'"],
        "readAs": ["'$appUserParty'"]
    }'
}

# Function to exercise transfer choid on Iou
exercise_transfer() {
  local appUserParty=$1
  local contractId=$2
  local receivingUserParty=$3
  curl_check "http://$participant:7575/v2/commands/submit-and-wait" "application/json" \
    --data-raw '{
        "commands" : [
           {"ExerciseCommand": {
                   "choiceArgument": {
                    "newOwner": "'$receivingUserParty'"
                   },
                   "templateId": "#model-tests:Iou:Iou",
                   "choice": "Iou_Transfer",
                   "contractId": "'$contractId'"
                 }
            }
        ],
        "applicationId": "ledger-api-user",
        "commandId": "example-app-transfer'${uniqueId}'",
        "actAs": ["'$appUserParty'"],
        "readAs": ["'$appUserParty'"]
    }'
}

# Function to accept Iou transfer
exercise_accept() {
  local appUserParty=$1
  local contractId=$2
  curl_check "http://$participant:7575/v2/commands/submit-and-wait" "application/json" \
    --data-raw '{
        "commands" : [
           {"ExerciseCommand": {
                   "choiceArgument": {},
                   "templateId": "#model-tests:Iou:IouTransfer",
                   "choice": "IouTransfer_Accept",
                   "contractId": "'$contractId'"
                 }
            }
        ],
        "applicationId": "ledger-api-user",
        "commandId": "example-app-accept'${uniqueId}'",
        "actAs": ["'$appUserParty'"],
        "readAs": ["'$appUserParty'"]
    }'
}

showContractsWs() {
  local contracts=$(echo $1 | jq -s .  )
  showContracts "$contracts"
}

showContracts() {
  local contracts=$1
  echo "$contracts" | jq '["contract", "owner", "amount", "currency"], (map (.contractEntry.JsActiveContract.createdEvent)
    | map([.contractId[0:10], .createArgument.owner[0:10], .createArgument.amount,  .createArgument.currency]) )[] | @tsv ' | sed 's/\\t/    /g' | column -t
}

echo "Step 1. Creating parties"
# Initially we create both parties
aliceParty=$(allocate_party_and_create_user "Alice" "$participant")
echo "$aliceParty"

bobParty=$(allocate_party_and_create_user "Bob" "$participant")
echo "$bobParty"

echo "Step 2. Alice creates an Iou contract"
create_contract_result=$(create_contract "$aliceParty")

completionOffset=$(echo $create_contract_result  |  jq ".completionOffset")
echo "created Iou contract at offset ${completionOffset}"

# at this stage ledgerEnd should be same as completionOffset (if no other transactions are in progress)
ledgerEnd=$(./getledgerend.sh)
echo "ledger end is $ledgerEnd"

if command -v websocat &>/dev/null; then
  acsws=$(./wsacs.sh "$participant" "$aliceParty" "$ledgerEnd")
  echo "Active contracts - read using websocket"
  showContractsWs "$acsws"
else
  echo "websocat not found reading active contracts using http"
  acs=$(./acs.sh  "$participant" "$aliceParty" "$ledgerEnd")
  echo "Active contracts - read using websocket"
  showContractsWs "$acs"
fi

acs_after_create=$(./acs.sh "$participant" "$aliceParty" "$ledgerEnd")

contractId=$(echo $acs_after_create | jq -r 'reverse | .[0].contractEntry.JsActiveContract.createdEvent.contractId')
echo "Contract created: $contractId"

echo "Alice executes Transfer on Iou"
exercise_contract_result=$(exercise_transfer $aliceParty $contractId $bobParty)
completionOffset=$(echo $exercise_contract_result  |  jq ".completionOffset")

acs_after_exercise=$(./acs.sh $participant $bobParty $completionOffset)
contractId=$(echo $acs_after_exercise | jq -r 'reverse | .[0].contractEntry.JsActiveContract.createdEvent.contractId')

echo "Contracts after Transfer"
showContracts "$acs_after_exercise"

echo "Bob accepts Iou_Transfer"
exercise_contract_result=$(exercise_accept "$bobParty" "$contractId")
#echo $exercise_contract_result
completionOffset=$(echo $exercise_contract_result  |  jq ".completionOffset")
acs_after_exercise_accept=$(./acs.sh "$participant" "$bobParty" "$completionOffset")

echo "Contracts after Accept"
showContracts "$acs_after_exercise_accept"

echo "Scenario finished"
