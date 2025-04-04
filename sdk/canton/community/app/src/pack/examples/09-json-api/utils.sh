#!/bin/bash
# Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -eo pipefail

allocate_party_and_create_user() {
  local userId=$1
  local participant=$2
  echo "create_user $userId $participant" >&2

  party=$(get_user_party "$userId" "$participant")
  if [ -n "$party" ] && [ "$party" != "null" ]; then
    echo $party
    return
  fi

  party=$(allocate_party "$userId" "$participant")

  if [ -n "$party" ] && [ "$party" != "null" ]; then
    curl_check "http://$participant:7575/v2/users" "application/json" \
      --data-raw '{
        "user" : {
            "id" : "'$userId'",
            "primaryParty" : "'$party'",
            "isDeactivated": false,
            "identityProviderId": ""
        },
          "rights": [
              {
                  "kind": {
                      "CanActAs": {
                          "value": {
                              "party": "'$party'"
                          }
                      },
                      "CanReadAs": {
                          "value": {
                              "party": "'$party'"
                          }
                      }
                  }
              }
          ]
      }' | jq -r .user.primaryParty
  else
    echo "Failed to allocate party for user $userId" >&2
    exit 1
  fi
}

get_user_party() {
  local user=$1
  local participant=$2
  echo "get_user_party $user $participant" >&2
  curl_check "http://$participant:7575/v2/users/$user" "application/json" | jq -r .user.primaryParty
}

allocate_party() {
  local partyIdHint=$1
  local participant=$2

  echo "allocate_party $partyIdHint $participant" >&2

  namespace=$(get_participant_namespace "$participant")

  party=$(curl_check "http://$participant:7575/v2/parties/party?parties=$partyIdHint::$namespace" "application/json" |
    jq -r '.partyDetails[0].party')

  if [ -n "$party" ] && [ "$party" != "null" ]; then
    echo "party exists $party" >&2
    echo $party
    return
  fi

  curl_check "http://$participant:7575/v2/parties" "application/json" \
    --data-raw '{
      "partyIdHint": "'$partyIdHint'",
      "displayName" : "'$partyIdHint'",
      "identityProviderId": ""
    }' | jq -r .partyDetails.party
}

get_participant_namespace() {
  local participant=$1
  echo "get_participant_namespace $participant" >&2
  curl_check "http://$participant:7575/v2/parties/participant-id"  "application/json" |
    jq -r .participantId | sed 's/^participant:://'
}


curl_check() {
  local url=$1
  local contentType=${2:-application/json}
  shift 2

  local args=("$@")
  if [ $DEBUG == true ]; then
    echo "DEBUG: $url" >&2
  fi

  if [ $DEBUG == true ] && [ ${#args[@]} -ne 0 ]; then
    echo "DEBUG: ${args[@]}" >&2
  fi

  response=$(curl -s -S -w "\n%{http_code}" "$url" \
      -H "Content-Type: $contentType" \
      "${args[@]}"
      )

  local httpCode=$(echo "$response" | tail -n1 | tr -d '\r')
  local responseBody=$(echo "$response" | sed '$d')

  if [ $DEBUG == true ] ||  [ "$httpCode" -ne "200" ]; then
   echo "DEBUG: Request HTTP status code $httpCode" >&2
   echo "DEBUG: Response body: $responseBody" >&2
  fi

  if [ "$httpCode" -ne "200" ]; then
    exit 1
  fi

  echo "$responseBody"
}
