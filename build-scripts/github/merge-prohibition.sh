#!/usr/bin/env bash
# Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0


# This parameterized scripts adds, removes and lists the required status check
# contexts on the master branch via GitHub API calls.
# It can also set the 'master-is-red' context to success on a given commit ID.

set -eux

export SCRIPT=$0
export TOKEN=$1
export REPO=$2
export BRANCH=$3
export PIPELINE=$4
export COMMAND=$5
export RED_CONTEXT="continuous-integration/jenkins/master-is-red-$PIPELINE"
export SCRIPT_DIR=$(dirname "$BASH_SOURCE")
export SCRIPT_NAME=$(basename $0)

function list-contexts() {
  echo "WARNING: calling GitHub preview API"
  curl \
    -s \
    -XGET \
    -H "Authorization: token ${TOKEN}"  \
    -H "Accept: application/vnd.github.loki-preview+json" \
    ${REPO}/branches/${BRANCH}/protection/required_status_checks/contexts
}

function add-context() {
  if ${SCRIPT_DIR}/${SCRIPT_NAME} $TOKEN $REPO $BRANCH "$PIPELINE" --list | grep -Fq "\"${RED_CONTEXT}\""; then
    echo "${RED_CONTEXT} context is already added"
  else
    echo "WARNING: calling GitHub preview API"
    curl \
      -XPOST \
      -H "Authorization: token ${TOKEN}"  \
      -H "Accept: application/vnd.github.loki-preview+json" \
      ${REPO}/branches/${BRANCH}/protection/required_status_checks/contexts \
      -d "[ \"${RED_CONTEXT}\" ]"
  fi
}

function remove-context() {
  echo "WARNING: calling GitHub preview API"
  curl \
    -XDELETE \
    -H "Authorization: token ${TOKEN}"  \
    -H "Accept: application/vnd.github.loki-preview+json" \
    ${REPO}/branches/${BRANCH}/protection/required_status_checks/contexts \
    -d "[ \"${RED_CONTEXT}\" ]"
}

function set-success() {
  if [[ ${COMMIT_MSG} == *"${FIX_MSG}"* ]]; then
    echo "Setting ${RED_CONTEXT} context on ${COMMIT_ID} to 'success'..."
    echo "WARNING: calling GitHub preview API"
    curl \
      -XPOST \
      -H "Authorization: token ${TOKEN}"  \
      -H "Accept: application/vnd.github.loki-preview+json" \
      ${REPO}/statuses/${COMMIT_ID} \
      -d "{
        \"state\": \"success\",
        \"target_url\": \"${BUILD_URL}\",
        \"description\": \"${FIX_MSG}\",
        \"context\": \"${RED_CONTEXT}\"
    }"
  else
    echo "Commit message does not contain '${FIX_MSG}'. Skipping GitHub API call."
  fi
}


case $COMMAND in
    --list)
        list-contexts
        ;;
    --on)
        add-context
        ;;
    --off)
        remove-context
        ;;
    --set-success)
        export COMMIT_ID=$6
        export COMMIT_MSG=$7
        export BUILD_URL=$8
        export FIX_MSG="fix-red-master"
        set-success
        ;;

    *)
        echo "Unknown command: $COMMAND"
        exit 1
        ;;
esac
