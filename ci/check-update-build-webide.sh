#!/usr/bin/env bash
# Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
set -euo pipefail

errcho(){ >&2 echo $@; }

GITHUB_API_TOKEN=${GITHUB_API_TOKEN:-""}
GOOGLE_APPLICATION_CREDENTIALS_CONTENT=${GOOGLE_APPLICATION_CREDENTIALS_CONTENT:-""}

getLatestSdk() {
  local TMP_FILE=$(mktemp)
  local JQ_COMMAND='. | map(select(.prerelease == false)) | map([.tag_name,.published_at]) | map(.[0] + "," + .[1] + "," + (.[1]|fromdate|tostring))[]'
  if [ -n "$GITHUB_API_TOKEN" ]; then
    curl -H "Authorization: $GITHUB_API_TOKEN" https://api.github.com/repos/digital-asset/daml/releases -s > "$TMP_FILE"
  else
    curl https://api.github.com/repos/digital-asset/daml/releases -s > "$TMP_FILE"
  fi
  local RELEASES=$(jq -r "$JQ_COMMAND" "$TMP_FILE")
  local LAST_COMMAND_RESULT=$?
  if [ $LAST_COMMAND_RESULT -eq 2 ]; then
    errcho "ERROR: could not read daml releases `cat $TMP_FILE`"
    return 1
  fi
  echo $(echo $RELEASES | awk '{if(substr($1,1,1)=="v") print substr($1,2); else print substr($1,1);}')
}

dockerAuth() {
  if [ -n "$GOOGLE_APPLICATION_CREDENTIALS_CONTENT" ]; then
    GCS_KEY=$(mktemp)
    echo "$GOOGLE_APPLICATION_CREDENTIALS_CONTENT" > $GCS_KEY
    gcloud auth activate-service-account --key-file=$GCS_KEY

    docker-credential-gcr config --token-source="gcloud"
    docker-credential-gcr configure-docker
  fi
}


getImageTimestampByVersion() {
  local VERSION=$1
  local TMP_FILE=$(mktemp)
  #complications here are parsing the datetime and conforming it to strptime readable dates
  local JQ_COMMAND=". | map(select(.tags as \$t | \"$VERSION\" | IN(\$t[]))) | map(.timestamp.datetime)[] | sub(\" \";\"T\") | sub(\"(?<before>.*):\"; (.before)) | strptime(\"%Y-%m-%dT%H:%M:%S%z\") | todate | fromdate"
  #echo "running jq command: $JQ_COMMAND"
  gcloud container images list-tags gcr.io/da-gcp-web-ide-project/daml-webide --format=json > $TMP_FILE
  if [ $? -ne 0 ]; then
    exit 1
  fi
  echo $(TZ=/usr/share/zoneinfo/UTC jq -r "$JQ_COMMAND" $TMP_FILE)
}

build() {
  bash $(dirname "${BASH_SOURCE[0]}")/build-webide.sh $@
}

#####################################################################################
echo "Loading dev-env..."
eval "$(dev-env/bin/dade-assist)"

LATEST_SDK_WITH_TIME=$(getLatestSdk)
SDK_VERSION=${LATEST_SDK_WITH_TIME%,*,*} # retain the part before the comma
SDK_TS=${LATEST_SDK_WITH_TIME##*,} # retain the part after the comma
[ -z "$SDK_VERSION" ] && errcho "Could not find latest sdk version" && exit 1

echo "got latest release SDK_VERSION=$SDK_VERSION, SDK_TIMESTAMP=$SDK_TS"

dockerAuth

WEBIDE_TS=$(getImageTimestampByVersion $SDK_VERSION)
if [ -z "$WEBIDE_TS" ]; then
  echo "webide version $SDK_VERSION does not exist."
  build $SDK_VERSION
else
  echo "webide image version $SDK_VERSION already exists with timestamp $WEBIDE_TS"
fi
