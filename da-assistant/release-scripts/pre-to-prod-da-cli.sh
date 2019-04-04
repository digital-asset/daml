#!/usr/bin/env bash
# Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -eu

readonly VERSION=$1

TMP=$(mktemp -d)

function cleanup() {
  rm -rf "${TMP}"
}
trap cleanup EXIT

RELEASED_PROD_VERSIONS=($(jfrog bt ps digitalassetsdk/DigitalAssetSDK/da-cli | jq -r -c .versions[]))

if [[ ${#RELEASED_PROD_VERSIONS[@]} -ne 0 ]];
then
  if [[ " ${RELEASED_PROD_VERSIONS[@]} " =~ " ${VERSION} " ]]; then
    echo "Error: version ${VERSION} is already released to the production channel."
    echo "Aborting..."
    exit 1
  fi
fi

echo "------------------------------------------------------------------------"
echo "Downloading version ${VERSION}..."
echo "------------------------------------------------------------------------"

jfrog bt download-ver --flat=true \
  "digitalassetsdk/DigitalAssetSDK/da-cli-pre/${VERSION}" \
  "/${TMP}/"

echo "------------------------------------------------------------------------"
echo "Downloaded package content:"
echo "------------------------------------------------------------------------"

tree "${TMP}"

pushd "${TMP}"

echo "------------------------------------------------------------------------"
echo "Renaming package content..."
echo "------------------------------------------------------------------------"

mv com/digitalasset/da-cli-pre com/digitalasset/da-cli

for f in com/digitalasset/da-cli/${VERSION}/*.run; do mv "$f" "$(echo "$f" | sed s/-pre//)"; done

echo "------------------------------------------------------------------------"
echo "Package content to be uploaded:"
echo "------------------------------------------------------------------------"

tree

echo "------------------------------------------------------------------------"
echo "Publishing version..."
echo "------------------------------------------------------------------------"

jfrog bt upload --flat=false --publish=true \
  "com*" \
  "digitalassetsdk/DigitalAssetSDK/da-cli/${VERSION}"

# Add the published version to the download list
BTUSER="$(cat ~/.jfrog/jfrog-cli.conf | jq -r .bintray.user)"
BTKEY="$(cat ~/.jfrog/jfrog-cli.conf | jq -r .bintray.key)"

set +e

# This seems to be timing sensitive. Publishing is asynchronous and we may
# need to retry multiple times in order to succeed.
for f in com/digitalasset/da-cli/${VERSION}/*.run; do
  echo "Marking ${f} to appear in download list..."
  for retry in $(seq 15); do
    curl --fail -XPUT --user "$BTUSER:$BTKEY" -s \
      "-HContent-Type: application/json" \
      -d '{ "list_in_downloads": true }' \
      "https://api.bintray.com/file_metadata/digitalassetsdk/DigitalAssetSDK/${f}"
    if [ $? -eq 0 ]; then
      echo "File marked in download list."
      break
    else
      echo "File marking failed, retrying in 1s..."
      sleep 1
    fi
  done
done

popd
