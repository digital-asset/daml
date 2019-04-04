#!/usr/bin/env bash
# Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -eu

OS="unknown"
case $(uname -s) in
  Darwin)
    OS="osx"
    ;;
  Linux)
    OS="linux"
    ;;
esac

bazel build //da-assistant:VERSION
VERSION=`cat $(bazel info bazel-genfiles)/da-assistant/VERSION`
VERSIONNUM=`echo -n $VERSION | sed s/-.*//`
echo "VERSION: $VERSION"
echo "VERSIONNUM: $VERSIONNUM"

# NOTE(JM): Use the first 10 chars as the commit revision. This will keep
# it deterministic, but might clash. This is due to having observed a
# different version number for build 66, where the version was 66-f922f4812f
# on osx and 66-f922f48 on Linux, which in turn messes up the latest version
# lookup.

RELEASED_VERSIONS=($(jfrog bt ps digitalassetsdk/DigitalAssetSDK/da-cli-pre | jq -r -c .versions[]))

if [[ ${#RELEASED_VERSIONS[@]} -ne 0 ]];
then
  for i in "${RELEASED_VERSIONS[@]}"
  do
    if [[ "$i" =~ ^"${VERSIONNUM}-".* ]]; then
      echo "Error: there is already a released version ${VERSIONNUM}, released as ${i}."
      echo "Aborting..."
      exit 1
    fi
  done
fi

echo "------------------------------------------------------------------------"
echo "Building da-cli..."
echo "------------------------------------------------------------------------"

OUTDIR="$PWD/da-cli-${VERSION}"
mkdir -p "$OUTDIR"
OUT_FILE="da-cli-pre-${VERSION}-${OS}.run"
OUT="$OUTDIR/$OUT_FILE"
bazel build //da-assistant:installer.run
cp -f $(bazel info bazel-genfiles)/da-assistant/installer.run $OUT

echo "------------------------------------------------------------------------"
echo "Publishing ${VERSION}..."
echo "------------------------------------------------------------------------"

jfrog bt upload --publish=true --flat=true \
  "$OUT" \
  "digitalassetsdk/DigitalAssetSDK/da-cli-pre/${VERSION}" \
  "com/digitalasset/da-cli-pre/${VERSION}/"

# Wait for the publishing to take effect.
sleep 5

# Add the published version to the download list
BTUSER="$(cat ~/.jfrog/jfrog-cli.conf | jq -r .bintray.user)"
BTKEY="$(cat ~/.jfrog/jfrog-cli.conf | jq -r .bintray.key)"
echo "Marking $OUT_FILE to appear in download list..."

# This seems to be timing sensitive. Publishing is asynchronous and we may
# need to retry multiple times in order to succeed.
set +e
for retry in $(seq 15); do
  curl --fail -XPUT --user "$BTUSER:$BTKEY" -s \
    "-HContent-Type: application/json" \
    -d '{ "list_in_downloads": true }' \
    "https://api.bintray.com/file_metadata/digitalassetsdk/DigitalAssetSDK/com/digitalasset/da-cli-pre/${VERSION}/${OUT_FILE}"
  if [ $? -eq 0 ]; then
    echo "File marked in download list."
    break
  else
    echo "File marking failed, retrying in 1s..."
    sleep 1
  fi
done
