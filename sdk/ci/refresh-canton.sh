#!/usr/bin/env bash
# Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
set -euo pipefail

DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

cd "$DIR/.."

LOG=$(mktemp)

trap "cat $LOG" EXIT

if [ -n "${1:-}" ]; then
  # A specific tag was provided as argument
  CANTON_TAG="$1"
  MSG_PREFIX="Using provided canton tag"
else
  # No argument provided, extract the floating tag from NIGHTLY_PREFIX
  CANTON_TAG=$(grep -E -o '^[0-9]+\.[0-9]+' NIGHTLY_PREFIX)
  MSG_PREFIX="Latest canton snapshot"
fi

REPO_URL="europe-docker.pkg.dev/da-images/public-unstable/components/canton-open-source:$CANTON_TAG"
MANIFEST_JSON=$(oras manifest fetch "$REPO_URL")
CANTON_VERSION=$(echo "$MANIFEST_JSON" | jq -r '.annotations["com.digitalasset.version"]')
CANTON_DIGEST=$(echo "$MANIFEST_JSON" | jq -r '.manifests[0].digest')
echo "> $MSG_PREFIX: $CANTON_VERSION" >&2

echo "> Writing canton/canton_version.bzl" >&2
cat > canton/canton_version.bzl <<EOF
CANTON_OPEN_SOURCE_TAG = "${CANTON_VERSION}"
CANTON_OPEN_SOURCE_SHA = "${CANTON_DIGEST}"
EOF


# version strings look like "3.5.0-snapshot.20260203.17930.0.v8a849517", this extracts the part after the last 'v'
CANTON_COMMIT="${CANTON_VERSION##*v}"
TMPDIR=$(mktemp -d)
trap 'rm -rf "$TMPDIR"' EXIT
echo "> Checking out shared_dependencies.json at revision $CANTON_COMMIT into $TMPDIR" >&2
# We can't use sparse checkouts without full commit hashes, so we use the next best thing: --filter=blob:none and
# --no-checkout to download the commit history, followed by a checkout of just the file we need.
if [ -z "${GITHUB_TOKEN:-}" ]; then
  echo "> GITHUB_TOKEN is not set, assuming ssh." >&2
  CANTON_GITHUB=git@github.com:DACH-NY/canton.git
else
  CANTON_GITHUB=https://$GITHUB_TOKEN@github.com/DACH-NY/canton.git
fi
git clone --filter=blob:none --no-checkout "$CANTON_GITHUB" "$TMPDIR" >$LOG 2>&1
git -C "$TMPDIR" show $CANTON_COMMIT:shared_dependencies.json > canton/shared_dependencies.json
rm -rf "$TMPDIR"

echo "> Pinning the maven dependences" >&2
REPIN=1 bazel run @maven//:pin >$LOG 2>&1

echo "> Formatting changed files" >&2
./fmt.sh --diff >$LOG 2>&1

# The caller of this script (ci/cron/daily-compat.yml) expects it to ouput the canton version
# and interpolates it in the title of the code drop PR it creates.
echo "${CANTON_VERSION}"

trap - EXIT
