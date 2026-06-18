#!/usr/bin/env bash
# Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
set -euo pipefail

DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

cd "$DIR/.."

LOG=$(mktemp)

trap "cat $LOG" EXIT

BUCKET="public-unstable"
if [ "${1:-}" = "--bucket" ]; then
  BUCKET="$2"
  shift 2
fi

if [ -n "${1:-}" ]; then
  # A specific tag was provided as argument
  CANTON_TAG="$1"
  MSG_PREFIX="Using provided canton tag"
else
  # No argument provided, extract the floating tag from NIGHTLY_PREFIX
  CANTON_TAG=$(grep -E -o '^[0-9]+\.[0-9]+' NIGHTLY_PREFIX)
  MSG_PREFIX="Latest canton snapshot"
fi

REPO_URL="europe-docker.pkg.dev/da-images/${BUCKET}/components/canton-open-source:$CANTON_TAG"
MANIFEST_JSON=$(oras manifest fetch "$REPO_URL")
CANTON_VERSION=$(echo "$MANIFEST_JSON" | jq -r '.annotations["com.digitalasset.version"]')
CANTON_DIGEST=$(echo "$MANIFEST_JSON" | jq -r '.manifests[0].digest')
echo "> $MSG_PREFIX: $CANTON_VERSION" >&2

echo "> Writing canton/canton_version.bzl" >&2
cat > canton/canton_version.bzl <<EOF
CANTON_OPEN_SOURCE_TAG = "${CANTON_VERSION}"
CANTON_OPEN_SOURCE_SHA = "${CANTON_DIGEST}"

# Use an alternative canton JAR & artifacts from the local maven cache by setting this to an absolute path
# Consult canton/README.md
LOCAL_CANTON_OVERRIDE = None
EOF


if [ "$BUCKET" = "public" ]; then
  # For the stable "public" bucket, check out the release tag rather than a commit hash.
  CANTON_COMMIT="tags/v${CANTON_VERSION}"
else
  # version strings look like "3.5.0-snapshot.20260203.17930.0.v8a849517", this extracts the part after the last 'v'
  CANTON_COMMIT="${CANTON_VERSION##*v}"
fi
TMPDIR=$(mktemp -d)
# Keep dumping $LOG on failure (otherwise the error from the commands below is
# silently swallowed); also clean up the temporary checkout.
trap 'cat "$LOG"; rm -rf "$TMPDIR"' EXIT
echo "> Checking out shared_dependencies.json at revision $CANTON_COMMIT into $TMPDIR" >&2
# We can't use sparse checkouts without full commit hashes, so we use the next best thing: --filter=blob:none and
# --no-checkout to download the commit history, followed by a checkout of just the file we need.
if [ -z "${GITHUB_TOKEN:-}" ]; then
  echo "> GITHUB_TOKEN is not set, assuming ssh." >&2
  CANTON_GITHUB=git@github.com:DACH-NY/canton.git
else
  # Supply the token as the HTTP *password* with a placeholder username. The
  # bare "https://$TOKEN@github.com" form provides a username with no password,
  # so git tries to read a password interactively and fails in CI with
  # "could not read Password ...: terminal prompts disabled" (exit 128) before
  # ever authenticating. The "x-access-token:$TOKEN" form works for classic and
  # fine-grained PATs as well as GitHub App tokens.
  CANTON_GITHUB=https://x-access-token:$GITHUB_TOKEN@github.com/DACH-NY/canton.git
fi
git clone --filter=blob:none --no-checkout "$CANTON_GITHUB" "$TMPDIR" >$LOG 2>&1
git -C "$TMPDIR" show $CANTON_COMMIT:shared_dependencies.json > canton/shared_dependencies.json
rm -rf "$TMPDIR"

echo "> Pinning the maven dependencies" >&2
REPIN=1 bazel run @maven//:pin >$LOG 2>&1

echo "> Updating daml-lf version and feature JSON files" >&2
bazel run //daml-lf:update-daml-lf-versions >$LOG 2>&1
bazel run //daml-lf:update-daml-lf-features >$LOG 2>&1

echo "> Formatting changed files" >&2
./fmt.sh --diff >$LOG 2>&1

# The caller of this script (ci/cron/daily-compat.yml) expects it to ouput the canton version
# and interpolates it in the title of the code drop PR it creates.
echo "${CANTON_VERSION}"

trap - EXIT
