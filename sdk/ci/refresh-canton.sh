#!/usr/bin/env bash
# Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
set -euo pipefail

DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

cd "$DIR/.."

LOG=$(mktemp)

trap "cat $LOG" EXIT

CANTON_VERSION=$(grep -E -o '^[0-9]+\.[0-9]+' NIGHTLY_PREFIX)
REPO_URL="europe-docker.pkg.dev/da-images/public-unstable/components/canton-open-source:$CANTON_VERSION"
MANIFEST_JSON=$(oras manifest fetch "$REPO_URL")
CANTON_VERSION=$(echo "$MANIFEST_JSON" | jq -r '.annotations["com.digitalasset.version"]')
CANTON_DIGEST=$(echo "$MANIFEST_JSON" | jq -r '.manifests[0].digest')
echo "> Latest canton snapshot: $CANTON_VERSION" >&2

cat > canton/canton_version.bzl <<EOF
CANTON_OPEN_SOURCE_TAG = "${CANTON_VERSION}"
CANTON_OPEN_SOURCE_SHA = "${CANTON_DIGEST}"
EOF

REPIN=1 bazel run @maven//:pin >$LOG 2>&1

trap - EXIT
