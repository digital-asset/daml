#!/usr/bin/env bash
# Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
set -euo pipefail

eval "$(dev-env/bin/dade assist)"

readonly BUF_IMAGE_TMPDIR="$(mktemp -d)"
trap 'rm -rf ${BUF_IMAGE_TMPDIR}' EXIT

readonly CURRENT_BRANCH="$(git rev-parse --abbrev-ref HEAD)"

if [[ "${CURRENT_BRANCH}" == "main" || "${SYSTEM_PULLREQUEST_TARGETBRANCH}" == "main" ]]; then
  GIT_TAG_SCOPE="--no-merged" # Release tags are not reachable from `main` because release branches predate them
else
  GIT_TAG_SCOPE="--merged"    # The relevant most recent release tag is always reachable from its release branch
fi

readonly LATEST_STABLE_TAG="$(git tag "${GIT_TAG_SCOPE}" | grep -v "snapshot" | sort -V | tail -1)"
echo "Checking protobuf against tag '${LATEST_STABLE_TAG}' (i.e., the most recent stable tag w.r.t. )"
git checkout "${LATEST_STABLE_TAG}"

readonly BUF_IMAGE="${BUF_IMAGE_TMPDIR}/buf.bin"
buf build -o "${BUF_IMAGE}"
git checkout "${CURRENT_BRANCH}"
buf breaking --against "${BUF_IMAGE}"
