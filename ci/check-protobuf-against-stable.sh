#!/usr/bin/env bash
# Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
set -euo pipefail

eval "$(dev-env/bin/dade assist)"

readonly BUF_IMAGE_TMPDIR="$(mktemp -d)"
trap 'rm -rf ${BUF_IMAGE_TMPDIR}' EXIT

readonly CURRENT_BRANCH="$(git rev-parse --abbrev-ref HEAD)"

readonly SYSTEM_PULLREQUEST_TARGETBRANCH=""

# For `main` and PRs targeting `main`, we simply check against the most recent stable tag.
#
# For release branches and PRs targeting them, we should really check against all the most recent stable tags
# reachable from either the current branch or from previous release branches (say, against both `1.17.1` and
# `1.16.2` created after the `release/1.17.x` branch).
#
# Instead, we approximate by checking only against the most recent stable tag reachable from the current branch,
# under the assumption that if a lesser release branch contains a protobuf change, then it will also be present in
# higher ones either through a shared commit or a back-port from `main`.
readonly RELEASE_BRANCH_REGEX="^release/.*"
GIT_TAG_SCOPE=""
if [[ "${CURRENT_BRANCH}" =~ ${RELEASE_BRANCH_REGEX} || "${SYSTEM_PULLREQUEST_TARGETBRANCH}" =~ ${RELEASE_BRANCH_REGEX} ]]; then
  GIT_TAG_SCOPE="--merged"
fi

readonly LATEST_STABLE_TAG="$(git tag ${GIT_TAG_SCOPE} | grep -v "snapshot" | sort -V | tail -1)"
echo "Checking protobuf against tag '${LATEST_STABLE_TAG}'"
git checkout "${LATEST_STABLE_TAG}"

readonly BUF_IMAGE="${BUF_IMAGE_TMPDIR}/buf.bin"
buf build -o "${BUF_IMAGE}"
git checkout "${CURRENT_BRANCH}"
buf breaking --against "${BUF_IMAGE}"
