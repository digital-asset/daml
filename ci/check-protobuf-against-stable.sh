#!/usr/bin/env bash
# Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
set -euo pipefail

readonly BUF_IMAGE_TMPDIR="$(mktemp -d)"
trap 'rm -rf ${BUF_IMAGE_TMPDIR}' EXIT

echo "The target branch is '${SYSTEM_PULLREQUEST_TARGETBRANCH}'."

# For `main` and PRs targeting `main`, we simply check against the most recent
# stable tag.
#
# For PRs targeting release branches, we should really check against
# all the most recent stable tags reachable from either the current branch or
# from previous release branches (say, against both `1.17.1` and `1.16.2`
# created after the `release/1.17.x` branch).
# Instead, we approximate by checking only against the most recent stable tag
# reachable from the current branch, under the assumption that if a lesser
# release branch contains a protobuf change, then it will also be present in
# higher ones either through a shared commit or a back-port from `main`.
#
# Finally, this check does not need to run on release branch commits because
# they are built sequentially, so no conflicts are possible and the per-PR
# check is enough.
readonly RELEASE_BRANCH_REGEX="^release/.*"
GIT_TAG_SCOPE=""
if [[ "${SYSTEM_PULLREQUEST_TARGETBRANCH}" =~ ${RELEASE_BRANCH_REGEX} ]]; then
  GIT_TAG_SCOPE="--merged"
fi

readonly LATEST_STABLE_TAG="$(git tag ${GIT_TAG_SCOPE} | grep -v "snapshot" | sort -V | tail -1)"
echo "Checking protobuf against tag '${LATEST_STABLE_TAG}'"
(eval "$(dev-env/bin/dade assist)" ; buf breaking --against ".git#tag=${LATEST_STABLE_TAG}")
