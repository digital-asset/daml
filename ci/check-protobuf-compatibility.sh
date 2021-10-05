#!/usr/bin/env bash
# Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
set -euxo pipefail

readonly RELEASE_BRANCH_REGEX="^release/.*"

declare -a BUF_MODULES=(
  "buf-kvutils.yaml"
  "buf-ledger-api.yaml"
  "buf-participant-integration-api.yaml"
)

readonly BUF_IMAGE_TMPDIR="$(mktemp -d)"
trap 'rm -rf ${BUF_IMAGE_TMPDIR}' EXIT

# The `SYSTEM_PULLREQUEST_TARGETBRANCH` environment variable is defined by
# Azure Pipelines; in order to run this script locally, define it beforehand
# as the branch being targeted. For example:
#
#   SYSTEM_PULLREQUEST_TARGETBRANCH=main bash -x ci/check-protobuf-compatibility.sh
#
TARGET="${SYSTEM_PULLREQUEST_TARGETBRANCH:-main}"
echo "The target branch is '${TARGET}'."
BUF_TAG_TO_CHECK=""

## Main ##
case "${1:---stable}" in
-h | --help)
  cat <<USAGE
Usage: ./check-protobuf-compatibility.sh [options]

Options:
  -h, --help: shows this help
  --stable:     check against the latest stable tag (default)
  --head:       check against the head commit
USAGE
  exit
  ;;
--stable)
  echo "Using stable target"
  # Check against the most recent stable tag.
  #
  # This check does not need to run on release branch commits because
  # they are built sequentially, so no conflicts are possible and the per-PR
  # check is enough.
  GIT_TAG_SCOPE=""
  if [[ "${TARGET}" =~ ${RELEASE_BRANCH_REGEX} ]]; then
    GIT_TAG_SCOPE="--merged"
  fi
  readonly LATEST_STABLE_TAG="$(git tag ${GIT_TAG_SCOPE} | grep -v "snapshot" | sort -V | tail -1)"
  BUF_TAG_TO_CHECK=".git#tag=${LATEST_STABLE_TAG}"
  ;;
--head)
  # Check against the head of the target branch.
  #
  # This check ensures that backwards compatibility is never broken on the target branch,
  # which is stricter than guaranteeing compatibility between release tags.
  echo "Using head target"
  BUF_TAG_TO_CHECK=".git#branch=${TARGET}"
  ;;
*)
  echo "unknown argument $1" >&2
  exit 1
  ;;
esac

echo "Checking protobufs against git target '${BUF_TAG_TO_CHECK}'"
for buf_module in "${BUF_MODULES[@]}"; do
  (
    eval "$(dev-env/bin/dade assist)"
    buf breaking --config "${buf_module}" --against "$BUF_TAG_TO_CHECK"
  )
done
