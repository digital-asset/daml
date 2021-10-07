#!/usr/bin/env bash
# Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
set -euo pipefail

eval "$(dev-env/bin/dade assist)"

# The `SYSTEM_PULLREQUEST_TARGETBRANCH` environment variable is defined by
# Azure Pipelines; in order to run this script locally, define it beforehand
# as the branch being targeted. For example:
#
#   SYSTEM_PULLREQUEST_TARGETBRANCH=main bash -x ci/check-protobuf-stability.sh
#
TARGET="${SYSTEM_PULLREQUEST_TARGETBRANCH:-main}"
echo "The target branch is '${TARGET}'."

# LF protobufs are checked against local snapshots.
function check_lf_protos() {

    readonly stable_snapshot_dir="daml-lf/transaction/src/stable/protobuf"

    declare -a checkSums=(
      "73572a9d1a8985a611a5d8c94c983bd3d03617ddf7782161fdba5f90c3b20672  ${stable_snapshot_dir}/com/daml/lf/value.proto"
      "e29470fb6077a5872cf4ffb5bc12a4b307aed81879f3b93bac1646c08dcdb8e2  ${stable_snapshot_dir}/com/daml/lf/transaction.proto"
    )

    for checkSum in "${checkSums[@]}"; do
      echo ${checkSum} | sha256sum -c
    done

    buf breaking --config "buf-lf-transaction.yaml" --against ${stable_snapshot_dir}

}

# Other protobufs are checked against the chosen git target
function check_non_lf_protos() {

  declare -a BUF_MODULES_AGAINST_STABLE=(
    "buf-kvutils.yaml"
    "buf-ledger-api.yaml"
    "buf-participant-integration-api.yaml"
  )

  readonly BUF_IMAGE_TMPDIR="$(mktemp -d)"
  trap 'rm -rf ${BUF_IMAGE_TMPDIR}' EXIT

  echo "Checking protobufs against git target '${BUF_GIT_TARGET_TO_CHECK}'"
  for buf_module in "${BUF_MODULES_AGAINST_STABLE[@]}"; do
    # Starting with version 1.17 we split the default `buf.yaml` file into multiple config files
    # This in turns requires that we pass the `--against-config` flag for any check that is run on versions > 1.17
    if [[ $BUF_CONFIG_UPDATED == true ]]; then
      buf breaking --config "${buf_module}" --against "$BUF_GIT_TARGET_TO_CHECK" --against-config "${buf_module}"
    else
      buf breaking --config "${buf_module}" --against "$BUF_GIT_TARGET_TO_CHECK"
    fi
  done

}

is_check_skipped() {
  for sha in $(git rev-list "$TARGET"..); do
    breaks_proto_trailer_value=$(git show -s --format='%(trailers:key=Breaks-Proto,valueonly)' "$sha" | xargs)
    if [[ $breaks_proto_trailer_value == "true" ]]; then
      return 0
    fi
  done
  return 1
}

case "${1:---stable}" in
-h | --help)
  cat <<USAGE
Usage: ./check-protobuf-stability.sh [options]

Options:
  -h, --help: shows this help
  --stable:     check against the latest stable tag (default)
  --target:     check against the tip of the target branch
      When using target, the check can be skipped by adding to the commit message a trailer line with key "Breaks-Proto" and value "true"
      Example commit message:
         This commit breaks protobufs when checking against the target branch.

         Breaks-Proto: true
USAGE
  exit
  ;;
--stable)
  # Check against the most recent stable tag.
  #
  # This check does not need to run on release branch commits because
  # they are built sequentially, so no conflicts are possible and the per-PR
  # check is enough.
  readonly RELEASE_BRANCH_REGEX="^release/.*"
  GIT_TAG_SCOPE=""
  # For PRs targeting release branches, we should really check against
  # all the most recent stable tags reachable from either the current branch or
  # from previous release branches (say, against both 1.17.1 and 1.16.2
  # created after the release/1.17.x branch).
  #
  # We rather check against the most recent stable (non-snapshot) tag reachable from the current branch,
  # under the assumption that if a previous release branch contains a protobuf change,
  # then it will also be present in later ones (previous/later with regards to versioning).
  if [[ "${TARGET}" =~ ${RELEASE_BRANCH_REGEX} ]]; then
    GIT_TAG_SCOPE="--merged"
  fi
  readonly LATEST_STABLE_TAG="$(git tag ${GIT_TAG_SCOPE} | grep -v "snapshot" | sort -V | tail -1)"
  # The v1.17 stable release includes the buf config file with the default name `buf.yml`.
  # Starting with v1.18 we have multiple buf config files.
  if [[ $LATEST_STABLE_TAG =~ "v1.17."* ]]; then
    BUF_CONFIG_UPDATED=false
  else
    BUF_CONFIG_UPDATED=true
  fi
  BUF_GIT_TARGET_TO_CHECK=".git#tag=${LATEST_STABLE_TAG}"
  check_lf_protos
  check_non_lf_protos
  ;;
--target)
  # Check against the tip of the target branch.
  #
  # This check ensures that backwards compatibility is never broken on the target branch,
  # which is stricter than guaranteeing compatibility between release tags.
  #
  # The files are always split for versions > 1.17, and there is no way of opening a PR against a target <= 1.17 which includes this check
  BUF_CONFIG_UPDATED=true
  BUF_GIT_TARGET_TO_CHECK=".git#branch=origin/${TARGET}"
  # The target check can be skipped by including a git trailer key-value into one of the commits (`Breaks-Proto: true`)
  if is_check_skipped; then
    echo "Skipping check for protobuf compatibility"
  else
    check_lf_protos
    check_non_lf_protos
  fi
  ;;
*)
  echo "unknown argument $1" >&2
  exit 1
  ;;
esac
