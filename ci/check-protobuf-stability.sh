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

readonly BREAKING_PROTOBUF_CHANGE_TRAILER_KEY="Breaks-protobuf"
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
  for sha in $(git rev-list origin/"$TARGET"..); do
    breaks_proto_trailer_value=$(git show -s --format="%(trailers:key=${BREAKING_PROTOBUF_CHANGE_TRAILER_KEY},valueonly)" "$sha" | xargs)
    if [[ $breaks_proto_trailer_value == "true" ]]; then
      return 0
    fi
  done
  return 1
}

check_protos() {
  check_lf_protos
  check_non_lf_protos
}

case "${1:---stable}" in
-h | --help)
  cat <<USAGE
Usage: ./check-protobuf-stability.sh [options]

Options:
  -h, --help: shows this help
  --stable:     check against the latest stable tag (default)
  --target:     check against the tip of the target branch
      When checking against a target branch, the check can be skipped by adding the following trailer:
          $BREAKING_PROTOBUF_CHANGE_TRAILER_KEY: true
      Example commit message:
         This commit breaks protobufs when checking against the target branch.

         $BREAKING_PROTOBUF_CHANGE_TRAILER_KEY: true
USAGE
  exit
  ;;
--stable)
  if [[ $TARGET == "main" ]]; then
    # Check against the most recent stable tag.
    # This check runs only on main or PRs targeting main.
    #
    # This check does not need to run on release branch commits because
    # they are built sequentially, so no conflicts are possible and the per-PR
    # check is enough.
    readonly LATEST_STABLE_TAG="$(git tag | grep -v "snapshot" | sort -V | tail -1)"
    # The v1.17 stable release includes the buf config file with the default name `buf.yml`.
    # Starting with v1.18 we have multiple buf config files.
    if [[ $LATEST_STABLE_TAG =~ "v1.17."* ]]; then
      BUF_CONFIG_UPDATED=false
    else
      BUF_CONFIG_UPDATED=true
    fi
    BUF_GIT_TARGET_TO_CHECK=".git#tag=${LATEST_STABLE_TAG}"
    check_protos
  else
    echo "Skipping check for protobuf compatibility because the target is '${TARGET}' and not 'main'"
  fi
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
  # The target check can be skipped by including the following trailer `Breaks-Proto: true` into the commit message
  if is_check_skipped; then
    echo "Skipping check for protobuf compatibility"
  else
    check_protos
  fi
  ;;
*)
  echo "unknown argument $1" >&2
  exit 1
  ;;
esac
