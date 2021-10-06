#!/usr/bin/env bash
# Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
set -euo pipefail

# The `SYSTEM_PULLREQUEST_TARGETBRANCH` environment variable is defined by
# Azure Pipelines; in order to run this script locally, define it beforehand
# as the branch being targeted. For example:
#
#   SYSTEM_PULLREQUEST_TARGETBRANCH=main bash -x ci/check-protobuf-stability.sh
#
TARGET="${SYSTEM_PULLREQUEST_TARGETBRANCH:-main}"
echo "The target branch is '${TARGET}'."

BUF_GIT_TARGET_TO_CHECK=""
# The `buf.yml` file was split into multiple files after v1.17
# This flag indicates if the against target we use for buf is before or after the split
# Will be removed once we have a stable tag released after the split point
BUF_CONFIG_UPDATED=true

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

    (eval "$(dev-env/bin/dade assist)"; buf breaking --config "buf-lf-transaction.yaml" --against ${stable_snapshot_dir})

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

  echo "Checking protobufs against target '${BUF_GIT_TARGET_TO_CHECK}'"
  eval "$(dev-env/bin/dade assist)"
  for buf_module in "${BUF_MODULES_AGAINST_STABLE[@]}"; do
    # Starting with version 1.17 we split the default `buf.yaml` file into multiple config files
    # This in turns requires that we pass the `--against-config` flag for any check that is run on versions > 1.17
    if [[ $BUF_CONFIG_UPDATED ]]
    then
      buf breaking --config "${buf_module}" --against "$BUF_GIT_TARGET_TO_CHECK" --against-config "${buf_module}"
    else
      buf breaking --config "${buf_module}" --against "$BUF_GIT_TARGET_TO_CHECK"
    fi
  done

}

case "${1:---stable}" in
-h | --help)
  cat <<USAGE
Usage: ./check-protobuf-stability.sh [options]

Options:
  -h, --help: shows this help
  --stable:     check against the latest stable tag (default)
  --target:     check against the tip of the target branch
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
  readonly RELEASE_BRANCH_REGEX="^release/.*"
  GIT_TAG_SCOPE=""
  if [[ "${TARGET}" =~ ${RELEASE_BRANCH_REGEX} ]]; then
    GIT_TAG_SCOPE="--merged"
  fi
  readonly LATEST_STABLE_TAG="$(git tag ${GIT_TAG_SCOPE} | grep -v "snapshot" | sort -V | tail -1)"
  # For the latest stable snapshot the config is still the default "buf.yaml"
  BUF_CONFIG_UPDATED=false
  BUF_GIT_TARGET_TO_CHECK=".git#tag=${LATEST_STABLE_TAG}"
  ;;
--target)
  # Check against the tip of the target branch.
  #
  # This check ensures that backwards compatibility is never broken on the target branch,
  # which is stricter than guaranteeing compatibility between release tags.
  echo "Using head target"
  # The files are always split for head targets
  BUF_CONFIG_UPDATED=true
  # This lets buf checkout the head commit of the TARGET
  BUF_GIT_TARGET_TO_CHECK=".git#branch=${TARGET}"
  ;;
*)
  echo "unknown argument $1" >&2
  exit 1
  ;;
esac
check_lf_protos
check_non_lf_protos
