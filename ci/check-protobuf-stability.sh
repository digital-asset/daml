#!/usr/bin/env bash
# Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
set -euo pipefail

# LF protobufs are checked against local snapshosts.
function check_lf_protos() {

    readonly stable_snapshot_dir="daml-lf/transaction/src/stable/protobuf"

    declare -a checkSums=(
      "397f3f8ca2e58cf6b4df903c0ed7346a2895585e4d8362fe5ac4506abf1b3514  ${stable_snapshot_dir}/com/daml/lf/value.proto"
      "e29470fb6077a5872cf4ffb5bc12a4b307aed81879f3b93bac1646c08dcdb8e2  ${stable_snapshot_dir}/com/daml/lf/transaction.proto"
    )

    for checkSum in "${checkSums[@]}"; do
      echo ${checkSum} | sha256sum -c
    done

    (eval "$(dev-env/bin/dade assist)"; buf breaking --config "buf-lf-transaction.yaml" --against ${stable_snapshot_dir})

}

# Other protobufs are checked against last recent stable branch
function check_non_lf_protos() {

  readonly RELEASE_BRANCH_REGEX="^release/.*"

  declare -a BUF_MODULES_AGAINST_STABLE=(
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
  #   SYSTEM_PULLREQUEST_TARGETBRANCH=main bash -x ci/check-protobuf-stability.sh
  #
  TARGET="${SYSTEM_PULLREQUEST_TARGETBRANCH:-main}"
  echo "The target branch is '${TARGET}'."

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
  GIT_TAG_SCOPE=""
  if [[ "${TARGET}" =~ ${RELEASE_BRANCH_REGEX} ]]; then
    GIT_TAG_SCOPE="--merged"
  fi

  readonly LATEST_STABLE_TAG="$(git tag ${GIT_TAG_SCOPE} | grep -v "snapshot" | sort -V | tail -1)"
  echo "Checking protobufs against tag '${LATEST_STABLE_TAG}'"
  for buf_module in "${BUF_MODULES_AGAINST_STABLE[@]}"; do
    (eval "$(dev-env/bin/dade assist)" ; buf breaking --config "${buf_module}" --against ".git#tag=${LATEST_STABLE_TAG}")
  done

}

check_lf_protos
check_non_lf_protos
