#!/usr/bin/env bash
# Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
set -euo pipefail

DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
cd $DIR/..

eval "$(dev-env/bin/dade assist)"

cd ..

# The `SYSTEM_PULLREQUEST_TARGETBRANCH` environment variable is defined by
# Azure Pipelines; in order to run this script locally, define it beforehand
# as the branch being targeted. For example:
#
#   SYSTEM_PULLREQUEST_TARGETBRANCH=main-2.x bash -x ci/check-protobuf-stability.sh
#
TARGET="${SYSTEM_PULLREQUEST_TARGETBRANCH:-main-2.x}"
echo "The target branch is '${TARGET}'."

readonly BREAKING_PROTOBUF_CHANGE_TRAILER_KEY="Breaks-protobuf"
# LF protobufs are checked against local snapshots.
function check_lf_protos() {

    readonly stable_snapshot_dir="sdk/daml-lf/transaction/src/stable/protobuf"

    declare -a checkSums=(
      "73572a9d1a8985a611a5d8c94c983bd3d03617ddf7782161fdba5f90c3b20672  ${stable_snapshot_dir}/com/daml/lf/value.proto"
      "e29470fb6077a5872cf4ffb5bc12a4b307aed81879f3b93bac1646c08dcdb8e2  ${stable_snapshot_dir}/com/daml/lf/transaction.proto"
    )

    for checkSum in "${checkSums[@]}"; do
      echo ${checkSum} | sha256sum -c
    done

    buf breaking --config "sdk/buf-lf-transaction.yaml" --against ${stable_snapshot_dir}

}

# Other protobufs are checked against the chosen git target
function check_non_lf_protos() {
  local commitish_type=$1
  local commitish=$2

  declare -a BUF_MODULES_AGAINST_STABLE=(
    "buf-ledger-api.yaml"
  )

  echo "Checking protobufs against git target '$commitish_type=$commitish'"

  if [ "$commitish_type" = "branch" ]; then
    git fetch origin
  fi

  if [ "$commitish" = "v2.8.3" ]; then # TODO remove when 2.8.4 gets published
    against_prefix=.
  else
    against_prefix=sdk
  fi


  against_config=$(mktemp -d)
  for buf_module in "${BUF_MODULES_AGAINST_STABLE[@]}"; do
    git show $commitish:$against_prefix/$buf_module > $against_config/$buf_module
    if [ "$commitish" = "origin/main-2.x" ]; then # TODO delete as soon as this is merged
      cp sdk/$buf_module $against_config/$buf_module
    fi
    buf breaking --config "sdk/${buf_module}" --against "./.git#$commitish_type=$commitish" --against-config "$against_config/$buf_module"
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
  check_non_lf_protos $1 $2
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
  # Check against the highest stable tag (according to semver) for the target branch.
  #
  # This check does not need to run on release branch commits because
  # they are built sequentially, so no conflicts are possible and the per-PR
  # check is enough.
  readonly RELEASE_BRANCH_REGEX="^release/.*"
  LATEST_STABLE_TAG=""
  if [[ "${TARGET}" =~ ${RELEASE_BRANCH_REGEX} ]]; then
    readonly BRANCH_SUFFIX="${TARGET#release/}"
    readonly MINOR_VERSION="${BRANCH_SUFFIX%.x}"
    readonly NEXT_MINOR_VERSION=$(semver bump minor "$MINOR_VERSION.0")
    readonly STABLE_TAGS=($(git tag | grep -P '^v\d+\.\d+\.\d+$' | sort -V))
    LATEST_STABLE_TAG="$(
      for TAG in "${STABLE_TAGS[@]}"; do
        if [[ $(semver compare "${TAG#v}" "$NEXT_MINOR_VERSION") == "-1" ]]; then
          echo "$TAG";
        fi;
      done | tail -1)"
  elif [[ "${TARGET}" == "main-2.x" ]]; then
    LATEST_STABLE_TAG="$(git tag | grep "v.*" | grep -v "snapshot" | sort -V | tail -1)"
  else
    echo "unsupported target branch $TARGET" >&2
    exit 1
  fi
  check_protos tag $LATEST_STABLE_TAG
  ;;
--target)
  # Check against the tip of the target branch.
  #
  # This check ensures that backwards compatibility is never broken on the target branch,
  # which is stricter than guaranteeing compatibility between release tags.
  # The target check can be skipped by including the following trailer `Breaks-Protobuf: true` into the commit message
  if is_check_skipped; then
    echo "Skipping check for protobuf compatibility"
  else
    check_protos branch origin/$TARGET
  fi
  ;;
*)
  echo "unknown argument $1" >&2
  exit 1
  ;;
esac
