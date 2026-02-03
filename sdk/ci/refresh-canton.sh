#!/usr/bin/env bash
# Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
set -euo pipefail

DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

cd "$DIR/.."

LOG=$(mktemp)

trap "cat $LOG" EXIT

CANTON_DIR=${1:-//unset}

if [ "//unset" = "$CANTON_DIR" ]; then
  CANTON_DIR=$(realpath "$DIR/../.canton")
  echo "> Using '$CANTON_DIR' as '\$1' was not provided." >&2
  if [ -z "${GITHUB_TOKEN:-}" ]; then
    echo "> GITHUB_TOKEN is not set, assuming ssh." >&2
    canton_github=git@github.com:DACH-NY/canton.git
  else
    canton_github=https://$GITHUB_TOKEN@github.com/DACH-NY/canton.git
  fi
  if ! [ -d "$CANTON_DIR" ]; then
    echo "> Cloning canton for the first time, this may take a while..." >&2
    git clone $canton_github "$CANTON_DIR" >$LOG 2>&1
  fi
  (
    cd "$CANTON_DIR"
    git checkout main >$LOG 2>&1
    git pull >$LOG 2>&1
  )
fi

if ! [ -d "$CANTON_DIR" ]; then
  echo "> CANTON_DIR '$CANTON_DIR' does not seem to exist." >&2
  exit 1
fi

sed -i 's|SKIP_DEV_CANTON_TESTS=.*|SKIP_DEV_CANTON_TESTS=false|' "$DIR/../build.sh"

CODE_DROP_DIR="$DIR"/../canton
EXCLUDED_DIRS=(
  "community/daml-lf/api-type-signature"
  "community/daml-lf/ide-ledger"
  "community/daml-lf/interpreter"
  "community/daml-lf/notes"
  "community/daml-lf/parser"
  "community/daml-lf/snapshot-proto"
  "community/daml-lf/spec"
  "community/daml-lf/stable-packages"
  "community/daml-lf/tests"
  "community/daml-lf/transaction"
  "community/daml-lf/verification"
  "community/daml-lf/data"
  "community/daml-lf/data-scalacheck"
  "community/daml-lf/data-tests"
  "community/daml-lf/transaction-test-lib"
  "community/daml-lf/transaction-tests"
  "community/daml-lf/upgrades-matrix"
  "community/daml-lf/engine"
  "community/daml-lf/snapshot"
  "community/daml-lf/validation"
  "base/contextualized-logging"
  "base/crypto"
  "base/executors"
  "base/grpc-test-utils"
  "base/http-test-utils"
  "base/ledger-resources"
  "base/logging-entries"
  "base/observability"
  "base/nameof"
  "base/nonempty"
  "base/nonempty-cats"
  "base/ports"
  "base/resources"
  "base/resources-grpc"
  "base/resources-pekko"
  "base/rs-grpc-bridge"
  "base/rs-grpc-pekko"
  "base/rs-grpc-testing-utils"
  "base/safe-proto"
  "base/sample-service"
  "base/scalatest-utils"
  "base/scala-utils"
  "base/struct-json"
)

is_excluded() {
  local file="$1"
  for dir in "${EXCLUDED_DIRS[@]}"; do
    if [[ "$file" == "$dir"* ]]; then
      return 0
    fi
  done
  return 1
}

for path in community base README.md VERSION; do
  rm -rf "$CODE_DROP_DIR/$path"
  for f in $(git -C "$CANTON_DIR" ls-files "$path"); do
    if is_excluded "$f"; then
      continue
    fi
    if [[ -f "$CANTON_DIR/$f" ]]; then
      mkdir -p "$CODE_DROP_DIR/$(dirname $f)"
      cp "$CANTON_DIR/$f" "$CODE_DROP_DIR/$f"
    fi
  done
done

commit_sha_8=$(git -C "$CANTON_DIR" log -n1 --format=%h --abbrev=8 HEAD)
commit_date=$(git -C "$CANTON_DIR" log -n1 --format=%cd --date=format:%Y%m%d HEAD)
number_of_commits=$(git -C "$CANTON_DIR" rev-list --count HEAD)
is_modified=$(if ! git -C "$CANTON_DIR" diff-index --quiet HEAD; then echo "-dirty"; fi)

echo $commit_date.$number_of_commits.v$commit_sha_8$is_modified > canton/ref
echo $commit_date.$number_of_commits.v$commit_sha_8$is_modified

trap - EXIT
