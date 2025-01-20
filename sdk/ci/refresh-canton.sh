#!/usr/bin/env bash
# Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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
    git checkout 'release-line-2.10' >$LOG 2>&1
    git pull >$LOG 2>&1
  )
fi

if ! [ -d "$CANTON_DIR" ]; then
  echo "> CANTON_DIR '$CANTON_DIR' does not seem to exist." >&2
  exit 1
fi

sed -i 's|SKIP_DEV_CANTON_TESTS=.*|SKIP_DEV_CANTON_TESTS=false|' "$DIR/../build.sh"

CODE_DROP_DIR="$DIR"/../canton
for path in community daml-common-staging README.md VERSION; do
  rm -rf "$CODE_DROP_DIR/$path"
  for f in  $(git -C "$CANTON_DIR" ls-files "$path"); do
    # we're only interested in copying files, not directories, as git-ls has
    # explicitly expanded all directories
    if [[ -f "$CANTON_DIR/$f" ]]; then
      # we create the parent directories of f under canton/ if they don't exist
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
