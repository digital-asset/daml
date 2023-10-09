#!/usr/bin/env bash
# Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0


set -xeuo pipefail

date=$(date -u "+%Y-%m-%d %H:%M")
canton_dir=canton
canton_repo="git@github.com:DACH-NY/canton.git"

DEBUG=1

err() {
  (>&2 echo "ERROR: $1")
}

debug() {
  [ -z "$DEBUG" ] || (>&2 echo "DEBUG: $1")
}

run() {
  local message="$1"
  shift
  local -ra cmd=( "$@" )
  local output

  debug "$message [${cmd[*]}]"

  if ! output=$("$@" 2>&1); then
    err "$message failed: $output"
    exit 1
  fi
}

function refresh() {
  root_canton=$1
  file=$2
  src=${root_canton}/${file}
  dst=${canton_dir}/${file}
  dir_dst=$(dirname $dst)
  if [ -e ${dst} ]; then
    run "Removing ${dst} in preparation for refresh" rm -rf ${dst}
  fi
  if [ ! -d ${dir_dst} ]; then
    run "Creating the ${dir_dst} directory" mkdir -p ${dir_dst}
  fi
  run "Copying from ${src} to ${dst}" cp -a ${src} ${dst}
  run "Adding ${dst} to git" git add ${dst}
}

function commit() {

  hash_private=$1

  if git diff-index --quiet HEAD ${canton_dir}; then

    echo "No code changes - skipping PR"

  else

    message="Update Canton Community source ${date}"
    ref_commit="Reference commit: ${hash_private}"

    run "Committing ${message} ${ref_commit}" git commit -m "${message}" -m "${ref_commit}"

  fi

}

root_canton=$(mktemp -d)

trap "rm -rf ${root_canton}" EXIT

run "clone canton private repo" git clone --branch main ${canton_repo} --depth 1 ${root_canton}

hash_private=$(cd $root_canton; git rev-parse HEAD)

# Canton Community directories and files inherited from private repo
for artifact in community daml-common-staging; do
  run "Refreshing $artifact" refresh ${root_canton} $artifact
done

commit ${hash_private}

echo ${hash_private}
