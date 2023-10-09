#!/usr/bin/env bash
# Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

date=$(date -u "+%Y-%m-%d %H:%M")
canton_dir=canton
canton_repo="git@github.com:DACH-NY/canton.git"
root_canton=$(mktemp -d)

trap "rm -rf ${root_canton}" EXIT

git clone ${canton_repo} --depth 1 ${root_canton}

canton_sha=$(git -C $root_canton rev-parse HEAD)

for path in community daml-common-staging; do
  src=${root_canton}/${path}
  dst=${canton_dir}/${path}
  rm -rf $dst
  mkdir -p $(dirname ${dst})
  cp -rf ${src} ${dst}
  git add $dst
done

if git diff-index --quiet HEAD ${canton_dir}; then
  echo "No code changes (we skip)"
else
    message="Update Canton Community source at ${date}"
    ref_commit="Reference commit: ${canton_sha}"
    git commit -m "${message}" -m "${ref_commit}"
fi

echo ${canton_sha}
