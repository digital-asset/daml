#!/bin/bash
# Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

eval "$(./dev-env/bin/dade-assist)"

tmp=$(mktemp -d)
trap 'rm -rf ${tmp}' EXIT

if [ -z "${GITHUB_TOKEN:-}" ]; then
  repo_url="git@github.com:DACH-NY/canton.git"
else
  repo_url="https://$GITHUB_TOKEN@github.com/DACH-NY/canton"
fi

git clone --depth 1 --branch main $repo_url $tmp
head=$(git -C $tmp rev-parse HEAD)
echo "cloned at revision $head"

for path in community daml-common-staging; do
  src=$tmp/$path
  dst=canton-3x/$path
  rm -rf $dst
  mkdir -p $(dirname $dst)
  cp -rf $src $dst
  git add $dst
done

sed -i 's/canton-3x\///' .bazelignore
bazel build //canton-3x/...
