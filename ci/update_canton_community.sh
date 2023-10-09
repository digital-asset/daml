#!/usr/bin/env bash
# Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

tmp=$(mktemp -d)
#trap "rm -rf ${tmp}" EXIT

git clone git@github.com:DACH-NY/canton.git --depth 1  $tmp

canton_sha=$(git -C $tmp rev-parse HEAD)

for path in community daml-common-staging; do
  src=$tmp/$path
  dst=canton/$path
  rm -rf $dst
  mkdir -p $(dirname $dst)
  cp -rf $src $dst
  git add $dst
done

if git diff --exit-code -- canton; then
  git commit -m "update canton to $canton_sha"
fi

echo $canton_sha
