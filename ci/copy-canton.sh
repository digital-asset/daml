#!/usr/bin/env bash
# Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
set -euo pipefail

if [ "$#" -ne 1 ]; then
  echo "Usage: $0 <canton_directory>"
  exit 1
else
  canton_dir=$1
fi

for path in community daml-common-staging README.md; do
  rm -rf $DIR/canton/$path
  for f in  $(git -C "$canton_dir" ls-files $path); do
    # we're only interested in copying files, not directories, as git-ls has
    # explicitly expanded all directories
    if [[ -f $canton_dir/$f ]]; then
      # we create the parent directories of f under canton/ if they don't exist
      mkdir -p $DIR/canton/$(dirname $f)
      cp $canton_dir/$f $DIR/canton/$f
    fi
  done
  git add $DIR/canton/$path
done
