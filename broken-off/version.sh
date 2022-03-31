#!/usr/bin/env bash
# Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

DIR="$( cd -- "${BASH_SOURCE[0]%/*}" &> /dev/null && pwd )"

if [ -n "${DAML_SDK_RELEASE_VERSION+x}" ] && [ "0.0.0" != $DAML_SDK_RELEASE_VERSION ]; then
    echo $DAML_SDK_RELEASE_VERSION
    exit 0
fi

path="$1"

commit_date=$(git log -n1 --format=%cd --date=format:%Y%m%d "$DIR"/$path)

if [ -z "$(git status --porcelain "$DIR"/$path)" ]; then
      # no changes, use git SHA as version
      echo "0.0.0-$commit_date-clean-$(git log -n1 --format=%h --abbrev=8 "$DIR"/$path)"
  else
      # uncommitted changes, use md5 instead
      echo "0.0.0-$commit_date-dirty-$(git ls-files "$DIR"/$path | sort | xargs cat | md5sum | awk '{print $1}')"
fi
