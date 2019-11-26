#!/usr/bin/env bash
# Copyright (c) 2019 The DAML Authors. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

if [ "$#" -ne 1 ]; then
    echo >&2 "Usage: ./unreleased.sh <revision range>"
    echo >&2 "Prints all changelog entries added by the given revision range"
    echo >&2 "For info about <revision range> please see gitrevisions(7)"
    exit 64
fi

for SHA in $(git log --format=%H "$1"); do
  git show --quiet --format=%b "$SHA" \
    | awk '/^$/{next} toupper($0) ~ /CHANGELOG_END/{flag=0; next} toupper($0) ~ /CHANGELOG_BEGIN/{flag=1; next} flag'
done
