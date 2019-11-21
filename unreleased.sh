#!/usr/bin/env bash
# Copyright (c) 2019 The DAML Authors. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

for SHA in $(git log --format=%H "$1"); do git show --quiet --format=%b "$SHA" | awk '/^$/{next} toupper($0) ~ /CHANGELOG_END/{flag=0; next} toupper($0) ~ /CHANGELOG_BEGIN/{flag=1; next} flag'; done
