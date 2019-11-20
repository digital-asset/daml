#!/usr/bin/env bash
# Copyright (c) 2019 The DAML Authors. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

git log $1 | awk '/^$/{next} /CHANGELOG_END/{flag=0; next} /CHANGELOG_BEGIN/{flag=1; next} flag'
