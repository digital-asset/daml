#!/usr/bin/env bash
# Copyright (c) 2019 The DAML Authors. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -eu -o pipefail

cd "${0%/*}"
scalafmt --git true --config .scalafmt.conf "$@"
