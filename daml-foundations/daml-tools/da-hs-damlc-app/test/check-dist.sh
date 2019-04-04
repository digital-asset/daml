#!/usr/bin/env bash
# Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -eu
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
TEST_TMP="$(mktemp -d)"
function cleanup() {
  rm -rf "$TEST_TMP"
}
trap cleanup EXIT

export PATH=$(dirname $3):$(dirname $4):$PATH

tar xzf "$1" -C "$TEST_TMP"

"$TEST_TMP/da-hs-damlc-app/da-hs-damlc-app" \
  compile "$2" \
  -o /dev/null
