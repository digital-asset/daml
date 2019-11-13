# Copyright (c) 2019 The DAML Authors. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

YARN=$PWD/$1
DAML2TS=$PWD/$2
DAR=$PWD/$3
TS_DIR=$PWD/$4

TMP_DIR=$(mktemp -d)
cleanup() {
  rm -rf $TMP_DIR
}
trap cleanup EXIT

cp -r $TS_DIR/* $TMP_DIR
cd $TMP_DIR

$DAML2TS -o generated/src $DAR
$YARN install
$YARN workspaces run build
$YARN workspaces run lint
