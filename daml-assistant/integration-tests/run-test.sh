#!/usr/bin/env bash
# Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
set -euxo pipefail

RELEASE_TARBALL=$1
TMP_DIR=$(mktemp -d)
QUICKSTART_DIR=$(mktemp -u -d)
export DAML_HOME=$(mktemp -u -d)
export PATH="$DAML_HOME/bin":$PATH

tar xf "$RELEASE_TARBALL" --strip-components=1 -C "$TMP_DIR"
"$TMP_DIR"/install.sh

daml version
daml --help
daml new --list
daml new "$QUICKSTART_DIR"
cd "$QUICKSTART_DIR"
daml package daml/Main.daml target/daml/iou
daml test daml/Main.daml
