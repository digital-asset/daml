#!/bin/bash
# Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
set -euxo pipefail
TMP_DIR=$(mktemp -d)
tar xf /data/sdk-release-tarball.tar.gz --strip-components=1 -C "$TMP_DIR"
export PATH=~/.daml/bin:$PATH
"$TMP_DIR"/install.sh
daml version
daml --help
daml new --list
daml new ~/quickstart
cd ~/quickstart
daml package daml/Main.daml target/daml/iou
daml test daml/Main.daml
